#!/usr/bin/env python3
"""Operator commands for the FlashOrders experiments (boto3 only).

  python tools/ops.py --stack flashorders status
  python tools/ops.py --stack flashorders fault slow --delay-ms 800 --minutes 10
  python tools/ops.py --stack flashorders fault fail --minutes 10
  python tools/ops.py --stack flashorders fault off
  python tools/ops.py --stack flashorders dlq
  python tools/ops.py --stack flashorders redrive            # dry run
  python tools/ops.py --stack flashorders redrive --apply
  python tools/ops.py --stack flashorders inject-poison
  python tools/ops.py --stack flashorders verify run1.jsonl
  python tools/ops.py --stack flashorders stuck --minutes 5
  python tools/ops.py --stack flashorders requeue --minutes 5 --apply
"""
import argparse
import json
import sys
import time

import boto3

FAULT_KEY = "CONTROL#fault"
MAX_FAULT_MINUTES = 30
MAX_SLOW_MS = 1000


class Env:
    def __init__(self, table, queue_url, dlq_url, sqs=None, ddb=None):
        self.sqs = sqs or boto3.client("sqs")
        self.table = (ddb or boto3.resource("dynamodb")).Table(table)
        self.queue_url, self.dlq_url = queue_url, dlq_url


def env_from_stack(stack):
    outs = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
    o = {x["OutputKey"]: x["OutputValue"] for x in outs}
    return Env(o["TableName"], o["QueueUrl"], o["DlqUrl"])


def queue_counts(env, url):
    a = env.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    return {
        "visible": int(a["ApproximateNumberOfMessages"]),
        "in_flight": int(a["ApproximateNumberOfMessagesNotVisible"]),
        "delayed": int(a["ApproximateNumberOfMessagesDelayed"]),
    }


def get_fault(env):
    item = env.table.get_item(Key={"orderId": FAULT_KEY}).get("Item")
    if not item:
        return None
    left = int(item["expiresAt"]) - int(time.time())
    return {"mode": item["mode"], "delayMs": int(item.get("delayMs", 0)), "secondsLeft": max(0, left)}


def set_fault(env, mode, minutes=10, delay_ms=0):
    if mode == "off":
        env.table.delete_item(Key={"orderId": FAULT_KEY})
        return None
    if mode not in ("fail", "slow"):
        raise ValueError("mode must be fail, slow or off")
    if not (1 <= minutes <= MAX_FAULT_MINUTES):
        raise ValueError("--minutes must be 1-%d" % MAX_FAULT_MINUTES)
    if mode == "slow" and not (1 <= delay_ms <= MAX_SLOW_MS):
        raise ValueError("--delay-ms must be 1-%d for slow mode" % MAX_SLOW_MS)
    exp = int(time.time()) + minutes * 60
    env.table.put_item(Item={"orderId": FAULT_KEY, "mode": mode, "delayMs": delay_ms, "expiresAt": exp, "ttl": exp + 3600})
    return get_fault(env)


def _valid_message(env, body):
    """A DLQ message is worth redriving only if it parses AND points at a stored, still-open order."""
    try:
        msg = json.loads(body)
        order_id = msg["orderId"]
        if msg.get("v") != 1 or not isinstance(order_id, str):
            return False, "unsupported message shape"
    except Exception:
        return False, "not valid JSON or missing orderId"
    item = env.table.get_item(Key={"orderId": order_id}, ConsistentRead=True).get("Item")
    if item is None:
        return False, "order not found"
    if item["status"] != "RECEIVED":
        return False, "order already %s (redrive would be a harmless duplicate)" % item["status"]
    return True, order_id


def drain_dlq(env, apply=False, discard_invalid=False, quarantine_path="quarantine.jsonl", max_rounds=50):
    """Redrive valid messages to the main queue. Never rewrites payloads and never loses a bad one silently.

    Invalid messages are copied to quarantine_path first. They stay in the DLQ unless
    discard_invalid is set, because a malformed payload does not become valid by being replayed.
    """
    report = {"redriven": 0, "invalid": 0, "already_done": 0, "discarded": 0, "details": []}
    seen = set()
    for _ in range(max_rounds):
        resp = env.sqs.receive_message(QueueUrl=env.dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                       VisibilityTimeout=120 if apply else 1)
        got = resp.get("Messages", [])
        msgs = [m for m in got if m["MessageId"] not in seen]
        for m in got:
            if m["MessageId"] in seen:  # already examined and left in the DLQ: put it back, do not hide it again
                env.sqs.change_message_visibility(QueueUrl=env.dlq_url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
        if not msgs:
            break
        for m in msgs:
            seen.add(m["MessageId"])
            ok, info = _valid_message(env, m["Body"])
            if ok:
                report["redriven"] += 1
                report["details"].append(("redrive", info))
                if apply:
                    env.sqs.send_message(QueueUrl=env.queue_url, MessageBody=m["Body"])
                    env.sqs.delete_message(QueueUrl=env.dlq_url, ReceiptHandle=m["ReceiptHandle"])
            elif info.startswith("order already"):
                report["already_done"] += 1
                report["details"].append(("already-done", info))
                if apply:
                    env.sqs.delete_message(QueueUrl=env.dlq_url, ReceiptHandle=m["ReceiptHandle"])
            else:
                report["invalid"] += 1
                report["details"].append(("invalid", info + ": " + m["Body"][:80]))
                if apply:
                    with open(quarantine_path, "a") as fh:
                        fh.write(json.dumps({"messageId": m["MessageId"], "body": m["Body"], "reason": info}) + "\n")
                    if discard_invalid:
                        env.sqs.delete_message(QueueUrl=env.dlq_url, ReceiptHandle=m["ReceiptHandle"])
                        report["discarded"] += 1
                    else:
                        # leave it in the DLQ and make it visible again right away
                        env.sqs.change_message_visibility(QueueUrl=env.dlq_url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    return report


def verify(env, path):
    """Compare what the API accepted with what the table says. Reads an ndjson file written by loadgen."""
    ids = set()
    with open(path) as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("status") in (200, 202) and r.get("orderId"):
                ids.add(r["orderId"])
    ids = sorted(ids)
    ddb = boto3.resource("dynamodb")
    table = env.table.name
    by_status, latencies, missing = {}, [], 0
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        pending = {table: {"Keys": [{"orderId": x} for x in chunk], "ProjectionExpression": "orderId, #s, createdAt, confirmedAt",
                           "ExpressionAttributeNames": {"#s": "status"}}}
        found = 0
        while pending:
            resp = ddb.batch_get_item(RequestItems=pending)
            for it in resp["Responses"].get(table, []):
                found += 1
                by_status[it["status"]] = by_status.get(it["status"], 0) + 1
                if it["status"] == "CONFIRMED":
                    latencies.append(int(it["confirmedAt"]) - int(it["createdAt"]))
            pending = resp.get("UnprocessedKeys") or {}
        missing += len(chunk) - found
    latencies.sort()
    pick = lambda p: latencies[min(len(latencies) - 1, int(round(p / 100.0 * (len(latencies) - 1))))] if latencies else None
    return {"accepted_orders": len(ids), "by_status": by_status, "missing": missing,
            "intake_to_confirm_ms": {"p50": pick(50), "p95": pick(95), "max": latencies[-1] if latencies else None}}


def stuck(env, minutes):
    """Orders still RECEIVED after N minutes. Scan is fine at demo scale; use a GSI for production volume."""
    cutoff = int((time.time() - minutes * 60) * 1000)
    out, kw = [], dict(FilterExpression="#s = :r AND createdAt < :c", ExpressionAttributeNames={"#s": "status"},
                       ExpressionAttributeValues={":r": "RECEIVED", ":c": cutoff}, ProjectionExpression="orderId, createdAt")
    while True:
        resp = env.table.scan(**kw)
        out += [i["orderId"] for i in resp["Items"]]
        if "LastEvaluatedKey" not in resp:
            return out
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def requeue_stuck(env, minutes, apply=False):
    """Re-send queue messages for orders that are still RECEIVED after N minutes.

    This repairs the one gap in intake: the order was stored but the queue send failed or the
    function died before sending, and the client never retried. A duplicate message is harmless
    because the worker only confirms an order that is still RECEIVED. During a large backlog,
    old RECEIVED orders may simply be waiting in the queue; requeueing them adds duplicate load.
    """
    ids = stuck(env, minutes)
    if apply:
        for oid in ids:
            env.sqs.send_message(QueueUrl=env.queue_url, MessageBody=json.dumps({"v": 1, "orderId": oid}))
    return ids


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stack", default="flashorders")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    f = sub.add_parser("fault")
    f.add_argument("mode", choices=["fail", "slow", "off"])
    f.add_argument("--minutes", type=int, default=10)
    f.add_argument("--delay-ms", type=int, default=0)
    sub.add_parser("dlq")
    r = sub.add_parser("redrive")
    r.add_argument("--apply", action="store_true")
    r.add_argument("--discard-invalid", action="store_true")
    r.add_argument("--quarantine", default="quarantine.jsonl")
    sub.add_parser("inject-poison")
    v = sub.add_parser("verify")
    v.add_argument("file")
    s = sub.add_parser("stuck")
    s.add_argument("--minutes", type=int, default=5)
    q = sub.add_parser("requeue")
    q.add_argument("--minutes", type=int, default=5)
    q.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    env = env_from_stack(a.stack)

    if a.cmd == "status":
        print("queue:", queue_counts(env, env.queue_url))
        print("dlq:  ", queue_counts(env, env.dlq_url))
        print("fault:", get_fault(env) or "off")
    elif a.cmd == "fault":
        print("fault:", set_fault(env, a.mode, a.minutes, a.delay_ms) or "off")
    elif a.cmd == "dlq":
        rep = drain_dlq(env, apply=False)
        print({k: v for k, v in rep.items() if k != "details"})
        for kind, info in rep["details"]:
            print(" ", kind, info)
    elif a.cmd == "redrive":
        rep = drain_dlq(env, a.apply, a.discard_invalid, a.quarantine)
        print(("APPLIED " if a.apply else "DRY RUN ") + str({k: v for k, v in rep.items() if k != "details"}))
        for kind, info in rep["details"]:
            print(" ", kind, info)
        if not a.apply:
            print("Nothing changed. Re-run with --apply to move valid messages back to the main queue.")
    elif a.cmd == "inject-poison":
        env.sqs.send_message(QueueUrl=env.queue_url, MessageBody="this is not an order message")
        print("sent one malformed message. It will reach the DLQ after the configured receive attempts.")
    elif a.cmd == "verify":
        print(json.dumps(verify(env, a.file), indent=2))
    elif a.cmd == "requeue":
        ids = requeue_stuck(env, a.minutes, a.apply)
        print(("QUEUED " if a.apply else "DRY RUN: would queue ") + "%d orders that were RECEIVED for more than %d minutes" % (len(ids), a.minutes))
        if not a.apply:
            print("Check the queue is not simply backlogged (status) before using --apply.")
    elif a.cmd == "stuck":
        ids = stuck(env, a.minutes)
        print("%d orders still RECEIVED after %d minutes" % (len(ids), a.minutes))
        for i in ids[:20]:
            print(" ", i)


if __name__ == "__main__":
    main()
