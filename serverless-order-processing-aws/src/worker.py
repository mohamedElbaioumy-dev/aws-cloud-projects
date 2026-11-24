"""SQS worker: confirm each order once, report failures per message.

Idempotency scope: the only side effect is one conditional DynamoDB update
(RECEIVED -> CONFIRMED/REJECTED). A duplicate delivery finds a final status and does nothing.
Nothing here calls an external system, so there is nothing else to protect.
"""
import json
import time

from botocore.exceptions import ClientError

import common


def _fault(tbl):
    """Read the optional fault switch. Expired or malformed switches are ignored (fail safe)."""
    item = tbl.get_item(Key={"orderId": common.FAULT_KEY}).get("Item")
    if not item:
        return None
    if int(item.get("expiresAt", 0)) <= int(time.time()):
        return None
    mode = item.get("mode")
    if mode == "fail":
        return ("fail", 0)
    if mode == "slow":
        return ("slow", max(0, min(int(item.get("delayMs", 0)), common.MAX_SLOW_MS)))
    return None


def _process(tbl, body, fault):
    try:
        msg = json.loads(body)
        order_id = msg["orderId"]
        assert msg.get("v") == 1 and isinstance(order_id, str)
    except Exception as exc:  # malformed message: retries cannot fix it, let it reach the DLQ
        raise common.ValidationError("malformed queue message") from exc

    if fault and fault[0] == "fail":
        raise RuntimeError("injected worker failure")
    if fault and fault[0] == "slow":
        time.sleep(fault[1] / 1000.0)

    order = tbl.get_item(Key={"orderId": order_id}, ConsistentRead=True).get("Item")
    if order is None:
        raise LookupError("order not found: " + order_id)
    if order["status"] != "RECEIVED":
        return "duplicate", None

    items = [{"sku": i["sku"], "qty": int(i["qty"])} for i in order["items"]]
    now = common.now_ms()
    if any(i["sku"] not in common.CATALOG for i in items):
        status, extra = "REJECTED", {":r": "sku no longer in catalog", ":t": 0}
    else:
        status, extra = "CONFIRMED", {":r": "", ":t": common.total_cents(items)}
    try:
        tbl.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET #s = :new, confirmedAt = :now, totalCents = :t, reason = :r",
            ConditionExpression="#s = :recv",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":new": status, ":now": now, ":recv": "RECEIVED", **extra},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "duplicate", None  # another delivery won the race
        raise
    return status, now - int(order["createdAt"])


def handler(event, context):
    tbl = common.table()
    fault = _fault(tbl)
    failures, confirmed, rejected, dup, latencies = [], 0, 0, 0, []
    for rec in event["Records"]:
        try:
            status, latency = _process(tbl, rec["body"], fault)
        except Exception as exc:
            print(json.dumps({"level": "ERROR", "messageId": rec["messageId"], "error": type(exc).__name__ + ": " + str(exc)}))
            failures.append({"itemIdentifier": rec["messageId"]})
            continue
        if status == "CONFIRMED":
            confirmed += 1
            latencies.append(latency)
        elif status == "REJECTED":
            rejected += 1
        else:
            dup += 1
    metrics = {
        "OrdersConfirmed": (confirmed, "Count"),
        "OrdersRejected": (rejected, "Count"),
        "DuplicateDeliveries": (dup, "Count"),
        "MessageFailures": (len(failures), "Count"),
    }
    if latencies:
        metrics["ConfirmLatencyMs"] = (max(latencies), "Milliseconds")
    common.emit_metrics("FlashOrders", metrics)
    return {"batchItemFailures": failures}
