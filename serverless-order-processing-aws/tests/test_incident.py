"""The whole story without AWS: load, worker failure, DLQ, recovery, poison message."""
import json
import time

import ops
from helpers import GOOD, api_event, depth, poll, total


def submit(aws, n):
    ids = []
    for i in range(n):
        body = dict(GOOD, customerId="cust-%d" % i)
        ids.append(json.loads(aws.intake.handler(api_event(body, "load-key-%04d" % i), None)["body"])["orderId"])
    return ids


def drain(aws, rounds=30):
    for _ in range(rounds):
        if poll(aws) == 0:
            return


def states(aws, ids):
    return {aws.table.get_item(Key={"orderId": i})["Item"]["status"] for i in ids}


def test_normal_load_confirms_everything(aws):
    ids = submit(aws, 40)
    drain(aws)
    assert states(aws, ids) == {"CONFIRMED"} and depth(aws, aws.queue) == 0 and depth(aws, aws.dlq) == 0


def test_worker_outage_fills_dlq_then_redrive_recovers_every_order(aws):
    ids = submit(aws, 25)
    ops.set_fault(aws.env, "fail", minutes=5)
    drain(aws)  # every delivery fails; after 3 receives SQS moves the message to the DLQ
    assert depth(aws, aws.dlq) == 25 and states(aws, ids) == {"RECEIVED"}

    ops.set_fault(aws.env, "off")
    dry = ops.drain_dlq(aws.env, apply=False)
    assert dry["redriven"] == 25 and total(aws, aws.dlq) == 25  # dry run moved nothing
    time.sleep(1.2)  # dry run hides messages for 1 s
    rep = ops.drain_dlq(aws.env, apply=True)
    assert rep["redriven"] == 25 and rep["invalid"] == 0
    drain(aws)
    assert states(aws, ids) == {"CONFIRMED"} and depth(aws, aws.dlq) == 0


def test_redrive_twice_is_safe(aws):
    ids = submit(aws, 3)
    ops.set_fault(aws.env, "fail", minutes=5)
    drain(aws)
    ops.set_fault(aws.env, "off")
    ops.drain_dlq(aws.env, apply=True)
    # operator panics and queues the same orders again by hand
    for oid in ids:
        aws.sqs.send_message(QueueUrl=aws.queue, MessageBody=json.dumps({"v": 1, "orderId": oid}))
    drain(aws)
    assert states(aws, ids) == {"CONFIRMED"}
    assert aws.table.scan(FilterExpression="attribute_exists(confirmedAt)")["Count"] == 3


def test_poison_message_is_not_blindly_redriven(aws, tmp_path):
    good = submit(aws, 2)
    aws.sqs.send_message(QueueUrl=aws.queue, MessageBody="this is not an order message")
    drain(aws)
    assert states(aws, good) == {"CONFIRMED"}  # one bad message did not block the good ones
    assert depth(aws, aws.dlq) == 1

    q = str(tmp_path / "quarantine.jsonl")
    dry = ops.drain_dlq(aws.env, apply=False, quarantine_path=q)
    assert dry["invalid"] == 1 and dry["redriven"] == 0
    time.sleep(1.2)
    rep = ops.drain_dlq(aws.env, apply=True, quarantine_path=q)
    assert rep["invalid"] == 1 and depth(aws, aws.queue) == 0  # not sent back to the main queue
    assert total(aws, aws.dlq) == 1  # still in the DLQ for a human
    saved = [json.loads(l) for l in open(q)]
    assert saved and saved[0]["body"] == "this is not an order message"

    time.sleep(1.2)  # moto only re-shows a released message after the clock ticks over
    rep = ops.drain_dlq(aws.env, apply=True, discard_invalid=True, quarantine_path=q)
    assert rep["discarded"] == 1 and depth(aws, aws.dlq) == 0


def test_verify_reports_accepted_vs_confirmed(aws, tmp_path):
    ids = submit(aws, 5)
    run = tmp_path / "run.jsonl"
    run.write_text("".join(json.dumps({"status": 202, "orderId": i}) + "\n" for i in ids)
                   + json.dumps({"status": 429}) + "\n")
    assert ops.verify(aws.env, str(run))["by_status"] == {"RECEIVED": 5}
    drain(aws)
    rep = ops.verify(aws.env, str(run))
    assert rep["by_status"] == {"CONFIRMED": 5} and rep["missing"] == 0 and rep["intake_to_confirm_ms"]["max"] is not None


def test_stuck_finds_old_received_orders(aws):
    ids = submit(aws, 2)
    aws.table.update_item(Key={"orderId": ids[0]}, UpdateExpression="SET createdAt = :c",
                          ExpressionAttributeValues={":c": int((time.time() - 3600) * 1000)})
    assert ops.stuck(aws.env, 5) == [ids[0]]


def test_fault_switch_guards(aws):
    import pytest

    with pytest.raises(ValueError):
        ops.set_fault(aws.env, "slow", minutes=5, delay_ms=0)
    with pytest.raises(ValueError):
        ops.set_fault(aws.env, "fail", minutes=999)
    with pytest.raises(ValueError):
        ops.set_fault(aws.env, "slow", minutes=5, delay_ms=5000)
    f = ops.set_fault(aws.env, "slow", minutes=5, delay_ms=500)
    assert f["mode"] == "slow" and 0 < f["secondsLeft"] <= 300
    ops.set_fault(aws.env, "off")
    assert ops.get_fault(aws.env) is None


def test_intake_crash_between_store_and_send_is_found_and_repaired(aws):
    """Simulate the function dying after the DynamoDB put and before the SQS send."""
    import intake  # noqa: F401
    from helpers import total

    order = aws.common.validate_order(GOOD)
    oid = aws.common.order_id_for("crashed-key-0001")
    old = int((time.time() - 3600) * 1000)
    aws.table.put_item(Item={"orderId": oid, "status": "RECEIVED", "customerId": order["customerId"], "items": order["items"],
                             "requestHash": aws.common.request_hash(order), "createdAt": old})
    assert total(aws, aws.queue) == 0  # silently unqueued: no alarm would fire for this order
    assert ops.stuck(aws.env, 5) == [oid]

    assert ops.requeue_stuck(aws.env, 5, apply=False) == [oid] and total(aws, aws.queue) == 0  # dry run
    ops.requeue_stuck(aws.env, 5, apply=True)
    drain(aws)
    assert states(aws, [oid]) == {"CONFIRMED"} and ops.stuck(aws.env, 5) == []
    assert ops.requeue_stuck(aws.env, 5, apply=True) == []  # nothing left to requeue


def test_client_retry_after_crash_repairs_without_a_second_order(aws):
    crashed = aws.common.validate_order(GOOD)
    oid = aws.common.order_id_for("crashed-key-0002")
    aws.table.put_item(Item={"orderId": oid, "status": "RECEIVED", "customerId": crashed["customerId"], "items": crashed["items"],
                             "requestHash": aws.common.request_hash(crashed), "createdAt": aws.common.now_ms()})
    r = aws.intake.handler(api_event(GOOD, "crashed-key-0002"), None)
    assert r["statusCode"] == 202 and json.loads(r["body"])["orderId"] == oid  # 202 only because it is queued now
    drain(aws)
    assert states(aws, [oid]) == {"CONFIRMED"} and aws.table.scan(FilterExpression="begins_with(orderId, :p)", ExpressionAttributeValues={":p": "ord_"})["Count"] == 1
