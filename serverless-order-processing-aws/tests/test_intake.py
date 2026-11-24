import json

from botocore.exceptions import ClientError

from helpers import GOOD, api_event, depth


def call(aws, body=GOOD, key="key-0000001", raw=None):
    r = aws.intake.handler(api_event(body, key, raw), None)
    return r["statusCode"], json.loads(r["body"])


def test_accepts_order_stores_it_then_queues_it(aws):
    code, body = call(aws)
    assert code == 202 and body["status"] == "RECEIVED"
    item = aws.table.get_item(Key={"orderId": body["orderId"]})["Item"]
    assert item["status"] == "RECEIVED" and item["customerId"] == "cust-1"
    msg = aws.sqs.receive_message(QueueUrl=aws.queue)["Messages"][0]
    assert json.loads(msg["Body"]) == {"v": 1, "orderId": body["orderId"]}


def test_requires_idempotency_key(aws):
    assert call(aws, key=None)[0] == 400
    assert call(aws, key="short")[0] == 400
    assert depth(aws, aws.queue) == 0


def test_rejects_bad_input_without_storing_anything(aws):
    cases = [
        {"customerId": "c", "items": [{"sku": "NOPE", "qty": 1}]},
        {"customerId": "c", "items": [{"sku": "MUG-CER", "qty": 0}]},
        {"customerId": "c", "items": [{"sku": "MUG-CER", "qty": True}]},
        {"customerId": "c", "items": [{"sku": "MUG-CER", "qty": 1}, {"sku": "MUG-CER", "qty": 1}]},
        {"customerId": "", "items": [{"sku": "MUG-CER", "qty": 1}]},
        {"customerId": "c", "items": []},
        [],
    ]
    for i, c in enumerate(cases):
        assert call(aws, c, key="bad-key-%04d" % i)[0] == 400
    assert call(aws, raw="{not json", key="bad-key-json1")[0] == 400
    assert aws.table.scan()["Count"] == 0 and depth(aws, aws.queue) == 0


def test_oversized_body_is_413(aws):
    assert call(aws, raw="x" * 20000)[0] == 413


def test_same_key_same_order_does_not_create_a_second_order(aws):
    a = call(aws)[1]["orderId"]
    b = call(aws)[1]["orderId"]
    assert a == b and aws.table.scan()["Count"] == 1


def test_same_key_different_order_is_409(aws):
    call(aws)
    other = {"customerId": "cust-2", "items": [{"sku": "TOTE-CNV", "qty": 1}]}
    assert call(aws, other)[0] == 409


def test_duplicate_of_finished_order_returns_status_and_does_not_enqueue(aws):
    oid = call(aws)[1]["orderId"]
    aws.table.update_item(Key={"orderId": oid}, UpdateExpression="SET #s = :c", ExpressionAttributeNames={"#s": "status"},
                          ExpressionAttributeValues={":c": "CONFIRMED"})
    before = depth(aws, aws.queue)
    code, body = call(aws)
    assert (code, body["status"]) == (200, "CONFIRMED") and depth(aws, aws.queue) == before


def test_queue_failure_returns_503_and_retry_recovers_with_one_order(aws, monkeypatch):
    real = aws.intake.sqs.send_message

    def boom(**kw):
        raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "x"}}, "SendMessage")

    monkeypatch.setattr(aws.intake.sqs, "send_message", boom)
    assert call(aws)[0] == 503
    assert aws.table.scan()["Count"] == 1 and depth(aws, aws.queue) == 0  # stored, not yet queued
    monkeypatch.setattr(aws.intake.sqs, "send_message", real)
    code, body = call(aws)  # client retries with the same key
    assert code == 202 and aws.table.scan()["Count"] == 1 and depth(aws, aws.queue) == 1
