import json


def api_event(body, key="key-0000001", raw=None):
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    return {"headers": headers, "body": raw if raw is not None else json.dumps(body)}


GOOD = {"customerId": "cust-1", "items": [{"sku": "MUG-CER", "qty": 2}, {"sku": "CAP-NVY", "qty": 1}]}


def sqs_event(*bodies):
    return {"Records": [{"messageId": "m%d" % i, "body": b} for i, b in enumerate(bodies)]}


def poll(aws, max_messages=10):
    """Act like the Lambda event source mapping: receive, invoke the worker, delete only the successes."""
    resp = aws.sqs.receive_message(QueueUrl=aws.queue, MaxNumberOfMessages=max_messages)
    msgs = resp.get("Messages", [])
    if not msgs:
        return 0
    event = {"Records": [{"messageId": m["MessageId"], "body": m["Body"]} for m in msgs]}
    out = aws.worker.handler(event, None)
    failed = {f["itemIdentifier"] for f in out["batchItemFailures"]}
    for m in msgs:
        if m["MessageId"] not in failed:
            aws.sqs.delete_message(QueueUrl=aws.queue, ReceiptHandle=m["ReceiptHandle"])
    return len(msgs)


def depth(aws, url):
    a = aws.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
    return int(a["ApproximateNumberOfMessages"])


def total(aws, url):
    a = aws.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    return int(a["ApproximateNumberOfMessages"]) + int(a["ApproximateNumberOfMessagesNotVisible"])
