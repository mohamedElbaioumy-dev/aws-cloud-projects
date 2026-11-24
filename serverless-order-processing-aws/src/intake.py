"""API Gateway proxy handler: validate, store the order durably, then enqueue it.

Order of operations matters: the DynamoDB write happens first, so a queued message always
points at a stored order. If the enqueue fails the client gets 503 and retries with the same
Idempotency-Key; the retry finds the stored RECEIVED order and enqueues it again.
"""
import json
import os

import boto3
from botocore.exceptions import ClientError

import common

sqs = boto3.client("sqs")


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _enqueue(order_id):
    sqs.send_message(
        QueueUrl=os.environ["QUEUE_URL"],
        MessageBody=json.dumps({"v": 1, "orderId": order_id}),
    )


def handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    key = headers.get("idempotency-key", "")
    if not (8 <= len(key) <= 128) or not key.isascii() or not key.isprintable():
        return _resp(400, {"error": "Idempotency-Key header (8-128 printable ASCII characters) is required"})

    raw = event.get("body") or ""
    if len(raw.encode()) > common.MAX_BODY_BYTES:
        return _resp(413, {"error": "body too large"})
    try:
        order = common.validate_order(json.loads(raw))
    except (json.JSONDecodeError, common.ValidationError) as exc:
        common.emit_metrics("FlashOrders", {"OrdersRejectedAtIntake": (1, "Count")})
        return _resp(400, {"error": str(exc)})

    order_id = common.order_id_for(key)
    rhash = common.request_hash(order)
    now = common.now_ms()
    item = {
        "orderId": order_id,
        "status": "RECEIVED",
        "customerId": order["customerId"],
        "items": order["items"],
        "requestHash": rhash,
        "createdAt": now,
        "ttl": now // 1000 + common.ORDER_TTL_SECONDS,
    }
    tbl = common.table()
    duplicate = False
    try:
        tbl.put_item(Item=item, ConditionExpression="attribute_not_exists(orderId)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        duplicate = True
        existing = tbl.get_item(Key={"orderId": order_id}, ConsistentRead=True).get("Item")
        if existing is None:
            return _resp(503, {"error": "try again"})
        if existing["requestHash"] != rhash:
            return _resp(409, {"error": "Idempotency-Key was already used with a different order"})
        if existing["status"] != "RECEIVED":
            common.emit_metrics("FlashOrders", {"OrdersDuplicate": (1, "Count")})
            return _resp(200, {"orderId": order_id, "status": existing["status"]})

    try:
        _enqueue(order_id)  # for a duplicate of a RECEIVED order this is a safe re-send
    except ClientError:
        return _resp(503, {"error": "could not queue the order, retry with the same Idempotency-Key"})

    common.emit_metrics(
        "FlashOrders", {("OrdersDuplicate" if duplicate else "OrdersAccepted"): (1, "Count")}
    )
    return _resp(202, {"orderId": order_id, "status": "RECEIVED"})
