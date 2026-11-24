"""Shared helpers for the FlashOrders intake and worker functions (standard library + boto3 only)."""
import hashlib
import json
import os
import time

MAX_ITEMS = 25
MAX_QTY = 20
MAX_BODY_BYTES = 16 * 1024
ORDER_TTL_SECONDS = 30 * 24 * 3600
FAULT_KEY = "CONTROL#fault"
MAX_SLOW_MS = 1000  # per message; a batch of 10 must stay well below the 15 s function timeout

# Fixed catalog. Real inventory is out of scope: the catalog only prices and validates SKUs.
CATALOG = {
    "TSHIRT-BLK": 1999,
    "TSHIRT-WHT": 1999,
    "HOODIE-GRY": 4999,
    "CAP-NVY": 1499,
    "MUG-CER": 1299,
    "BOTTLE-STL": 2499,
    "TOTE-CNV": 999,
    "STICKER-PK": 499,
}


class ValidationError(ValueError):
    """The request or message can never become valid by retrying unchanged."""


def validate_order(payload):
    """Return a normalized order dict or raise ValidationError."""
    if not isinstance(payload, dict):
        raise ValidationError("body must be a JSON object")
    customer = payload.get("customerId")
    if not isinstance(customer, str) or not (1 <= len(customer) <= 64):
        raise ValidationError("customerId must be a string of 1-64 characters")
    items = payload.get("items")
    if not isinstance(items, list) or not (1 <= len(items) <= MAX_ITEMS):
        raise ValidationError("items must be a list of 1-%d entries" % MAX_ITEMS)
    clean = []
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            raise ValidationError("each item must be an object")
        sku, qty = it.get("sku"), it.get("qty")
        if sku not in CATALOG:
            raise ValidationError("unknown sku: %s" % str(sku)[:40])
        if sku in seen:
            raise ValidationError("duplicate sku in order: %s" % sku)
        if isinstance(qty, bool) or not isinstance(qty, int) or not (1 <= qty <= MAX_QTY):
            raise ValidationError("qty must be an integer from 1 to %d" % MAX_QTY)
        seen.add(sku)
        clean.append({"sku": sku, "qty": qty})
    return {"customerId": customer, "items": clean}


def request_hash(order):
    canon = json.dumps(order, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def order_id_for(idempotency_key):
    return "ord_" + hashlib.sha256(idempotency_key.encode()).hexdigest()[:24]


def total_cents(items):
    return sum(CATALOG[i["sku"]] * i["qty"] for i in items)


def now_ms():
    return int(time.time() * 1000)


def emit_metrics(namespace, metrics, dimensions=None):
    """Write one CloudWatch Embedded Metric Format line to stdout (no API call, no extra IAM).

    metrics: {name: (value, unit)}
    """
    dims = dimensions or {}
    doc = {
        "_aws": {
            "Timestamp": now_ms(),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [sorted(dims.keys())] if dims else [[]],
                    "Metrics": [{"Name": n, "Unit": u} for n, (_, u) in metrics.items()],
                }
            ],
        }
    }
    doc.update(dims)
    for n, (v, _) in metrics.items():
        doc[n] = v
    print(json.dumps(doc))


def table():
    import boto3

    return boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
