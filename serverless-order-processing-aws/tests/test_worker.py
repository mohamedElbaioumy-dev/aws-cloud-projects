import json
import time

import pytest

from helpers import GOOD, api_event, sqs_event


def make_order(aws, key="key-0000001", body=GOOD):
    return json.loads(aws.intake.handler(api_event(body, key), None)["body"])["orderId"]


def msg(oid):
    return json.dumps({"v": 1, "orderId": oid})


def status(aws, oid):
    return aws.table.get_item(Key={"orderId": oid})["Item"]


def test_confirms_order_and_prices_it_from_the_catalog(aws):
    oid = make_order(aws)
    out = aws.worker.handler(sqs_event(msg(oid)), None)
    assert out == {"batchItemFailures": []}
    item = status(aws, oid)
    assert item["status"] == "CONFIRMED" and item["totalCents"] == 2 * 1299 + 1499


def test_duplicate_delivery_changes_nothing(aws):
    oid = make_order(aws)
    aws.worker.handler(sqs_event(msg(oid)), None)
    first = status(aws, oid)["confirmedAt"]
    time.sleep(0.01)
    assert aws.worker.handler(sqs_event(msg(oid), msg(oid)), None) == {"batchItemFailures": []}
    assert status(aws, oid)["confirmedAt"] == first


def test_lost_race_is_treated_as_duplicate(aws, monkeypatch):
    oid = make_order(aws)
    real_table = aws.common.table()

    class RacingTable:
        def get_item(self, **kw):
            item = real_table.get_item(**kw)
            real_table.update_item(Key={"orderId": oid}, UpdateExpression="SET #s = :c, confirmedAt = :n, totalCents = :t, reason = :r",
                                   ExpressionAttributeNames={"#s": "status"},
                                   ExpressionAttributeValues={":c": "CONFIRMED", ":n": 1, ":t": 5, ":r": ""})
            return item

        def update_item(self, **kw):
            return real_table.update_item(**kw)

    monkeypatch.setattr(aws.common, "table", lambda: RacingTable())
    assert aws.worker.handler(sqs_event(msg(oid)), None) == {"batchItemFailures": []}
    assert status(aws, oid)["totalCents"] == 5  # the winner's value is untouched


def test_bad_messages_fail_individually_and_good_ones_still_confirm(aws):
    good = make_order(aws)
    batch = sqs_event("not json", msg(good), json.dumps({"v": 1}), msg("ord_missing"))
    out = aws.worker.handler(batch, None)
    assert [f["itemIdentifier"] for f in out["batchItemFailures"]] == ["m0", "m2", "m3"]
    assert status(aws, good)["status"] == "CONFIRMED"


def test_fail_switch_fails_every_message_and_leaves_orders_untouched(aws):
    oid = make_order(aws)
    aws.table.put_item(Item={"orderId": "CONTROL#fault", "mode": "fail", "expiresAt": int(time.time()) + 60})
    out = aws.worker.handler(sqs_event(msg(oid)), None)
    assert len(out["batchItemFailures"]) == 1 and status(aws, oid)["status"] == "RECEIVED"


def test_expired_or_broken_switch_is_ignored(aws):
    oid = make_order(aws)
    aws.table.put_item(Item={"orderId": "CONTROL#fault", "mode": "fail", "expiresAt": int(time.time()) - 1})
    assert aws.worker.handler(sqs_event(msg(oid)), None)["batchItemFailures"] == []
    oid2 = make_order(aws, "key-0000002", {"customerId": "c", "items": [{"sku": "MUG-CER", "qty": 1}]})
    aws.table.put_item(Item={"orderId": "CONTROL#fault", "mode": "explode", "expiresAt": int(time.time()) + 60})
    assert aws.worker.handler(sqs_event(msg(oid2)), None)["batchItemFailures"] == []


def test_slow_switch_sleeps_but_is_capped(aws, monkeypatch):
    sleeps = []
    monkeypatch.setattr(aws.worker.time, "sleep", lambda s: sleeps.append(s))
    oid = make_order(aws)
    aws.table.put_item(Item={"orderId": "CONTROL#fault", "mode": "slow", "delayMs": 999999, "expiresAt": int(time.time()) + 60})
    aws.worker.handler(sqs_event(msg(oid)), None)
    assert sleeps == [aws.common.MAX_SLOW_MS / 1000.0]


def test_retired_sku_is_rejected_not_retried(aws):
    oid = make_order(aws)
    aws.table.update_item(Key={"orderId": oid}, UpdateExpression="SET #i = :i", ExpressionAttributeNames={"#i": "items"},
                          ExpressionAttributeValues={":i": [{"sku": "DISCONTINUED", "qty": 1}]})
    assert aws.worker.handler(sqs_event(msg(oid)), None) == {"batchItemFailures": []}
    assert status(aws, oid)["status"] == "REJECTED"


def test_emits_cloudwatch_embedded_metrics(aws, capsys):
    oid = make_order(aws)
    capsys.readouterr()
    aws.worker.handler(sqs_event(msg(oid)), None)
    lines = [json.loads(l) for l in capsys.readouterr().out.splitlines() if l.startswith("{")]
    emf = [l for l in lines if "_aws" in l][-1]
    names = {m["Name"] for m in emf["_aws"]["CloudWatchMetrics"][0]["Metrics"]}
    assert {"OrdersConfirmed", "MessageFailures", "ConfirmLatencyMs"} <= names
    assert emf["OrdersConfirmed"] == 1
