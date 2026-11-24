import importlib
import json
import os
import pathlib
import sys

import boto3
import pytest
from moto import mock_aws

ROOT = pathlib.Path(__file__).resolve().parents[1]
for sub in ("src", "tools", "tests"):
    sys.path.insert(0, str(ROOT / sub))


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_DEFAULT_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def aws(monkeypatch):
    """Fake DynamoDB + SQS (moto). VisibilityTimeout is 0 so a failed message is redelivered at once."""
    with mock_aws():
        ddb = boto3.resource("dynamodb")
        ddb.create_table(
            TableName="orders",
            AttributeDefinitions=[{"AttributeName": "orderId", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "orderId", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        sqs = boto3.client("sqs")
        dlq = sqs.create_queue(QueueName="dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        q = sqs.create_queue(
            QueueName="orders",
            Attributes={
                "VisibilityTimeout": "0",
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}),
            },
        )["QueueUrl"]
        monkeypatch.setenv("TABLE_NAME", "orders")
        monkeypatch.setenv("QUEUE_URL", q)
        import common, intake, worker  # noqa: E401

        importlib.reload(common)
        importlib.reload(intake)  # recreate its SQS client inside the mock
        importlib.reload(worker)
        from ops import Env

        yield type("Aws", (), dict(table=ddb.Table("orders"), sqs=sqs, queue=q, dlq=dlq, intake=intake, worker=worker,
                                   common=common, env=Env("orders", q, dlq)))
