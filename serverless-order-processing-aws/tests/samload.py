"""Expand the SAM template offline (no AWS calls) so tests can inspect the real CloudFormation."""
import os
import pathlib

import boto3
from samtranslator.public.translator import ManagedPolicyLoader
from samtranslator.translator.transform import transform
from samtranslator.yaml_helper import yaml_parse

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_raw():
    doc = yaml_parse((ROOT / "template.yaml").read_text())
    # `sam build`/`sam deploy` upload src/ for you. Offline, the transform wants an S3 URI, so the
    # in-memory copy gets a placeholder. template.yaml on disk is not changed.
    doc["Globals"]["Function"]["CodeUri"] = "s3://placeholder-bucket/code.zip"
    return doc


def expand(params=None):
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    iam = boto3.client("iam", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")
    values = {"AlertEmail": "ops@example.com"}
    values.update(params or {})
    return transform(load_raw(), values, ManagedPolicyLoader(iam))
