"""The template is the contract between code and AWS. These checks catch drift without deploying."""
import json
import pathlib
import re
import shutil
import subprocess

import pytest

import samload

ROOT = samload.ROOT
SRC = {p.name: p.read_text() for p in (ROOT / "src").glob("*.py")}


@pytest.fixture(scope="module")
def cfn():
    return samload.expand()


def of_type(cfn, t):
    return {k: v["Properties"] for k, v in cfn["Resources"].items() if v["Type"] == t}


def role_actions(cfn, role):
    stmts = of_type(cfn, "AWS::IAM::Role")[role]["Policies"][0]["PolicyDocument"]["Statement"]
    acts = set()
    for s in stmts:
        a = s["Action"]
        acts |= set([a] if isinstance(a, str) else a)
    return acts, stmts


def test_cfn_lint_is_clean():
    exe = shutil.which("cfn-lint")
    if not exe:
        pytest.skip("cfn-lint not installed")
    r = subprocess.run([exe, str(ROOT / "template.yaml")], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


def test_iam_has_no_wildcards(cfn):
    for role in ("IntakeRole", "WorkerRole"):
        acts, stmts = role_actions(cfn, role)
        assert not any("*" in a for a in acts)
        assert all(s["Resource"] != "*" for s in stmts)


def test_iam_covers_exactly_the_calls_the_code_makes(cfn):
    calls = {"put_item": "dynamodb:PutItem", "get_item": "dynamodb:GetItem", "update_item": "dynamodb:UpdateItem",
             "send_message": "sqs:SendMessage"}
    for role, module in (("IntakeRole", "intake.py"), ("WorkerRole", "worker.py")):
        acts, _ = role_actions(cfn, role)
        code = SRC[module] + SRC["common.py"] * 0
        used = {a for call, a in calls.items() if re.search(r"\.%s\(" % call, code)}
        data_actions = {a for a in acts if a.startswith(("dynamodb:", "sqs:SendMessage"))}
        assert used == data_actions, (role, used, data_actions)
    worker_acts, _ = role_actions(cfn, "WorkerRole")
    assert {"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"} <= worker_acts
    assert "sqs:SendMessage" not in worker_acts and "dynamodb:PutItem" not in worker_acts


def test_queue_settings_follow_lambda_guidance(cfn):
    fns = of_type(cfn, "AWS::Lambda::Function")
    worker_timeout = fns["WorkerFunction"]["Timeout"]
    q = of_type(cfn, "AWS::SQS::Queue")["OrdersQueue"]
    assert q["VisibilityTimeout"] >= 6 * worker_timeout
    assert q["RedrivePolicy"]["maxReceiveCount"] >= 5
    assert q["RedrivePolicy"]["deadLetterTargetArn"] == {"Fn::GetAtt": ["OrdersDlq", "Arn"]}
    assert of_type(cfn, "AWS::SQS::Queue")["OrdersDlq"]["MessageRetentionPeriod"] > q["MessageRetentionPeriod"]


def test_event_source_mapping_reports_partial_failures_and_caps_concurrency(cfn):
    esm = of_type(cfn, "AWS::Lambda::EventSourceMapping")["WorkerFunctionOrders"]
    assert esm["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert "MaximumConcurrency" in esm["ScalingConfig"] and esm["BatchSize"] == 10
    import common

    # Worst case the slow switch adds MAX_SLOW_MS per message, so a full batch must fit in the timeout.
    worst_batch_seconds = esm["BatchSize"] * common.MAX_SLOW_MS / 1000.0
    assert worst_batch_seconds < of_type(cfn, "AWS::Lambda::Function")["WorkerFunction"]["Timeout"]


def test_api_requires_key_and_has_usage_plan(cfn):
    api = of_type(cfn, "AWS::ApiGateway::RestApi")["OrdersApi"]["Body"]
    assert api["paths"]["/orders"]["post"]["security"] == [{"api_key": []}]
    plan = of_type(cfn, "AWS::ApiGateway::UsagePlan")["OrdersApiUsagePlan"]
    assert {"RateLimit", "BurstLimit"} <= set(plan["Throttle"])
    assert plan["Quota"]["Period"] == "DAY" and "Limit" in plan["Quota"]
    assert "OrdersApiUsagePlanKey" in cfn["Resources"]


def test_every_alarm_notifies_the_topic_and_resolves(cfn):
    alarms = of_type(cfn, "AWS::CloudWatch::Alarm")
    assert len(alarms) == 5
    for name, a in alarms.items():
        assert a["AlarmActions"] == [{"Ref": "AlertTopic"}] and a["OKActions"] == [{"Ref": "AlertTopic"}], name
        assert a["TreatMissingData"] == "notBreaching"


def test_alarms_watch_the_right_queues(cfn):
    alarms = of_type(cfn, "AWS::CloudWatch::Alarm")
    dim = lambda n: alarms[n]["Dimensions"][0]["Value"]
    assert dim("DlqNotEmptyAlarm") == {"Fn::GetAtt": ["OrdersDlq", "QueueName"]}
    assert dim("BacklogAgeAlarm") == {"Fn::GetAtt": ["OrdersQueue", "QueueName"]}
    assert dim("BacklogDepthAlarm") == {"Fn::GetAtt": ["OrdersQueue", "QueueName"]}


def test_custom_metrics_used_by_alarms_and_dashboard_are_emitted_by_code(cfn):
    emitted = set(re.findall(r'"((?:Orders|Duplicate|Message|Confirm)\w+)"', SRC["intake.py"] + SRC["worker.py"]))
    raw = (ROOT / "template.yaml").read_text()
    used = set(re.findall(r'\["FlashOrders","(\w+)"\]', raw)) | set(re.findall(r'\[\"\.\",\"(\w+)\"', raw))
    used = {u for u in used if u.startswith(("Orders", "Duplicate", "Message", "Confirm"))}
    alarm_metrics = {a["MetricName"] for a in of_type(cfn, "AWS::CloudWatch::Alarm").values()
                     if a.get("Namespace") == "FlashOrders"}
    assert used and used <= emitted, used - emitted
    assert alarm_metrics <= emitted


def test_dashboard_body_is_valid_json_with_widgets(cfn):
    body = of_type(cfn, "AWS::CloudWatch::Dashboard")["Dashboard"]["DashboardBody"]["Fn::Sub"]
    doc = json.loads(re.sub(r"\$\{[^}]+\}", "0", body))
    assert len(doc["widgets"]) == 9
    titles = [w["properties"]["title"] for w in doc["widgets"]]
    assert any("Dead-letter" in t for t in titles) and any("oldest" in t for t in titles)


def test_functions_have_explicit_log_groups_with_retention(cfn):
    groups = of_type(cfn, "AWS::Logs::LogGroup")
    assert set(groups) == {"IntakeLogGroup", "WorkerLogGroup"}
    assert all(g["RetentionInDays"] == 14 for g in groups.values())


def test_no_secrets_or_account_ids_in_source():
    for name, text in SRC.items():
        assert not re.search(r"AKIA[0-9A-Z]{16}|\b\d{12}\b", text), name
