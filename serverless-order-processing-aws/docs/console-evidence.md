# Console evidence

Screenshots from the AWS console (`us-east-1`), taken 24 Nov 2026. They show which parts of the system exist and how they are configured. Screenshots show configuration, not behavior: nothing below proves a code path ran. The account ID and the alert email address are blacked out.

## 1. DynamoDB table overview

`OrdersTable`: on-demand capacity, status Active, partition key `orderId`, item count 1, no active alarms, point-in-time recovery off.

![DynamoDB OrdersTable overview](../images/console/01-dynamodb-ordersTable-overview.png)

## 2. Stored order item (historical record)

The table holds one item with `status` `CONFIRMED`, `totalCents` 2598, one line item (`MUG-CER`, quantity 2), and `confirmedAt` 3882 ms after `createdAt`. This is the end state of one earlier order. It was not created or re-tested while producing this document, and it does not show how the order was submitted or whether a retry was made.

![DynamoDB order item in JSON view](../images/console/02-dynamodb-order-item-json.png)

## 3. Lambda functions

`orders-worker` and `orders-intake` exist, both Python 3.12, Zip package.

![Lambda function list](../images/console/03-lambda-functions.png)

## 4. SQS queues

`OrdersQueue` and `OrdersDLQ` exist, both Standard with SSE-SQS encryption, 0 messages available and 0 in flight at capture time.

![SQS queue list](../images/console/04-sqs-queues.png)

## 5. API Gateway

REST API `orders-api`, Regional endpoint.

![API Gateway API list](../images/console/05-api-gateway-apis.png)

## 6. `POST /orders` method

The method execution diagram shows Client, method request, integration request, Lambda integration and proxy integration.

![API Gateway POST /orders method execution](../images/console/06-api-gateway-post-orders.png)

Method request: Authorization NONE, API key required True.

![Method request settings](../images/console/07-api-gateway-method-request.png)

Integration request: Lambda proxy integration True, function `orders-intake`, timeout default (29 seconds).

![Integration request settings](../images/console/08-api-gateway-integration-request.png)

## 7. Stage `prod`

Stage `prod` of `orders-api`, active deployment `ccn4t8`, stage rate 10000 and burst 5000, cache cluster inactive.

![API Gateway stage prod](../images/console/09-api-gateway-stage-prod.png)

## 8. `OrdersQueue` and dead-letter queue

`OrdersQueue`: Standard, SSE-SQS, dead-letter queue enabled.

![OrdersQueue details](../images/console/10-sqs-ordersqueue-details.png)

Dead-letter queue tab: target `OrdersDLQ`, maximum receives 5. No dead-letter delivery was tested.

![OrdersQueue dead-letter queue tab](../images/console/12-sqs-ordersqueue-dlq.png)

## 9. Queue to worker trigger

Lambda triggers tab of `OrdersQueue`: one trigger, function `orders-worker`, status Enabled, last modified 11/23/2026 2:51:26 AM.

![OrdersQueue Lambda triggers](../images/console/11-sqs-lambda-trigger.png)

## 10. SNS alert topic and subscription

Standard topic `orders-alerts` and one email subscription with status Confirmed. The subscription was confirmed by the owner. No test notification was sent.

![SNS topics](../images/console/13-sns-topics.png)

![SNS subscription confirmed](../images/console/14-sns-subscription-confirmed.png)

## 11. CloudWatch alarms

Five alarms, all in state OK with actions enabled, all publishing ALARM and OK transitions to `orders-alerts`: `orders-dlq-not-empty`, `orders-backlog-age`, `orders-backlog-depth`, `orders-worker-failures`, `orders-intake-errors`. They were created manually after the initial review, with the same thresholds as `template.yaml` (but without the stack-name prefix). They have not fired, and notification delivery has not been tested.

![CloudWatch alarms](../images/console/15-cloudwatch-alarms.png)

## 12. CloudWatch dashboard

Dashboard `orders`, nine widgets matching `template.yaml`: intake, queue depth, age of oldest message, worker outcomes, worker duration and concurrency, order wait, DLQ, API Gateway, and alarm status. Seven of the eight `FlashOrders` metrics exist; `OrdersRejectedAtIntake` has no data because no request has been rejected at intake yet. Sparse lines reflect low traffic.

![Dashboard, top](../images/console/16-cloudwatch-dashboard-top.png)

![Dashboard, bottom, with alarm status](../images/console/17-cloudwatch-dashboard-bottom.png)

## Lambda code and configuration

The deployed code editor contents for `orders-intake` and `orders-worker` (`intake.py`, `worker.py`, `common.py`) were read from the console editor and compared with `src/` after normalizing trailing whitespace; they match. This is not a byte-level or hash comparison. Both functions: Python 3.12, arm64, 256 MB, no layers; intake timeout 10 s, worker timeout 15 s. Environment variable values were not read or recorded.
