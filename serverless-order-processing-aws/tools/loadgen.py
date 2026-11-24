#!/usr/bin/env python3
"""Generate promotional-style order traffic against the FlashOrders API (standard library only).

Example:
  python tools/loadgen.py --stack flashorders --rps 30 --duration 60 --out run1.jsonl
The key is read from --api-key or FLASH_API_KEY. Each request has its own Idempotency-Key;
--duplicate-rate re-sends a share of earlier keys to prove duplicates do not create orders.
"""
import argparse
import concurrent.futures as cf
import json
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.request

SKUS = ["TSHIRT-BLK", "TSHIRT-WHT", "HOODIE-GRY", "CAP-NVY", "MUG-CER", "BOTTLE-STL", "TOTE-CNV", "STICKER-PK"]


def make_body(rng):
    chosen = rng.sample(SKUS, rng.randint(1, 3))
    return {"customerId": "cust-%04d" % rng.randint(1, 500), "items": [{"sku": s, "qty": rng.randint(1, 3)} for s in chosen]}


def post(url, api_key, key, body, timeout=15):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "x-api-key": api_key, "Idempotency-Key": key},
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status, payload = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, payload = e.code, e.read()
    except Exception as e:  # network failure or timeout
        return {"key": key, "status": 0, "ms": (time.perf_counter() - start) * 1000, "error": type(e).__name__}
    ms = (time.perf_counter() - start) * 1000
    out = {"key": key, "status": status, "ms": round(ms, 1)}
    try:
        out["orderId"] = json.loads(payload).get("orderId")
    except Exception:
        pass
    return out


def stack_outputs(stack):
    import boto3

    outs = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
    return {o["OutputKey"]: o["OutputValue"] for o in outs}


def pct(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100.0 * (len(values) - 1))))]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stack", help="CloudFormation stack name; used to look up the URL and API key")
    ap.add_argument("--url")
    ap.add_argument("--api-key", default=os.environ.get("FLASH_API_KEY"))
    ap.add_argument("--rps", type=int, default=10)
    ap.add_argument("--duration", type=int, default=30, help="seconds")
    ap.add_argument("--duplicate-rate", type=float, default=0.05)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="run.jsonl")
    ap.add_argument("--max-rps", type=int, default=300, help="safety cap on --rps")
    a = ap.parse_args(argv)

    if a.rps > a.max_rps:
        sys.exit("refusing --rps %d (cap %d). Raise --max-rps deliberately." % (a.rps, a.max_rps))
    url, key = a.url, a.api_key
    if a.stack and not (url and key):
        import boto3

        o = stack_outputs(a.stack)
        url = url or o["ApiUrl"]
        key = key or boto3.client("apigateway").get_api_key(apiKey=o["ApiKeyId"], includeValue=True)["value"]
    if not (url and key):
        sys.exit("need --stack, or --url and --api-key (or FLASH_API_KEY)")

    rng = random.Random(a.seed)
    run = "run%d" % int(time.time())
    sent, results = [], []
    with cf.ThreadPoolExecutor(max_workers=min(256, max(8, a.rps * 2))) as pool:
        futures = []
        t0 = time.time()
        for sec in range(a.duration):
            tick = t0 + sec
            for _ in range(a.rps):
                if sent and rng.random() < a.duplicate_rate:
                    k, body = rng.choice(sent)  # a client retry of an earlier request
                else:
                    k, body = "%s-%06d" % (run, len(sent)), make_body(rng)
                    sent.append((k, body))
                futures.append(pool.submit(post, url, key, k, body))
            time.sleep(max(0, tick + 1 - time.time()))
        results = [f.result() for f in futures]

    with open(a.out, "w") as fh:
        for r in results:
            fh.write(json.dumps(r) + "\n")
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    ok = [r["ms"] for r in results if r["status"] in (200, 202)]
    print("requests sent:      %d (%d unique idempotency keys)" % (len(results), len(sent)))
    print("status codes:       %s" % dict(sorted(counts.items())))
    if ok:
        print("client latency ms:  p50=%.0f p95=%.0f p99=%.0f (measured from this machine)" % (pct(ok, 50), pct(ok, 95), pct(ok, 99)))
    accepted = {r["orderId"] for r in results if r["status"] in (200, 202) and r.get("orderId")}
    print("distinct orderIds accepted: %d" % len(accepted))
    print("results written to %s. Next: python tools/ops.py verify %s" % (a.out, a.out))


if __name__ == "__main__":
    main()
