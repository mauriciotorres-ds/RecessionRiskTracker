# recession-tracker (subproject)

This is the deployable code for the Recession Risk Tracker project. **The full project README — including project significance, architecture, scoring rules, API contracts, and how it maps to the DS5220 Data Project 3 spec — lives in [the repo root README](../README.md).**

## What's in here

```
recession-tracker/
├── ingest/
│   ├── lambda_function.py    # Part 1 — daily FRED → DynamoDB ingest Lambda
│   ├── backfill.py           # one-time historical backfill (12 months)
│   └── requirements.txt
├── api/
│   ├── app.py                # Part 2 — Chalice REST API
│   ├── requirements.txt
│   └── .chalice/
│       ├── config.json       # env vars + matplotlib layer reference
│       └── policy-dev.json   # IAM: DynamoDB + S3 + CloudWatch
└── README.md                 # this file
```

## Quick deploy reference

**Ingest Lambda** (one-time setup, then runs daily on EventBridge):

1. Build zip: `cd ingest && zip -r ../ingest_lambda.zip lambda_function.py` (plus bundle `fredapi`)
2. Upload via Lambda console → `recession-tracker-ingest`
3. Set env vars: `FRED_API_KEY`, `DYNAMODB_TABLE=recession-tracker`
4. Attach AWSSDKPandas Python 3.11 layer
5. EventBridge trigger: `rate(1 day)`

**Backfill** (one-time historical population):

```bash
export FRED_API_KEY=your_key
cd ingest && python backfill.py
```

**API** (Chalice deploy):

```bash
cd api
pip install chalice boto3 matplotlib
chalice deploy --stage dev
```

The API references a custom Lambda layer for matplotlib (built once with `pip --platform manylinux2014_x86_64`, published to AWS, ARN baked into `config.json`). This avoids bundling ~50MB of wheels into every chalice deploy.

## Logging & error handling

Both halves use Python's `logging` module. Every external call (FRED HTTP, DynamoDB write/scan, matplotlib render, S3 upload) is wrapped in `try/except` so a single failure doesn't crash the entire run. Final scores and triggered flags are logged on every ingest run.

## API resource summary

| Resource | Purpose |
|---|---|
| `GET /` | Zone apex per DP3 contract: `{about, resources}` |
| `GET /current` | Most recent score with severity label and reason snippet |
| `GET /trend` | 30-day average, direction, and min/max range |
| `GET /plot` | Public S3 URL of a 3-panel 12-month chart (Risk / T10Y2Y / VIX) |
| `GET /indicators` | Per-series traffic-light status |
| `GET /momentum` | Derived velocity metrics, fresh on every call |

See the [root README](../README.md) for the full response shapes and scoring rules.
