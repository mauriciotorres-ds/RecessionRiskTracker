# Recession Risk Tracker

A two-part serverless AWS project for DS5220 Data Project 3.

It pulls six macro indicators from the [FRED API](https://fred.stlouisfed.org/) every day, computes a rule-based composite recession risk score (0–100), persists each day's snapshot in DynamoDB, and exposes the results through a Chalice REST API that integrates with the course Discord bot.

## Data source & rationale

All six series come from the Federal Reserve Bank of St. Louis (FRED). They are the textbook leading and coincident indicators that economists watch when assessing recession risk:

- **T10Y2Y** — 10-year minus 2-year Treasury yield spread. Inversion has preceded every U.S. recession since 1955.
- **UNRATE** — Civilian unemployment rate. Rising unemployment is a coincident recession signal.
- **CPIAUCSL** — Consumer Price Index. We compute year-over-year change to detect inflation pressure.
- **UMCSENT** — University of Michigan Consumer Sentiment. Weak sentiment correlates with reduced spending.
- **FEDFUNDS** — Federal funds rate. A restrictive policy stance raises recession risk.
- **RECPROB** — NY Fed Smoothed Recession Probability (model-based).

## Sampling cadence & schema

The ingest Lambda runs daily on an EventBridge `rate(1 day)` schedule. T10Y2Y is daily; the others are monthly — we always read the most recent observation FRED has published, which is what a real-time tracker would do.

DynamoDB table **`recession-tracker`**:

- Partition key: `date` (string, `YYYY-MM-DD`)
- Attributes: `t10y2y`, `unrate`, `cpiaucsl`, `cpi_yoy_pct`, `umcsent`, `fedfunds`, `recprob`, `risk_score`, `timestamp` (unix), `flags` (map of triggered rules)

Idempotency: same-day re-runs `PutItem` over the existing row, so retries don't create duplicates.

## Risk scoring

Each rule contributes a fixed point value when triggered; the total is capped at 100.

| Rule | Points |
|---|---|
| Yield curve inverted (T10Y2Y < 0) | 30 |
| Unemployment rising 3 consecutive months | 20 |
| CPI YoY > 4% | 15 |
| Consumer sentiment < 70 | 15 |
| Fed funds > 4% | 10 |
| NY Fed RECPROB > 25% | 10 |

Severity bands: 0–25 Low, 26–50 Moderate, 51–75 Elevated, 76–100 High.

## API resources

Base URL: your deployed API Gateway endpoint.

| Resource | What it returns |
|---|---|
| `GET /` | DP3 contract: `{about, resources}`. |
| `GET /current` | Most recent score with severity label and human-readable reason snippet. |
| `GET /trend` | 30-day average, direction (Rising / Falling / Stable), and min–max range. |
| `GET /plot` | Public S3 URL of a 90-day chart with risk score (top, colored by tier) and yield-curve spread (bottom, with zero line). |
| `GET /indicators` | Per-series traffic-light status (green / yellow / red) for each of the six indicators. |

All resources except `/` return `{"response": ...}` per the DP3 contract. Decimals from DynamoDB are cast to floats before serialization.

## Project layout

```
recession-tracker/
├── ingest/
│   ├── lambda_function.py    # Part 1 ingest Lambda
│   └── requirements.txt
├── api/
│   ├── app.py                # Part 2 Chalice app
│   ├── requirements.txt
│   └── .chalice/
│       ├── config.json
│       └── policy-dev.json
└── README.md
```

## Logging & error handling

Both halves use Python's `logging` module (no `print`). Every FRED call, DynamoDB write/scan, matplotlib render, and S3 upload is wrapped in `try/except` so a single failure (a flaky FRED endpoint, a missing series) doesn't take the whole run down. The ingest Lambda logs the final score and which flags triggered every run.

## Stretch goals

- **Year-over-year CPI** — instead of comparing the raw CPI index against 4%, the ingest Lambda pulls 13 months of CPIAUCSL and computes the YoY percent change, then applies the threshold to that.
- **Severity-colored plot points** — each point on the risk-score plot is colored by tier (green / yellow / orange / red) instead of using a single line color.
- **Indicators dashboard** — `/indicators` adds a fourth resource beyond the required three, giving a per-series traffic-light view that's more useful than the composite score alone.

## Deployment notes

See the project root for full step-by-step deploy instructions. Quick version: provision DynamoDB + S3, set the three env vars in `.chalice/config.json`, `chalice deploy --stage dev`, package the ingest Lambda as a zip and wire it to a daily EventBridge rule, then `/register` the API URL in the course `#dp3` Discord channel.
