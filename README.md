# Recession Risk Tracker

**DS5220 — Data Project 3 (Mauricio Torres)**

A two-part serverless AWS project that pulls macroeconomic indicators from the [FRED API](https://fred.stlouisfed.org/) every day, computes a rule-based composite recession risk score (0–100), persists the snapshot in DynamoDB, and exposes the results through a Chalice REST API that integrates with the course Discord bot.

- **Live API:** https://je4ky6hvbd.execute-api.us-east-1.amazonaws.com/api/
- **Discord registration:** `recession-risk` (in `#dp3`, run `/project recession-risk`)
- **Live plot:** https://recession-tracker-plots-mt0925.s3.amazonaws.com/latest.png

---

## Project significance

Most macro dashboards either show single indicators in isolation (a yield curve here, a CPI print there) or rely on opaque proprietary models. This project does the opposite: it composes seven independent, well-known leading and coincident indicators into a single, transparent 0–100 score that a non-technical stakeholder can read at a glance, while keeping every input visible behind it.

Each rule that contributes to the score is documented and traceable to a specific economic signal that economists watch when assessing recession risk. A score of "65/100 (Elevated)" comes back with the human-readable reason — "yield curve inverted, consumer sentiment weak" — so the user understands not just the number but *why* it's where it is.

The ingestion side runs entirely serverless on AWS (EventBridge → Lambda → DynamoDB), and the integration API runs as a Chalice app on API Gateway + Lambda. The whole thing sits inside the AWS free tier and runs without manual intervention.

---

## How this project maps to the assignment

`Instructions_DP3.md` lays out the deliverables for DS5220 Data Project 3. Here's how each requirement is met:

| Requirement | Implementation |
|---|---|
| **Meaningful, time-changing data source** | Seven FRED series — daily yield curve and VIX, monthly UNRATE / CPI / sentiment / fed funds / recession-probability |
| **EventBridge → Lambda → DynamoDB pipeline** | `ingest/lambda_function.py` triggered by `rate(1 day)`, writes to `recession-tracker` DynamoDB table |
| **Persistent timestamped store** | DynamoDB row per day, primary key `date` (YYYY-MM-DD), unix `timestamp` attribute |
| **Idempotent ingest** | Same-day `PutItem` overwrites the existing row, no duplicates on retries |
| **Logging copious + try/except everywhere** | Python `logging` module; every FRED call, DynamoDB write, matplotlib render, and S3 upload is wrapped |
| **Chalice app named per spec** | `recession-tracker-api`, deployed to `dev` stage |
| **Zone apex contract** | `GET /` returns `{"about": "...", "resources": [...]}` exactly as the bot expects |
| **At least 3 resources, all returning `{response: ...}`** | 5 resources: `current`, `trend`, `plot`, `indicators`, `momentum` |
| **A current / point-in-time resource** | `GET /current` |
| **A trend / summary resource** | `GET /trend` (30-day average, direction, range) |
| **A plot resource returning a public S3 URL** | `GET /plot` uploads PNG to `s3://recession-tracker-plots-mt0925/latest.png` with `ACL='public-read'` |
| **Decimals cast to float before JSON** | All `_normalize()` paths in `api/app.py` |
| **Discord-bot-registered API** | Registered as `recession-risk` in `#dp3`; `/project recession-risk` lists the 5 resources |
| **Stretch goals** | VIX as 7th indicator with intraday-meaningful daily updates; `/momentum` derived endpoint with velocity metrics; `/indicators` traffic-light dashboard; CPI YoY computation; severity-tiered plot bands |

---

## Part 1 — Ingestion Pipeline

### Data sources

All seven series come from the Federal Reserve Bank of St. Louis (FRED). They are the textbook leading and coincident indicators that economists watch when assessing recession risk:

| FRED ID | What it measures | Why it matters | Cadence |
|---|---|---|---|
| `T10Y2Y` | 10-year minus 2-year Treasury yield spread | Has inverted before every U.S. recession since 1955 | Daily |
| `UNRATE` | Civilian unemployment rate | Rising unemployment is a coincident recession signal | Monthly |
| `CPIAUCSL` | Consumer Price Index (all urban consumers) | We compute YoY change to detect inflation pressure | Monthly |
| `UMCSENT` | University of Michigan Consumer Sentiment | Weak sentiment correlates with reduced consumer spending | Monthly |
| `FEDFUNDS` | Federal funds effective rate | A restrictive policy stance raises recession risk | Monthly |
| `RECPROB` | NY Fed Smoothed Recession Probability | A model-based probability published monthly | Monthly |
| `VIXCLS` | CBOE Volatility Index ("fear gauge") | Daily-changing market stress indicator | Daily |

### Sampling cadence

The ingest Lambda runs **daily** on an EventBridge `rate(1 day)` schedule. T10Y2Y and VIX are daily series; the others are monthly — the Lambda always reads the most recent observation FRED has published as of the run, which matches what a real-time tracker would do. The function is idempotent: a same-day re-run `PutItem`s over the existing row.

### Storage schema

DynamoDB table **`recession-tracker`**:

- **Partition key:** `date` (String, `YYYY-MM-DD`)
- **Attributes:** `t10y2y`, `unrate`, `cpiaucsl`, `cpi_yoy_pct`, `umcsent`, `fedfunds`, `recprob`, `vix`, `risk_score`, `timestamp` (unix), `flags` (map of triggered rule names)

### Risk scoring

Each rule contributes a fixed point value when triggered; the total is capped at 100. Severity bands: 0–25 Low · 26–50 Moderate · 51–75 Elevated · 76–100 High.

| Rule | Points |
|---|---|
| Yield curve inverted (T10Y2Y < 0) | 30 |
| Unemployment rising 3 consecutive months | 20 |
| CPI YoY > 4% | 15 |
| Consumer sentiment < 70 | 15 |
| Fed funds > 4% | 10 |
| NY Fed RECPROB > 25% | 10 |
| VIX > 25 (mild stress) | 5 |
| VIX > 35 (high stress) | 10 |

### Logging & error handling

Both halves use Python's `logging` module (no `print`). Every FRED call, DynamoDB write/scan, matplotlib render, and S3 upload is wrapped in `try/except` so a single failure (a flaky FRED endpoint, a missing series) doesn't bring the run down. The ingest Lambda logs the final score and which flags triggered every run.

---

## Part 2 — Integration API

Chalice app `recession-tracker-api` deployed to API Gateway + Lambda. Reads from the DynamoDB table populated by Part 1 and exposes five resources. Every non-root resource returns `{"response": ...}` per the DP3 contract; Decimals from DynamoDB are cast to float before JSON serialization.

### `GET /` — zone apex

```json
{
  "about": "Tracks key Federal Reserve macro indicators daily and computes a composite recession risk score (0-100) for non-technical stakeholders.",
  "resources": ["current", "trend", "plot", "indicators", "momentum"]
}
```

### `GET /current` — point-in-time

Most recent score with severity label and a human-readable reason snippet built from the triggered flags.

```json
{ "response": "Recession risk is 15/100 (Low) as of 2026-05-06. Consumer sentiment weak." }
```

### `GET /trend` — 30-day summary

```json
{ "response": "30-day avg risk: 16/100. Trend: Stable. Range: 15-35." }
```

### `GET /plot` — 365-day chart (returns S3 URL)

Three-panel matplotlib figure, regenerated on each call and uploaded to the public-read S3 bucket. Panels: (top) Risk Score with severity bands, (middle) T10Y2Y spread with inversion threshold line, (bottom) VIX with elevated/stress threshold lines.

```json
{ "response": "https://recession-tracker-plots-mt0925.s3.amazonaws.com/latest.png" }
```

### `GET /indicators` — traffic-light dashboard (stretch goal)

Per-series red/yellow/green status for each of the seven indicators.

```json
{
  "response": {
    "yield_curve": { "value": 0.49, "status": "yellow", "label": "Flattening" },
    "unemployment": { "value": 4.3, "status": "yellow", "label": "Watching" },
    "cpi": { "value": 3.32, "status": "yellow", "label": "Elevated" },
    "consumer_sentiment": { "value": 53.3, "status": "red", "label": "Weak" },
    "fed_funds": { "value": 3.64, "status": "yellow", "label": "Neutral" },
    "recprob": { "value": null, "status": "gray", "label": "Unknown" },
    "vix": { "value": 17.5, "status": "green", "label": "Calm" }
  }
}
```

### `GET /momentum` — derived velocity metrics (stretch goal)

Computed fresh on every call from whatever's in DynamoDB right now — useful for a dashboard poll loop. The `computed_at_utc` timestamp updates by the second, so the endpoint feels live even between daily ingests.

```json
{
  "response": {
    "as_of": "2026-05-06",
    "current_score": 15,
    "change_1d": 0,
    "change_7d": -10,
    "change_30d": -10,
    "change_90d": -20,
    "days_at_current_score": 173,
    "trajectory": "Stable",
    "vix_today": 17.5,
    "vix_change_1d": -0.4,
    "active_flags": ["consumer_sentiment_weak"],
    "computed_at_utc": "2026-05-07T01:42:11Z"
  }
}
```

---

## Architecture

```
┌──────────────┐       ┌──────────────────┐      ┌────────────────┐
│ EventBridge  │──────▶│ Ingest Lambda    │─────▶│ DynamoDB       │
│ rate(1 day)  │       │ (Part 1)         │      │ recession-     │
└──────────────┘       │ + AWSSDKPandas   │      │   tracker      │
                       │   Layer          │      └───────┬────────┘
                       │                  │              │
                       │ FRED API ────────┘              │
                       └──────────────────┘              │
                                                         ▼
┌──────────────┐       ┌──────────────────┐      ┌────────────────┐
│ Discord bot  │──────▶│ API Gateway      │─────▶│ Chalice Lambda │
│ /project     │       │                  │      │ (Part 2)       │
└──────────────┘       └──────────────────┘      │ + matplotlib   │
                                                 │   Layer        │
                                                 └───────┬────────┘
                                                         │
                                                         ▼
                                                 ┌────────────────┐
                                                 │ S3 (public)    │
                                                 │ latest.png     │
                                                 └────────────────┘
```

### AWS resources provisioned

| Resource | Name |
|---|---|
| DynamoDB table | `recession-tracker` |
| S3 bucket | `recession-tracker-plots-mt0925` |
| Ingest Lambda | `recession-tracker-ingest` (Python 3.11, AWSSDKPandas layer) |
| API Lambda | `recession-tracker-api-dev` (Python 3.12, custom matplotlib layer) |
| Custom layer | `matplotlib-py312:1` (matplotlib 3.9.4 + numpy 1.26.4 + contourpy 1.2.1) |
| EventBridge rule | `recession-tracker-daily` (rate(1 day)) |
| API Gateway | `recession-tracker-api` |

---

## Repository layout

```
RecessionRiskTracker/                 # this repo
├── README.md                         # this file
├── Instructions_DP3.md               # original course assignment
├── .gitignore
└── recession-tracker/                # the project
    ├── README.md                     # subproject README
    ├── ingest/
    │   ├── lambda_function.py        # Part 1 — daily ingest Lambda
    │   ├── backfill.py               # one-time historical backfill (12 months)
    │   └── requirements.txt
    └── api/
        ├── app.py                    # Part 2 — Chalice REST API
        ├── requirements.txt
        └── .chalice/
            ├── config.json           # references matplotlib layer
            └── policy-dev.json       # IAM: DynamoDB + S3 + CloudWatch
```

---

## Stretch goals

Beyond the three required resources, this project added:

1. **VIX as a 7th indicator** — adds a second daily-changing input alongside T10Y2Y, gives the chart real fine-grained motion, and contributes to scoring (mild +5, high stress +10).
2. **`/momentum` resource** — derives velocity (1d/7d/30d/90d score deltas, days-at-current-score, trajectory label, VIX 1-day change) from the existing daily data. Returns fresh-computed metrics on every call, so the endpoint feels live even between daily ingests.
3. **`/indicators` traffic-light dashboard** — per-series red/yellow/green status with human-readable labels ("Inverted", "Hot", "Restrictive", etc.) — more useful for non-technical readers than the composite score alone.
4. **CPI YoY computation** — the spec said "CPI > 4%" but `CPIAUCSL` is a price index (~300), not a percentage. The pipeline computes year-over-year % change from a 13-month window and applies the 4% threshold to that, matching the economic intent.
5. **365-day historical backfill** — `ingest/backfill.py` reconstructs daily risk scores from FRED history and bulk-writes 12+ months of rows to DynamoDB so the chart and trend resources have something real to show on day one.
6. **Custom matplotlib Lambda Layer** — published to AWS as `matplotlib-py312:1`, used by the API Lambda. Sidesteps chalice's wheel-bundling limits and keeps the deploy package under 1 MB.

---

## Discord registration

Registered with the course bot in `#dp3`:

```
/register recession-risk matorres0925 https://je4ky6hvbd.execute-api.us-east-1.amazonaws.com/api/
```

Verify with:

```
/project recession-risk
/project recession-risk current
/project recession-risk indicators
/project recession-risk plot
/project recession-risk momentum
```

---

## License & attribution

- Macro data: Federal Reserve Bank of St. Louis (FRED), https://fred.stlouisfed.org/
- Matplotlib Lambda layer: built locally from official PyPI manylinux2014 wheels
- Course framework: DS5220 Data Project 3 — see `Instructions_DP3.md`
