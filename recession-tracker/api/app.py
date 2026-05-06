"""
Recession Risk Tracker — Chalice API (Part 2)

Reads from the DynamoDB table populated by the Part 1 ingest Lambda and exposes
four resources:

  GET /            — about + resources list (DP3 contract)
  GET /current     — most recent risk score, with severity label + reasoning
  GET /trend       — 30-day average / direction / range
  GET /plot        — URL of a 90-day chart in S3
  GET /indicators  — latest value + traffic-light status for each series

All response bodies follow the DP3 contract: zone apex returns
{about, resources}; every other resource returns {response: ...}.
"""

import io
import logging
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError
from chalice import Chalice

# Matplotlib must run headless inside Lambda (no display)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

app = Chalice(app_name="recession-tracker-api")
app.log.setLevel(logging.INFO)
logger = app.log

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "recession-tracker")
S3_BUCKET = os.environ.get("S3_BUCKET", "recession-tracker-plots")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
PLOT_KEY = "latest.png"

# Lazy clients — created on first use so cold-start cost only hits the
# resources that actually need them.
_dynamodb_table = None
_s3_client = None


def _table():
    global _dynamodb_table
    if _dynamodb_table is None:
        logger.info("Initializing DynamoDB resource for table=%s", DYNAMODB_TABLE)
        _dynamodb_table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE)
    return _dynamodb_table


def _s3():
    global _s3_client
    if _s3_client is None:
        logger.info("Initializing S3 client for bucket=%s", S3_BUCKET)
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def _to_float(val):
    """Cast DynamoDB Decimal → float so json can serialize it."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def _normalize(item):
    """Walk a DynamoDB item and convert all Decimals to floats."""
    if isinstance(item, list):
        return [_normalize(x) for x in item]
    if isinstance(item, dict):
        return {k: _normalize(v) for k, v in item.items()}
    return _to_float(item)


def _severity_label(score):
    if score is None:
        return "Unknown"
    if score <= 25:
        return "Low"
    if score <= 50:
        return "Moderate"
    if score <= 75:
        return "Elevated"
    return "High"


def _severity_color(score):
    """For plot coloring — red/orange/yellow/green by tier."""
    if score is None:
        return "#888888"
    if score <= 25:
        return "#2ecc71"
    if score <= 50:
        return "#f1c40f"
    if score <= 75:
        return "#e67e22"
    return "#e74c3c"


def _scan_recent(days):
    """Pull all rows from the table and filter to the last N days client-side.

    Single-PK design (date is the partition key) means we can't Query by
    range — DP3 expects a small dataset (~90 rows max) so a Scan is cheap and
    correct here. If this grew, we'd switch to a single-partition design
    (e.g. PK='reading', SK=date) and use Query.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    logger.info("Scanning DynamoDB for rows since %s (last %d days)", cutoff, days)
    try:
        resp = _table().scan(FilterExpression=Attr("date").gte(cutoff))
        items = resp.get("Items", [])
        # Paginate if needed
        while "LastEvaluatedKey" in resp:
            resp = _table().scan(
                FilterExpression=Attr("date").gte(cutoff),
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items.extend(resp.get("Items", []))
        items.sort(key=lambda r: r.get("date", ""))
        logger.info("DynamoDB scan returned %d rows", len(items))
        return [_normalize(i) for i in items]
    except (BotoCoreError, ClientError) as exc:
        logger.exception("DynamoDB scan failed: %s", exc)
        return []


def _latest_record():
    """Get the single most recent row by scanning + sorting. Same rationale
    as _scan_recent: tiny table, simple PK design."""
    logger.info("Fetching latest record from DynamoDB")
    try:
        resp = _table().scan()
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = _table().scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        if not items:
            logger.warning("DynamoDB scan returned zero rows")
            return None
        items.sort(key=lambda r: r.get("date", ""))
        latest = _normalize(items[-1])
        logger.info("Latest record date=%s score=%s",
                    latest.get("date"), latest.get("risk_score"))
        return latest
    except (BotoCoreError, ClientError) as exc:
        logger.exception("DynamoDB scan failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", cors=True)
def index():
    """Zone apex — DP3 contract. Must return about + resources."""
    logger.info("GET / called")
    return {
        "about": (
            "Tracks key Federal Reserve macro indicators daily and computes a "
            "composite recession risk score (0-100) for non-technical stakeholders."
        ),
        "resources": ["current", "trend", "plot", "indicators"],
    }


@app.route("/current", cors=True)
def current():
    logger.info("GET /current called")
    rec = _latest_record()
    if rec is None:
        return {"response": "No data yet — the ingest pipeline hasn't written a record."}

    score = rec.get("risk_score")
    label = _severity_label(score)
    flags = rec.get("flags", {}) or {}

    # Human-readable reason snippets — only mention triggered flags
    reasons = []
    if flags.get("yield_curve_inverted"):
        reasons.append("yield curve inverted")
    if flags.get("unemployment_rising_3mo"):
        reasons.append("unemployment rising")
    if flags.get("cpi_above_4pct"):
        reasons.append("inflation hot")
    if flags.get("consumer_sentiment_weak"):
        reasons.append("consumer sentiment weak")
    if flags.get("fed_funds_restrictive"):
        reasons.append("Fed funds restrictive")
    if flags.get("recprob_elevated"):
        reasons.append("NY Fed recession prob elevated")

    reason_str = (", ".join(reasons).capitalize() + ".") if reasons else "No major flags."
    msg = f"Recession risk is {score}/100 ({label}) as of {rec.get('date')}. {reason_str}"
    logger.info("GET /current response: %s", msg)
    return {"response": msg}


@app.route("/trend", cors=True)
def trend():
    logger.info("GET /trend called")
    rows = _scan_recent(days=30)
    scores = [r.get("risk_score") for r in rows if r.get("risk_score") is not None]
    if not scores:
        return {"response": "No data in the last 30 days."}

    avg = sum(scores) / len(scores)
    lo, hi = min(scores), max(scores)

    # Direction: compare first vs last in window. Stable if delta < 5.
    delta = scores[-1] - scores[0]
    if delta > 5:
        direction = "Rising"
    elif delta < -5:
        direction = "Falling"
    else:
        direction = "Stable"

    msg = f"30-day avg risk: {avg:.0f}/100. Trend: {direction}. Range: {lo}-{hi}."
    logger.info("GET /trend response: %s (n=%d)", msg, len(scores))
    return {"response": msg}


@app.route("/plot", cors=True)
def plot():
    logger.info("GET /plot called")
    rows = _scan_recent(days=90)
    if not rows:
        return {"response": "No data available to plot."}

    try:
        dates = [datetime.strptime(r["date"], "%Y-%m-%d") for r in rows]
        scores = [r.get("risk_score") for r in rows]
        spreads = [r.get("t10y2y") for r in rows]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

        # ---- Top: risk score, points colored by severity tier ----
        ax1.plot(dates, scores, color="#444", linewidth=1.2, zorder=1)
        for d, s in zip(dates, scores):
            ax1.scatter([d], [s], color=_severity_color(s), s=36,
                        edgecolor="white", linewidth=0.5, zorder=2)
        ax1.set_ylabel("Risk Score (0-100)")
        ax1.set_title("Recession Risk Score — 90 days")
        ax1.set_ylim(0, 100)
        ax1.axhspan(0, 25, color="#2ecc71", alpha=0.07)
        ax1.axhspan(25, 50, color="#f1c40f", alpha=0.07)
        ax1.axhspan(50, 75, color="#e67e22", alpha=0.07)
        ax1.axhspan(75, 100, color="#e74c3c", alpha=0.07)
        ax1.grid(True, linestyle=":", alpha=0.5)

        # ---- Bottom: T10Y2Y spread with 0 line ----
        ax2.plot(dates, spreads, color="#2c3e50", linewidth=1.5)
        ax2.axhline(y=0, color="red", linestyle="--", linewidth=1)
        ax2.set_ylabel("10y-2y Spread (%)")
        ax2.set_xlabel("Date")
        ax2.set_title("Treasury Yield Curve (T10Y2Y)")
        ax2.grid(True, linestyle=":", alpha=0.5)

        ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        fig.autofmt_xdate()
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=110)
        plt.close(fig)
        buf.seek(0)

        logger.info("Uploading plot to s3://%s/%s", S3_BUCKET, PLOT_KEY)
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=PLOT_KEY,
            Body=buf.getvalue(),
            ContentType="image/png",
            ACL="public-read",
        )
        url = f"https://{S3_BUCKET}.s3.amazonaws.com/{PLOT_KEY}"
        logger.info("Plot uploaded successfully: %s", url)
        return {"response": url}

    except Exception as exc:
        logger.exception("Plot generation/upload failed: %s", exc)
        return {"response": f"Plot temporarily unavailable: {type(exc).__name__}"}


@app.route("/indicators", cors=True)
def indicators():
    """Per-series traffic-light view of the latest values.

    Status thresholds mirror the scoring rules from Part 1, just expressed as
    green / yellow / red bands so a non-technical reader can scan the dashboard.
    """
    logger.info("GET /indicators called")
    rec = _latest_record()
    if rec is None:
        return {"response": "No data yet — the ingest pipeline hasn't written a record."}

    def cls(value, *, green_if, red_if, label_green, label_yellow, label_red):
        """Helper: classify a value into red/yellow/green via two callables."""
        if value is None:
            return {"value": None, "status": "gray", "label": "Unknown"}
        if red_if(value):
            return {"value": value, "status": "red", "label": label_red}
        if green_if(value):
            return {"value": value, "status": "green", "label": label_green}
        return {"value": value, "status": "yellow", "label": label_yellow}

    out = {
        "yield_curve": cls(
            rec.get("t10y2y"),
            green_if=lambda v: v >= 0.5,
            red_if=lambda v: v < 0,
            label_green="Normal", label_yellow="Flattening", label_red="Inverted",
        ),
        "unemployment": cls(
            rec.get("unrate"),
            green_if=lambda v: v < 4.0,
            red_if=lambda v: v > 5.0,
            label_green="Healthy", label_yellow="Watching", label_red="Rising",
        ),
        "cpi": cls(
            rec.get("cpi_yoy_pct"),
            green_if=lambda v: v < 2.5,
            red_if=lambda v: v > 4.0,
            label_green="On target", label_yellow="Elevated", label_red="Hot",
        ),
        "consumer_sentiment": cls(
            rec.get("umcsent"),
            green_if=lambda v: v >= 85,
            red_if=lambda v: v < 70,
            label_green="Strong", label_yellow="Soft", label_red="Weak",
        ),
        "fed_funds": cls(
            rec.get("fedfunds"),
            green_if=lambda v: v < 2.5,
            red_if=lambda v: v > 4.0,
            label_green="Accommodative", label_yellow="Neutral", label_red="Restrictive",
        ),
        "recprob": cls(
            rec.get("recprob"),
            green_if=lambda v: v < 10,
            red_if=lambda v: v > 25,
            label_green="Low", label_yellow="Moderate", label_red="Elevated",
        ),
    }

    logger.info("GET /indicators response built for %d series", len(out))
    return {"response": out}
