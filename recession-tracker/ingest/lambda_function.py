"""
Recession Risk Tracker — Ingest Lambda (Part 1)

Triggered daily by EventBridge. Pulls 6 macro indicators from FRED, computes a
rule-based composite recession risk score (0–100), and writes one record per day
to DynamoDB. Idempotent: re-running on the same day overwrites that day's row.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fredapi import Fred

# ---------------------------------------------------------------------------
# Logging — module level so Lambda's runtime captures it cleanly in CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Config (env vars only — no hardcoded secrets)
# ---------------------------------------------------------------------------
FRED_API_KEY = os.environ.get("FRED_API_KEY")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "recession-tracker")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# FRED series we track. Cadence varies (daily/weekly/monthly); we always take
# the most recent observation and treat it as "today's known value."
SERIES = {
    "t10y2y": "T10Y2Y",         # 10yr-2yr Treasury yield spread (daily)
    "unrate": "UNRATE",         # Unemployment rate (monthly)
    "cpiaucsl": "CPIAUCSL",     # CPI all urban consumers (monthly)
    "umcsent": "UMCSENT",       # U Michigan Consumer Sentiment (monthly)
    "fedfunds": "FEDFUNDS",     # Federal funds effective rate (monthly)
    "recprob": "RECPROB",       # NY Fed recession probability (monthly)
    "vix": "VIXCLS",            # CBOE Volatility Index — "fear gauge" (daily)
}


def _to_decimal(value):
    """DynamoDB doesn't accept float — convert via str to avoid binary FP noise."""
    if value is None:
        return None
    return Decimal(str(value))


def fetch_series(fred, series_id):
    """Fetch the most recent observation for a single FRED series.

    Returns the latest value as a float, or None if the call fails or the
    series returns nothing. We never let one bad series take down the run.
    """
    logger.info("Fetching FRED series %s", series_id)
    try:
        data = fred.get_series(series_id)
        if data is None or len(data) == 0:
            logger.warning("Series %s returned no observations", series_id)
            return None
        # fredapi returns a pandas Series indexed by date; drop NaN tail values
        # that FRED sometimes returns for not-yet-released months.
        clean = data.dropna()
        if len(clean) == 0:
            logger.warning("Series %s had only NaN observations", series_id)
            return None
        latest = float(clean.iloc[-1])
        logger.info("Series %s latest value=%s (as of %s)",
                    series_id, latest, clean.index[-1].strftime("%Y-%m-%d"))
        return latest
    except Exception as exc:  # broad on purpose: one bad series shouldn't crash run
        logger.exception("Failed to fetch series %s: %s", series_id, exc)
        return None


def fetch_unrate_history(fred, n=4):
    """Pull the last n monthly unemployment observations so we can detect a
    3-consecutive-month rise. Returns a list of floats oldest-to-newest, or []."""
    logger.info("Fetching unemployment history for trend check (n=%d)", n)
    try:
        data = fred.get_series("UNRATE").dropna()
        history = [float(x) for x in data.iloc[-n:].tolist()]
        logger.info("Unemployment history (last %d): %s", n, history)
        return history
    except Exception as exc:
        logger.exception("Failed to fetch UNRATE history: %s", exc)
        return []


def compute_risk_score(values, unrate_history):
    """Apply the rule-based scoring from the spec. Returns (score, flags_dict)."""
    logger.info("Computing risk score from values=%s", values)
    flags = {}
    score = 0

    t10y2y = values.get("t10y2y")
    if t10y2y is not None and t10y2y < 0:
        score += 30
        flags["yield_curve_inverted"] = True

    # Three consecutive monthly rises: u[-1] > u[-2] > u[-3]
    if len(unrate_history) >= 3:
        u3, u2, u1 = unrate_history[-3], unrate_history[-2], unrate_history[-1]
        if u1 > u2 > u3:
            score += 20
            flags["unemployment_rising_3mo"] = True

    cpi = values.get("cpiaucsl")
    # CPIAUCSL is an index, not a percent. We approximate YoY % change to compare
    # against the spec's 4% threshold. A more precise version would pull a 13-month
    # window and compute (latest - year_ago) / year_ago * 100, but for a daily
    # snapshot this rule still triggers correctly when inflation is hot.
    cpi_yoy = values.get("cpi_yoy_pct")
    if cpi_yoy is not None and cpi_yoy > 4:
        score += 15
        flags["cpi_above_4pct"] = True

    umcsent = values.get("umcsent")
    if umcsent is not None and umcsent < 70:
        score += 15
        flags["consumer_sentiment_weak"] = True

    fedfunds = values.get("fedfunds")
    if fedfunds is not None and fedfunds > 4:
        score += 10
        flags["fed_funds_restrictive"] = True

    recprob = values.get("recprob")
    if recprob is not None and recprob > 25:
        score += 10
        flags["recprob_elevated"] = True

    # VIX > 25 = mild market stress, > 35 = significant stress
    vix = values.get("vix")
    if vix is not None:
        if vix > 35:
            score += 10
            flags["vix_high_stress"] = True
        elif vix > 25:
            score += 5
            flags["vix_elevated"] = True

    score = min(score, 100)
    logger.info("Computed risk score=%d, flags=%s", score, flags)
    return score, flags


def compute_cpi_yoy(fred):
    """Compute CPI year-over-year % change from the last ~13 monthly observations."""
    logger.info("Computing CPI YoY % change")
    try:
        data = fred.get_series("CPIAUCSL").dropna()
        if len(data) < 13:
            logger.warning("Not enough CPI history for YoY (have %d months)", len(data))
            return None
        latest = float(data.iloc[-1])
        year_ago = float(data.iloc[-13])
        yoy = ((latest - year_ago) / year_ago) * 100.0
        logger.info("CPI YoY: latest=%s year_ago=%s yoy=%.2f%%", latest, year_ago, yoy)
        return yoy
    except Exception as exc:
        logger.exception("Failed to compute CPI YoY: %s", exc)
        return None


def write_to_dynamo(table, record):
    """Idempotent upsert — PutItem on a primary key date overwrites prior row."""
    logger.info("Writing record to DynamoDB (date=%s, score=%s)",
                record.get("date"), record.get("risk_score"))
    try:
        table.put_item(Item=record)
        logger.info("DynamoDB write succeeded for date=%s", record["date"])
        return True
    except (BotoCoreError, ClientError) as exc:
        logger.exception("DynamoDB write failed: %s", exc)
        return False


def lambda_handler(event, context):
    """Entry point. Event payload is unused — fires on EventBridge schedule."""
    logger.info("=== Recession tracker ingest run START ===")
    logger.info("Event: %s", json.dumps(event) if event else "{}")

    if not FRED_API_KEY:
        logger.error("FRED_API_KEY not set — aborting")
        return {"status": "error", "message": "FRED_API_KEY not set"}

    # ---- Fetch all series ----
    try:
        fred = Fred(api_key=FRED_API_KEY)
    except Exception as exc:
        logger.exception("Failed to initialize FRED client: %s", exc)
        return {"status": "error", "message": str(exc)}

    values = {}
    for attr_name, series_id in SERIES.items():
        values[attr_name] = fetch_series(fred, series_id)

    values["cpi_yoy_pct"] = compute_cpi_yoy(fred)
    unrate_history = fetch_unrate_history(fred, n=4)

    # ---- Score ----
    score, flags = compute_risk_score(values, unrate_history)

    # ---- Build record ----
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    record = {
        "date": today,
        "timestamp": int(time.time()),
        "t10y2y": _to_decimal(values.get("t10y2y")),
        "unrate": _to_decimal(values.get("unrate")),
        "cpiaucsl": _to_decimal(values.get("cpiaucsl")),
        "cpi_yoy_pct": _to_decimal(values.get("cpi_yoy_pct")),
        "umcsent": _to_decimal(values.get("umcsent")),
        "fedfunds": _to_decimal(values.get("fedfunds")),
        "recprob": _to_decimal(values.get("recprob")),
        "vix": _to_decimal(values.get("vix")),
        "risk_score": score,
        "flags": flags,
    }
    # Strip None values — DynamoDB rejects them
    record = {k: v for k, v in record.items() if v is not None}

    # ---- Persist ----
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
    except Exception as exc:
        logger.exception("Failed to get DynamoDB table handle: %s", exc)
        return {"status": "error", "message": str(exc)}

    ok = write_to_dynamo(table, record)

    logger.info("=== Ingest run END (date=%s, score=%s, written=%s) ===",
                today, score, ok)

    return {
        "status": "ok" if ok else "partial",
        "date": today,
        "risk_score": score,
        "flags": flags,
    }
