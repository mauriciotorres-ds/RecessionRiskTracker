"""
One-time backfill — populate DynamoDB with 12+ months of recession-risk history.

For each calendar day in the lookback window, this script reconstructs what the
ingest Lambda would have written if it had been running back then: it grabs the
most recent FRED observation of each series as of that day, recomputes the
composite risk score using the same rules, and PutItems one row per day.

Run locally (not in Lambda):
    export FRED_API_KEY=your_key_here
    python backfill.py

Optional env vars:
    BACKFILL_DAYS=400           # how far back to backfill (default 400)
    DYNAMODB_TABLE=...          # default 'recession-tracker'
    AWS_REGION=us-east-1
"""

import logging
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from fredapi import Fred

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger()

FRED_API_KEY = os.environ.get("FRED_API_KEY")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "recession-tracker")
REGION = os.environ.get("AWS_REGION", "us-east-1")
DAYS_BACK = int(os.environ.get("BACKFILL_DAYS", "400"))

SERIES = {
    "t10y2y": "T10Y2Y",
    "unrate": "UNRATE",
    "cpiaucsl": "CPIAUCSL",
    "umcsent": "UMCSENT",
    "fedfunds": "FEDFUNDS",
    "recprob": "RECPROB",
    "vix": "VIXCLS",
}


def to_dec(v):
    return Decimal(str(v)) if v is not None else None


def compute_score(values, unrate_hist):
    """Same scoring rules as the live ingest Lambda."""
    score = 0
    flags = {}
    if values.get("t10y2y") is not None and values["t10y2y"] < 0:
        score += 30
        flags["yield_curve_inverted"] = True
    if len(unrate_hist) >= 3:
        u3, u2, u1 = unrate_hist[-3], unrate_hist[-2], unrate_hist[-1]
        if u1 > u2 > u3:
            score += 20
            flags["unemployment_rising_3mo"] = True
    if values.get("cpi_yoy_pct") is not None and values["cpi_yoy_pct"] > 4:
        score += 15
        flags["cpi_above_4pct"] = True
    if values.get("umcsent") is not None and values["umcsent"] < 70:
        score += 15
        flags["consumer_sentiment_weak"] = True
    if values.get("fedfunds") is not None and values["fedfunds"] > 4:
        score += 10
        flags["fed_funds_restrictive"] = True
    if values.get("recprob") is not None and values["recprob"] > 25:
        score += 10
        flags["recprob_elevated"] = True
    vix = values.get("vix")
    if vix is not None:
        if vix > 35:
            score += 10
            flags["vix_high_stress"] = True
        elif vix > 25:
            score += 5
            flags["vix_elevated"] = True
    return min(score, 100), flags


def main():
    if not FRED_API_KEY:
        raise SystemExit("FRED_API_KEY env var not set. Run: export FRED_API_KEY=your_key")

    log.info("=== Backfill START — %d days, table=%s ===", DAYS_BACK, TABLE_NAME)

    fred = Fred(api_key=FRED_API_KEY)

    log.info("Fetching all FRED series (need ~18 months for CPI YoY computation)")
    series_data = {}
    for attr, sid in SERIES.items():
        try:
            data = fred.get_series(sid).dropna()
            series_data[attr] = data
            log.info("  %s: %d obs, %s -> %s", sid, len(data),
                     data.index[0].date(), data.index[-1].date())
        except Exception as exc:
            log.exception("Failed to fetch %s: %s", sid, exc)
            series_data[attr] = None

    cpi_series = series_data.get("cpiaucsl")
    unrate_series = series_data.get("unrate")

    table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

    today = datetime.utcnow().date()
    start = today - timedelta(days=DAYS_BACK)
    written = 0

    log.info("Backfilling %s through %s", start, today)

    cur = start
    while cur <= today:
        cur_ts = datetime.combine(cur, datetime.min.time())

        # Latest value of each series as of `cur`
        values = {}
        for attr, data in series_data.items():
            if data is None or len(data) == 0:
                values[attr] = None
                continue
            mask = data.index <= cur_ts
            values[attr] = float(data[mask].iloc[-1]) if mask.any() else None

        # CPI YoY as of `cur`
        cpi_yoy = None
        if cpi_series is not None:
            mask = cpi_series.index <= cur_ts
            sub = cpi_series[mask] if mask.any() else None
            if sub is not None and len(sub) >= 13:
                latest = float(sub.iloc[-1])
                year_ago = float(sub.iloc[-13])
                cpi_yoy = ((latest - year_ago) / year_ago) * 100.0
        values["cpi_yoy_pct"] = cpi_yoy

        # Unemployment trend window (last 4 monthly observations)
        unrate_hist = []
        if unrate_series is not None:
            mask = unrate_series.index <= cur_ts
            if mask.any():
                unrate_hist = [float(x) for x in unrate_series[mask].iloc[-4:].tolist()]

        score, flags = compute_score(values, unrate_hist)

        date_str = cur.strftime("%Y-%m-%d")
        record = {
            "date": date_str,
            "timestamp": int(time.mktime(cur.timetuple())),
            "t10y2y": to_dec(values.get("t10y2y")),
            "unrate": to_dec(values.get("unrate")),
            "cpiaucsl": to_dec(values.get("cpiaucsl")),
            "cpi_yoy_pct": to_dec(values.get("cpi_yoy_pct")),
            "umcsent": to_dec(values.get("umcsent")),
            "fedfunds": to_dec(values.get("fedfunds")),
            "recprob": to_dec(values.get("recprob")),
            "vix": to_dec(values.get("vix")),
            "risk_score": score,
            "flags": flags,
        }
        record = {k: v for k, v in record.items() if v is not None}

        try:
            table.put_item(Item=record)
            written += 1
            if written % 30 == 0:
                log.info("  ... wrote %d rows (latest: %s, score=%d, flags=%s)",
                         written, date_str, score, list(flags.keys()))
        except Exception as exc:
            log.exception("Write failed for %s: %s", date_str, exc)

        cur += timedelta(days=1)

    log.info("=== Backfill DONE — wrote %d rows ===", written)


if __name__ == "__main__":
    main()
