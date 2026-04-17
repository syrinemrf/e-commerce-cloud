"""
monitoring/health_check.py
Purpose : Lightweight pipeline health check using only BigQuery partition-filtered
          queries and gcloud CLI. No Cloud Monitoring API calls (which can incur cost).
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : This script uses only gcloud CLI and partition-filtered BQ queries.
          No Cloud Monitoring API is called. Estimated query cost: < 1 MB scanned = $0.
"""

import json
import logging
import os
import subprocess
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("health_check")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ.get("DATASET", "ecommerce_analytics")
PUBSUB_SUB = os.environ.get("PUBSUB_SUB", "orders-sub")

# ---------------------------------------------------------------------------
# Expected minimum row counts per table (validation thresholds)
# ---------------------------------------------------------------------------
TABLE_THRESHOLDS: dict[str, int] = {
    "clients":     1800,
    "products":      45,
    "orders":      13000,
    "order_items": 18000,
    "incidents":    2700,
    "page_views":  45000,
}

# Partition filters to avoid full table scans (cost-safe)
PARTITION_FILTERS: dict[str, str] = {
    "clients":     "DATE(registration_date) >= '2022-01-01'",
    "products":    "TRUE",
    "orders":      "DATE(order_date) >= '2022-01-01'",
    "order_items": "TRUE",  # no date column in order_items
    "incidents":   "DATE(report_date) >= '2022-01-01'",
    "page_views":  "DATE(event_datetime) >= '2022-01-01'",
}

STATUS_OK      = "✅ OK"
STATUS_WARN    = "⚠️  WARN"
STATUS_ERROR   = "❌ ERROR"


def check_bq_row_counts(client: bigquery.Client) -> list[dict]:
    """Check row counts per table with partition filters."""
    results: list[dict] = []
    for table, threshold in TABLE_THRESHOLDS.items():
        pfilter = PARTITION_FILTERS.get(table, "TRUE")
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{DATASET}.{table}`
            WHERE {pfilter}
            LIMIT 1
        """
        try:
            row = next(iter(client.query(query).result()))
            count = row.cnt
            status = STATUS_OK if count >= threshold else STATUS_WARN
            results.append({"component": f"BQ:{table}", "count": count, "threshold": threshold, "status": status})
            log.info("BQ table %s: %d rows (threshold: %d) → %s", table, count, threshold, status)
        except Exception as exc:
            results.append({"component": f"BQ:{table}", "count": -1, "threshold": threshold, "status": STATUS_ERROR, "error": str(exc)})
            log.error("BQ check failed for %s: %s", table, exc)
    return results


def check_pubsub_backlog() -> dict:
    """Check Pub/Sub subscription backlog via gcloud CLI."""
    try:
        result = subprocess.run(
            f"gcloud pubsub subscriptions describe {PUBSUB_SUB} --project={PROJECT_ID} --format=value(num_undelivered_messages)",
            capture_output=True,
            text=True,
            timeout=30,
            shell=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
        backlog_str = result.stdout.strip()
        backlog = int(backlog_str) if backlog_str.isdigit() else 0
        status = STATUS_WARN if backlog > 1000 else STATUS_OK
        log.info("Pub/Sub backlog for %s: %d messages → %s", PUBSUB_SUB, backlog, status)
        return {"component": "PubSub:backlog", "backlog": backlog, "status": status}
    except Exception as exc:
        log.error("Pub/Sub check failed: %s", exc)
        return {"component": "PubSub:backlog", "backlog": -1, "status": STATUS_ERROR, "error": str(exc)}


def check_function_errors() -> dict:
    """Check last 5 Cloud Function errors via Cloud Logging."""
    try:
        result = subprocess.run(
            f'gcloud logging read "resource.type=cloud_function severity>=ERROR" --project={PROJECT_ID} --limit=5 --format=json',
            capture_output=True,
            text=True,
            timeout=30,
            shell=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
        entries = json.loads(result.stdout) if result.stdout.strip() else []
        nb_errors = len(entries)
        status = STATUS_WARN if nb_errors > 0 else STATUS_OK
        log.info("Cloud Function recent errors: %d → %s", nb_errors, status)
        return {"component": "Function:errors", "recent_errors": nb_errors, "status": status}
    except Exception as exc:
        log.error("Function log check failed: %s", exc)
        return {"component": "Function:errors", "recent_errors": -1, "status": STATUS_ERROR, "error": str(exc)}


def print_report(results: list[dict]) -> None:
    now = datetime.now(tz=timezone.utc).isoformat()
    print()
    print("=" * 65)
    print(f"  PIPELINE HEALTH CHECK — {now}")
    print("=" * 65)
    for r in results:
        status = r.get("status", STATUS_ERROR)
        component = r.get("component", "unknown")
        detail = ""
        if "count" in r:
            detail = f"  rows={r['count']:,} (min={r['threshold']:,})"
        elif "backlog" in r:
            detail = f"  backlog={r['backlog']:,} messages"
        elif "recent_errors" in r:
            detail = f"  recent_errors={r['recent_errors']}"
        if "error" in r:
            detail += f"  [ERR: {r['error'][:60]}]"
        print(f"  {status}  {component:<30}{detail}")
    print("=" * 65)

    errors = [r for r in results if r["status"] == STATUS_ERROR]
    warnings = [r for r in results if r["status"] == STATUS_WARN]
    if errors:
        print(f"\n  {len(errors)} component(s) in ERROR state — check logs above")
    elif warnings:
        print(f"\n  {len(warnings)} component(s) in WARNING state — review thresholds")
    else:
        print("\n  All components healthy.")
    print()


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    all_results: list[dict] = []

    # 1. BigQuery row counts
    all_results.extend(check_bq_row_counts(client))

    # 2. Pub/Sub backlog
    all_results.append(check_pubsub_backlog())

    # 3. Cloud Function error log
    all_results.append(check_function_errors())

    print_report(all_results)


if __name__ == "__main__":
    main()
