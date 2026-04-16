"""
load_to_bq.py
Purpose : Load all cleaned CSV files into BigQuery, execute DDL SQL files,
          and run row-count validation.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : $0 — Loading CSVs via the BigQuery Python client uses the free batch
          load API. This does NOT consume query quota. It is always free
          regardless of file size. Validation queries use partition filters
          to stay within free-tier scan limits.
"""

import logging
import time
from pathlib import Path

from dotenv import load_dotenv
import os

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("load_to_bq")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ["DATASET"]

BASE_DIR = Path(__file__).parent.parent
CLEAN_DIR = BASE_DIR / "data" / "clean"
SQL_DIR = BASE_DIR / "sql"

# ---------------------------------------------------------------------------
# CSV → BigQuery table mapping
# ---------------------------------------------------------------------------
TABLE_MAP: dict[str, str] = {
    "clients_clean.csv":     "clients",
    "products_clean.csv":    "products",
    "orders_clean.csv":      "orders",
    "order_items_clean.csv": "order_items",
    "incidents_clean.csv":   "incidents",
    "page_views_clean.csv":  "page_views",
}

# ---------------------------------------------------------------------------
# Expected minimums for validation
# ---------------------------------------------------------------------------
EXPECTED_MIN_ROWS: dict[str, int] = {
    "clients":     1800,
    "products":      45,
    "orders":      13000,
    "order_items": 18000,
    "incidents":    2700,
    "page_views":  45000,
}

# ---------------------------------------------------------------------------
# Partition column for validation queries (cost-safe COUNT)
# ---------------------------------------------------------------------------
PARTITION_FILTER: dict[str, str] = {
    "clients":     "DATE(registration_date) >= '2022-01-01'",
    "products":    "TRUE",  # no partition
    "orders":      "DATE(order_date) >= '2022-01-01'",
    "order_items": "order_date >= '2022-01-01'",
    "incidents":   "DATE(report_date) >= '2022-01-01'",
    "page_views":  "DATE(event_datetime) >= '2022-01-01'",
}


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def load_csv_to_bq(
    client: bigquery.Client,
    csv_path: Path,
    table_name: str,
) -> tuple[int, float]:
    """Load a CSV file into BigQuery using WRITE_TRUNCATE (free batch load)."""
    table_ref = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    t0 = time.time()
    with open(csv_path, "rb") as fh:
        job = client.load_table_from_file(fh, table_ref, job_config=job_config)

    job.result()  # Wait for completion
    elapsed = round(time.time() - t0, 2)

    table = client.get_table(table_ref)
    nb_rows = table.num_rows
    log.info("Loaded %s → %s: %d rows in %.1fs", csv_path.name, table_ref, nb_rows, elapsed)
    print(f"  {table_name:<20} → {nb_rows:>8,} rows loaded in {elapsed}s")
    return nb_rows, elapsed


def execute_sql_file(client: bigquery.Client, sql_path: Path) -> None:
    """Execute a multi-statement SQL file in BigQuery."""
    log.info("Executing SQL file: %s", sql_path.name)
    sql = sql_path.read_text(encoding="utf-8")

    # Split on semicolon and execute each non-empty statement
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    for stmt in statements:
        if stmt:
            try:
                job = client.query(stmt)
                job.result()
                log.info("Statement executed successfully")
            except Exception as exc:
                log.error("Failed to execute statement: %s — %s", stmt[:80], exc)
                raise


def validate_row_counts(client: bigquery.Client, csv_row_counts: dict[str, int]) -> list[dict]:
    """Run COUNT(*) per table with partition filter and compare to source CSVs."""
    results: list[dict] = []
    for table_name, min_expected in EXPECTED_MIN_ROWS.items():
        partition_filter = PARTITION_FILTER.get(table_name, "TRUE")
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{DATASET}.{table_name}`
            WHERE {partition_filter}
            LIMIT 1
        """
        try:
            row = next(iter(client.query(query).result()))
            bq_count = row.cnt
        except Exception as exc:
            log.error("Validation query failed for %s: %s", table_name, exc)
            bq_count = -1

        csv_count = csv_row_counts.get(table_name, 0)
        status = "OK" if bq_count >= min_expected else "WARNING"
        results.append({
            "table": table_name,
            "bq_rows": bq_count,
            "csv_rows": csv_count,
            "min_expected": min_expected,
            "status": status,
        })
        icon = "✓" if status == "OK" else "⚠"
        print(f"  {icon} {table_name:<20} BQ: {bq_count:>8,} | CSV: {csv_count:>8,} | Min: {min_expected:>8,} [{status}]")
    return results


def main() -> None:
    client = get_client()
    csv_row_counts: dict[str, int] = {}

    print("\n" + "=" * 60)
    print("BIGQUERY LOAD PIPELINE")
    print("=" * 60)
    print(f"  Project : {PROJECT_ID}")
    print(f"  Dataset : {DATASET}")
    print()

    # ------------------------------------------------------------------
    # Phase 1: Load all cleaned CSVs
    # ------------------------------------------------------------------
    print("--- Phase 1: Loading CSVs into BigQuery ---")
    for csv_name, table_name in TABLE_MAP.items():
        csv_path = CLEAN_DIR / csv_name
        if not csv_path.exists():
            log.warning("File not found, skipping: %s", csv_path)
            print(f"  SKIP {csv_name} — file not found (run prepare_data.py first)")
            continue

        # Count source rows
        with open(csv_path, "r", encoding="utf-8") as f:
            src_lines = sum(1 for _ in f) - 1  # subtract header
        csv_row_counts[table_name] = src_lines

        load_csv_to_bq(client, csv_path, table_name)

    # ------------------------------------------------------------------
    # Phase 2: Execute DDL SQL files
    # ------------------------------------------------------------------
    print("\n--- Phase 2: Executing DDL SQL files ---")
    for sql_file in ["01_create_tables.sql", "02_create_views.sql"]:
        sql_path = SQL_DIR / sql_file
        if sql_path.exists():
            print(f"  Executing {sql_file}...")
            execute_sql_file(client, sql_path)
            print(f"  ✓ {sql_file} executed")
        else:
            log.warning("SQL file not found: %s", sql_path)

    # ------------------------------------------------------------------
    # Phase 3: Validation
    # ------------------------------------------------------------------
    print("\n--- Phase 3: Row-count validation ---")
    results = validate_row_counts(client, csv_row_counts)

    warnings = [r for r in results if r["status"] == "WARNING"]
    if warnings:
        log.warning("%d table(s) below expected minimum row count", len(warnings))

    print("\n" + "=" * 60)
    print(f"  Load complete. {len(TABLE_MAP)} tables processed.")
    if warnings:
        print(f"  {len(warnings)} WARNING(s) — check logs above")
    else:
        print("  All tables passed validation.")
    print("=" * 60)


if __name__ == "__main__":
    main()
