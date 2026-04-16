"""
functions/process_upload/main.py
Purpose : Cloud Function gen2 triggered on GCS object finalization.
          Downloads new CSV, cleans it, appends to BigQuery, and publishes
          a confirmation to Pub/Sub.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : Cloud Functions gen2 free tier = 2M invocations/month.
          This function is only triggered on file upload, not on a schedule.
          Expected invocations: < 100/month for this project → always free.
          Memory: 256 MB (not 512 — saves quota). Timeout: 120s. Max-instances: 3.
"""

import json
import logging
import os
import time
from datetime import datetime
from io import StringIO

import functions_framework
import pandas as pd
from google.cloud import bigquery, pubsub_v1, storage

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("process_upload")

# ---------------------------------------------------------------------------
# Configuration from environment variables (set at deploy time)
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "ecommerce-pipeline-493520")
DATASET = os.environ.get("DATASET", "ecommerce_analytics")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "orders-realtime")
PUBSUB_TOPIC_DLQ = os.environ.get("PUBSUB_TOPIC_DLQ", "orders-realtime-dlq")

# ---------------------------------------------------------------------------
# File → BigQuery table mapping
# ---------------------------------------------------------------------------
FILE_TABLE_MAP: dict[str, str] = {
    "clients_clean.csv":     "clients",
    "products_clean.csv":    "products",
    "orders_clean.csv":      "orders",
    "order_items_clean.csv": "order_items",
    "incidents_clean.csv":   "incidents",
    "page_views_clean.csv":  "page_views",
}

# Key columns that must not be NULL (basic cleaning)
TABLE_KEY_COLS: dict[str, list[str]] = {
    "clients":     ["client_id"],
    "products":    ["product_id"],
    "orders":      ["order_id", "client_id"],
    "order_items": ["item_id", "order_id"],
    "incidents":   ["incident_id", "client_id"],
    "page_views":  ["session_id"],
}


def _publish(topic_id: str, message: dict) -> None:
    """Publish a JSON message to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, topic_id)
    data = json.dumps(message).encode("utf-8")
    publisher.publish(topic_path, data=data)
    log.info("Published to topic %s", topic_id)


def _clean_df(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Minimal cleaning: drop nulls on key columns, deduplicate."""
    before = len(df)
    df = df.drop_duplicates()
    key_cols = [c for c in TABLE_KEY_COLS.get(table_name, []) if c in df.columns]
    if key_cols:
        df = df.dropna(subset=key_cols)
    log.info("Cleaned %s: %d → %d rows", table_name, before, len(df))
    return df


@functions_framework.cloud_event
def process_upload(cloud_event) -> None:
    """
    Cloud Function entry point.
    Triggered on GCS object finalize event.
    """
    t0 = time.time()
    data = cloud_event.data

    bucket_name: str = data.get("bucket", "")
    object_name: str = data.get("name", "")
    filename = object_name.split("/")[-1]

    log.info("GCS event received: gs://%s/%s", bucket_name, object_name)

    # ------------------------------------------------------------------
    # Map filename to BigQuery table
    # ------------------------------------------------------------------
    table_name = FILE_TABLE_MAP.get(filename)
    if not table_name:
        log.warning("Unknown file type '%s' — skipping", filename)
        return  # Return 200 implicitly via functions_framework

    table_ref = f"{PROJECT_ID}.{DATASET}.{table_name}"
    log.info("Mapping %s → %s", filename, table_ref)

    # ------------------------------------------------------------------
    # Download from GCS into memory (no /tmp write)
    # ------------------------------------------------------------------
    try:
        gcs_client = storage.Client(project=PROJECT_ID)
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        csv_content = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(csv_content), low_memory=False)
        log.info("Downloaded %s: %d rows, %d cols", filename, len(df), len(df.columns))
    except Exception as exc:
        log.error("Failed to download %s: %s", object_name, exc)
        _publish(PUBSUB_TOPIC_DLQ, {
            "filename": filename,
            "table": table_name,
            "error": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
            "status": "download_error",
        })
        return

    # ------------------------------------------------------------------
    # Clean
    # ------------------------------------------------------------------
    df = _clean_df(df, table_name)

    # ------------------------------------------------------------------
    # Insert into BigQuery with WRITE_APPEND
    # ------------------------------------------------------------------
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=0,  # df already parsed
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        nb_rows = len(df)
        log.info("Inserted %d rows into %s", nb_rows, table_ref)
    except Exception as exc:
        log.error("BigQuery insert failed for %s: %s", table_ref, exc)
        _publish(PUBSUB_TOPIC_DLQ, {
            "filename": filename,
            "table": table_name,
            "error": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
            "status": "bq_error",
        })
        return

    # ------------------------------------------------------------------
    # Publish confirmation to main Pub/Sub topic
    # ------------------------------------------------------------------
    processing_time_ms = int((time.time() - t0) * 1000)
    confirmation = {
        "filename": filename,
        "table": table_name,
        "nb_rows_inserted": nb_rows,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "success",
        "processing_time_ms": processing_time_ms,
    }
    _publish(PUBSUB_TOPIC, confirmation)

    log.info("Function completed in %dms for %s", processing_time_ms, filename)
