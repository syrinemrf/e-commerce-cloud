"""
beam/pipeline.py
Purpose : Local Apache Beam pipeline (DirectRunner only) that pulls messages
          from Pub/Sub, validates, enriches, and writes to BigQuery in batch mode.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : DirectRunner runs entirely on your local machine.
          No GCP compute resources are used. This is always $0.
          DataflowRunner is intentionally excluded — it bills per vCPU-hour.

IMPORTANT: DirectRunner only — no DataflowRunner, ever.
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from dotenv import load_dotenv
from google.cloud import pubsub_v1, bigquery as bq_client

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("beam_pipeline")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()
PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ.get("DATASET", "ecommerce_analytics")
PUBSUB_SUB = os.environ.get("PUBSUB_SUB", "orders-sub")

# ---------------------------------------------------------------------------
# BigQuery schemas
# ---------------------------------------------------------------------------
ORDERS_STREAM_SCHEMA = {
    "fields": [
        {"name": "order_id",             "type": "STRING",   "mode": "NULLABLE"},
        {"name": "client_id",            "type": "STRING",   "mode": "NULLABLE"},
        {"name": "total_amount",         "type": "FLOAT",    "mode": "NULLABLE"},
        {"name": "status",               "type": "STRING",   "mode": "NULLABLE"},
        {"name": "sent_at",              "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "processing_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ]
}

ERRORS_SCHEMA = {
    "fields": [
        {"name": "raw_message",          "type": "STRING",   "mode": "NULLABLE"},
        {"name": "error_reason",         "type": "STRING",   "mode": "NULLABLE"},
        {"name": "processing_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ]
}

# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------
class DecodeAndParse(beam.DoFn):
    """Decode bytes → UTF-8 → parse JSON → emit dict or error."""

    def process(self, element):
        try:
            if isinstance(element, bytes):
                element = element.decode("utf-8")
            record = json.loads(element)
            yield record
        except Exception as exc:
            yield beam.pvalue.TaggedOutput(
                "errors",
                {
                    "raw_message": str(element),
                    "error_reason": f"JSON parse error: {exc}",
                    "processing_timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                },
            )


class ValidateAndEnrich(beam.DoFn):
    """Validate required fields and add processing_timestamp."""

    REQUIRED_FIELDS = ["order_id", "client_id", "total_amount"]

    def process(self, element):
        missing = [f for f in self.REQUIRED_FIELDS if not element.get(f)]
        if missing:
            yield beam.pvalue.TaggedOutput(
                "errors",
                {
                    "raw_message": json.dumps(element),
                    "error_reason": f"Missing required fields: {missing}",
                    "processing_timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                },
            )
            return

        # Normalize sent_at — strip timezone offset if present
        if element.get("sent_at"):
            try:
                dt = datetime.fromisoformat(str(element["sent_at"]))
                element["sent_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

        # Enrich with processing timestamp
        element["processing_timestamp"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        yield element


# ---------------------------------------------------------------------------
# Pull messages from Pub/Sub (batch pull — stop after --limit messages)
# ---------------------------------------------------------------------------
def pull_messages(project_id: str, subscription: str, limit: int) -> list[bytes]:
    """Pull up to `limit` messages from Pub/Sub and acknowledge them."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription)

    messages: list[bytes] = []
    ack_ids: list[str] = []

    # Pull in batches of min(limit, 100)
    remaining = limit
    while remaining > 0:
        batch_size = min(remaining, 100)
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": batch_size,
            }
        )
        if not response.received_messages:
            break
        for msg in response.received_messages:
            messages.append(msg.message.data)
            ack_ids.append(msg.ack_id)
        # Acknowledge the batch
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
        ack_ids.clear()
        remaining -= len(response.received_messages)
        log.info("Pulled %d messages (total so far: %d)", len(response.received_messages), len(messages))

    log.info("Total messages pulled: %d", len(messages))
    return messages


# ---------------------------------------------------------------------------
# BQ writer DoFn — avoids WriteToBigQuery / google-cloud-bigquery version conflict
# ---------------------------------------------------------------------------
class WriteToBQFn(beam.DoFn):
    """Buffer rows and write them to BigQuery via streaming inserts."""

    def __init__(self, project_id: str, dataset: str, table_name: str):
        self._project = project_id
        self._dataset = dataset
        self._table_name = table_name

    def setup(self):
        self._client = bq_client.Client(project=self._project)
        self._buffer: list[dict] = []

    def process(self, element):
        self._buffer.append(element)
        if len(self._buffer) >= 500:
            self._flush()

    def finish_bundle(self):
        self._flush()

    def _flush(self):
        if not self._buffer:
            return
        table_id = f"{self._project}.{self._dataset}.{self._table_name}"
        errors = self._client.insert_rows_json(table_id, self._buffer)
        if errors:
            log.error("BQ insert errors for %s: %s", table_id, errors)
        else:
            log.info("Inserted %d rows into %s", len(self._buffer), table_id)
        self._buffer = []


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def build_pipeline(
    pipeline: beam.Pipeline,
    messages: list[bytes],
    project_id: str,
    dataset: str,
) -> None:
    results = (
        pipeline
        | "Create messages"    >> beam.Create(messages)
        | "Decode and parse"   >> beam.ParDo(DecodeAndParse()).with_outputs("errors", main="parsed")
    )

    validated = (
        results.parsed
        | "Validate and enrich" >> beam.ParDo(ValidateAndEnrich()).with_outputs("errors", main="valid")
    )

    # Valid messages → orders_stream
    (
        validated.valid
        | "Write valid to BQ" >> beam.ParDo(WriteToBQFn(project_id, dataset, "orders_stream"))
    )

    # Errors from parsing + validation → pipeline_errors
    parse_errors = results.errors
    validation_errors = validated.errors

    (
        (parse_errors, validation_errors)
        | "Flatten errors"     >> beam.Flatten()
        | "Write errors to BQ" >> beam.ParDo(WriteToBQFn(project_id, dataset, "pipeline_errors"))
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local Beam pipeline (DirectRunner) — Pub/Sub pull → BigQuery batch load"
    )
    parser.add_argument(
        "--project",
        type=str,
        default=PROJECT_ID,
        help="GCP project ID",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of messages to pull from Pub/Sub then stop (default: 100)",
    )
    args = parser.parse_args()

    log.info("Starting local DirectRunner pipeline (project=%s, limit=%d)", args.project, args.limit)

    # Pull messages from Pub/Sub (batch, not streaming)
    messages = pull_messages(args.project, PUBSUB_SUB, args.limit)

    if not messages:
        log.warning("No messages available in subscription. Run simulate_realtime.py first.")
        print("[WARN] No messages found in Pub/Sub subscription. Exiting.")
        return

    log.info("Processing %d messages with DirectRunner", len(messages))

    # Build DirectRunner pipeline options — hardcoded, no DataflowRunner
    options = PipelineOptions(
        runner="DirectRunner",
        project=args.project,
        temp_location=f"gs://ecommerce-raw-{args.project}/beam_temp",
    )
    options.view_as(StandardOptions).runner = "DirectRunner"

    with beam.Pipeline(options=options) as pipeline:
        build_pipeline(pipeline, messages, args.project, DATASET)

    log.info("Pipeline finished successfully")
    print(f"\n[OK] Pipeline finished. Processed {len(messages)} messages.")
    print(f"     Valid rows → {args.project}:{DATASET}.orders_stream")
    print(f"     Errors     → {args.project}:{DATASET}.pipeline_errors")


if __name__ == "__main__":
    main()
