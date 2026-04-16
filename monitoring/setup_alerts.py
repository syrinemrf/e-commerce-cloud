"""
monitoring/setup_alerts.py
Purpose : Set up 2 Cloud Logging-based alerts (log sinks / metric filters).
          Log-based alerts are free. Cloud Monitoring metric alerts are NOT used
          here to avoid potential billing at scale.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : Log-based alerts are free. Metric-based alerts (Cloud Monitoring)
          can generate costs at scale — not used here. This script uses only
          the gcloud CLI to create log-based metric filters.
"""

import json
import logging
import os
import subprocess

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("setup_alerts")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

PROJECT_ID = os.environ["PROJECT_ID"]
PUBSUB_TOPIC_DLQ = os.environ.get("PUBSUB_TOPIC_DLQ", "orders-realtime-dlq")


def run_gcloud(args: list[str], description: str) -> tuple[bool, str]:
    """Run a gcloud command and return (success, output)."""
    cmd = ["gcloud"] + args + [f"--project={PROJECT_ID}"]
    log.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            log.info("%s: OK", description)
            return True, result.stdout.strip()
        else:
            # Ignore "already exists" errors
            if "already exists" in result.stderr.lower():
                log.info("%s: already exists — skipping", description)
                return True, "already exists"
            log.error("%s FAILED: %s", description, result.stderr.strip())
            return False, result.stderr.strip()
    except Exception as exc:
        log.error("%s raised exception: %s", description, exc)
        return False, str(exc)


def create_function_error_metric() -> bool:
    """
    Alert 1: Log-based metric for Cloud Function ERROR logs.
    Creates a log-based metric that counts ERROR severity events from Cloud Functions.
    """
    metric_name = "cloud_function_errors"
    log_filter = (
        'resource.type="cloud_function" '
        'AND severity>=ERROR'
    )
    success, output = run_gcloud(
        [
            "logging", "metrics", "create", metric_name,
            f"--description=Count of Cloud Function ERROR log entries",
            f"--log-filter={log_filter}",
        ],
        "Create log-based metric: cloud_function_errors",
    )
    if success:
        print(f"  ✅ Log-based metric created: {metric_name}")
        print(f"     Filter : {log_filter}")
    else:
        print(f"  ❌ Failed to create metric: {metric_name} — {output}")
    return success


def create_dlq_alert_metric() -> bool:
    """
    Alert 2: Log-based metric when any message is published to the DLQ topic.
    """
    metric_name = "pubsub_dlq_messages"
    log_filter = (
        f'resource.type="pubsub_topic" '
        f'AND resource.labels.topic_id="{PUBSUB_TOPIC_DLQ}" '
        f'AND protoPayload.methodName="google.pubsub.v1.Publisher.Publish"'
    )
    success, output = run_gcloud(
        [
            "logging", "metrics", "create", metric_name,
            f"--description=Count of messages published to the dead-letter queue topic",
            f"--log-filter={log_filter}",
        ],
        "Create log-based metric: pubsub_dlq_messages",
    )
    if success:
        print(f"  ✅ Log-based metric created: {metric_name}")
        print(f"     Filter : {log_filter}")
    else:
        print(f"  ❌ Failed to create metric: {metric_name} — {output}")
    return success


def print_instructions() -> None:
    """Print manual instructions for wiring log-based metrics to email alerts."""
    print()
    print("=" * 65)
    print("  NEXT STEP: Wire log-based metrics to email alerts in GCP Console")
    print("=" * 65)
    print()
    print("  These metrics are now visible in Cloud Monitoring → Metrics Explorer.")
    print("  To create an email alert for each metric:")
    print()
    print("  1. Go to: https://console.cloud.google.com/monitoring/alerting")
    print(f"     Project: {PROJECT_ID}")
    print()
    print("  2. Click 'Create Policy'")
    print("  3. Select metric: logging/user/cloud_function_errors")
    print("     Condition: count > 0 over 5 minutes")
    print("     Notification: email")
    print()
    print("  4. Repeat for: logging/user/pubsub_dlq_messages")
    print()
    print("  NOTE: Log-based alert policies are free (Cloud Monitoring free tier).")
    print("        Only advanced metric-based policies incur cost at high volume.")
    print()


def main() -> None:
    print("\n[SETUP ALERTS]")
    print(f"  Project : {PROJECT_ID}")
    print()

    results: list[bool] = []
    results.append(create_function_error_metric())
    results.append(create_dlq_alert_metric())

    print_instructions()

    failed = results.count(False)
    if failed > 0:
        log.warning("%d alert(s) failed to create — check logs above", failed)
    else:
        log.info("All log-based metrics created successfully")


if __name__ == "__main__":
    main()
