"""
simulate_realtime.py
Purpose : Simulate real-time order events by publishing cleaned orders to Pub/Sub.
          Default limit is 200 messages — intentionally capped to control cost.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : Pub/Sub free tier = 10 GB/month.
          Each message is ~500 bytes. 200 messages = ~100 KB → negligible cost.
          Default --limit is 200. Never run with --limit 0 (unlimited) unless
          you monitor usage in the GCP Console.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import pubsub_v1

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("simulate_realtime")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

PROJECT_ID = os.environ["PROJECT_ID"]
PUBSUB_TOPIC = os.environ["PUBSUB_TOPIC"]

BASE_DIR = Path(__file__).parent.parent
CLEAN_DIR = BASE_DIR / "data" / "clean"


def build_payload(row: dict) -> dict:
    """Build a Pub/Sub message payload from an order row."""
    return {
        "order_id":     str(row.get("order_id", "")),
        "client_id":    str(row.get("client_id", "")),
        "total_amount": float(row.get("total_amount", 0.0) or 0.0),
        "status":       str(row.get("status", "Unknown")),
        "sent_at":      datetime.now(tz=timezone.utc).isoformat(),
    }


def publish_message(publisher: pubsub_v1.PublisherClient, topic_path: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    future.result()  # Ensure delivery before continuing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate real-time order stream → Pub/Sub"
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=2.0,
        help="Delay between messages in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max messages to send (default: 200 — intentionally capped, see cost note)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print full JSON payload per message",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        log.warning(
            "COST WARNING: --limit <= 0 means unlimited messages. "
            "This could consume significant Pub/Sub free tier quota. "
            "Forcing --limit=200 for safety."
        )
        args.limit = 200

    # ------------------------------------------------------------------
    # Load clean orders
    # ------------------------------------------------------------------
    orders_path = CLEAN_DIR / "orders_clean.csv"
    if not orders_path.exists():
        raise FileNotFoundError(
            f"Clean orders file not found: {orders_path}\n"
            "Run scripts/prepare_data.py first."
        )

    import pandas as pd  # local import to keep startup fast when orders not available
    orders_df = pd.read_csv(orders_path, low_memory=False)
    log.info("Loaded %d orders from %s", len(orders_df), orders_path)

    # ------------------------------------------------------------------
    # Pub/Sub setup
    # ------------------------------------------------------------------
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

    print(f"\n[REALTIME SIMULATOR]")
    print(f"  Topic   : {topic_path}")
    print(f"  Limit   : {args.limit} messages")
    print(f"  Speed   : {args.speed}s / message")
    print()

    sent = 0
    t_start = time.time()

    try:
        for _, row in orders_df.iterrows():
            if sent >= args.limit:
                break

            payload = build_payload(row.to_dict())
            publish_message(publisher, topic_path, payload)

            ts = datetime.now().strftime("%H:%M:%S")
            status_icon = {"Delivered": "✓", "Pending": "⏳", "Cancelled": "✗", "Refunded": "↩"}.get(
                payload["status"], "?"
            )
            print(
                f"  [{ts}] {status_icon} {payload['order_id']} → "
                f"Client {payload['client_id']} → "
                f"{payload['total_amount']:.2f} EUR · {payload['status']}"
            )
            if args.verbose:
                print(f"         Payload: {json.dumps(payload)}")

            sent += 1
            time.sleep(args.speed)

    except KeyboardInterrupt:
        print("\n  [INTERRUPTED by user]")
    finally:
        elapsed = round(time.time() - t_start, 2)
        rate = round(sent / elapsed, 2) if elapsed > 0 else 0.0
        print()
        print(f"  --- Stats ---")
        print(f"  Messages sent : {sent}")
        print(f"  Duration      : {elapsed}s")
        print(f"  Rate          : {rate} msg/s")
        log.info("Simulation finished: sent=%d duration=%ss rate=%s msg/s", sent, elapsed, rate)


if __name__ == "__main__":
    main()
