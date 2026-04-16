"""
prepare_data.py
Purpose : Clean and validate all synthetic e-commerce datasets.
          Outputs cleaned CSVs to data/clean/ and a quality report to docs/.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : $0 — runs entirely locally, no GCP resources used.
"""

import json
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Logging — structured JSON
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"}',
)
log = logging.getLogger("prepare_data")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"
DOCS_DIR = BASE_DIR / "docs"

# ---------------------------------------------------------------------------
# Key columns per dataset (must not be NULL)
# ---------------------------------------------------------------------------
KEY_COLUMNS: dict[str, list[str]] = {
    "clients": ["client_id"],
    "products": ["product_id"],
    "orders": ["order_id", "client_id"],
    "order_items": ["item_id", "order_id", "product_id"],
    "incidents": ["incident_id", "client_id"],
    "page_views": ["session_id"],
}

# ---------------------------------------------------------------------------
# Date columns per dataset
# ---------------------------------------------------------------------------
DATE_COLUMNS: dict[str, list[str]] = {
    "clients": ["registration_date"],
    "products": [],
    "orders": ["order_date"],
    "order_items": [],
    "incidents": ["report_date"],
    "page_views": ["event_datetime"],
}

# ---------------------------------------------------------------------------
# Monetary columns per dataset
# ---------------------------------------------------------------------------
MONETARY_COLUMNS: dict[str, list[str]] = {
    "clients": [],
    "products": ["unit_price"],
    "orders": ["total_amount"],
    "order_items": ["unit_price"],
    "incidents": [],
    "page_views": [],
}


def load_raw(name: str) -> pd.DataFrame:
    path = RAW_DIR / f"{name}.csv"
    df = pd.read_csv(path, low_memory=False)
    log.info("Loaded %s: %d rows, %d cols", name, len(df), len(df.columns))
    return df


def drop_full_duplicates(df: pd.DataFrame, name: str) -> tuple[pd.DataFrame, int]:
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    log.info("[%s] Dropped %d full duplicate rows", name, removed)
    return df, removed


def drop_null_keys(df: pd.DataFrame, name: str) -> tuple[pd.DataFrame, int]:
    key_cols = KEY_COLUMNS.get(name, [])
    before = len(df)
    valid_keys = [c for c in key_cols if c in df.columns]
    if valid_keys:
        df = df.dropna(subset=valid_keys)
    removed = before - len(df)
    log.info("[%s] Dropped %d rows with NULL key columns", name, removed)
    return df, removed


def fix_emails(df: pd.DataFrame, name: str) -> tuple[pd.DataFrame, int]:
    if "email" not in df.columns:
        return df, 0
    # Replace "at" with "@": match word boundaries e.g. "userATdomain.com" → "user@domain.com"
    pattern = re.compile(r"(?<=[A-Za-z0-9])at(?=[A-Za-z0-9])", re.IGNORECASE)
    before = df["email"].copy()
    df["email"] = df["email"].astype(str).apply(lambda v: pattern.sub("@", v) if pd.notna(v) else v)
    fixed = (df["email"] != before.astype(str)).sum()
    log.info("[%s] Fixed %d malformed email addresses", name, fixed)
    return df, int(fixed)


def remove_invalid_ages(df: pd.DataFrame, name: str) -> tuple[pd.DataFrame, int]:
    if "age" not in df.columns:
        return df, 0
    before = len(df)
    df = df[df["age"].isna() | ((df["age"] >= 14) & (df["age"] <= 100))]
    removed = before - len(df)
    log.info("[%s] Removed %d rows with age < 14 or > 100", name, removed)
    return df, removed


def parse_dates(df: pd.DataFrame, name: str) -> pd.DataFrame:
    for col in DATE_COLUMNS.get(name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            n_invalid = df[col].isna().sum()
            log.info("[%s] Parsed date column '%s' — %d NaT values", name, col, n_invalid)
    return df


def round_monetary(df: pd.DataFrame, name: str) -> pd.DataFrame:
    for col in MONETARY_COLUMNS.get(name, []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
            log.info("[%s] Rounded monetary column '%s' to 2 decimals", name, col)
    return df


def recompute_order_totals(
    orders_df: pd.DataFrame,
    items_df: pd.DataFrame,
) -> pd.DataFrame:
    """Recompute total_amount per order from order_items (source of truth)."""
    items_df = items_df.copy()
    items_df["quantity"] = pd.to_numeric(items_df["quantity"], errors="coerce").fillna(0)
    items_df["unit_price"] = pd.to_numeric(items_df["unit_price"], errors="coerce").fillna(0.0)
    items_df["line_total"] = items_df["quantity"] * items_df["unit_price"]
    totals = items_df.groupby("order_id")["line_total"].sum().reset_index()
    totals = totals.rename(columns={"line_total": "total_amount_recomputed"})
    orders_df = orders_df.merge(totals, on="order_id", how="left")
    orders_df["total_amount"] = orders_df["total_amount_recomputed"].fillna(0.0).round(2)
    orders_df = orders_df.drop(columns=["total_amount_recomputed"])
    log.info("Recomputed total_amount for %d orders from order_items", len(orders_df))
    return orders_df


def save_clean(df: pd.DataFrame, name: str) -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CLEAN_DIR / f"{name}_clean.csv"
    df.to_csv(out_path, index=False)
    log.info("Saved %s → %s (%d rows)", name, out_path, len(df))


def clean_dataset(name: str) -> dict:
    """Clean a single dataset and return a stats dict."""
    df = load_raw(name)
    original_count = len(df)
    stats: dict = {
        "dataset": name,
        "original_rows": original_count,
        "duplicates_removed": 0,
        "null_key_rows_removed": 0,
        "invalid_age_rows_removed": 0,
        "emails_fixed": 0,
    }

    df, n_dup = drop_full_duplicates(df, name)
    stats["duplicates_removed"] = n_dup

    df, n_keys = drop_null_keys(df, name)
    stats["null_key_rows_removed"] = n_keys

    df, n_emails = fix_emails(df, name)
    stats["emails_fixed"] = n_emails

    df, n_ages = remove_invalid_ages(df, name)
    stats["invalid_age_rows_removed"] = n_ages

    df = parse_dates(df, name)
    df = round_monetary(df, name)

    stats["final_rows"] = len(df)
    removed_total = original_count - len(df)
    stats["total_removed"] = removed_total
    stats["pct_valid"] = round(len(df) / original_count * 100, 2) if original_count > 0 else 100.0

    return df, stats


def build_cleaning_report(all_stats: list[dict]) -> str:
    lines = [
        "=" * 65,
        "DATA CLEANING REPORT",
        f"Generated at: {datetime.now().isoformat()}",
        "=" * 65,
        "",
    ]
    for s in all_stats:
        lines.append(f"  Dataset         : {s['dataset']}")
        lines.append(f"  Original rows   : {s['original_rows']:>8,}")
        lines.append(f"  Duplicates rm   : {s['duplicates_removed']:>8,}")
        lines.append(f"  Null-key rm     : {s['null_key_rows_removed']:>8,}")
        lines.append(f"  Invalid-age rm  : {s['invalid_age_rows_removed']:>8,}")
        lines.append(f"  Emails fixed    : {s['emails_fixed']:>8,}")
        lines.append(f"  Final rows      : {s['final_rows']:>8,}")
        lines.append(f"  Total removed   : {s['total_removed']:>8,}")
        lines.append(f"  % valid data    : {s['pct_valid']:>7.2f}%")
        lines.append("")
    lines.append("=" * 65)
    return "\n".join(lines)


def main() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    datasets = ["clients", "products", "orders", "order_items", "incidents", "page_views"]
    all_stats: list[dict] = []
    cleaned: dict[str, pd.DataFrame] = {}

    for name in datasets:
        df, stats = clean_dataset(name)
        cleaned[name] = df
        all_stats.append(stats)

    # Recompute order totals from clean items (source of truth)
    if "orders" in cleaned and "order_items" in cleaned:
        cleaned["orders"] = recompute_order_totals(cleaned["orders"], cleaned["order_items"])

    # Save all cleaned datasets
    for name, df in cleaned.items():
        save_clean(df, name)

    # Generate and save report
    report = build_cleaning_report(all_stats)
    report_path = DOCS_DIR / "cleaning_report.txt"
    report_path.write_text(report, encoding="utf-8")
    log.info("Cleaning report saved to %s", report_path)
    print(report)


if __name__ == "__main__":
    main()
