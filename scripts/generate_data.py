"""
generate_data.py
Purpose : Generate fully synthetic, internally consistent e-commerce datasets
          for the GCP decisional pipeline project.
Author  : ProjetCloud Team
Date    : 2024-06-01
Cost    : $0 — runs entirely locally, no GCP resources used.
"""

import os
import re
import random
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NB_CLIENTS = 2_000
NB_PRODUCTS = 50
NB_ORDERS = 15_000
NB_INCIDENTS = 3_000
NB_SESSIONS = 50_000
DATE_START = datetime(2022, 1, 1)
DATE_END = datetime(2024, 6, 1)
RANDOM_SEED = 42

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
DOCS_DIR = Path(__file__).parent.parent / "docs"

fake = Faker(["fr_FR", "en_CA"])
Faker.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# ---------------------------------------------------------------------------
# Country / city mapping
# ---------------------------------------------------------------------------
COUNTRY_CITIES: dict[str, list[str]] = {
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille", "Strasbourg", "Rennes", "Montpellier"],
    "Belgium": ["Brussels", "Antwerp", "Ghent", "Liège", "Bruges", "Namur"],
    "Switzerland": ["Zurich", "Geneva", "Bern", "Basel", "Lausanne", "Lucerne"],
    "Canada": ["Montreal", "Toronto", "Vancouver", "Calgary", "Ottawa", "Quebec City"],
    "Morocco": ["Casablanca", "Rabat", "Marrakech", "Fès", "Agadir", "Tanger"],
    "Tunisia": ["Tunis", "Sfax", "Sousse", "Kairouan", "Bizerte"],
    "Senegal": ["Dakar", "Thiès", "Saint-Louis", "Ziguinchor", "Kaolack"],
}

COUNTRY_WEIGHTS = [0.55, 0.15, 0.10, 0.08, 0.05, 0.04, 0.03]
COUNTRIES = list(COUNTRY_CITIES.keys())

COUNTRY_REGION_MAP: dict[str, str] = {
    "France": "Europe-West",
    "Belgium": "Europe-West",
    "Switzerland": "Europe-West",
    "Canada": "North America",
    "Morocco": "Africa-North",
    "Tunisia": "Africa-North",
    "Senegal": "Africa-West",
}

# ---------------------------------------------------------------------------
# Category / price ranges
# ---------------------------------------------------------------------------
CATEGORY_PRICE: dict[str, tuple[float, float]] = {
    "Electronics": (99.99, 499.99),
    "Audio": (49.99, 299.99),
    "Office Furniture": (79.99, 799.99),
    "Accessories": (9.99, 49.99),
    "Storage": (19.99, 149.99),
}
CATEGORIES = list(CATEGORY_PRICE.keys())

# ---------------------------------------------------------------------------
# Product names per category
# ---------------------------------------------------------------------------
PRODUCT_NAMES: dict[str, list[str]] = {
    "Electronics": [
        "Laptop Pro 15", "Tablet X10", "Smartphone Z5", "Smart TV 55\"",
        "Desktop Mini PC", "Chromebook Air", "Gaming Laptop RTX", "E-Reader Pro",
        "Portable Projector", "Wireless Router 6E",
    ],
    "Audio": [
        "Wireless Headphones ANC", "Bluetooth Speaker Max", "Studio Earbuds Pro",
        "Soundbar 2.1", "Vinyl Record Player", "Podcast Microphone USB",
        "Hi-Fi Amplifier", "Portable DAC",
    ],
    "Office Furniture": [
        "Ergonomic Chair Pro", "Standing Desk Electric", "Monitor Arm Dual",
        "Filing Cabinet 3-Drawer", "LED Desk Lamp", "Whiteboard 120cm",
        "Bookshelf Oak", "Cable Management Kit",
    ],
    "Accessories": [
        "USB-C Hub 7-in-1", "Laptop Sleeve 15\"", "Mechanical Keyboard TKL",
        "Wireless Mouse Compact", "Screen Cleaning Kit", "HDMI Cable 2.1 2m",
        "Webcam 1080p", "Laptop Stand Aluminum", "Mouse Pad XL",
        "USB-C Charger 65W",
    ],
    "Storage": [
        "External SSD 1TB", "NAS Drive 4TB", "USB Flash Drive 128GB",
        "SD Card 256GB UHS-II", "External HDD 2TB", "RAID Enclosure 4-bay",
        "M.2 SSD 512GB", "Cloud Backup Dongle",
    ],
}


def _rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def _inject_nulls(df: pd.DataFrame, rate: float = 0.02) -> pd.DataFrame:
    """Inject random NULL values on non-key columns."""
    key_cols = {c for c in df.columns if c.endswith("_id")}
    non_key = [c for c in df.columns if c not in key_cols]
    total_cells = len(df) * len(non_key)
    n_nulls = int(total_cells * rate)
    for _ in range(n_nulls):
        row = random.randint(0, len(df) - 1)
        col = random.choice(non_key)
        df.at[row, col] = None
    return df


def _inject_duplicates(df: pd.DataFrame, rate: float = 0.01) -> pd.DataFrame:
    """Inject 1% full duplicate rows."""
    n = max(1, int(len(df) * rate))
    sample = df.sample(n=n, replace=True, random_state=RANDOM_SEED)
    return pd.concat([df, sample], ignore_index=True)


def _mangle_emails(df: pd.DataFrame, col: str = "email", rate: float = 0.005) -> pd.DataFrame:
    """Replace '@' with 'at' in 0.5% of emails."""
    mask = np.random.random(len(df)) < rate
    df.loc[mask, col] = df.loc[mask, col].str.replace("@", "at", regex=False)
    return df


def _mangle_ages(df: pd.DataFrame, col: str = "age", rate: float = 0.003) -> pd.DataFrame:
    """Set age below 10 or above 100 in 0.3% of rows."""
    mask = np.random.random(len(df)) < rate
    idx = df[mask].index
    for i in idx:
        df.at[i, col] = random.choice([random.randint(1, 9), random.randint(101, 120)])
    return df


# ---------------------------------------------------------------------------
# STEP 1: Generate clients
# ---------------------------------------------------------------------------
def generate_clients() -> pd.DataFrame:
    log.info("Generating %d clients...", NB_CLIENTS)
    rows = []
    six_months_before_end = DATE_END - timedelta(days=182)

    for i in range(1, NB_CLIENTS + 1):
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        city = random.choice(COUNTRY_CITIES[country])
        reg_date = _rand_date(DATE_START, DATE_END)
        gender = random.choices(["M", "F", "Non-binary"], weights=[0.48, 0.48, 0.04], k=1)[0]
        age = random.randint(18, 75)
        segment = "new" if reg_date >= six_months_before_end else "regular"

        rows.append({
            "client_id": f"C{i:04d}",
            "last_name": fake.last_name(),
            "first_name": fake.first_name(),
            "email": fake.email(),
            "age": age,
            "gender": gender,
            "country": country,
            "city": city,
            "phone": fake.phone_number(),
            "registration_date": reg_date,
            "segment": segment,
        })

    df = pd.DataFrame(rows)
    df = _mangle_emails(df)
    df = _mangle_ages(df)
    df = _inject_nulls(df)
    df = _inject_duplicates(df)
    log.info("Clients generated: %d rows (with duplicates/nulls injected)", len(df))
    return df


# ---------------------------------------------------------------------------
# STEP 2: Generate products
# ---------------------------------------------------------------------------
def generate_products() -> pd.DataFrame:
    log.info("Generating %d products...", NB_PRODUCTS)
    rows = []
    used_names: set[str] = set()
    product_idx = 1

    for category, names in PRODUCT_NAMES.items():
        for name in names:
            if product_idx > NB_PRODUCTS:
                break
            base_price_min, base_price_max = CATEGORY_PRICE[category]
            unit_price = round(random.uniform(base_price_min, base_price_max), 2)
            rows.append({
                "product_id": f"P{product_idx:03d}",
                "product_name": name,
                "category": category,
                "unit_price": unit_price,
                "stock": random.randint(0, 500),
            })
            product_idx += 1

    # Fill remaining slots if needed
    while product_idx <= NB_PRODUCTS:
        cat = random.choice(CATEGORIES)
        base_price_min, base_price_max = CATEGORY_PRICE[cat]
        rows.append({
            "product_id": f"P{product_idx:03d}",
            "product_name": f"Generic Product {product_idx}",
            "category": cat,
            "unit_price": round(random.uniform(base_price_min, base_price_max), 2),
            "stock": random.randint(0, 500),
        })
        product_idx += 1

    df = pd.DataFrame(rows)
    log.info("Products generated: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# STEP 3: Generate orders + order_items
# ---------------------------------------------------------------------------
def generate_orders_and_items(
    clients_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info("Generating %d orders and items...", NB_ORDERS)

    # Build lookup: client_id → registration_date, country
    client_info: dict[str, tuple[datetime, str]] = {}
    for _, row in clients_df.iterrows():
        cid = row["client_id"]
        reg = row["registration_date"]
        if pd.isna(reg):
            reg = DATE_START
        if isinstance(reg, str):
            try:
                reg = datetime.fromisoformat(reg)
            except Exception:
                reg = DATE_START
        client_info[cid] = (reg, row.get("country", "France"))

    # Only use real client IDs (deduped, no NaN)
    valid_clients = clients_df["client_id"].dropna().unique().tolist()
    product_ids = products_df["product_id"].tolist()
    product_prices = dict(zip(products_df["product_id"], products_df["unit_price"]))

    order_rows = []
    item_rows = []
    item_id = 1

    for i in range(1, NB_ORDERS + 1):
        order_id = f"ORD{i:05d}"
        client_id = random.choice(valid_clients)
        reg_date, country = client_info.get(client_id, (DATE_START, "France"))
        order_date = _rand_date(reg_date, DATE_END)
        status = random.choices(
            ["Delivered", "Pending", "Cancelled", "Refunded"],
            weights=[0.65, 0.20, 0.10, 0.05],
            k=1,
        )[0]
        payment = random.choices(
            ["Credit card", "PayPal", "Bank transfer", "Cheque"],
            weights=[0.60, 0.25, 0.10, 0.05],
            k=1,
        )[0]
        region = COUNTRY_REGION_MAP.get(country, "Other")

        # Items: Poisson(λ=1.5) capped at [1,4]
        n_items = min(max(1, np.random.poisson(1.5)), 4)
        order_total = 0.0
        chosen_products = random.sample(product_ids, min(n_items, len(product_ids)))

        for pid in chosen_products:
            base = product_prices[pid]
            discount = random.uniform(-0.05, 0.05)
            unit_price = round(base * (1 + discount), 2)
            qty = random.randint(1, 4)
            item_total = round(unit_price * qty, 2)
            order_total += item_total
            item_rows.append({
                "item_id": f"IT{item_id:06d}",
                "order_id": order_id,
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
            })
            item_id += 1

        order_rows.append({
            "order_id": order_id,
            "client_id": client_id,
            "order_date": order_date,
            "status": status,
            "payment_method": payment,
            "region": region,
            "total_amount": round(order_total, 2),
        })

    orders_df = pd.DataFrame(order_rows)
    items_df = pd.DataFrame(item_rows)
    orders_df = _inject_nulls(orders_df)
    orders_df = _inject_duplicates(orders_df)
    items_df = _inject_nulls(items_df)
    log.info("Orders: %d rows, Items: %d rows", len(orders_df), len(items_df))
    return orders_df, items_df


# ---------------------------------------------------------------------------
# STEP 4: Generate incidents
# ---------------------------------------------------------------------------
def generate_incidents(
    clients_df: pd.DataFrame,
    orders_df: pd.DataFrame,
) -> pd.DataFrame:
    log.info("Generating %d incidents...", NB_INCIDENTS)
    valid_clients = clients_df["client_id"].dropna().unique().tolist()
    client_reg: dict[str, datetime] = {}
    for _, row in clients_df.iterrows():
        cid = row["client_id"]
        reg = row["registration_date"]
        if pd.isna(reg):
            reg = DATE_START
        if isinstance(reg, str):
            try:
                reg = datetime.fromisoformat(reg)
            except Exception:
                reg = DATE_START
        client_reg[cid] = reg

    # Build client → list of order_ids
    client_orders: dict[str, list[str]] = {}
    for _, row in orders_df.iterrows():
        cid = row.get("client_id")
        oid = row.get("order_id")
        if pd.isna(cid) or pd.isna(oid):
            continue
        client_orders.setdefault(cid, []).append(oid)

    rows = []
    for i in range(1, NB_INCIDENTS + 1):
        client_id = random.choice(valid_clients)
        reg_date = client_reg.get(client_id, DATE_START)
        report_date = _rand_date(reg_date, DATE_END)
        category = random.choices(
            ["Payment", "Delivery", "Defective product", "Login", "Customer service"],
            weights=[0.25, 0.35, 0.20, 0.10, 0.10],
            k=1,
        )[0]
        status = random.choices(
            ["Resolved", "In progress", "Escalated", "Closed"],
            weights=[0.60, 0.25, 0.10, 0.05],
            k=1,
        )[0]
        priority = random.choices(
            ["Low", "Medium", "High", "Critical"],
            weights=[0.30, 0.40, 0.20, 0.10],
            k=1,
        )[0]

        # 70% chance of linking to a real order
        order_id = None
        if random.random() < 0.70:
            client_order_list = client_orders.get(client_id, [])
            if client_order_list:
                order_id = random.choice(client_order_list)

        if status == "In progress":
            resolution_time_h = None
        elif priority == "Critical":
            resolution_time_h = random.randint(1, 24)
        else:
            resolution_time_h = random.randint(1, 168)

        rows.append({
            "incident_id": f"INC{i:04d}",
            "client_id": client_id,
            "report_date": report_date,
            "category": category,
            "order_id": order_id,
            "status": status,
            "priority": priority,
            "resolution_time_h": resolution_time_h,
        })

    df = pd.DataFrame(rows)
    df = _inject_nulls(df)
    df = _inject_duplicates(df)
    log.info("Incidents generated: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# STEP 5: Generate page_views
# ---------------------------------------------------------------------------
def generate_page_views(clients_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Generating %d page views...", NB_SESSIONS)
    valid_clients = clients_df["client_id"].dropna().unique().tolist()

    PAGES = [
        "/home", "/products", "/cart", "/checkout", "/profile",
        "/support", "/deals", "/category/electronics", "/category/audio",
    ]
    PAGE_WEIGHTS = [0.20, 0.25, 0.15, 0.10, 0.05, 0.05, 0.08, 0.07, 0.05]

    DURATION_MAP: dict[str, tuple[int, int]] = {
        "/home": (10, 60),
        "/products": (30, 300),
        "/cart": (20, 180),
        "/checkout": (60, 600),
        "/profile": (15, 120),
        "/support": (30, 240),
        "/deals": (20, 180),
        "/category/electronics": (30, 300),
        "/category/audio": (30, 300),
    }

    rows = []
    for i in range(1, NB_SESSIONS + 1):
        # Bimodal time distribution: peak at 12-14 and 19-22
        base_date = _rand_date(DATE_START, DATE_END).date()
        if random.random() < 0.5:
            hour = random.randint(12, 13)
        else:
            hour = random.randint(19, 21)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        event_dt = datetime.combine(base_date, datetime.min.time()).replace(
            hour=hour, minute=minute, second=second
        )

        page = random.choices(PAGES, weights=PAGE_WEIGHTS, k=1)[0]
        dur_min, dur_max = DURATION_MAP.get(page, (10, 60))
        duration_s = random.randint(dur_min, dur_max)

        # 80% authenticated
        client_id = random.choice(valid_clients) if random.random() < 0.80 else None

        rows.append({
            "session_id": f"S{i:06d}",
            "client_id": client_id,
            "page": page,
            "event_datetime": event_dt,
            "duration_seconds": duration_s,
            "device": random.choices(["Mobile", "Desktop", "Tablet"], weights=[0.55, 0.40, 0.05], k=1)[0],
            "browser": random.choices(["Chrome", "Safari", "Firefox", "Edge"], weights=[0.60, 0.20, 0.12, 0.08], k=1)[0],
            "traffic_source": random.choices(
                ["Direct", "Google", "Instagram", "Email", "Referral"],
                weights=[0.30, 0.35, 0.15, 0.12, 0.08],
                k=1,
            )[0],
        })

    df = pd.DataFrame(rows)
    df = _inject_nulls(df)
    df = _inject_duplicates(df)
    log.info("Page views generated: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def build_report(
    clients_df: pd.DataFrame,
    products_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    items_df: pd.DataFrame,
    incidents_df: pd.DataFrame,
    page_views_df: pd.DataFrame,
) -> str:
    lines: list[str] = [
        "=" * 60,
        "DATA GENERATION REPORT",
        f"Generated at: {datetime.now().isoformat()}",
        "=" * 60,
        "",
        "--- Row counts ---",
        f"clients.csv       : {len(clients_df):>8,} rows",
        f"products.csv      : {len(products_df):>8,} rows",
        f"orders.csv        : {len(orders_df):>8,} rows",
        f"order_items.csv   : {len(items_df):>8,} rows",
        f"incidents.csv     : {len(incidents_df):>8,} rows",
        f"page_views.csv    : {len(page_views_df):>8,} rows",
        "",
        "--- Country distribution (clients) ---",
    ]

    country_counts = clients_df["country"].value_counts(dropna=True)
    for country, count in country_counts.items():
        pct = count / len(clients_df) * 100
        lines.append(f"  {country:<20}: {count:>5,} ({pct:.1f}%)")

    # Total simulated revenue
    total_revenue = orders_df["total_amount"].sum(skipna=True)
    lines.append("")
    lines.append(f"--- Total simulated revenue : {total_revenue:>12,.2f} EUR ---")

    # Delivery rate
    delivered = (orders_df["status"] == "Delivered").sum()
    total_ord = len(orders_df)
    lines.append(f"--- Delivery rate           : {delivered:>6,} / {total_ord:>6,} ({delivered/total_ord*100:.1f}%) ---")

    # Top 5 products by revenue
    merged = items_df.merge(products_df[["product_id", "product_name"]], on="product_id", how="left")
    merged["revenue"] = merged["quantity"] * merged["unit_price"]
    top5 = merged.groupby("product_name")["revenue"].sum().nlargest(5)
    lines.append("")
    lines.append("--- Top 5 products by revenue ---")
    for name, rev in top5.items():
        lines.append(f"  {name:<35}: {rev:>10,.2f} EUR")

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)
    Faker.seed(args.seed)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    clients_df = generate_clients()
    products_df = generate_products()
    orders_df, items_df = generate_orders_and_items(clients_df, products_df)
    incidents_df = generate_incidents(clients_df, orders_df)
    page_views_df = generate_page_views(clients_df)

    # Save
    clients_df.to_csv(RAW_DIR / "clients.csv", index=False)
    products_df.to_csv(RAW_DIR / "products.csv", index=False)
    orders_df.to_csv(RAW_DIR / "orders.csv", index=False)
    items_df.to_csv(RAW_DIR / "order_items.csv", index=False)
    incidents_df.to_csv(RAW_DIR / "incidents.csv", index=False)
    page_views_df.to_csv(RAW_DIR / "page_views.csv", index=False)
    log.info("All datasets saved to %s", RAW_DIR)

    report = build_report(clients_df, products_df, orders_df, items_df, incidents_df, page_views_df)
    report_path = DOCS_DIR / "data_generation_report.txt"
    report_path.write_text(report, encoding="utf-8")
    log.info("Report saved to %s", report_path)

    print(report)


if __name__ == "__main__":
    main()
