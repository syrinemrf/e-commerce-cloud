# E-Commerce GCP Analytics Pipeline

![Python 3.11](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![GCP Free Tier](https://img.shields.io/badge/GCP-Free%20Tier%20%240-green?logo=google-cloud)
![BigQuery](https://img.shields.io/badge/BigQuery-analytics-blue?logo=google-cloud)
![Apache Beam DirectRunner](https://img.shields.io/badge/Apache%20Beam-DirectRunner-orange)

A production-ready, academic-grade Business Intelligence pipeline on GCP for an e-commerce company — running entirely within the **GCP Free Tier at $0/month**.

```
Simulated sources → Cloud Storage → Cloud Functions
                 → Pub/Sub → Apache Beam (DirectRunner) → BigQuery → Looker Studio
```

---

## Architecture

```
scripts/generate_data.py       → data/raw/         (local synthetic data)
scripts/prepare_data.py        → data/clean/        (cleaned CSVs)
deploy/setup_gcp.sh            → GCS + Pub/Sub + BigQuery dataset
scripts/load_to_bq.py          → BigQuery tables + views
functions/process_upload/      → Cloud Function (GCS trigger)
scripts/simulate_realtime.py   → Pub/Sub stream (200 msgs default)
beam/pipeline.py               → DirectRunner local pipeline → BigQuery
deploy/setup_scheduler.sh      → 3 Cloud Scheduler jobs
monitoring/health_check.py     → pipeline health check
monitoring/setup_alerts.py     → log-based metric alerts
```

---

## GCP Free Tier Reference

| Service             | Free limit           | This project uses |
|---------------------|----------------------|-------------------|
| BigQuery storage    | 10 GB/month          | ~50 MB            |
| BigQuery queries    | 1 TB/month           | ~100 MB           |
| Cloud Storage       | 5 GB/month           | ~20 MB            |
| Cloud Functions     | 2M invocations/month | < 100             |
| Pub/Sub             | 10 GB/month          | ~50 KB            |
| Cloud Scheduler     | 3 jobs/month         | 3                 |
| Looker Studio       | Always free          | Yes               |
| DirectRunner (Beam) | Always free (local)  | Yes               |

---

## Prerequisites

- Python 3.11
- Google Cloud CLI (`gcloud`) authenticated: `gcloud auth login`
- GCP project: `ecommerce-pipeline-493520`
- Git

---

## Quick Setup (5 commands)

```bash
# 1. Clone and install dependencies
git clone <repo-url> ecommerce-gcp-pipeline
cd ecommerce-gcp-pipeline
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env: set your PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS

# 3. Generate synthetic data (runs locally, ~30 sec)
python scripts/generate_data.py

# 4. Clean and validate data
python scripts/prepare_data.py

# 5. Set up GCP infrastructure ($0)
bash deploy/setup_gcp.sh
```

---

## Running Each Script

### Data Pipeline

```bash
# Generate 70k+ synthetic rows
python scripts/generate_data.py

# Clean all datasets
python scripts/prepare_data.py

# Set up GCP (GCS + Pub/Sub + BigQuery)
bash deploy/setup_gcp.sh
# Dry run (no execution):
bash deploy/setup_gcp.sh --dry-run

# Load data to BigQuery + execute SQL
python scripts/load_to_bq.py
```

### Cloud Function

```bash
# Deploy (requires gcloud auth)
bash functions/process_upload/deploy_function.sh
```

### Real-time Simulation

```bash
# Default: 200 messages, 2s delay
python scripts/simulate_realtime.py

# Custom options
python scripts/simulate_realtime.py --limit 50 --speed 0.5 --verbose
```

### Apache Beam Pipeline (DirectRunner — $0)

```bash
# Pull 100 messages from Pub/Sub and write to BigQuery
python beam/pipeline.py --limit 100

# Or use the shell wrapper
bash beam/run_pipeline.sh
```

### Cloud Scheduler

```bash
# Create exactly 3 jobs (free tier limit)
bash deploy/setup_scheduler.sh
# Dry run:
bash deploy/setup_scheduler.sh --dry-run
```

### Monitoring

```bash
# Check pipeline health
python monitoring/health_check.py

# Set up log-based alerts
python monitoring/setup_alerts.py
```

---

## Looker Studio Dashboard

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Add data source → BigQuery → `ecommerce-pipeline-493520` → `ecommerce_analytics`
3. Connect views: `v_revenue_by_region`, `v_weekly_kpis`, `v_top_products`, `v_client_360`

---

## Cost Safety Rules

- Always use `LIMIT 1000` on exploratory BigQuery queries
- Never leave a streaming pipeline running (DirectRunner only, manual stop)
- Default `--limit 200` in `simulate_realtime.py`
- Max 3 Cloud Scheduler jobs
- Never enable: Dataflow, Compute Engine, Cloud Run, Cloud SQL

---

## Dataset Overview

| Dataset       | Rows    | Description                          |
|---------------|---------|--------------------------------------|
| clients       | 2,000   | Customer master with demographics    |
| products      | 50      | Product catalogue with pricing       |
| orders        | 15,000  | Orders with status and payment method|
| order_items   | ~26,000 | Line items per order                 |
| incidents     | 3,000   | Customer service incidents           |
| page_views    | 50,000  | Web navigation sessions              |

---

## Project Structure

```
ecommerce-gcp-pipeline/
├── data/
│   ├── raw/              # Generated by generate_data.py (gitignored)
│   └── clean/            # Cleaned by prepare_data.py (gitignored)
├── scripts/
│   ├── generate_data.py
│   ├── prepare_data.py
│   ├── load_to_bq.py
│   └── simulate_realtime.py
├── sql/
│   ├── 01_create_tables.sql
│   ├── 02_create_views.sql
│   ├── 03_advanced_analytics.sql
│   └── 04_scheduled_queries.sql
├── functions/
│   └── process_upload/
│       ├── main.py
│       ├── requirements.txt
│       └── deploy_function.sh
├── beam/
│   ├── pipeline.py
│   └── run_pipeline.sh
├── monitoring/
│   ├── health_check.py
│   └── setup_alerts.py
├── deploy/
│   ├── setup_gcp.sh
│   └── setup_scheduler.sh
├── docs/
│   ├── architecture.md
│   ├── sql_explained.md
│   ├── data_generation_report.txt  # auto-generated
│   └── cleaning_report.txt         # auto-generated
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```
