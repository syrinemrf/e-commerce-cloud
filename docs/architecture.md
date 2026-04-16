# Architecture — E-Commerce GCP Analytics Pipeline

## Technical choices and justification

### Why DirectRunner instead of Dataflow?

Apache Beam's **DirectRunner** executes the pipeline locally on your machine using the same exact Beam code that would run on Dataflow. The reasons for choosing DirectRunner:

1. **Cost**: Dataflow bills per vCPU-hour and per GB processed. For this project (~200 messages, ~26 KB/batch), that would cost far more than the data is worth to process. DirectRunner is $0.
2. **Scale**: This project processes < 200 orders per simulation run. DirectRunner handles this trivially in under 10 seconds.
3. **Simplicity**: No Dataflow service account, no staging bucket for templates, no worker VM management.
4. **Code portability**: The same `pipeline.py` can be switched to DataflowRunner in a real production environment by changing a single `--runner` argument — all Beam transforms stay identical.

---

## End-to-end data flow

```
┌───────────────────────────────────────────────────────────┐
│ LOCAL MACHINE                                             │
│                                                           │
│  scripts/generate_data.py  ──►  data/raw/                │
│  scripts/prepare_data.py   ──►  data/clean/              │
│  scripts/load_to_bq.py     ──►  BigQuery (batch load)    │
│  scripts/simulate_realtime.py  ──►  Pub/Sub              │
│  beam/pipeline.py (DirectRunner)  ──►  BigQuery          │
└───────────────────────────────────────────────────────────┘
          │                    │
          ▼                    ▼
┌──────────────────┐  ┌─────────────────────────────────────┐
│  Cloud Storage   │  │  Pub/Sub                            │
│  gs://bucket/raw │  │  orders-realtime topic              │
│  (cleaned CSVs)  │  │  orders-sub subscription            │
└────────┬─────────┘  └────────────────┬────────────────────┘
         │                             │
         ▼                             │
┌──────────────────┐                   │ (Beam pull)
│  Cloud Function  │                   │
│  process_upload  │                   ▼
│  (GCS trigger)   │  ┌─────────────────────────────────────┐
└────────┬─────────┘  │  Apache Beam (DirectRunner)         │
         │            │  • DecodeAndParse                   │
         │            │  • ValidateAndEnrich                │
         │            │  → orders_stream (valid)            │
         │            │  → pipeline_errors (invalid)        │
         ▼            └────────────────┬────────────────────┘
┌──────────────────────────────────────▼────────────────────┐
│                     BigQuery                              │
│  Dataset: ecommerce_analytics                             │
│  Tables: clients, products, orders, order_items,          │
│          incidents, page_views, orders_stream,            │
│          pipeline_errors, kpis_daily, rfm_weekly          │
│  Views:  v_revenue_by_region, v_inactive_clients,         │
│          v_top_products, v_recurring_incidents,           │
│          v_navigation_funnel, v_weekly_kpis, v_client_360 │
└────────────────────────────┬──────────────────────────────┘
                             │
                             ▼
                  ┌──────────────────┐
                  │  Looker Studio   │
                  │  (always free)   │
                  └──────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Cloud Scheduler  (3 jobs — free tier max)               │
│  • daily-bq-refresh   → Pub/Sub → refresh_kpis           │
│  • weekly-kpi-export  → Pub/Sub → export_weekly_report   │
│  • monthly-cleanup    → Pub/Sub → delete_old_partitions  │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Monitoring                                              │
│  • health_check.py  → BQ counts + Pub/Sub backlog check  │
│  • setup_alerts.py  → Log-based metric alerts (free)     │
└──────────────────────────────────────────────────────────┘
```

---

## Free Tier Cost Breakdown

| Resource              | Usage              | Free Limit          | Cost   |
|-----------------------|--------------------|---------------------|--------|
| BigQuery storage      | ~50 MB             | 10 GB/month         | $0     |
| BigQuery queries      | ~100 MB scanned    | 1 TB/month          | $0     |
| Cloud Storage         | ~20 MB             | 5 GB/month          | $0     |
| Cloud Functions gen2  | < 100 invocations  | 2M/month            | $0     |
| Pub/Sub               | ~100 KB/run        | 10 GB/month         | $0     |
| Cloud Scheduler       | 3 jobs             | 3 jobs/month        | $0     |
| Looker Studio         | Always free        | —                   | $0     |
| DirectRunner (Beam)   | Local CPU          | —                   | $0     |
| **TOTAL**             |                    |                     | **$0** |

---

## What would change in real production

| Aspect               | This project (academic)       | Real production                        |
|----------------------|-------------------------------|----------------------------------------|
| Data ingestion       | Faker synthetic data          | Kafka / Cloud Pub/Sub ingestion connectors |
| Processing           | DirectRunner (local)          | DataflowRunner (autoscaling workers)   |
| Data volume          | 70k rows                      | Millions of rows/day                   |
| Scheduling           | Cloud Scheduler (3 jobs)      | Cloud Composer (Airflow)               |
| Monitoring           | gcloud CLI + log metrics      | Cloud Monitoring dashboards + SLOs     |
| Authentication       | ADC / service account key     | Workload Identity Federation           |
| Infrastructure       | Manual gcloud scripts         | Terraform / Deployment Manager         |
| CI/CD                | Git commits                   | Cloud Build / GitHub Actions           |
| Data quality         | Pandas cleaning                | dbt + data quality tests               |
| Costs                | $0 (free tier)                | ~$200–2000/month depending on volume   |
