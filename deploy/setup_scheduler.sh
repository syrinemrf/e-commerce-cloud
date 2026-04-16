#!/usr/bin/env bash
# deploy/setup_scheduler.sh
# Purpose : Create exactly 3 Cloud Scheduler jobs (free tier limit = 3 jobs/month).
# Author  : ProjetCloud Team
# Date    : 2024-06-01
#
# COST NOTE: Cloud Scheduler free tier = 3 jobs/month.
# This script creates exactly 3 jobs. Do not add more without checking billing.
# Exceeding 3 jobs incurs $0.10/job/month — avoid.
#
# Usage:
#   bash deploy/setup_scheduler.sh
#   bash deploy/setup_scheduler.sh --dry-run

set -euo pipefail

PROJECT_ID="ecommerce-pipeline-493520"
REGION="europe-west1"
PUBSUB_TOPIC="orders-realtime"

DRY_RUN=false
for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] $*"
  else
    echo "[EXEC] $*"
    eval "$@"
  fi
}

info() { echo "[INFO] $*"; }

info "Configuring Cloud Scheduler — exactly 3 jobs (free tier limit)"
info "Project: ${PROJECT_ID} | Region: ${REGION}"
echo ""

# ============================================================
# Job 1: daily-bq-refresh
# Every day at 06:00 UTC
# ============================================================
info "Creating job 1/3: daily-bq-refresh"
run "gcloud scheduler jobs create pubsub daily-bq-refresh \
  --schedule='0 6 * * *' \
  --time-zone='UTC' \
  --topic='projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}' \
  --message-body='{\"action\": \"refresh_kpis\"}' \
  --description='Daily BigQuery KPI table refresh at 06:00 UTC' \
  --location='${REGION}' \
  --project='${PROJECT_ID}' \
  2>/dev/null || gcloud scheduler jobs update pubsub daily-bq-refresh \
    --schedule='0 6 * * *' \
    --time-zone='UTC' \
    --topic='projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}' \
    --message-body='{\"action\": \"refresh_kpis\"}' \
    --location='${REGION}' \
    --project='${PROJECT_ID}'"

echo ""

# ============================================================
# Job 2: weekly-kpi-export
# Every Monday at 07:00 UTC
# ============================================================
info "Creating job 2/3: weekly-kpi-export"
run "gcloud scheduler jobs create pubsub weekly-kpi-export \
  --schedule='0 7 * * 1' \
  --time-zone='UTC' \
  --topic='projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}' \
  --message-body='{\"action\": \"export_weekly_report\"}' \
  --description='Weekly KPI export every Monday at 07:00 UTC' \
  --location='${REGION}' \
  --project='${PROJECT_ID}' \
  2>/dev/null || gcloud scheduler jobs update pubsub weekly-kpi-export \
    --schedule='0 7 * * 1' \
    --time-zone='UTC' \
    --topic='projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}' \
    --message-body='{\"action\": \"export_weekly_report\"}' \
    --location='${REGION}' \
    --project='${PROJECT_ID}'"

echo ""

# ============================================================
# Job 3: monthly-cleanup
# 1st of each month at 03:00 UTC
# ============================================================
info "Creating job 3/3: monthly-cleanup"
run "gcloud scheduler jobs create pubsub monthly-cleanup \
  --schedule='0 3 1 * *' \
  --time-zone='UTC' \
  --topic='projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}' \
  --message-body='{\"action\": \"delete_old_partitions\"}' \
  --description='Monthly partition cleanup on 1st at 03:00 UTC' \
  --location='${REGION}' \
  --project='${PROJECT_ID}' \
  2>/dev/null || gcloud scheduler jobs update pubsub monthly-cleanup \
    --schedule='0 3 1 * *' \
    --time-zone='UTC' \
    --topic='projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}' \
    --message-body='{\"action\": \"delete_old_partitions\"}' \
    --location='${REGION}' \
    --project='${PROJECT_ID}'"

echo ""
echo "============================================================"
echo " Cloud Scheduler jobs configured (3/3 — free tier used completely)"
echo " Console: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
echo "============================================================"
echo ""
echo "# COST REMINDER: Cloud Scheduler free tier = 3 jobs/month."
echo "# This script creates exactly 3 jobs. Do not add more without checking billing."
