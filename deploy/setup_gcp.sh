#!/usr/bin/env bash
# deploy/setup_gcp.sh
# Purpose : Bootstrap all GCP free-tier resources for the ecommerce pipeline.
# Author  : ProjetCloud Team
# Date    : 2024-06-01
#
# COST NOTE: Only free-tier GCP services are enabled and created here.
# Dataflow, Compute Engine, Cloud Run and Cloud SQL are intentionally excluded.
# Total expected monthly cost: $0.
#
# Usage:
#   bash deploy/setup_gcp.sh            # execute all steps
#   bash deploy/setup_gcp.sh --dry-run  # print commands without executing

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------
PROJECT_ID="ecommerce-pipeline-493520"
REGION="europe-west1"
DATASET="ecommerce_analytics"
BUCKET="ecommerce-raw-${PROJECT_ID}"
PUBSUB_TOPIC="orders-realtime"
PUBSUB_TOPIC_DLQ="orders-realtime-dlq"
PUBSUB_SUB="orders-sub"
CLEAN_DIR="$(dirname "$0")/../data/clean"

# ---------------------------------------------------------------------------
# Dry-run flag
# ---------------------------------------------------------------------------
DRY_RUN=false
for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo -e "${YELLOW}[DRY-RUN]${NC} $*"
  else
    echo -e "${CYAN}[EXEC]${NC} $*"
    eval "$@"
  fi
}

info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; }
section() { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}\n"; }

# ---------------------------------------------------------------------------
# 1. Verify gcloud authentication
# ---------------------------------------------------------------------------
section "1. Verifying gcloud authentication"
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null | grep -q "@"; then
  error "No active gcloud account found."
  error "Run: gcloud auth login"
  exit 1
fi
ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1)
info "Active account: ${ACTIVE_ACCOUNT}"

# ---------------------------------------------------------------------------
# 2. Set active project
# ---------------------------------------------------------------------------
section "2. Setting active project: ${PROJECT_ID}"
run "gcloud config set project ${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 3. Enable required free-tier APIs only
# ---------------------------------------------------------------------------
section "3. Enabling free-tier APIs (NO Dataflow / Compute / Run / SQL)"
APIS=(
  "storage.googleapis.com"
  "bigquery.googleapis.com"
  "pubsub.googleapis.com"
  "cloudfunctions.googleapis.com"
  "cloudscheduler.googleapis.com"
  "logging.googleapis.com"
  "monitoring.googleapis.com"
  "cloudbuild.googleapis.com"
)
for api in "${APIS[@]}"; do
  info "Enabling ${api}..."
  run "gcloud services enable ${api} --project=${PROJECT_ID}"
done

# ---------------------------------------------------------------------------
# 4. Create GCS bucket with versioning
# ---------------------------------------------------------------------------
section "4. Creating GCS bucket: gs://${BUCKET}"
run "gcloud storage buckets create gs://${BUCKET} \
  --location=EU \
  --project=${PROJECT_ID} \
  --uniform-bucket-level-access \
  2>/dev/null || echo 'Bucket already exists — continuing'"
run "gcloud storage buckets update gs://${BUCKET} --versioning"
info "Versioning enabled on gs://${BUCKET}"

# ---------------------------------------------------------------------------
# 5. Create subdirectories in the bucket
# ---------------------------------------------------------------------------
section "5. Creating bucket subdirectory placeholders"
for subdir in raw/clients raw/orders raw/incidents raw/page_views; do
  run "echo '' | gcloud storage cp - gs://${BUCKET}/${subdir}/.keep"
done

# ---------------------------------------------------------------------------
# 6. Create main Pub/Sub topic + subscription
# ---------------------------------------------------------------------------
section "6. Creating Pub/Sub topic and subscription"
run "gcloud pubsub topics create ${PUBSUB_TOPIC} \
  --project=${PROJECT_ID} \
  2>/dev/null || echo 'Topic already exists — continuing'"

run "gcloud pubsub subscriptions create ${PUBSUB_SUB} \
  --topic=${PUBSUB_TOPIC} \
  --ack-deadline=60 \
  --message-retention-duration=600s \
  --project=${PROJECT_ID} \
  2>/dev/null || echo 'Subscription already exists — continuing'"

# ---------------------------------------------------------------------------
# 7. Create DLQ Pub/Sub topic
# ---------------------------------------------------------------------------
section "7. Creating dead-letter topic: ${PUBSUB_TOPIC_DLQ}"
run "gcloud pubsub topics create ${PUBSUB_TOPIC_DLQ} \
  --project=${PROJECT_ID} \
  2>/dev/null || echo 'DLQ topic already exists — continuing'"

# ---------------------------------------------------------------------------
# 8. Create BigQuery dataset
# ---------------------------------------------------------------------------
section "8. Creating BigQuery dataset: ${DATASET}"
run "bq --location=EU mk \
  --dataset \
  --description='E-commerce analytics dataset — GCP free tier project' \
  --project_id=${PROJECT_ID} \
  ${PROJECT_ID}:${DATASET} \
  2>/dev/null || echo 'Dataset already exists — continuing'"

# ---------------------------------------------------------------------------
# 9. Upload cleaned data to GCS
# ---------------------------------------------------------------------------
section "9. Uploading cleaned data to gs://${BUCKET}/raw/"
if [[ -d "${CLEAN_DIR}" ]]; then
  for file in "${CLEAN_DIR}"/*.csv; do
    [[ -f "$file" ]] || continue
    filename=$(basename "$file")
    run "gcloud storage cp '${file}' gs://${BUCKET}/raw/${filename}"
  done
else
  warn "Clean data directory not found at ${CLEAN_DIR} — skipping upload"
  warn "Run scripts/prepare_data.py first, then re-run this script"
fi

# ---------------------------------------------------------------------------
# 10. Print colored summary
# ---------------------------------------------------------------------------
section "10. Resource Summary"
echo -e "${GREEN}${BOLD}Resources created successfully:${NC}"
echo -e "  ${CYAN}GCS Bucket${NC}       : gs://${BUCKET}"
echo -e "  ${CYAN}Pub/Sub Topic${NC}    : projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}"
echo -e "  ${CYAN}Pub/Sub DLQ${NC}      : projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC_DLQ}"
echo -e "  ${CYAN}Pub/Sub Sub${NC}      : projects/${PROJECT_ID}/subscriptions/${PUBSUB_SUB}"
echo -e "  ${CYAN}BigQuery Dataset${NC} : ${PROJECT_ID}:${DATASET}"
echo ""
echo -e "  ${CYAN}GCP Console URLs:${NC}"
echo -e "  Storage  : https://console.cloud.google.com/storage/browser/${BUCKET}"
echo -e "  Pub/Sub  : https://console.cloud.google.com/cloudpubsub/topic/list?project=${PROJECT_ID}"
echo -e "  BigQuery : https://console.cloud.google.com/bigquery?project=${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 11. Cost reminder
# ---------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}${BOLD}║  COST REMINDER: All resources created are within GCP Free   ║${NC}"
echo -e "${YELLOW}${BOLD}║  Tier. Never enable Dataflow, Compute Engine or Cloud Run   ║${NC}"
echo -e "${YELLOW}${BOLD}║  to keep costs at \$0.                                       ║${NC}"
echo -e "${YELLOW}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
