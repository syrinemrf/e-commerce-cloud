#!/usr/bin/env bash
# functions/process_upload/deploy_function.sh
# Purpose : Deploy the process_upload Cloud Function gen2 to GCP.
# Author  : ProjetCloud Team
# Date    : 2024-06-01
#
# COST NOTE: Cloud Functions gen2 free tier = 2M invocations/month + 400k GB-seconds.
# Memory capped at 256MB (not 512) to conserve quota.
# Max-instances=3 prevents runaway scaling.
# Timeout=120s to stay within reasonable bounds.
#
# Usage:
#   bash functions/process_upload/deploy_function.sh

set -euo pipefail

PROJECT_ID="ecommerce-pipeline-493520"
REGION="europe-west1"
BUCKET="ecommerce-raw-${PROJECT_ID}"
DATASET="ecommerce_analytics"
PUBSUB_TOPIC="orders-realtime"
PUBSUB_TOPIC_DLQ="orders-realtime-dlq"
FUNCTION_NAME="process-upload"
FUNCTION_DIR="$(dirname "$0")"

echo "[INFO] Deploying Cloud Function: ${FUNCTION_NAME}"
echo "[INFO] Project : ${PROJECT_ID}"
echo "[INFO] Region  : ${REGION}"
echo "[INFO] Trigger : GCS bucket ${BUCKET} — object finalize"
echo ""

gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --runtime=python311 \
  --region="${REGION}" \
  --source="${FUNCTION_DIR}" \
  --entry-point=process_upload \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=${BUCKET}" \
  --memory=256MB \
  --timeout=120s \
  --max-instances=3 \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},DATASET=${DATASET},PUBSUB_TOPIC=${PUBSUB_TOPIC},PUBSUB_TOPIC_DLQ=${PUBSUB_TOPIC_DLQ}" \
  --project="${PROJECT_ID}"

echo ""
echo "[OK] Function deployed successfully."
echo "[INFO] Console: https://console.cloud.google.com/functions/details/${REGION}/${FUNCTION_NAME}?project=${PROJECT_ID}"
