param([switch]$DryRun)
$ErrorActionPreference = "Stop"
$PROJECT_ID = "ecommerce-pipeline-493520"
$REGION = "europe-west1"
$DATASET = "ecommerce_analytics"
$BUCKET = "ecommerce-raw-ecommerce-pipeline-493520"
$PUBSUB_TOPIC = "orders-realtime"
$PUBSUB_TOPIC_DLQ = "orders-realtime-dlq"
$PUBSUB_SUB = "orders-sub"
$CLEAN_DIR = Join-Path $PSScriptRoot "..\data\clean"

function Invoke-Step([string]$Desc, [scriptblock]$Block) {
    if ($DryRun) { Write-Host "[DRY-RUN] $Desc" -ForegroundColor Yellow; return }
    Write-Host "[EXEC] $Desc" -ForegroundColor Cyan
    try { & $Block } catch { Write-Warning "Warning: $_" }
}

function Show-Section([string]$Title) {
    Write-Host ""; Write-Host "--- $Title ---" -ForegroundColor Green; Write-Host ""
}

Show-Section "1. Verify gcloud auth"
$acct = (gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>$null) | Select-Object -First 1
if (-not $acct) { Write-Error "No active gcloud account. Run: gcloud auth login"; exit 1 }
Write-Host "Active account: $acct"

Show-Section "2. Set project $PROJECT_ID"
Invoke-Step "gcloud config set project $PROJECT_ID" {
    gcloud config set project $PROJECT_ID
}

Show-Section "3. Enable free-tier APIs"
@("storage.googleapis.com","bigquery.googleapis.com","pubsub.googleapis.com",
  "cloudfunctions.googleapis.com","cloudscheduler.googleapis.com",
  "logging.googleapis.com","monitoring.googleapis.com","cloudbuild.googleapis.com") | ForEach-Object {
    $api = $_
    Invoke-Step "Enable $api" { gcloud services enable $api --project=$PROJECT_ID }
}

Show-Section "4. Create GCS bucket gs://$BUCKET"
Invoke-Step "Create bucket" {
    gcloud storage buckets create "gs://$BUCKET" --location=EU --project=$PROJECT_ID --uniform-bucket-level-access 2>$null
}
Invoke-Step "Enable versioning" {
    gcloud storage buckets update "gs://$BUCKET" --versioning
}

Show-Section "5. Create bucket subdirs"
@("raw/clients","raw/orders","raw/incidents","raw/page_views") | ForEach-Object {
    $sub = $_
    Invoke-Step "Create placeholder $sub/.keep" {
        "" | gcloud storage cp - "gs://$BUCKET/$sub/.keep"
    }
}

Show-Section "6. Create Pub/Sub topic + subscription"
Invoke-Step "Create topic $PUBSUB_TOPIC" {
    gcloud pubsub topics create $PUBSUB_TOPIC --project=$PROJECT_ID 2>$null
}
Invoke-Step "Create subscription $PUBSUB_SUB" {
    gcloud pubsub subscriptions create $PUBSUB_SUB --topic=$PUBSUB_TOPIC --ack-deadline=60 --message-retention-duration=600s --project=$PROJECT_ID 2>$null
}

Show-Section "7. Create DLQ topic $PUBSUB_TOPIC_DLQ"
Invoke-Step "Create DLQ topic" {
    gcloud pubsub topics create $PUBSUB_TOPIC_DLQ --project=$PROJECT_ID 2>$null
}

Show-Section "8. Create BigQuery dataset $DATASET (via Python SDK - no bq CLI wizard)"
Invoke-Step "Create dataset via Python" {
    python -c @"
from google.cloud import bigquery
bq = bigquery.Client(project='$PROJECT_ID')
ds = bigquery.Dataset('${PROJECT_ID}.${DATASET}')
ds.location = 'EU'
bq.create_dataset(ds, exists_ok=True)
print('Dataset OK:', ds.dataset_id)
"@
}

Show-Section "9. Upload cleaned CSVs to GCS (via Python SDK)"
Invoke-Step "Upload CSVs via Python" {
    python -c @"
from google.cloud import storage
import pathlib
gcs = storage.Client(project='$PROJECT_ID')
bucket = gcs.bucket('$BUCKET')
clean = pathlib.Path(r'$CLEAN_DIR')
if clean.exists():
    for csv in sorted(clean.glob('*.csv')):
        bucket.blob(f'raw/{csv.name}').upload_from_filename(str(csv))
        print('Uploaded:', csv.name)
else:
    print('WARN: clean dir not found at', clean)
"@
}

Show-Section "10. Summary"
Write-Host "  GCS Bucket       : gs://$BUCKET"
Write-Host "  Pub/Sub Topic    : $PUBSUB_TOPIC"
Write-Host "  Pub/Sub DLQ      : $PUBSUB_TOPIC_DLQ"
Write-Host "  BigQuery Dataset : ${PROJECT_ID}:${DATASET}"
Write-Host ""
Write-Host "  Storage  : https://console.cloud.google.com/storage/browser/$BUCKET"
Write-Host "  BigQuery : https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
Write-Host ""
Write-Host "COST REMINDER: All resources are within GCP Free Tier." -ForegroundColor Yellow
Write-Host "Never enable Dataflow, Compute Engine or Cloud Run." -ForegroundColor Yellow