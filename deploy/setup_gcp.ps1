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
    try { & $Block } catch { Write-Warning "Step warning: $_" }
}

function Show-Section([string]$Title) {
    Write-Host ""
    Write-Host "--- $Title ---" -ForegroundColor Green
    Write-Host ""
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

Show-Section "8. Create BigQuery dataset $DATASET"
Invoke-Step "Create dataset" {
    bq --location=EU mk --dataset --description="E-commerce analytics GCP free tier" --project_id=$PROJECT_ID "${PROJECT_ID}:${DATASET}" 2>$null
}

Show-Section "9. Upload cleaned CSVs to GCS"
if (Test-Path $CLEAN_DIR) {
    Get-ChildItem -Path $CLEAN_DIR -Filter "*.csv" | ForEach-Object {
        $f = $_.FullName; $n = $_.Name
        Invoke-Step "Upload $n" { gcloud storage cp $f "gs://$BUCKET/raw/$n" }
    }
} else {
    Write-Warning "Clean dir not found at $CLEAN_DIR. Run prepare_data.py first."
}

Show-Section "10. Summary"
Write-Host "  GCS Bucket       : gs://$BUCKET"
Write-Host "  Pub/Sub Topic    : $PUBSUB_TOPIC"
Write-Host "  Pub/Sub DLQ      : $PUBSUB_TOPIC_DLQ"
Write-Host "  BigQuery Dataset : ${PROJECT_ID}:${DATASET}"
Write-Host ""
Write-Host "  Consoles:"
Write-Host "  Storage  : https://console.cloud.google.com/storage/browser/$BUCKET"
Write-Host "  BigQuery : https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
Write-Host ""
Write-Host "COST REMINDER: All resources are within GCP Free Tier." -ForegroundColor Yellow
Write-Host "Never enable Dataflow, Compute Engine or Cloud Run - they are NOT free." -ForegroundColor Yellow