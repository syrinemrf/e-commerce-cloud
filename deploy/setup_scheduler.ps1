param([switch]$DryRun)
$PROJECT_ID = "ecommerce-pipeline-493520"
$REGION = "europe-west1"
$PUBSUB_TOPIC = "orders-realtime"

function Invoke-Step([string]$Desc, [scriptblock]$Block) {
    if ($DryRun) { Write-Host "[DRY-RUN] $Desc" -ForegroundColor Yellow; return }
    Write-Host "[EXEC] $Desc" -ForegroundColor Cyan
    try { & $Block } catch { Write-Warning "Warning: $_" }
}

Write-Host "Cloud Scheduler setup - exactly 3 jobs (free tier limit)" -ForegroundColor Green
Write-Host ""

# Job 1: daily-bq-refresh - every day at 06:00 UTC
Invoke-Step "Create job 1/3: daily-bq-refresh" {
    gcloud scheduler jobs create pubsub daily-bq-refresh `
        --schedule="0 6 * * *" --time-zone="UTC" `
        --topic="projects/$PROJECT_ID/topics/$PUBSUB_TOPIC" `
        --message-body='{"action":"refresh_kpis"}' `
        --description="Daily BQ KPI refresh 06:00 UTC" `
        --location=$REGION --project=$PROJECT_ID 2>$null
}

# Job 2: weekly-kpi-export - every Monday at 07:00 UTC
Invoke-Step "Create job 2/3: weekly-kpi-export" {
    gcloud scheduler jobs create pubsub weekly-kpi-export `
        --schedule="0 7 * * 1" --time-zone="UTC" `
        --topic="projects/$PROJECT_ID/topics/$PUBSUB_TOPIC" `
        --message-body='{"action":"export_weekly_report"}' `
        --description="Weekly KPI export Monday 07:00 UTC" `
        --location=$REGION --project=$PROJECT_ID 2>$null
}

# Job 3: monthly-cleanup - 1st of each month at 03:00 UTC
Invoke-Step "Create job 3/3: monthly-cleanup" {
    gcloud scheduler jobs create pubsub monthly-cleanup `
        --schedule="0 3 1 * *" --time-zone="UTC" `
        --topic="projects/$PROJECT_ID/topics/$PUBSUB_TOPIC" `
        --message-body='{"action":"delete_old_partitions"}' `
        --description="Monthly cleanup 1st at 03:00 UTC" `
        --location=$REGION --project=$PROJECT_ID 2>$null
}

Write-Host ""
Write-Host "3/3 jobs configured - free tier fully used" -ForegroundColor Green
Write-Host "Console: https://console.cloud.google.com/cloudscheduler?project=$PROJECT_ID"
Write-Host "COST REMINDER: 3 jobs = free tier max. Do not add more." -ForegroundColor Yellow