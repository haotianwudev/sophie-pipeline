# Deploy the SPX option-chain snapshot ETL as a Cloud Run Job + Cloud Scheduler trigger.
#
# Run from anywhere; the script builds from the pipeline repo root, since the job imports from
# src/ and the Dockerfile lives there.
#
#   .\deploy.ps1 -Project <gcp-project> -Bucket <gcs-archive-bucket>
#
# Secrets are read from Secret Manager at runtime, never baked into the image. Create it once:
#   echo -n "postgresql://..." | gcloud secrets create sophie-database-url --data-file=-

param (
    [Parameter(Mandatory = $true)][string]$Project,
    [string]$Bucket = "",
    [string]$Region = "us-central1",
    [string]$JobName = "spx-snapshot-etl",
    [string]$DbSecret = "sophie-database-url",
    # The project's default compute service account. Deliberately not the App Engine
    # (@appspot) account the Cloud Scheduler docs reach for by default -- that one only exists
    # if App Engine was ever initialised, and in this project it never was.
    [string]$ServiceAccount = "282034489414-compute@developer.gserviceaccount.com",
    # 17:00 ET. SPX/SPXW trade until 16:15 ET -- index options close 15 minutes after the equity
    # market -- and the Cboe feed is 15-minute delayed on top of that, so nothing before ~16:30
    # sees a settled chain. 17:00 leaves margin: quotes are final and no longer moving, which
    # also means a retry fetches byte-identical data rather than a slightly different snapshot.
    # It is still comfortably ahead of the Global Trading Hours session (~20:15 ET), whose thin
    # overnight quotes would misrepresent the close.
    [string]$Schedule = "0 17 * * 1-5"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Deploying SPX Snapshot ETL (Cloud Run Job)" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  project : $Project"
Write-Host "  region  : $Region"
Write-Host "  source  : $RepoRoot"
Write-Host "  schedule: $Schedule  (America/New_York)"
Write-Host "  archive : $(if ($Bucket) { "gs://$Bucket" } else { '<disabled>' })"

Push-Location $RepoRoot
try {
    $envVars = "GCS_ARCHIVE_BUCKET=$Bucket"

    # --- 1. Build + deploy the job -------------------------------------------------------
    # A Job, not a Service: it runs to completion and exits, with retries and a long timeout,
    # and needs no HTTP listener.
    $deploy = @(
        "gcloud run jobs deploy $JobName",
        "--source .",
        "--project $Project",
        "--region $Region",
        "--tasks 1",
        "--max-retries 2",
        "--task-timeout 10m",
        "--memory 1Gi",
        "--set-env-vars `"$envVars`"",
        "--set-secrets `"DATABASE_URL=${DbSecret}:latest`""
    ) -join " "

    Write-Host "`n> $deploy" -ForegroundColor Yellow
    Invoke-Expression $deploy

    # --- 2. Let the scheduler's identity actually start the job ---------------------------
    $invoker = @(
        "gcloud run jobs add-iam-policy-binding $JobName",
        "--project $Project",
        "--region $Region",
        "--member `"serviceAccount:$ServiceAccount`"",
        "--role roles/run.invoker"
    ) -join " "
    Write-Host "`n> $invoker" -ForegroundColor Yellow
    Invoke-Expression $invoker

    # --- 3. Schedule it -------------------------------------------------------------------
    # --schedule-timezone is not cosmetic. A UTC cron drifts an hour at each DST boundary,
    # which would silently shift the snapshot time twice a year -- and the entire value of this
    # series is that every row is sampled at the same point in the session.
    $sched = @(
        "gcloud scheduler jobs create http $JobName-trigger",
        "--project $Project",
        "--location $Region",
        "--schedule `"$Schedule`"",
        # `--time-zone`, not `--schedule-timezone`: the latter is the Cloud Run Jobs flag, and
        # gcloud scheduler rejects it. Either way this must be set explicitly -- a UTC cron would
        # drift an hour at each DST boundary and silently move the sample point twice a year.
        "--time-zone `"America/New_York`"",
        "--uri `"https://$Region-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$Project/jobs/${JobName}:run`"",
        "--http-method POST",
        "--oauth-service-account-email `"$ServiceAccount`""
    ) -join " "

    # create-or-update: redeploys are routine, and a second `create` errors with ALREADY_EXISTS,
    # which would fail the whole script for a no-op.
    $exists = $null
    try {
        $exists = gcloud scheduler jobs describe "$JobName-trigger" --project $Project --location $Region --format="value(name)" 2>$null
    } catch { $exists = $null }

    if ($exists) {
        $sched = $sched -replace "jobs create http", "jobs update http"
        Write-Host "`n  trigger exists - updating in place" -ForegroundColor DarkGray
    }
    Write-Host "`n> $sched" -ForegroundColor Yellow
    Invoke-Expression $sched

    Write-Host "`nDone. Run once by hand to verify:" -ForegroundColor Green
    Write-Host "  gcloud run jobs execute $JobName --region $Region --project $Project" -ForegroundColor Green
}
finally {
    Pop-Location
}
