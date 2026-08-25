# Deploy the volatility-regime / VRP ETL as a Cloud Run Job + Cloud Scheduler trigger.
#
#   .\deploy.ps1 -Project <gcp-project>
#
# Deliberately a SEPARATE job from spx-snapshot-etl rather than another step inside it. The
# option-chain snapshot captures data that cannot be re-fetched -- a missed run loses that
# session permanently. This job is fully self-healing: a 10-day price window and a 30-day
# regime rewrite mean a missed run is repaired by the next one. Merging them would let a Yahoo
# outage or a FRED timeout abort the process before the irreplaceable snapshot committed, and
# recovering that isolation inside one container means rebuilding what two jobs give for free.
#
# Secrets are read from Secret Manager at runtime, never baked into the image:
#   echo -n "postgresql://..." | gcloud secrets create sophie-database-url --data-file=-
#   echo -n "<fred key>"       | gcloud secrets create sophie-fred-api-key --data-file=-

param (
    [Parameter(Mandatory = $true)][string]$Project,
    [string]$Region = "us-central1",
    [string]$JobName = "vol-regime-etl",
    [string]$DbSecret = "sophie-database-url",
    [string]$FredSecret = "sophie-fred-api-key",
    [string]$ServiceAccount = "282034489414-compute@developer.gserviceaccount.com",
    # 17:30 ET, half an hour behind the option snapshot. Yahoo's daily bar is settled well before
    # this, and the offset keeps the two jobs off the same instant so a shared Postgres hiccup
    # cannot take both out at once. Mon-Fri: the ETL is idempotent, so a holiday run is a no-op
    # rather than an error, but there is no reason to pay for one.
    [string]$Schedule = "30 17 * * 1-5"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")

# ErrorActionPreference does not cover native executables: gcloud exiting non-zero sets
# $LASTEXITCODE but throws nothing, so without this a failed build would sail on to deploy a
# non-existent image, then cheerfully create the IAM binding and scheduler around it.
function Invoke-Step([string]$Cmd) {
    Write-Host "`n> $Cmd" -ForegroundColor Yellow
    Invoke-Expression $Cmd
    if ($LASTEXITCODE -ne 0) { throw "Command failed (exit $LASTEXITCODE): $Cmd" }
}
$Image = "$Region-docker.pkg.dev/$Project/cloud-run-source-deploy/${JobName}:latest"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Deploying Vol-Regime ETL (Cloud Run Job)" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  project : $Project"
Write-Host "  region  : $Region"
Write-Host "  source  : $RepoRoot"
Write-Host "  image   : $Image"
Write-Host "  schedule: $Schedule  (America/New_York)"

Push-Location $RepoRoot
try {
    # --- 1. Build the image ---------------------------------------------------------------
    # Explicit build rather than `run jobs deploy --source`, since that flag only finds a
    # Dockerfile at the context root and the snapshot job already owns that slot.
    $build = @(
        "gcloud builds submit .",
        "--project $Project",
        "--config services/vol-regime-etl/cloudbuild.yaml",
        "--substitutions _IMAGE=$Image"
    ) -join " "
    Invoke-Step $build

    # --- 2. Deploy the job ------------------------------------------------------------------
    # FRED_API_KEY is optional by design: without it the agent logs a warning and writes NULL
    # term_slope rather than failing, so the secret is only wired in when it exists.
    $secrets = "DATABASE_URL=${DbSecret}:latest"
    $fredExists = $null
    try {
        $fredExists = gcloud secrets describe $FredSecret --project $Project --format="value(name)" 2>$null
    } catch { $fredExists = $null }
    if ($fredExists) {
        $secrets = "$secrets,FRED_API_KEY=${FredSecret}:latest"
    } else {
        Write-Host "`n  $FredSecret not found - term structure will be NULL" -ForegroundColor DarkYellow
    }

    # A newly created secret carries no bindings, and Cloud Run refuses to deploy a revision that
    # references one it cannot read -- so grant before deploying, not after. Idempotent: re-adding
    # an existing binding is a no-op.
    foreach ($secret in @($DbSecret) + $(if ($fredExists) { @($FredSecret) } else { @() })) {
        Invoke-Step (@(
            "gcloud secrets add-iam-policy-binding $secret",
            "--project $Project",
            "--member `"serviceAccount:$ServiceAccount`"",
            "--role roles/secretmanager.secretAccessor"
        ) -join " ")
    }

    $deploy = @(
        "gcloud run jobs deploy $JobName",
        "--image $Image",
        "--project $Project",
        "--region $Region",
        "--tasks 1",
        "--max-retries 2",
        "--task-timeout 10m",
        "--memory 1Gi",
        "--set-secrets `"$secrets`""
    ) -join " "
    Invoke-Step $deploy

    # --- 3. Let the scheduler's identity actually start the job -----------------------------
    $invoker = @(
        "gcloud run jobs add-iam-policy-binding $JobName",
        "--project $Project",
        "--region $Region",
        "--member `"serviceAccount:$ServiceAccount`"",
        "--role roles/run.invoker"
    ) -join " "
    Invoke-Step $invoker

    # --- 4. Schedule it ---------------------------------------------------------------------
    $sched = @(
        "gcloud scheduler jobs create http $JobName-trigger",
        "--project $Project",
        "--location $Region",
        "--schedule `"$Schedule`"",
        # `--time-zone`, not `--schedule-timezone`: the latter is the Cloud Run Jobs flag and
        # gcloud scheduler rejects it. Setting it explicitly matters -- a UTC cron drifts an hour
        # at each DST boundary and would silently move the run twice a year.
        "--time-zone `"America/New_York`"",
        "--uri `"https://$Region-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$Project/jobs/${JobName}:run`"",
        "--http-method POST",
        "--oauth-service-account-email `"$ServiceAccount`""
    ) -join " "

    # create-or-update: redeploys are routine, and a second `create` errors with ALREADY_EXISTS,
    # failing the whole script for a no-op.
    $exists = $null
    try {
        $exists = gcloud scheduler jobs describe "$JobName-trigger" --project $Project --location $Region --format="value(name)" 2>$null
    } catch { $exists = $null }

    if ($exists) {
        $sched = $sched -replace "jobs create http", "jobs update http"
        Write-Host "`n  trigger exists - updating in place" -ForegroundColor DarkGray
    }
    Invoke-Step $sched

    Write-Host "`nDone. Run once by hand to verify:" -ForegroundColor Green
    Write-Host "  gcloud run jobs execute $JobName --region $Region --project $Project" -ForegroundColor Green
}
finally {
    Pop-Location
}
