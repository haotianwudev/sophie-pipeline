# Google Cloud Run One-Click Deployment Script (PowerShell)
param (
    [string]$Project = "",
    [string]$Region = "us-central1",
    [string]$ServiceName = "spx-options-api"
)

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Deploying SPX Options API to Cloud Run   " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

$cmd = "gcloud run deploy $ServiceName --source . --region $Region --allow-unauthenticated --memory 512Mi --concurrency 80 --min-instances 0 --max-instances 2"

if ($Project -ne "") {
    $cmd += " --project $Project"
}

Write-Host "Executing: $cmd" -ForegroundColor Yellow
Invoke-Expression $cmd
