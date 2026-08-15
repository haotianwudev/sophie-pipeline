#!/usr/bin/env bash
# Google Cloud Run One-Click Deployment Script (Bash)
set -e

SERVICE_NAME="${1:-spx-options-api}"
REGION="${2:-us-central1}"

echo "=========================================="
echo " Deploying SPX Options API to Cloud Run   "
echo "=========================================="

gcloud run deploy "$SERVICE_NAME" \
  --source . \
  --region "$REGION" \
  --allow-unauthenticated \
  --memory 512Mi \
  --concurrency 80 \
  --min-instances 0 \
  --max-instances 2
