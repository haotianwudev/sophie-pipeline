---
name: sophie-etl-tracker
description: Inspects, monitors, and verifies the health and status of all Sophie cloud ETL pipelines, Cloud Scheduler triggers, Cloud Run Jobs, GCS archives, and database freshness.
---

# Sophie Cloud ETL Status & Health Tracker

When invoked, execute the following operational checks to diagnose and report the real-time status of all cloud ETL jobs, schedules, archives, and database ingestion in the `longsky` GCP project and Neon/Postgres database.

---

## Step 1: Check Cloud Scheduler Status

Check whether the scheduled trigger is `ENABLED` and configured with the correct schedule:

```powershell
gcloud scheduler jobs list --project longsky --location us-central1 --format="table(ID,STATE,SCHEDULE,TIME_ZONE,TARGET_TYPE)"
```

- Verify `spx-snapshot-etl-trigger` is in the `ENABLED` state.
- Verify the schedule is `0 17 * * 1-5` and time zone is `America/New_York`.

---

## Step 2: Check Cloud Run Job Executions

Check the recent execution runs of the `spx-snapshot-etl` job:

```powershell
gcloud run jobs executions list --job spx-snapshot-etl --project longsky --region us-central1 --limit 5 --format="table(EXECUTION,RUNNING,COMPLETE,CREATED,RUN_BY)"
```

- Confirm the latest execution status shows `1 / 1` completed.
- Note any failed executions or retries.

---

## Step 3: Inspect Logs & Gap Detection

Read the last execution log to confirm successful completion and verify no data gap warnings:

```powershell
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=spx-snapshot-etl" --project longsky --limit 20 --format="value(textPayload)"
```

- Check for the JSON payload summary (e.g., `{"status": "ok", "biz_date": "YYYY-MM-DD", "spot": ..., "contracts_stored": ..., "archive": "..."}`).
- Ensure `"warning": null` and that no `GAP:` warning was raised.

---

## Step 4: Verify GCS Option Archive Storage

Verify the latest Parquet partition was archived to Google Cloud Storage:

```powershell
gcloud storage ls --project longsky gs://sophie-option-archive/**
```

- Confirm the parquet file for the most recent trading session exists under `spx/chain/year=YYYY/month=MM/spx_chain_YYYY-MM-DD.parquet`.

---

## Step 5: Verify Live Cloud Services

Check that all supporting web services are in a `READY` state:

```powershell
gcloud run services list --project longsky --region us-central1 --format="table(SERVICE,REGION,URL,STATUS)"
```

- Confirm `spx-options-api` and `ai-stock-suggestion-server` are healthy.

---

## Step 6: Output Consolidated Report

Synthesize the findings into a clear, structured operational report:

1. **Overall Pipeline Status**: | Healthy / Warning / Down
2. **Latest Ingestion Date (`biz_date`)**: Confirm date and contract counts.
3. **Trigger Status**: Scheduler state and next expected trigger window.
4. **Archive Status**: Confirmed GCS parquet archive path and size.
5. **Action Items / Diagnostics**: Any failed jobs, missing sessions, or maintenance needed.
