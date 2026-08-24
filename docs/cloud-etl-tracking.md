# Cloud ETL Tracking & Monitoring Guide

This document describes how to track, inspect, and verify the health of all Sophie pipeline cloud ETL jobs and schedules.

---

## 1. Quick Status Check (Using the Skill)

You can ask the agent at any time:
> *"Track cloud ETL status"* or *"Check if SPX snapshot landed today"*

The `sophie-etl-tracker` skill automatically queries GCP Cloud Scheduler, Cloud Run Jobs, GCS Parquet archives, and Cloud Logging to provide an instant health summary.

---

## 2. Cloud Infrastructure Map

| Component | Resource Name | Schedule / Role |
| :--- | :--- | :--- |
| **Cloud Scheduler** | `spx-snapshot-etl-trigger` | `0 17 * * 1-5` (17:00 ET, Mon-Fri) |
| **Cloud Run Job** | `spx-snapshot-etl` | Captures Cboe SPX chain & writes Postgres |
| **GCS Archive** | `gs://sophie-option-archive` | Full 28k+ contract Parquet archive |
| **Secret Manager** | `sophie-database-url` | Neon PostgreSQL connection string |
| **API Service** | `spx-options-api` | Live SPX viewer HTTP service |

---

## 3. CLI Commands for Operations

### A. Check Scheduler Status
```powershell
gcloud scheduler jobs list --project longsky --location us-central1
```

### B. Trigger a Manual Cloud Execution
```powershell
gcloud run jobs execute spx-snapshot-etl --project longsky --region us-central1
```

### C. View Execution Logs
```powershell
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=spx-snapshot-etl" --project longsky --limit 15
```

---

## 4. Gap Detection & Alerts

Each snapshot run automatically verifies that the previous NYSE session exists in the database. If a session was missed, a `GAP: ` warning is logged.
