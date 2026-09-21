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
| **Cloud Scheduler** | `vol-regime-etl-trigger` | `30 17 * * 1-5` (17:30 ET, Mon-Fri) |
| **Cloud Run Job** | `vol-regime-etl` | Refreshes SPX/VIX in `prices`, recomputes VRP/regime, plus SqueezeMetrics DIX/GEX and Cboe DSPX |
| **GCS Archive** | `gs://sophie-option-archive` | Full 28k+ contract Parquet archive |
| **Secret Manager** | `sophie-database-url` | Neon PostgreSQL connection string |
| **Secret Manager** | `sophie-fred-api-key` | FRED key — VIX3M (`VXVCLS`) term structure |
| **API Service** | `spx-options-api` | Live SPX viewer HTTP service |

The two ETL jobs are deliberately separate rather than one combined run: the option snapshot
captures data that cannot be re-fetched, while the vol-regime job is fully self-healing on trailing
windows. See [vol-regime-etl.md § 1](vol-regime-etl.md#1-why-this-is-a-separate-job-from-the-option-snapshot).

> **Not covered by any cloud schedule:** the weekly Windows task `sophie-pipeline-upload`
> (Fridays 19:00) still runs `src/run_uploads_tickers_free.py` — AAPL/MSFT/NVDA, investment clock,
> quant trending. That one depends on the workstation being awake.

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

---

## 5. Is the local archive keeping up?

The cloud archive is only half of what backtests depend on: they read a **local copy** (`data/spx_chain_unified`),
which nothing updates automatically. Compare the newest day on each side:

```powershell
gsutil ls gs://sophie-option-archive/spx/chain/year=2026/month=09/
Get-ChildItem F:\workspace\sophie-pipeline\data\spx_chain_unified\year=2026\month=09 | Sort-Object Name | Select-Object -Last 3
```

- Bucket newer than local → a **sync gap**, recoverable: pull it (manual procedure and proposed automation in
  `docs/spx-chain-local-sync.md`).
- A session present in neither → a **capture gap**, permanent: look at that date in Steps 2–3 of the tracker.
- The research viewer's *Data availability* page shows the local side (sessions behind, gaps, per-day contents).

`gsutil` on the workstation lives in `C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin` and may not be on
`PATH` inside a scheduled or non-interactive shell.
