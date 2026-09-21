# Keeping the local SPX chain archive in sync with the live capture

**Status: design only — not built.** Written 2026-09-20 after investigating why the local archive was six
sessions behind. Nothing here changes the ETL, the bucket or the local archive. Every fact in section 2 was
checked that day with read-only commands (listed alongside), so it can be re-checked rather than trusted.

Owner of the local archive and its schema writer: this repo (`src/tools/spx_chain_schema.py`,
`spx-option-snapshot/`). Consumers: `sophie-option-research` (backtests, and its viewer's `/data` page).

---

## 1. The problem

The live ETL captures the full Cboe chain to GCS once per session. The unified archive that backtests read
(`data/spx_chain_unified`, `year=/month=/day=/chain.parquet`) is a **local** copy, and today nothing keeps it
current: someone has to pull the bucket and run the reshape script by hand. The viewer's data page flagged the
archive as 6 sessions stale on 2026-09-20.

Two different failures look the same from the local side, and the right response differs:

| Failure | Where the data is | Recoverable? |
| :--- | :--- | :--- |
| **Sync gap** — captured, not yet pulled locally | in the bucket | yes, pull it |
| **Capture gap** — the ETL never recorded that session | nowhere | **no** (a chain ceases to exist after the session; see `spx-option-snapshot-etl.md` §1) |

A sync design must therefore (a) make sync gaps self-heal and (b) detect capture gaps and shout, because those
are the only ones that are permanent.

---

## 2. What was verified (2026-09-20)

| Fact | How to re-check |
| :--- | :--- |
| **The lag was a sync gap.** The bucket held all six sessions the local archive lacked (2026-09-11, 14, 15, 16, 17, 18). 20 session files in the bucket, none missing since the first live day 2026-08-21. | `gsutil ls gs://sophie-option-archive/spx/chain/year=2026/month=09/` |
| **Files land about 17:01–17:06 ET**, matching the 17:00 ET scheduler, on each of the last 8 sessions checked. | `gsutil ls -L -r gs://sophie-option-archive/spx/chain/` → `Creation time` |
| **The whole bucket is tiny:** 20 files, about 27 MiB (about 1.4 MB a day). Cost is not a design factor. | `gsutil du -s -h gs://sophie-option-archive/spx/chain/` |
| **The bucket only holds live days (2026-08-21 onward).** The ThetaData backfill and the OptionsDX-derived history exist only locally. | `gsutil ls gs://sophie-option-archive/spx/` |
| **No lifecycle rule; versioning is suspended.** Nothing expires, but an object overwritten in place is gone, so the local copy is the only record of what was captured before an overwrite. | `gsutil lifecycle get gs://sophie-option-archive`, `gsutil versioning get gs://sophie-option-archive` |
| **The 14 files already pulled are byte-identical to the bucket** (MD5 match). Nothing upstream was rewritten. | compare `Hash (md5)` in `gsutil ls -L` with the local file's base64 MD5 |
| **`gsutil rsync` cannot tell which files are current.** A dry run (`-n`) proposed re-copying all 20 files, including the 14 identical ones, because the pulled files carry no preserved modification time and it falls back to hashing (it also warned that `crcmod` has no C extension). Harmless at this size, but it cannot be trusted to identify *new* or *changed* days. | `gsutil -m rsync -n -r gs://sophie-option-archive/spx/chain data/live_daily_gcs` |
| **rsync does keep the `year=/month=` prefixes.** The earlier `cp -r .../*` pull landed one file flat at the top level (recorded in the `spx-option-chain-unify` skill); the dry-run destinations were all correctly nested. | same dry run, read the `Would copy … to` paths |
| **`gsutil` is installed and authenticated** on the workstation (`C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin`). | any read-only `gsutil ls` |
| **Local-only, no second copy:** `spx_chain_unified` 2.3 GB, `spx_chain_archive` 0.9 GB, `raw_theta` 2.2 GB, OptionsDX raw `spx option/` 0.7 GB. The ThetaData free tier only reaches back about two years, so that backfill may not be re-fetchable. | `du -sh` on those directories; see the `spx-option-backfill` skill |

---

## 3. What exists today

- `spx-option-snapshot/reshape_live_gcs_to_unified.py` maps live files (read from `data/live_daily_gcs`) to the
  canonical 24-column schema, tagged `source='cboe_live'`, Cboe's own greeks untouched.
- `spx-option-snapshot/validate_unified.py` validates the **whole** archive (schema, date coverage against a
  `yfinance` calendar, source tags, open-interest transition, both boundary joins).

Limits that matter for automation (from reading the code, not from running it): the reshape script **reprocesses
every file each run** rather than only new ones; it **validates nothing per day**; `write_canonical_chain`
**writes straight to the final path**, so a reader (a backtest) can see a half-written day; and nothing records
when a sync last ran or what the bucket held at the time.

### Manual procedure until the sync exists

Not executed on 2026-09-20 (this session only read and dry-ran); taken from the scripts as written.

```powershell
# 1. mirror the bucket (over-copies, but correct; keeps the year=/month= layout)
gsutil -m rsync -r gs://sophie-option-archive/spx/chain F:\workspace\sophie-pipeline\data\live_daily_gcs
# 2. reshape into the unified archive (reprocesses all live days; overwrites their day folders)
poetry run python spx-option-snapshot/reshape_live_gcs_to_unified.py
# 3. check the result: the research viewer's /data page should show the archive up to date
```

---

## 4. Proposed design: `spx-option-snapshot/sync_live_chain.py`

One idempotent script, safe to run at any time, any number of times.

1. **Diff by checksum, not by rsync.** List the bucket (`name`, `size`, `md5`), compare with a local manifest
   (`data/live_daily_gcs/_manifest.json`: day → md5, size, pulled_at). Download only new or changed objects to a
   temporary file, verify the MD5, then move into place.
2. **A past day that changed upstream** (the ETL re-ran and overwrote it; versioning is off, so this would
   otherwise vanish silently): keep the old local copy in `_superseded/`, take the new one, and report it loudly.
   A backtest may already have used the old snapshot.
3. **Per-day validation before publishing** into the unified archive. A day must have: the 24 canonical columns,
   `biz_date` equal to the filename date, no contract expiring before `biz_date`, row and expiration counts not far
   below the previous session (the viewer's thin-day rule: under 50% of the local median), roots only `SPX`/`SPXW`,
   and no duplicate contracts.
4. **Atomic publish.** Write the day into a temporary directory and `os.replace` it onto
   `year=/month=/day=/`, so no reader ever sees a partial day. A day that fails validation goes to
   `_quarantine/` and the run exits non-zero; it is never published.
5. **Capture-gap check.** Compare bucket days with the trading calendar since 2026-08-21 and report any session
   with no object. These are the unrecoverable ones. The NYSE calendar is already implemented and validated
   against real SPX price days in `sophie-option-research/src/lab/api/availability.py`; reuse or copy it rather
   than adding a `yfinance` dependency.
6. **Write `data/spx_chain_unified/_control/sync_status.json`** on every run:

   ```json
   { "synced_at": "…", "ok": true, "auth_ok": true,
     "bucket_latest": "2026-09-18", "local_latest": "2026-09-18",
     "pulled": [], "changed": [], "quarantined": [], "capture_gaps": [] }
   ```

   An expired `gcloud` login must produce `auth_ok: false` and a distinct message, because a scheduled task that
   fails on auth otherwise fails silently and the archive just quietly ages.
7. **Trigger.** Windows Task Scheduler on weekdays at about 18:00 ET (files land 17:01–17:06 ET), set to run as
   soon as possible after a missed start, plus a manual `--now`. Because the data waits in the bucket, a missed
   sync costs nothing; the design property is **catch-up, not capture**.
8. **Viewer integration.** The research viewer's `/data` page and home strip read `sync_status.json` and show
   "last synced … / bucket newest …", so "the sync did not run" is distinguishable from "nothing newer exists
   upstream". No "Sync now" button at first: it would make the viewer spawn a process with cloud credentials, a
   larger step that would need the same guard as the removal routes.

Reshape stays a pure column mapping, per decision point 2 of the `spx-option-chain-unify` skill: Cboe's greeks are
never recomputed.

---

## 5. Open decisions (not yet made)

| Decision | Leaning | Why it is open |
| :--- | :--- | :--- |
| **Back up the local-only history** (unified, ThetaData archive and raw, OptionsDX raw) to another drive or bucket | yes | It is the only copy of data that may not be re-fetchable; but where to (a second disk, a different bucket prefix) is a cost and trust choice |
| **Refresh the SPX/VIX price caches** in the same run | yes | `data/market/*_daily.parquet` were 76 days stale on 2026-09-20 while backtests used the fresher Postgres `prices` table; the caches only matter offline, and the loader lives in the research repo |
| **Where the script lives** | this repo, `spx-option-snapshot/` | It owns the archive and its schema; the research repo only reads |
| **"Sync now" button in the viewer** | not initially | See section 4, item 8 |
| **Policy on a changed past day** | keep both, flag | Alternative is to refuse and alert; keeping both is less disruptive but needs a person to look |

---

## 6. Not in scope

The 35 sessions missing from the OptionsDX span cannot be filled on the free ThetaData tier (a closed decision,
recorded in the `spx-option-chain-unify` skill). Recomputing Cboe's greeks. Pointing any live consumer or Postgres
at the unified archive (still a separate decision).

## Related

- `docs/spx-option-snapshot-etl.md` — the capture side (why a missed run is unrecoverable, the archive layout).
- `docs/cloud-etl-tracking.md` and the `sophie-etl-tracker` skill — cloud-side health checks.
- `spx-option-chain-unify` skill — how the unified archive was built, schemas, validation, known data caveats.
- `option-research-viewer` skill — the `/data` page that shows the archive's freshness and gaps.
- Desk task `spx-chain-local-sync` — the queued work item for this design.
