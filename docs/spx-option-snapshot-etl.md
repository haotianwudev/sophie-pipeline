# SPX Option Snapshot ETL

Daily capture of the SPX option chain — surface metrics, per-strike positioning, and a full-chain
archive. Runs as a Cloud Run Job on a Cloud Scheduler trigger.

**Date**: August 23, 2026
**Agent**: `src/agents/spx_option_snapshot.py`
**Runner**: `spx-option-snapshot/run.py`
**Deploy**: `services/spx-snapshot-etl/deploy.ps1`
**Schema**: `sql/create_spx_option_snapshot_tables.sql`

---

## 1. Why this runs in the cloud

Every other ETL in this repo tolerates a missed run. `investment_clock` pulls FRED series that can
be re-fetched for any historical date, so a laptop that was asleep costs nothing but a delay.

**An option chain snapshot is not reproducible from this feed.** The chain quoted at 16:00 ET on a
given session ceases to exist when the session ends — Cboe publishes a live/delayed feed, not a
history. A missed run is a hole that no re-run can fill, not a late row.

(Repairable at a price: a vendor that recorded OPRA at the time can serve historical chains, which
is what `.claude/skills/spx-option-backfill/` exists to do for the pre-2026-08-21 gap. The point
stands for operations — capture cheaply now rather than buy it back later.)

That single property drives most of the design decisions below: the cloud scheduler, the holiday
guard, the gap check, the idempotent upserts, and above all the Parquet archive.

Everything time-comparative in the Options Viewer depends on this table existing:

| Consumer | Needs |
|---|---|
| Percentile ranks on skew / GEX | Daily summary history |
| Day-over-day open-interest deltas | Per-strike prior-session OI |
| Skew-vs-price divergence (Wall of Worry vs. Euphoria) | ~20 sessions of summary |
| Sticky Strike vs. Sticky Delta diagnostic | Prior session's full smile |

---

## 2. Data flow

```
Cboe delayed-quotes CDN  (_SPX.json — SPX + SPXW, greeks and IV included)
          │
          ▼
  src/tools/api_cboe.py :: fetch_cboe_spx_raw()      ← shared with the live viewer API
          │
          ▼
  src/agents/spx_option_snapshot.py :: build_snapshot()
          │
          ├── summary        one row   ──► spx_option_snapshot         (Postgres)
          ├── ±25% slice     ~7.5k rows ──► spx_option_chain_snapshot  (Postgres)
          └── full chain     ~28k rows ──► gs://<bucket>/spx/chain/…   (Parquet)
```

The feed already carries IV, delta, gamma, theta, vega and rho per contract, so nothing is solved
or modelled at ingest — the ETL stores what Cboe publishes and derives only aggregates from it.

---

## 3. What gets computed

### Surface metrics — measured on a constant-maturity cycle

Anchored to the expiration nearest **30 DTE**, not a fixed date. This matters more than it looks: a
fixed cycle rolls down over its life, so a series built on one would be measuring time decay rather
than the market. 30 days matches the convention VIX itself uses.

| Column | Definition |
|---|---|
| `atm_iv` | IV at the strike nearest spot (call/put averaged when both quote) |
| `put25_iv`, `call25_iv` | IV of the contract closest to ±25 delta |
| `rr25` | `put25_iv − call25_iv`, vol points |
| `fly25` | `(put25_iv + call25_iv)/2 − atm_iv` |
| `normalized_skew` | `rr25 / atm_iv` |

> **Sign convention**: `rr25` is **put minus call**, so a normal equity surface reads positive. This
> is the inverse of the FX/dealer convention used by most published RR series, where the same
> surface reads negative. Complacency shows up here as a *falling* value.

`normalized_skew` is the one that survives regime changes: 5 vol points of skew against a 12% ATM is
a heavy hedging bid, while the same 5 points against a 35% ATM is close to complacent. The viewer's
morphology bands (Forward Skew / Flattening / Normal Smirk / Elevated / Extreme) cut on this value,
not on raw points.

### Surface dynamics — Skew Stickiness Ratio

`atm_skew_slope` is the local smile slope at the money, `d(IV vol pts)/d(ln K/S)`, fit by least
squares over OTM quotes within ±5% of spot on the reference cycle. Three deliberate choices: a tight
band (the smile is curved, so a wide window measures curvature as much as slope); OTM quotes only
(the tradeable side of the spread on each wing); and ln-moneyness rather than strike, which keeps it
scale-free as the index drifts over years and dimensionally matches the `d(ln S)` below.

`ssr` divides the realised move in ATM IV per unit log-spot by the skew that was already in place
(the *prior* session's slope — the skew that existed when the move happened):

| SSR | Regime |
|---|---|
| ~ 0 | Sticky delta — the smile travelled with spot; ATM vol barely moved |
| ~ 1 | Sticky strike — the smile stayed pinned to strikes; ATM vol slid along the existing skew |
| > 1.3 | Repricing — vol moved further than the skew implied |
| < 0 | Inverted — ATM vol moved against the skew's implication; a vol-regime shift |

Verified against the canonical cases: a synthetic sticky-strike move returns exactly 1.0000, sticky
delta 0.0000, and a 2× repricing 2.0000.

**Why it earns a column**: the viewer's gamma-flip solver hardcodes the sticky-strike assumption
when repricing the book. SSR is the only measurement that says whether that assumption held on a
given day — a reading far from 1 means the published flip level is on shakier ground than its
precision implies.

`ssr` is null on the first stored session, and on any day the index moved less than ~0.1%: the ratio
divides by the spot change, so a flat tape turns quote noise in ATM IV into an enormous meaningless
number. Normalisation is platform-specific — some published SSR conventions scale sticky delta to 1
and sticky strike to 2, so do not cross-compare without checking.

### Book metrics — whole chain, every expiration

`pcr_volume`, `pcr_oi`, `total_volume`, `total_open_interest`, `net_gex_m`, `call_wall`, `put_wall`.

Scope is deliberate. A single front-month cycle can print negative net GEX while the whole book
prints strongly positive — opposite regime calls from identical data. Published provider levels are
whole-book, so these are too.

Two caveats carried in the code comments and worth repeating:

- **The dealer sign convention is an assumption.** Calls positive, puts negative is the standard
  "naive dealer" model shared with SqueezeMetrics and SpotGamma. Public OI cannot establish which
  side the dealer is actually on. It is the largest single source of model risk in `net_gex_m`.
- **Walls are gamma-weighted, not raw OI**, and constrained to their own side of spot. A deep
  ITM/OTM strike can hold enormous legacy OI while contributing almost no gamma.

### How `biz_date` is decided

**The Cboe timestamp is the feed's publication date, not the session's.** Outside market hours —
weekends, holidays, overnight — the feed keeps serving the last session's final quotes while
stamping them with the current calendar date. Taking that at face value files Friday's closing chain
under Saturday, which then reads as a phantom session in every day-over-day comparison.

`_resolve_biz_date()` therefore snaps the feed date back to the **most recent NYSE session on or
before it**. A run on Saturday or Sunday labels the data `2026-08-21` (Friday), which is when the
quotes were actually struck.

The holiday guard is deliberately checked against the **run date**, not `biz_date`. Since `biz_date`
always snaps to a real session it would never test as a non-session, and the skip would never fire.

### Deliberately not computed

**Gamma flip** is absent, and that is a considered omission rather than a gap. Computing it properly
means repricing every contract at candidate spot levels — and unlike everything else here, it is
fully recoverable later from `spx_option_chain_snapshot`, which stores gamma and OI per strike. It
is not one of the non-reproducible things this ETL exists to protect.

---

## 4. Storage layout and real sizes

Measured against the live chain (2026-08-21 close): **28,140 contracts across 55 expirations**.
The Postgres figures below are *observed* `pg_total_relation_size` after `VACUUM ANALYZE`, not
estimates.

| Destination | Grain | Per session | Per year (252) |
|---|---|---|---|
| `spx_option_snapshot` | 1 row | ~150 B | ~40 KB |
| `spx_option_chain_snapshot` | 8,922 rows | **1.16 MB** | **293 MB** |
| GCS Parquet (zstd) | 28,140 rows | **1.32 MB** | **332 MB** |
| *(full chain in Postgres — rejected)* | *~28,100 rows* | *3.7 MB* | *0.93 GB* |

Postgres costs **137 bytes/row** measured (816 kB heap + 368 kB index for 8,922 rows). The primary
key leads with `biz_date`, so date-range scans and day-over-day joins are covered without a
secondary index — each extra index would add roughly 0.4 MB/day.

> **Watch the free-tier ceiling.** The Neon database sat at **55 MB** when this shipped. At 293 MB/yr
> the slice alone reaches a 0.5 GB free-tier limit inside about 18 months. Two dials if that becomes
> pressing: narrowing `STRIKE_BAND` from 0.25 to 0.15, or raising `OI_SHARE_FLOOR` from 0.15 to 0.20
> (which alone drops it to ~240 MB/yr, at the cost of the far-dated quarterlies). The GCS archive is
> unaffected either way, so nothing is lost permanently by tightening.

**Why the split.** Postgres holds a third of the contracts for roughly the same bytes as the full
Parquet archive, and it is the expensive storage; Neon also bills storage-*time* including history
retention. Keeping the entire chain in GCS costs about **five cents a year** and removes the pressure
to guess today which metrics matter later — anything not precomputed can be recovered by reprocessing
the archive. The ±25% band and the key-expiration filter cover what the viewer reads per-strike, at
32% of the contract count.

(The archive is measured, not estimated: 1,383,037 bytes for the 2026-08-21 session. It runs larger
per row than a trimmed frame would because it preserves every field the Cboe feed publishes —
including string columns like `symbol` and `last_trade_time` that compress poorly. That is the
intended trade: the archive exists precisely so nothing has to be discarded up front.)

The archive is Hive-partitioned (`spx/chain/year=YYYY/month=MM/spx_chain_YYYY-MM-DD.parquet`) so it
stays queryable by date without a manifest.

### Which cycles are stored per-strike

Three rules, unioned, because they capture different things:

**Structural** — near-term dailies (DTE ≤ 2), the next 4 Friday weeklies, the next 4 standard
monthlies. These matter for flow and gamma even when resting OI is modest; 0DTE in particular
carries enormous volume against small open interest.

**Measured** — any cycle holding at least `OI_SHARE_FLOOR` (15%) of the busiest cycle's open
interest, wherever it falls on the calendar.

**Carried** — every cycle stored in the previous session that has not yet expired. See
[Why membership is sticky](#why-membership-is-sticky) below.

The measured rule exists because a purely structural filter systematically misses the far-dated
quarterlies where institutional hedges sit. The original implementation had a date-proximity "LEAPS
anchor" that picked whichever cycle sat nearest 365 DTE; on live data that selected **2027-08-20
(54k OI, 1% of the busiest cycle)** while skipping **2027-12-17 (1.19M OI, 22%)** — a 22× liquidity
miss caused by ranking on date proximity, the exact proxy this design warns against elsewhere.
Measuring instead of guessing fixes it and makes the anchor unnecessary: liquid far-dated cycles now
qualify on merit.

The 15% floor is the knee of the curve, measured on live data:

| Floor | Cycles | Book OI captured | Storage |
|---|---|---|---|
| structural only | 11 | 59.5% | 271 MB/yr |
| **≥ 15%** | **16** | **82.0%** | **293 MB/yr** |
| ≥ 10% | 18 | 85.4% | 387 MB/yr |
| ≥ 5% | 21 | 90.8% | 431 MB/yr |

Dropping to 10% buys 3 more percentage points for another ~90 MB/yr — sharply diminishing. When open
interest is unavailable the selection degrades to structural-only, which yields a smaller slice,
never a wrong one.

The remaining ~18% of book OI sits in cycles below the floor. They stay in the Parquet archive in
full, so widening the filter later is a reprocessing job rather than a lost opportunity.

### Why membership is sticky

Both rules above are *rolling*. The DTE ≤ 2 window turns over every session and `weeklies[:4]` turns
over every week, so on their own the stored set churns by 1–4 cycles a session:

```
2026-08-24 (Mon): +3 entering  -1 leaving
2026-08-25 (Tue): +1 entering  -1 leaving
2026-08-31 (Mon): +4 entering  -1 leaving
```

That churn is what breaks the day-over-day reads downstream. A cycle appearing for the first time has
no prior row to difference against, and the flow queries could not distinguish *"this cycle was not
stored yesterday"* from *"this cycle held no open interest yesterday"* — so its entire resting open
interest booked as a same-day build, which is exactly what genuine conviction looks like. On a
measured test where one 320-contract cycle (159,209 OI) was missing from the prior session, the
reported net OI change came out **3.4× inflated** and both sides flipped from CHURNING to BUILDING.

The carried rule makes membership **monotone over a cycle's life**: once stored, a cycle stays stored
until it expires. Exits become expiries only; entries become a one-time event per cycle. Verified
against the simulation above — every subsequent departure is an expiry, none is a drift.

Hysteresis cannot remove *entries*, because a cycle listing for the first time genuinely has no prior
row. That residue is handled on the read side instead: the GraphQL layer measures changes only over
contracts present in **both** sessions and reports the coverage as `comparableShare`
(`server/src/db/option-snapshot.js`). The two fixes are complementary — the ETL guarantees no
unexplained exits, the query tolerates the unavoidable entries. Neither is sufficient alone: a
skipped or failed run still leaves gaps that only the query-side guard survives.

Cost is bounded, since cycles leave by expiring rather than by drifting: a handful of extra cycles
carried past the point where they would otherwise qualify, in exchange for a table whose own history
is self-consistent.

### SPX vs. SPXW

Both roots are stored as **separate rows**, never merged. They are genuinely different products
sharing a calendar date with their own OI pools, and correct handling differs by consumer: an IV
surface must dedup to one contract per strike, while OI, volume and GEX must sum across both.
Storing both preserves that choice downstream — merging at ingest would destroy it irreversibly.

---

## 5. Schedule and timing

```
0 17 * * 1-5     America/New_York
```

**17:00 ET**, chosen to sit inside a window where the chain is provably finished moving:

| Time (ET) | State |
|---|---|
| 16:00 | Equity market closes — **SPX options keep trading** |
| 16:15 | SPX/SPXW regular session closes (expiring PM-settled contracts stop at 16:00) |
| ~16:30 | Earliest the 15-minute delayed feed reflects the final chain |
| **17:00** | **Snapshot — quotes settled, nothing further to change** |
| ~20:15 | Global Trading Hours opens; thin overnight quotes resume |

Index options close 15 minutes after the equity market, so any fire before ~16:30 captures a chain
that is still live and mislabels it as a close. 17:00 leaves margin on that, and stops well short of
the overnight session whose thin quotes would misrepresent the close in the other direction.

A useful side effect: because the feed is static by then, a retry fetches byte-identical data rather
than a slightly different snapshot — so the job's retry is idempotent in *content*, not just in
database semantics.

Whatever time is chosen, it must never move — day-over-day comparability is the entire point of the
series.

**`--schedule-timezone America/New_York` is not cosmetic.** A UTC cron drifts by an hour at each DST
boundary, silently shifting the sample point twice a year with no error raised. Cloud Scheduler
handles DST natively when the timezone is set explicitly.

---

## 6. Deployment

### Provisioned configuration

Everything below is already created in GCP as of 2026-08-23. Recorded here so it does not have to be
rediscovered from the console.

| Item | Value |
|---|---|
| GCP project | **`longsky`** (number `282034489414`) — same project as `spx-options-api` |
| Region | `us-central1` |
| Archive bucket | **`gs://sophie-option-archive`** (US-CENTRAL1, standard, uniform access) |
| Secret | **`sophie-database-url`** — holds `DATABASE_URL`, version 1 |
| Runtime service account | `282034489414-compute@developer.gserviceaccount.com` |
| SA grants | `roles/secretmanager.secretAccessor` on the secret; `roles/storage.objectAdmin` on the bucket |
| Database | Neon `neondb`, PostgreSQL 17.11 (pooler endpoint) |
| APIs enabled | `run`, `cloudbuild`, `storage`, `secretmanager`, `cloudscheduler` |

Schema was applied 2026-08-23; both tables exist. A seed row for **2026-08-21** (Friday's close) was
written by hand so the frontend has something to render before the first scheduled run.

To rotate the DB credential later:

```bash
printf '%s' "postgresql://…" | gcloud secrets versions add sophie-database-url --data-file=- --project longsky
```

### Deploying the job

```powershell
cd F:/workspace/sophie-pipeline/services/spx-snapshot-etl
.\deploy.ps1 -Project longsky -Bucket sophie-option-archive
```

Creates a Cloud Run **Job** (not a Service — it runs to completion and exits, with retries and no
HTTP listener) plus the Scheduler trigger. Lives in the same project and region as the existing
`spx-options-api` service.

Both flags have defaults matching the provisioned values above, so the bare `.\deploy.ps1
-Project longsky -Bucket sophie-option-archive` is the whole command.

The image is built from the repo-root `Dockerfile`, which copies only the modules the job imports
rather than the whole `src/` tree, and installs `services/spx-snapshot-etl/requirements.txt` — a
minimal closure, not the full Poetry environment, which carries langchain/xgboost/shap and would add
several GB for a job that fetches JSON and writes to Postgres.

**Secrets** come from Secret Manager at runtime, never baked into the image:

```bash
echo -n "postgresql://user:pass@host/db?sslmode=require" | \
  gcloud secrets create sophie-database-url --data-file=-
```

**Cost**: roughly 30–60 seconds of 1 vCPU per day sits inside the Cloud Run free tier, and Scheduler
allows 3 free jobs per month. Realistically $0, with GCS storage in the low cents per year.

---

## 7. Operations

### Run by hand

```powershell
# Local, writes nothing — fastest way to check feed health and metric sanity
poetry run python spx-option-snapshot/run.py --dry-run

# Local, real write
poetry run python spx-option-snapshot/run.py

# Force a run on a non-session date (weekend/holiday testing)
poetry run python spx-option-snapshot/run.py --dry-run --force

# In the cloud
gcloud run jobs execute spx-snapshot-etl --region us-central1
```

### Guards

**Holiday check** — Cloud Scheduler fires Monday–Friday regardless of the market calendar. The job
checks the NYSE calendar via `pandas_market_calendars` and exits cleanly on holidays. Without it,
every Thanksgiving and Good Friday would write a stale duplicate of Wednesday's frozen quotes.

A skipped holiday **exits 0**. Exiting non-zero would trip the job's retry and alerting for
something entirely expected.

**Gap detection** — each run checks that the previous session landed and logs a `GAP:` warning if
not. This matters because alerting purely on execution failure misses the case that actually bites:
a run that never fired at all, or one that "succeeded" while writing nothing useful. Set a log-based
alert on `GAP:` in Cloud Logging.

**Stale-cycle exclusion** — when the feed is frozen (weekend, holiday, after hours), an
already-expired contract can still be labelled 0 DTE. Its quotes sit at end-of-day levels with
near-zero time value, and inverting IV on them is numerically unstable enough to produce solved vols
above 300% on strikes $5 apart — enough to flip the term-structure reading from contango to
backwardation on its own. `build_snapshot()` drops any cycle whose expiration is earlier than the
session date, regardless of what the feed's DTE field claims.

### The seed row

`2026-08-21` was written by hand (`run.py --force` on the following Sunday) so the frontend has real
data to render before the first scheduled run. It is not synthetic — it is Friday's genuine closing
chain, correctly dated by the session-snapping rule in §3. The first scheduled run adds `2026-08-24`
(Monday); Friday's row stays as the series' first entry.

`--force` exists for exactly this: seeding, and testing outside market hours. It bypasses only the
holiday guard, never the date-resolution logic, so a forced run still files its data under the
correct session.

### Idempotency

Both tables upsert on their primary key, so a retried or manually re-run job is harmless. The GCS
archive overwrites the same object key for a given date.

### If the archive write fails

The Postgres commit happens **first**, and an archive failure is logged rather than raised. Failing
the whole run after a successful DB write would turn a recoverable problem into a lost session — the
archive can be regenerated from the slice or simply skipped for that day, but the summary row cannot
be recovered once the session closes.

---

## 8. Local development

The agent runs without `google-cloud-storage` installed: `archive_to_gcs()` imports it lazily and
no-ops when `GCS_ARCHIVE_BUCKET` is unset. So a local run writes Postgres only.

Environment (from `.env`): `DATABASE_URL`, or the `DB_USER` / `DB_PASSWORD` / `DB_HOST` / `DB_NAME`
quartet that `src/tools/api_db.py :: get_db_connection()` falls back to.

Apply the schema once:

```bash
psql "$DATABASE_URL" -f sql/create_spx_option_snapshot_tables.sql
```

---

## 9. Known limitations

| Limitation | Impact |
|---|---|
| Dealer sign convention is an assumption | `net_gex_m` inverts on a day when customers are net sellers of puts |
| Open interest is published once daily | Intraday OI changes are invisible; ΔOI is strictly session-over-session |
| 15-minute delayed feed | Handled by the 17:00 ET schedule, but the marks are Cboe's delayed quotes, not official settlement prices |
| No intraday samples | Cannot reconstruct intraday skew dynamics, only session-to-session |
| ±25% band on the Postgres slice | Deep-wing per-strike history exists only in the Parquet archive |
| Gamma flip not stored | Recomputable from the slice, but not queryable directly |
| Backfill is impossible **from this source** | Cboe's CDN publishes current state only, with no history — so a gap cannot be repaired from the feed this ETL uses, which is the whole reason for the cloud schedule. It is *not* true of the data itself: a vendor that recorded OPRA at the time (ThetaData, OptionsDX, and similar) can still serve the missing sessions. See `.claude/skills/spx-option-backfill/`. Treat a gap as expensive, not irrecoverable |

---

## 10. Related

- `docs/cloud-etl-tracking.md` — operational tracking and status verification guide
- `.agents/skills/sophie-etl-tracker/SKILL.md` — automated health check & tracking skill
- `docs/../sql/create_spx_option_snapshot_tables.sql` — schema with inline rationale
- `services/spx-options-api/` — the live Cloud Run service the viewer reads from (same Cboe source)
- `ai-stock-suggestion-client/docs/option-viewer-deep-research.md` — how these metrics are consumed
- `public/wiki/option-strategy/volatility-surface-analytics.md` — user-facing methodology
