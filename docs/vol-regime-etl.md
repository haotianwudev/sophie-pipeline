# Volatility Regime / VRP ETL

Daily job that refreshes SPX and VIX daily bars in `prices`, then recomputes the VRP / volatility-regime
signals into `vol_regime_data`.

| | |
|---|---|
| Entry point | `vol-regime/run.py` |
| Agent | `src/agents/vol_regime.py` |
| Cloud Run Job | `vol-regime-etl` (project `longsky`, `us-central1`) |
| Scheduler | `vol-regime-etl-trigger`, `30 17 * * 1-5` America/New_York |
| Deploy | `services/vol-regime-etl/deploy.ps1 -Project longsky` |
| Secrets | `sophie-database-url`, `sophie-fred-api-key` |
| Tables | `prices` (SPX, VIX), `vol_regime_data` |
| External feeds | Yahoo (yfinance), FRED (`VXVCLS`), SqueezeMetrics `DIX.csv`, Cboe `DSPX_History.csv` (all no auth) |

## 1. Why this is a separate job from the option snapshot

The obvious move is to bolt this onto `spx-snapshot-etl` — both run daily, both fire after the
close. The reason not to is that the two have **opposite failure semantics**.

The option-chain snapshot captures data that cannot be re-fetched. Cboe's feed publishes current
state only, so a session missed there is gone from the live feed permanently — that irreversibility
is the entire reason that job exists at all.

This job is the opposite: fully self-healing. It refreshes a **10-day** trailing price window and
rewrites the last **30 days** of regime rows, both idempotent. A missed run is repaired by the next
one; a week of missed runs is repaired by the next one.

Merging them would let a Yahoo outage or a FRED timeout abort the process *before* the
irreplaceable snapshot committed. Recovering that isolation inside a single container means
hand-rolling try/except boundaries to rebuild exactly what two Cloud Run Jobs provide for free —
separate retries, separate timeouts, separate alerting, separate blast radius. They also share no
dependency but Postgres: Cboe's CDN on one side, Yahoo and FRED on the other.

**Where merging *is* right, and already done:** the price refresh and the signal computation live in
one process, because step 2 reads what step 1 wrote. That is a genuine data dependency, and
splitting it across two schedulers would be a race with no dependency graph to arbitrate it.
So: two jobs, split on reproducibility — not three, and not one.

The 17:30 ET schedule sits half an hour behind the snapshot job. Yahoo's daily bar is settled well
before then, and the offset keeps the two off the same instant so one shared Postgres hiccup cannot
take both out.

## 2. What it computes

```
realized_vol_20d = stdev(log returns, 20d) * sqrt(252) * 100
vrp              = VIX - realized_vol_20d
vix_rank         = 252d percentile rank of VIX
term_slope       = VIX3M - VIX          (negative = backwardation = stress)
```

`VIX3M` comes from **FRED (`VXVCLS`)**, not Yahoo, which no longer serves `^VIX3M` history. It is
optional by design: without `FRED_API_KEY` the agent logs a warning and writes `NULL` term structure
rather than failing the run.

`dix` / `dix_gex` come from **SqueezeMetrics' free public CSV**
(`https://squeezemetrics.com/monitor/static/DIX.csv`, no auth) — dark-pool sentiment and
market-maker gamma exposure, independent of both the Yahoo price feed and this platform's own
option-chain data. Same optional treatment as VIX3M: a fetch failure logs a warning and writes
`NULL` rather than failing the run, since it's a third-party feed this ETL doesn't control the
uptime of. History only goes back to **2011-05-02** (vs. 2000 for SPX/VIX), so expect `NULL`
before then. `dix_gex` is deliberately not named just `gex` — it's SqueezeMetrics' own derivation
(dollar-denominated dealer hedging obligation from their dark-pool data), not something computed
from this platform's option-chain OI/gamma; a future OI-derived GEX (e.g. `spx_tape_data`,
`spx_option_snapshot.net_gex_m`) is a different number by construction, and disagreement between
the two is a signal, not a bug — don't merge the columns.

`dspx` comes from **Cboe's own public CDN** (`https://cdn.cboe.com/api/global/us_indices/daily_prices/DSPX_History.csv`,
no auth) — the S&P 500 Dispersion Index, an implied-correlation proxy (spread between index-level
and single-name implied vol). Same optional/NULL-on-failure treatment as the other two feeds.
Shortest history of the three: starts **2014-06-19**.

## 3. Data sources

| Series | DB ticker | Source | Yahoo symbol |
|---|---|---|---|
| S&P 500 | `SPX` | yfinance | `^GSPC` (not `^SPX`) |
| VIX | `VIX` | yfinance | `^VIX` |
| VIX3M | — | FRED `VXVCLS` | not stored in `prices` |

The price refresh pins `data_source="yfinance"` rather than using the `auto` waterfall. Auto already
hardcodes SPX/VIX/SPY/VVIX to Yahoo, so this changes nothing about which source runs — but it removes
the fallback branches into Polygon and Financial Datasets, which need API keys this job is not given
and whose modules the image does not carry. A Yahoo outage should fail loudly, not silently reach for
a paid API that cannot authenticate.

FRED is a viable backup if Yahoo ever becomes unusable from GCP: `VIXCLS` covers 1990→present and
`SP500` a trailing 10-year window — both more than the 10-day refresh needs. The catch is that FRED
serves **close only**, where `prices` carries full OHLCV, so this has deliberately not been wired in
while yfinance works.

## 4. Three things that bit during the cloud migration

**`poetry run` inside a container.** `refresh_prices()` shelled out to
`poetry run python src/upload/raw_data_table_uploader.py`. No Poetry exists in the image, and that
CLI drags in the full uploader surface (company facts, news, insider trades, line items) for a job
that fetches two index series. Now calls `upload_prices()` in-process — the same function the CLI
resolves to via `TABLE_UPLOAD_CONFIG`, so the work is identical and local and cloud share one path.

**`.gitignore` is not an upload manifest.** With no `.gcloudignore` present, gcloud falls back to
`.gitignore` — where `data/` is unanchored and therefore also matched `src/data/`, the pydantic
models both ETL images import. A directory exclusion cannot be undone by a file-level negation, so no
`!src/data/...` line would have rescued it. There is now an explicit
[`.gcloudignore`](../.gcloudignore) with `/data/` anchored to the root. This also retires the
per-service `!services/*/requirements.txt` negations as a source of deploy breakage.

**Pinning a dependency independently of the working environment.** The image pinned
`yfinance==0.2.44` while the Poetry environment ran `0.2.61`. In Cloud Run the old client failed
every fetch with `YFTzMissingError` and `Expecting value: line 1 column 1` — Yahoo returning non-JSON
to its auth flow. It read like a datacenter-IP block; it was a stale pin. Keep this in step with
`pyproject.toml` rather than pinning it separately.

## 5. Operating it

```powershell
# Deploy / redeploy (build -> grant secrets -> deploy job -> IAM -> schedule)
.\services\vol-regime-etl\deploy.ps1 -Project longsky

# Run once by hand
gcloud run jobs execute vol-regime-etl --region us-central1 --project longsky --wait

# Logs
gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=vol-regime-etl' `
  --project longsky --limit 30 --format="value(textPayload)"
```

Locally:

```powershell
poetry run python vol-regime/run.py                 # daily trailing window
poetry run python vol-regime/run.py --full          # rebuild all history
poetry run python vol-regime/run.py --skip-prices   # signals only
```

`deploy.ps1` routes every gcloud call through `Invoke-Step`, which checks `$LASTEXITCODE`.
`$ErrorActionPreference = "Stop"` does **not** cover native executables — without that check a failed
build sails on to deploy a non-existent image and then builds the IAM binding and scheduler around
it, reporting success. That happened once; the guard is why it cannot happen quietly again.

## 6. History note

Before 2026-08-24 there was no automation for this at all. The only scheduled task on the workstation
(`sophie-pipeline-upload`, weekly Fridays 19:00) ran `src/run_uploads_tickers_free.py`, which covers
AAPL/MSFT/NVDA plus the investment clock and quant trending — and never touched SPX or VIX.
`src/run_uploads_market_indices.py` existed but nothing called it: 64 `upload_free_*` logs and zero
`upload_market_indices_*`. `prices` had been loaded by hand on 2026-08-21 and stopped at 08-19, so
the regime signals were computed on a stale tape.

`src/run_uploads_market_indices.py` was deleted in the same change: its entire job — a trailing-window
SPX/VIX refresh into `prices` — is now step 1 here, on a schedule that actually fires. Nothing
referenced it outside this document.
