# Investment Clock

Macro cycle tracker based on the Merrill Lynch Investment Clock framework.
Maps the US economy onto 4 phases using FRED data + Hodrick-Prescott filter,
then uses a human-in-the-loop workflow with Gemini Deep Research + Claude analysis.

---

## Phases

| Phase | Growth | Inflation | Best Asset | ETFs |
|---|---|---|---|---|
| Reflation | Below trend | Below trend | Government Bonds | IYF, IYK, IYC |
| Recovery | Above trend | Below trend | Equities | IYW, IYZ, IYM |
| Overheat | Above trend | Above trend | Commodities | IYE, IYJ, IYM |
| Stagflation | Below trend | Above trend | Cash | IDU, IYH, IYK |

---

## Weekly Workflow

### Step 1 — Fetch data + generate prompt (combined)

```bash
cd F:/workspace/sophie-pipeline
poetry run python investment-clock/run.py
```

This does two things in one command:
1. Fetches 5 FRED series, applies the Hodrick-Prescott filter, computes composite
   Z-scores, determines the current phase and clock angle, and upserts all monthly
   rows into `investment_clock_data`
2. Reads the fresh data and generates the Gemini Deep Research prompt, saved to
   `investment-clock/prompts/YYYY-MM-DD.md` and printed to the terminal

Requires `FRED_API_KEY` in your `.env` file.

---

### Step 2 — Run Gemini Deep Research

Copy the full prompt from `investment-clock/prompts/YYYY-MM-DD.md` and paste it
into [Gemini Deep Research](https://gemini.google.com).

Save the response as a markdown file in `investment-clock/prompts/`, e.g.:

```
investment-clock/prompts/Result-2026-04-04.md
```

---

### Step 3 — Analyze and save evaluation

In Claude Code, run:

```
/investment-clock-analyze F:/workspace/sophie-pipeline/investment-clock/prompts/Result-YYYY-MM-DD.md
```

Claude reads the Gemini paper + DB data + prior evaluations, produces a final
structured judgment, and saves it to `investment_clock_evaluation`.

---

## File Structure

```
investment-clock/
├── README.md                        this file
├── run.py                           weekly runner: ETL + prompt generation (one command)
├── generate_prompt.py               prompt generation logic (imported by run.py)
└── prompts/
    ├── YYYY-MM-DD.md                generated research prompt (input to Gemini)
    └── Result-YYYY-MM-DD.md         Gemini Deep Research output (input to Claude)
```

Related files elsewhere in the repo:

```
src/agents/investment_clock.py       ETL agent (FRED fetch + HP filter + DB upsert)
sql/create_investment_clock_tables.sql   DB schema (run once to create tables)
```

---

## Database Tables

| Table | Updated by | Purpose |
|---|---|---|
| `investment_clock_data` | ETL agent (Step 1) | FRED metrics + HP filter Z-scores for every month |
| `investment_clock_evaluation` | Claude skill (Step 4) | Weekly LLM judgment after Gemini research |

---

## One-time Setup

### 1. Create DB tables

```bash
cd F:/workspace/sophie-pipeline
poetry run python -c "
from src.tools.api_db import get_db_connection
conn = get_db_connection()
cur = conn.cursor()
cur.execute(open('sql/create_investment_clock_tables.sql').read())
conn.commit(); cur.close(); conn.close()
print('Done')
"
```

### 2. Add FRED API key to `.env`

```
FRED_API_KEY=your_key_here
```

Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html

### 3. Install new dependencies

```bash
cd F:/workspace/sophie-pipeline
poetry install
```
