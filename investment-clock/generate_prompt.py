"""
Generates the weekly Gemini Deep Research prompt from investment_clock_data.
Can be called directly or imported by run.py.
"""

import sys
import json
import datetime
import pathlib

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.tools.api_db import get_db_connection
from psycopg2.extras import RealDictCursor

CLOCK_CYCLE = ["Reflation", "Recovery", "Overheat", "Stagflation"]
PROMPTS_DIR = pathlib.Path(__file__).parent / "prompts"


def generate_prompt() -> str:
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT
          TO_CHAR(biz_date, 'YYYY-MM-DD') as biz_date,
          ROUND(growth_z_score::numeric, 3) as growth_z,
          ROUND(inflation_z_score::numeric, 3) as inflation_z,
          data_phase,
          ROUND(clock_angle::numeric, 1) as clock_angle,
          ROUND(gdp_value::numeric, 2) as gdp,
          ROUND(cpi_value::numeric, 2) as cpi,
          ROUND(tcu_value::numeric, 1) as tcu,
          ROUND(unrate_value::numeric, 1) as unrate,
          ROUND(indpro_value::numeric, 2) as indpro,
          ROUND(cli_value::numeric, 2) as cli,
          ROUND(icsa_value::numeric, 0) as icsa,
          ROUND(cpi_yoy::numeric, 2) as cpi_yoy,
          ROUND(cpi_mom_ann::numeric, 2) as cpi_mom_ann,
          ROUND(t5yie_value::numeric, 2) as t5yie,
          ROUND(ppi_yoy::numeric, 2) as ppi_yoy
        FROM investment_clock_data
        ORDER BY biz_date DESC LIMIT 1
    """)
    latest = dict(cur.fetchone() or {})

    if not latest:
        cur.close(); conn.close()
        raise RuntimeError("investment_clock_data is empty — run the ETL first.")

    # One entry per calendar month (latest biz_date within each month).
    # This prevents intra-month churn from showing multiple rows and keeps
    # closed-month phases stable at their end-of-month snapshot.
    cur.execute("""
        SELECT DISTINCT ON (date_trunc('month', biz_date))
          TO_CHAR(biz_date, 'YYYY-MM') as month,
          ROUND(growth_z_score::numeric, 3) as growth_z,
          ROUND(inflation_z_score::numeric, 3) as inflation_z,
          data_phase
        FROM investment_clock_data
        WHERE biz_date >= CURRENT_DATE - INTERVAL '6 months'
        ORDER BY date_trunc('month', biz_date), biz_date DESC
    """)
    trajectory = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT
          TO_CHAR(biz_date, 'YYYY-MM-DD') as biz_date,
          final_phase,
          ROUND(phase_confidence::numeric, 1) as confidence,
          phase_direction,
          LEFT(reasoning, 300) as reasoning_preview
        FROM investment_clock_evaluation
        ORDER BY biz_date DESC LIMIT 1
    """)
    prev = dict(cur.fetchone() or {})

    # Fallback for CPI MoM Ann when not yet released in the current row
    cpi_mom_ann_fallback = None
    if latest.get("cpi_mom_ann") is None:
        cur.execute("""
            SELECT ROUND(cpi_mom_ann::numeric, 2) as val,
                   TO_CHAR(biz_date, 'YYYY-MM') as month
            FROM investment_clock_data
            WHERE cpi_mom_ann IS NOT NULL
            ORDER BY biz_date DESC LIMIT 1
        """)
        fb = cur.fetchone()
        cpi_mom_ann_fallback = dict(fb) if fb else None

    cur.close(); conn.close()

    today       = datetime.date.today().strftime("%Y-%m-%d")
    biz_date    = latest["biz_date"]
    growth_z    = float(latest["growth_z"])
    inflation_z = float(latest["inflation_z"])
    data_phase  = latest["data_phase"]
    clock_angle = latest["clock_angle"]
    gdp         = latest.get("gdp", "N/A")
    cpi         = latest.get("cpi", "N/A")
    tcu         = latest.get("tcu", "N/A")
    unrate      = latest.get("unrate", "N/A")
    indpro      = latest.get("indpro", "N/A")
    cli         = latest.get("cli", "N/A")
    icsa_raw    = latest.get("icsa")
    icsa        = f"{int(icsa_raw) // 1000}" if icsa_raw is not None else "N/A"
    cpi_yoy     = latest.get("cpi_yoy", "N/A")
    cpi_mom_ann_raw = latest.get("cpi_mom_ann")
    if cpi_mom_ann_raw is not None:
        cpi_mom_ann = f"{float(cpi_mom_ann_raw):.2f}%"
    elif cpi_mom_ann_fallback:
        cpi_mom_ann = f"{float(cpi_mom_ann_fallback['val']):.2f}% (as of {cpi_mom_ann_fallback['month']})"
    else:
        cpi_mom_ann = "N/A (no prior data)"
    t5yie       = latest.get("t5yie", "N/A")
    ppi_yoy     = latest.get("ppi_yoy", "N/A")

    growth_dir    = "above-trend (expansionary)" if growth_z > 0 else "below-trend (contractionary)"
    inflation_dir = "above-trend (inflationary)" if inflation_z > 0 else "below-trend (disinflationary)"

    trajectory_lines = "\n".join(
        f"  {r['month']}  growth_z={float(r['growth_z']):+.3f}  inflation_z={float(r['inflation_z']):+.3f}  phase={r['data_phase']}"
        for r in trajectory
    ) or "  (no data)"

    prev_block = (
        f"Previous Claude evaluation ({prev['biz_date']}):\n"
        f"  Phase:     {prev['final_phase']} at {prev['confidence']}% confidence\n"
        f"  Direction: {prev['phase_direction']}\n"
        f"  Summary:   {prev['reasoning_preview']}"
        if prev else "Previous Claude evaluation: None (first evaluation)"
    )

    next_phase = CLOCK_CYCLE[(CLOCK_CYCLE.index(data_phase) + 1) % 4] if data_phase in CLOCK_CYCLE else "unknown"

    prompt = f"""GEMINI DEEP RESEARCH PROMPT — Investment Clock Weekly Analysis
Date: {today}

================================================================
QUANTITATIVE CONTEXT (Merrill Lynch Investment Clock Framework)
================================================================

As of {biz_date}, the US economy shows the following EWM Z-score cycle signals:

  Composite Growth Z-score:    {growth_z:+.3f}  ({growth_dir})
    [CLI 50% + INDPRO 20% + inv. Jobless Claims 15% + inv. UNRATE 15%]
  Composite Inflation Z-score: {inflation_z:+.3f}  ({inflation_dir})
    [5Y Breakeven 30% + CPI YoY vs 2% 25% + PPI YoY 20% + CPI MoM Ann vs 2% 15% + TCU 10%]

Growth Indicators (latest available):
  OECD CLI (USALOLITONOSTSAM):     {cli}  (>100 = expansion, <100 = contraction; 50% weight)
  Initial Jobless Claims (ICSA):   {icsa}k/week  (inverted; rising = weakening labor; 15% weight)
  Industrial Production (INDPRO):  {indpro}  (coincident output; 20% weight)
  Unemployment Rate (UNRATE):      {unrate}%  (inverted; 15% weight)

Inflation Indicators (latest available):
  5Y Breakeven Inflation (T5YIE):  {t5yie}%  (market-implied forward inflation; 30% weight)
  Core CPI YoY (CPILFESL):         {cpi_yoy}%  (vs 2% Fed target; 25% weight)
  PPI Final Demand YoY (PPIFID):   {ppi_yoy}%  (pipeline inflation; 20% weight)
  CPI MoM Annualized:              {cpi_mom_ann}  (real-time inflection; vs 2% target; 15% weight)
  Capacity Utilization (TCU):      {tcu}%  (demand-pull pressure; 10% weight)

Reference (not in composite):
  Real GDP (GDPC1):                {gdp}

Algorithmically-determined phase: {data_phase}
Clock hand position: {clock_angle} degrees (0 = 12 o'clock, clockwise)

6-Month Trajectory:
{trajectory_lines}

{prev_block}

================================================================
RESEARCH QUESTIONS
================================================================

Please conduct a comprehensive deep research analysis addressing ALL of the following:

1. PHASE VALIDATION
   Is the US economy currently in the "{data_phase}" phase of the Merrill Lynch
   Investment Clock? What specific economic data, policy signals, and market dynamics
   support or contradict the quantitative signals above?

2. CYCLE DIRECTION & MOMENTUM
   Is the clock moving clockwise (normal progression), counter-clockwise (stress/shock),
   or stalling in place? What does the 6-month trajectory suggest about velocity and
   direction of the next phase transition?

3. FEDERAL RESERVE & MONETARY POLICY
   What are the current Fed signals (rate path, dot plot, balance sheet, forward guidance)?
   How do these align with or diverge from the expected monetary stance for "{data_phase}"?

4. INFLATION DYNAMICS
   Is inflation (Core CPI, PCE, PPI, 5-year breakevens) rising, falling, or sticky?
   How does the current trajectory compare to the Fed's 2% target?

5. GROWTH DYNAMICS
   What is the latest GDP print and near-term outlook (Atlanta Fed GDPNow, consensus)?
   Are recessionary signals (yield curve inversion, credit spreads, PMI contraction)
   present, or is expansion continuing?

6. ASSET CLASS & SECTOR IMPLICATIONS
   Based on historical Investment Clock performance in the "{data_phase}" phase:
   - Which broad asset class (Government Bonds / Equities / Commodities / Cash) is favored?
   - Which equity sectors historically outperform in this phase?
   - What specific ETFs (IYW, IYE, IDU, IYF, IYK, IYH, IYJ, IYM, IYZ, IYC) are most aligned?

7. KEY RISKS & DISRUPTIONS
   What exogenous shocks (geopolitical, policy error, commodity spike, financial instability,
   trade policy) could force the clock to skip phases or move counter-clockwise?

8. HISTORICAL ANALOGS
   Which historical periods most closely resemble today's environment (similar Z-score
   coordinates, similar Fed stance, similar growth-inflation mix)?
   What happened to asset prices in those analogs over the following 3-6 months?

9. FORWARD OUTLOOK (3-6 months)
   Given all the above, what is your probabilistic assessment of the next phase transition?
   How likely is a move to "{next_phase}" vs remaining in "{data_phase}"?
   What leading indicators should be monitored for early signs of transition?

================================================================
OUTPUT FORMAT REQUESTED
================================================================

Please structure your response with:
1. Executive Summary (3-5 sentences)
2. Phase Determination: final phase, confidence 0-100, direction (clockwise/counterclockwise/stable)
3. Key Supporting Evidence (3-5 bullet points)
4. Key Risks (2-3 bullet points)
5. Asset Allocation Recommendation (best asset class + top 3 equity sectors)
6. 3-6 Month Outlook paragraph
7. Historical Analog comparison

Cite specific sources (Fed statements, BLS/BEA releases, ISM PMI, earnings data, etc.)
"""
    return prompt, today


def save_and_print(prompt: str, today: str):
    PROMPTS_DIR.mkdir(exist_ok=True)
    output_file = PROMPTS_DIR / f"{today}.md"
    output_file.write_text(prompt, encoding="utf-8")
    sys.stdout.flush()
    print(f"Prompt saved to {output_file}\n")
    print(prompt)


if __name__ == "__main__":
    prompt, today = generate_prompt()
    save_and_print(prompt, today)
