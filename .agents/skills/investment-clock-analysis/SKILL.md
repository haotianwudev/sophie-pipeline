---
name: investment-clock-analysis
description: Performs deep research on the economy based on the latest investment clock prompt, generates a research paper, and saves the final phase evaluation to the database.
---

# Investment Clock Analysis Workflow

When invoked, follow these exact steps to complete the weekly Investment Clock Deep Research & Analysis.

## Step 1: Find the Latest Prompt
1. List the files in `F:\workspace\sophie-pipeline\investment-clock\prompts\`.
2. Identify the most recent prompt file (e.g., `YYYY-MM-DD.md`) that does NOT start with `Result-`.
3. Use the `view_file` tool to read the entire contents of this prompt file. Extract the date from the filename (this is the `biz_date`).

## Step 2: Perform Deep Research
1. The prompt contains "QUANTITATIVE CONTEXT" and 9 "RESEARCH QUESTIONS".
2. Use your `search_web` and `read_url_content` tools to thoroughly research the current macroeconomic environment to answer these questions. Look up:
   - Recent Fed speeches, rate path expectations, and dot plot updates.
   - Latest CPI, PPI, and PCE prints.
   - GDPNow (Atlanta Fed) and latest ISM PMI readings.
   - Recent exogenous shocks or market volatility.
3. Synthesize your research into a comprehensive "Deep Research Paper" that fully answers all 9 questions in the prompt. Do not take shortcuts; this should be as detailed as a Gemini Deep Research paper.

## Step 3: Save the Result Paper
1. Save your synthesized research paper using the `write_to_file` tool to `F:\workspace\sophie-pipeline\investment-clock\prompts\Result-{biz_date}.md`.
2. DO NOT output the entire paper in your conversational response to the user. Just confirm it was saved.

## Step 4: Final JSON Evaluation
1. Based on the quantitative data in the prompt AND your newly generated research paper, determine the final Investment Clock phase.
2. Generate a JSON object matching this exact structure:
```json
{
  "final_phase": "Recovery" | "Overheat" | "Stagflation" | "Reflation",
  "phase_confidence": 0-100,
  "phase_direction": "clockwise" | "counterclockwise" | "stable",
  "reasoning": "2-3 paragraph analysis explaining the final determination",
  "outlook": "Forward-looking paragraph for the next 3-6 months",
  "key_indicators": ["3-5 bullet point strings of the most important signals"],
  "risks": ["2-3 key risks to the current phase assessment"],
  "best_asset": "Government Bonds" | "Equities" | "Commodities" | "Cash",
  "recommended_sectors": ["Sector1", "Sector2", "Sector3"],
  "gemini_research_summary": "2-3 sentence summary of key findings from your research"
}
```
Phase -> Asset mapping for reference:
- Reflation   -> Government Bonds  | Financials, Consumer Staples, Consumer Discretionary
- Recovery    -> Equities          | Technology, Telecom, Materials
- Overheat    -> Commodities       | Energy, Industrials, Materials
- Stagflation -> Cash              | Utilities, Healthcare, Consumer Staples

3. Save this JSON using the `write_to_file` tool to a temporary file: `F:\workspace\sophie-pipeline\investment-clock\prompts\eval_{biz_date}.json`.

## Step 5: Save to Database
1. Execute the insertion script using the `run_command` tool:
   `poetry run python F:\workspace\sophie-pipeline\investment-clock\insert_evaluation.py F:\workspace\sophie-pipeline\investment-clock\prompts\eval_{biz_date}.json --date {biz_date}`
2. Make sure the `Cwd` for the `run_command` is `F:\workspace\sophie-pipeline`.
3. Once successful, print a nicely formatted summary of the evaluation for the user (Date, Final Phase, Confidence, Best Asset, and a brief summary).
