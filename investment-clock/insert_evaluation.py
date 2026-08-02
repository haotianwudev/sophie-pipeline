import sys
import json
import argparse
from datetime import date
import pathlib
import psycopg2

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
from src.tools.api_db import get_db_connection  # type: ignore

def insert_evaluation(json_path: str, eval_date: str = None):
    if not eval_date:
        eval_date = date.today().strftime("%Y-%m-%d")

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
    INSERT INTO investment_clock_evaluation (
      biz_date,
      final_phase, phase_confidence, phase_direction,
      reasoning, outlook,
      key_indicators, risks,
      best_asset, recommended_sectors,
      gemini_research_summary
    ) VALUES (
      %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s
    )
    ON CONFLICT (biz_date) DO UPDATE SET
      final_phase              = EXCLUDED.final_phase,
      phase_confidence         = EXCLUDED.phase_confidence,
      phase_direction          = EXCLUDED.phase_direction,
      reasoning                = EXCLUDED.reasoning,
      outlook                  = EXCLUDED.outlook,
      key_indicators           = EXCLUDED.key_indicators,
      risks                    = EXCLUDED.risks,
      best_asset               = EXCLUDED.best_asset,
      recommended_sectors      = EXCLUDED.recommended_sectors,
      gemini_research_summary  = EXCLUDED.gemini_research_summary;
    """
    
    cur.execute(query, (
        eval_date,
        data.get("final_phase"),
        data.get("phase_confidence"),
        data.get("phase_direction"),
        data.get("reasoning"),
        data.get("outlook"),
        json.dumps(data.get("key_indicators", [])),
        json.dumps(data.get("risks", [])),
        data.get("best_asset"),
        json.dumps(data.get("recommended_sectors", [])),
        data.get("gemini_research_summary")
    ))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully inserted/updated evaluation for {eval_date}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert Investment Clock Evaluation JSON into DB")
    parser.add_argument("json_file", help="Path to the JSON file containing the evaluation")
    parser.add_argument("--date", help="Date of the evaluation (YYYY-MM-DD), defaults to today", default=None)
    args = parser.parse_args()
    
    insert_evaluation(args.json_file, args.date)
