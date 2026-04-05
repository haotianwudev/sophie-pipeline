-- Add 5Y Breakeven and PPI columns for leading inflation signals
ALTER TABLE investment_clock_data
  ADD COLUMN IF NOT EXISTS t5yie_value  NUMERIC,   -- 5-Year Breakeven Inflation Rate
  ADD COLUMN IF NOT EXISTS ppi_yoy      NUMERIC;   -- PPI Final Demand YoY % change
