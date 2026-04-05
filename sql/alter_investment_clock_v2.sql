-- Add columns for new methodology signals (OECD CLI, ICSA, CPI YoY, CPI MoM annualized)
ALTER TABLE investment_clock_data
  ADD COLUMN IF NOT EXISTS cli_value    NUMERIC,
  ADD COLUMN IF NOT EXISTS icsa_value   NUMERIC,
  ADD COLUMN IF NOT EXISTS cpi_yoy      NUMERIC,
  ADD COLUMN IF NOT EXISTS cpi_mom_ann  NUMERIC;
