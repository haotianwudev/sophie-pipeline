# SPX Options API & Pipeline Guide

A free, 15-minute delayed SPX option chain API and data pipeline powered by direct Cboe market quotes.

---

## 1. Live Google Cloud Run API

- **Base URL:** `https://spx-options-api-282034489414.us-central1.run.app`
- **Swagger Docs:** `https://spx-options-api-282034489414.us-central1.run.app/docs`
- **Cost:** $0.00 (under GCP Free Tier — 2M requests/month allowance)

### Main Endpoints

| Endpoint | Method | Description | Example Query |
| :--- | :--- | :--- | :--- |
| `/health` | `GET` | Service health status | `/health` |
| `/spx/metadata` | `GET` | Spot price & all 55+ expiration dates | `/spx/metadata` |
| `/spx/atm` | `GET` | Near-the-money strikes around spot | `/spx/atm?points=30` |
| `/spx/chain` | `GET` | Filtered chain by strike, type, expiry | `/spx/chain?type=CALL&strike_range=50` |
| `/spx/gex` | `GET` | Net Gamma Exposure (GEX) analytics | `/spx/gex?strike_range=100` |

---

## 2. Python Tool inside `sophie-pipeline`

You can use the Python module directly in any script:

```python
from src.tools.api_cboe import get_spx_metadata, get_spx_option_chain, calculate_spx_gex

# 1. Spot price and available dates
meta = get_spx_metadata()
print(f"Spot: {meta['spot_price']}, Expirations: {len(meta['expirations'])}")

# 2. Get nearest expiration chain (+/- 30 points around spot)
spot, exp_date, contracts = get_spx_option_chain(strike_range=30.0)

# 3. Calculate GEX (Gamma Exposure)
gex = calculate_spx_gex(strike_range=50.0)
print(f"Net GEX: ${gex['total_net_gex_m']}M")
```

---

## 3. Export Data for Frontend Client

To update the sample snapshot used by `ai-stock-suggestion-client`:

```powershell
python scripts/export_spx_chain_sample.py
```

- **Target Output:** `F:\workspace\ai-stock-suggestion-client\public\data\spx-chain-sample.json`
- **Schema:** Conforms to `OptionChainSnapshot` (sorted calls & puts, signed delta, IV, volume, DTE).

---

## 4. Re-deploying Updates to Cloud Run

If you make changes to `services/spx-options-api/main.py`:

```powershell
cd services/spx-options-api
.\deploy.ps1
```
