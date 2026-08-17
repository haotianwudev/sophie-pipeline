"""
Cboe Options API Tool for SPX and Index Options
Fetches 15-minute delayed full option chains with Greeks (Delta, Gamma, Theta, Vega, IV)
and provides filtering, ATM slice, and GEX (Gamma Exposure) calculation.
"""

import re
import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

# In-memory cache
_CACHE: Dict[str, Any] = {
    "data": None,
    "last_fetched": 0,
}

DEFAULT_TTL_SECONDS = 60  # Cache for 1 minute


def fetch_cboe_spx_raw(ttl_seconds: int = DEFAULT_TTL_SECONDS) -> Dict[str, Any]:
    """
    Fetch and cache raw SPX options JSON from Cboe CDN.
    Combines both SPX (AM monthly) and SPXW (PM weekly/daily) options.
    """
    now = time.time()
    if _CACHE["data"] is not None and (now - _CACHE["last_fetched"] < ttl_seconds):
        return _CACHE["data"]

    url = "https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.cboe.com/",
        "Accept": "application/json"
    }

    res = requests.get(url, headers=headers, timeout=12)
    res.raise_for_status()

    raw_json = res.json()
    data = raw_json.get("data", {})
    spot_price = float(data.get("current_price", 0.0))
    raw_options = data.get("options", [])
    cboe_timestamp = raw_json.get("timestamp")

    pattern = re.compile(r"^(SPXW|SPX)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")
    parsed_contracts = []
    expirations_set = set()

    for opt in raw_options:
        sym = opt.get("option", "")
        m = pattern.match(sym)
        if not m:
            continue

        root, yy, mm, dd, opt_type, strike_raw = m.groups()
        exp_date = f"20{yy}-{mm}-{dd}"
        expirations_set.add(exp_date)
        strike = int(strike_raw) / 1000.0

        parsed_contracts.append({
            "symbol": sym,
            "root": root,
            "expiration": exp_date,
            "type": "CALL" if opt_type == "C" else "PUT",
            "strike": strike,
            "bid": float(opt.get("bid", 0.0)),
            "ask": float(opt.get("ask", 0.0)),
            "last": float(opt.get("last_trade_price", 0.0)),
            "volume": float(opt.get("volume", 0.0)),
            "open_interest": float(opt.get("open_interest", 0.0)),
            "iv": float(opt.get("iv", 0.0)),
            "delta": float(opt.get("delta", 0.0)),
            "gamma": float(opt.get("gamma", 0.0)),
            "theta": float(opt.get("theta", 0.0)),
            "vega": float(opt.get("vega", 0.0)),
            "rho": float(opt.get("rho", 0.0)),
            "theo": float(opt.get("theo", 0.0)),
            "change": float(opt.get("change", 0.0)),
            "percent_change": float(opt.get("percent_change", 0.0)),
            "prev_day_close": float(opt.get("prev_day_close", 0.0)),
            "last_trade_time": opt.get("last_trade_time")
        })

    sorted_expirations = sorted(list(expirations_set))

    cache_payload = {
        "spot_price": spot_price,
        "cboe_timestamp": cboe_timestamp,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "expirations": sorted_expirations,
        "contracts": parsed_contracts
    }

    _CACHE["data"] = cache_payload
    _CACHE["last_fetched"] = now
    return cache_payload


def get_spx_metadata(ttl_seconds: int = DEFAULT_TTL_SECONDS) -> Dict[str, Any]:
    """Get underlying SPX spot price, timestamps, and available expiration dates."""
    data = fetch_cboe_spx_raw(ttl_seconds=ttl_seconds)
    return {
        "symbol": "^SPX",
        "spot_price": data["spot_price"],
        "cboe_timestamp": data["cboe_timestamp"],
        "fetched_at_utc": data["fetched_at_utc"],
        "total_expirations": len(data["expirations"]),
        "expirations": data["expirations"],
        "total_contracts": len(data["contracts"])
    }


def get_spx_option_chain(
    expiration: Optional[str] = None,
    option_type: Optional[str] = None,
    strike_min: Optional[float] = None,
    strike_max: Optional[float] = None,
    strike_range: Optional[float] = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS
) -> Tuple[float, str, List[Dict[str, Any]]]:
    """
    Get filtered SPX options contracts.
    
    Args:
        expiration: 'YYYY-MM-DD' (defaults to nearest available expiration)
        option_type: 'CALL' or 'PUT' or None
        strike_min: Minimum strike price
        strike_max: Maximum strike price
        strike_range: Range around spot price (spot - range <= strike <= spot + range)
        ttl_seconds: Cache duration in seconds
    
    Returns:
        (spot_price, expiration_used, list_of_contracts)
    """
    data = fetch_cboe_spx_raw(ttl_seconds=ttl_seconds)
    spot = data["spot_price"]
    
    target_exp = expiration or (data["expirations"][0] if data["expirations"] else "")
    contracts = [c for c in data["contracts"] if c["expiration"] == target_exp]

    if option_type:
        opt_upper = option_type.upper()
        contracts = [c for c in contracts if c["type"] == opt_upper]

    if strike_range is not None and strike_range > 0:
        contracts = [c for c in contracts if abs(c["strike"] - spot) <= strike_range]
    else:
        if strike_min is not None:
            contracts = [c for c in contracts if c["strike"] >= strike_min]
        if strike_max is not None:
            contracts = [c for c in contracts if c["strike"] <= strike_max]

    return spot, target_exp, contracts


def calculate_spx_gex(
    expiration: Optional[str] = None,
    strike_range: float = 150.0,
    ttl_seconds: int = DEFAULT_TTL_SECONDS
) -> Dict[str, Any]:
    """
    Calculate Net Gamma Exposure (GEX) by strike for SPX.
    GEX formula:
      Call GEX = Gamma * Open Interest * 100 * Spot Price
      Put GEX  = -Gamma * Open Interest * 100 * Spot Price
    """
    spot, target_exp, contracts = get_spx_option_chain(
        expiration=expiration,
        strike_range=strike_range,
        ttl_seconds=ttl_seconds
    )

    strike_map: Dict[float, Dict[str, float]] = {}
    total_call_gex = 0.0
    total_put_gex = 0.0

    for c in contracts:
        strike = c["strike"]
        gamma = c["gamma"]
        oi = c["open_interest"]
        opt_type = c["type"]

        # Dollar GEX ($ / 1% move standard convention or spot*spot*gamma*oi*100)
        # Standard GEX in $M: Gamma * OI * 100 * Spot * Spot * 0.01 / 1,000,000
        dollar_gex = gamma * oi * 100.0 * spot * (spot * 0.01) / 1_000_000.0

        if strike not in strike_map:
            strike_map[strike] = {
                "strike": strike,
                "call_gex_m": 0.0,
                "put_gex_m": 0.0,
                "net_gex_m": 0.0,
                "call_oi": 0,
                "put_oi": 0,
                "call_vol": 0,
                "put_vol": 0
            }

        if opt_type == "CALL":
            strike_map[strike]["call_gex_m"] += dollar_gex
            strike_map[strike]["call_oi"] += int(oi)
            strike_map[strike]["call_vol"] += int(c["volume"])
            total_call_gex += dollar_gex
        else:
            # Dealers are short the puts customers bought -> negative gamma, per this
            # function's own docstring convention (Put GEX = -Gamma * OI * 100 * Spot).
            strike_map[strike]["put_gex_m"] -= dollar_gex
            strike_map[strike]["put_oi"] += int(oi)
            strike_map[strike]["put_vol"] += int(c["volume"])
            total_put_gex -= dollar_gex

    for s in strike_map.values():
        s["net_gex_m"] = round(s["call_gex_m"] + s["put_gex_m"], 4)
        s["call_gex_m"] = round(s["call_gex_m"], 4)
        s["put_gex_m"] = round(s["put_gex_m"], 4)

    sorted_strikes = sorted(strike_map.values(), key=lambda x: x["strike"])

    return {
        "spot_price": spot,
        "expiration": target_exp,
        "total_net_gex_m": round(total_call_gex + total_put_gex, 4),
        "total_call_gex_m": round(total_call_gex, 4),
        "total_put_gex_m": round(total_put_gex, 4),
        "strikes_count": len(sorted_strikes),
        "strikes": sorted_strikes
    }
