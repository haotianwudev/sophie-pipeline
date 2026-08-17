"""
SPX & Multi-Ticker Options Chain Microservice for Google Cloud Run
Provides fast, in-memory cached 15-minute delayed options data, ATM slices, GEX analytics,
and backward-compatible endpoints for Sophie Options Viewer and SPX Payoff Builder.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, date
import requests
import re
import time
from typing import Optional, List, Dict, Any

app = FastAPI(
    title="Sophie Options Analytics API",
    description="High-performance cached Options Chain and GEX Analytics API powered by Cboe quotes.",
    version="1.1.0"
)

# Enable CORS for client web applications
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory multi-ticker cache: { ticker: { "data": ..., "last_fetched": timestamp } }
TICKER_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 15 * 60  # 15 minutes (900 seconds)


@app.middleware("http")
async def add_cache_headers(request, call_next):
    response = await call_next(request)
    if request.method == "GET" and not request.url.path.endswith("/health"):
        response.headers["Cache-Control"] = f"public, max-age={CACHE_TTL_SECONDS}, s-maxage={CACHE_TTL_SECONDS}, stale-while-revalidate=60"
        response.headers["X-Cache-TTL"] = f"{CACHE_TTL_SECONDS}s"
    return response


def normalize_cboe_symbol(ticker: str) -> str:
    cleaned = ticker.upper().strip()
    if cleaned in ["^SPX", "SPX", "SPXW", "$SPX", "$SPX.X"]:
        return "_SPX"
    if cleaned.startswith("^"):
        return cleaned[1:]
    return cleaned


def fetch_cboe_ticker_data(ticker: str = "^SPX") -> Dict[str, Any]:
    cboe_sym = normalize_cboe_symbol(ticker)
    now = time.time()

    if cboe_sym in TICKER_CACHE:
        cached_entry = TICKER_CACHE[cboe_sym]
        if now - cached_entry["last_fetched"] < CACHE_TTL_SECONDS:
            return cached_entry["data"]

    url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{cboe_sym}.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.cboe.com/",
        "Accept": "application/json"
    }

    try:
        res = requests.get(url, headers=headers, timeout=12)
        res.raise_for_status()
    except Exception as e:
        if cboe_sym in TICKER_CACHE:
            return TICKER_CACHE[cboe_sym]["data"]
        raise HTTPException(status_code=502, detail=f"Failed to fetch upstream Cboe data for {ticker}: {str(e)}")

    raw_json = res.json()
    data = raw_json.get("data", {})
    spot_price = float(data.get("current_price", 0.0))
    raw_options = data.get("options", [])
    cboe_timestamp = raw_json.get("timestamp", "")
    prev_day_close = float(data.get("prev_day_close", spot_price))
    price_change = float(data.get("price_change", 0.0))
    price_change_pct = float(data.get("price_change_percent", 0.0))

    # Regex matches standard OCC option symbols
    pattern = re.compile(r"^([A-Z0-9]+?)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")
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

        bid = float(opt.get("bid", 0.0))
        ask = float(opt.get("ask", 0.0))
        last_px = float(opt.get("last_trade_price", 0.0))
        mid_px = round((bid + ask) / 2.0, 4) if (bid + ask) > 0 else last_px

        parsed_contracts.append({
            "symbol": sym,
            "root": root,
            "expiration": exp_date,
            "type": "CALL" if opt_type == "C" else "PUT",
            "strike": strike,
            "bid": bid,
            "ask": ask,
            "last": last_px,
            "mid": mid_px,
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
        "symbol": ticker.upper(),
        "spot_price": spot_price,
        "previous_close": prev_day_close,
        "price_change": price_change,
        "price_change_percent": price_change_pct,
        "cboe_timestamp": cboe_timestamp,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "expirations": sorted_expirations,
        "contracts": parsed_contracts
    }

    TICKER_CACHE[cboe_sym] = {
        "data": cache_payload,
        "last_fetched": now
    }
    return cache_payload


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "spx-options-api",
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "cache_ttl_minutes": CACHE_TTL_SECONDS // 60,
        "cached_tickers": list(TICKER_CACHE.keys())
    }


# ==============================================================================
# Backward-Compatible Endpoint for OptionsViewer & Live Chain Provider
# ==============================================================================

@app.get("/options-analytics")
def get_options_analytics(ticker: str = Query("^SPX", description="Ticker symbol (e.g. ^SPX, SPY, QQQ)")):
    """
    Returns full options chain structured for Sophie Options Viewer component.
    """
    data = fetch_cboe_ticker_data(ticker)
    spot = data["spot_price"]
    quote_date_str = data["cboe_timestamp"][:10] if len(data["cboe_timestamp"]) >= 10 else date.today().strftime("%Y-%m-%d")
    try:
        quote_date = datetime.strptime(quote_date_str, "%Y-%m-%d").date()
    except ValueError:
        quote_date = date.today()

    # Group by expiration
    exp_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for c in data["contracts"]:
        exp = c["expiration"]
        if exp not in exp_map:
            exp_map[exp] = {"calls": [], "puts": []}

        # Convert to OptionsViewer OptionContract interface with full Greeks
        contract_item = {
            "contractSymbol": c["symbol"],
            "strike": c["strike"],
            "bid": c["bid"],
            "ask": c["ask"],
            "lastPrice": c["last"],
            "midPrice": c["mid"],
            "delta": c["delta"],
            "gamma": c.get("gamma", 0.0),
            "theta": c.get("theta", 0.0),
            "vega": c.get("vega", 0.0),
            "rho": c.get("rho", 0.0),
            "theo": c.get("theo", 0.0),
            "impliedVolatilityMid": c["iv"] if c["iv"] > 0 else None,
            "volume": int(c["volume"]),
            "openInterest": int(c["open_interest"]),
            "lastTradeDate": c.get("last_trade_time", quote_date_str)
        }

        if c["type"] == "CALL":
            exp_map[exp]["calls"].append(contract_item)
        else:
            exp_map[exp]["puts"].append(contract_item)

    expiration_dates_list = []
    for exp_str in sorted(exp_map.keys()):
        try:
            exp_d = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = max(0, (exp_d - quote_date).days)
            label = exp_d.strftime("%b %d, %Y")
        except ValueError:
            dte = 0
            label = exp_str

        expiration_dates_list.append({
            "expiration": exp_str,
            "daysToExpiration": dte,
            "expirationLabel": label,
            "calls": sorted(exp_map[exp_str]["calls"], key=lambda x: x["strike"]),
            "puts": sorted(exp_map[exp_str]["puts"], key=lambda x: x["strike"])
        })

    return {
        "ticker": data["symbol"],
        "stock": {
            "price": spot,
            "previousClose": data["previous_close"],
            "percentChange": data["price_change_percent"],
            "timestamp": data["cboe_timestamp"]
        },
        "vix": {
            "value": 15.0,
            "previousClose": 15.0,
            "percentChange": 0.0,
            "timestamp": data["cboe_timestamp"]
        },
        "expirationDates": expiration_dates_list
    }


@app.get("/spx/snapshot")
def get_spx_snapshot():
    """
    Returns full OptionChainSnapshot format directly.
    """
    res = get_options_analytics("^SPX")
    quote_date = res["stock"]["timestamp"][:10] if len(res["stock"]["timestamp"]) >= 10 else date.today().strftime("%Y-%m-%d")

    expirations_list = []
    for exp in res["expirationDates"]:
        calls = []
        for c in exp["calls"]:
            calls.append({
                "strike": c["strike"],
                "bid": c["bid"],
                "ask": c["ask"],
                "mid": c["midPrice"],
                "delta": abs(c["delta"]),
                "iv": c["impliedVolatilityMid"],
                "volume": c["volume"]
            })
        puts = []
        for p in exp["puts"]:
            puts.append({
                "strike": p["strike"],
                "bid": p["bid"],
                "ask": p["ask"],
                "mid": p["midPrice"],
                "delta": -abs(p["delta"]),
                "iv": p["impliedVolatilityMid"],
                "volume": p["volume"]
            })
        expirations_list.append({
            "expiration": exp["expiration"],
            "dte": exp["daysToExpiration"],
            "calls": calls,
            "puts": puts
        })

    return {
        "symbol": "SPX",
        "quoteDate": quote_date,
        "underlyingPrice": res["stock"]["price"],
        "expirations": expirations_list
    }


# ==============================================================================
# Slicing & Analytics Endpoints
# ==============================================================================

def _filter_chain(
    ticker: str = "^SPX",
    expiration: Optional[str] = None,
    opt_type: Optional[str] = None,
    strike_min: Optional[float] = None,
    strike_max: Optional[float] = None,
    strike_range: Optional[float] = None
) -> Dict[str, Any]:
    data = fetch_cboe_ticker_data(ticker)
    spot = data["spot_price"]

    target_exp = expiration or (data["expirations"][0] if data["expirations"] else "")
    contracts = [c for c in data["contracts"] if c["expiration"] == target_exp]

    if opt_type:
        opt_type_upper = str(opt_type).upper()
        contracts = [c for c in contracts if c["type"] == opt_type_upper]

    if strike_range is not None and strike_range > 0:
        contracts = [c for c in contracts if abs(c["strike"] - spot) <= strike_range]
    else:
        if strike_min is not None:
            contracts = [c for c in contracts if c["strike"] >= strike_min]
        if strike_max is not None:
            contracts = [c for c in contracts if c["strike"] <= strike_max]

    return {
        "symbol": data["symbol"],
        "spot_price": spot,
        "expiration": target_exp,
        "contracts_count": len(contracts),
        "contracts": contracts
    }


@app.get("/spx/metadata")
def get_metadata(ticker: str = "^SPX"):
    """Get spot price, timestamps, and list of available expirations."""
    data = fetch_cboe_ticker_data(ticker)
    return {
        "symbol": data["symbol"],
        "spot_price": data["spot_price"],
        "cboe_timestamp": data["cboe_timestamp"],
        "fetched_at_utc": data["fetched_at_utc"],
        "total_expirations": len(data["expirations"]),
        "expirations": data["expirations"],
        "total_contracts": len(data["contracts"])
    }


@app.get("/spx/chain")
def get_chain(
    ticker: str = Query("^SPX", description="Ticker symbol"),
    expiration: Optional[str] = Query(None, description="Expiration date YYYY-MM-DD (defaults to nearest)"),
    type: Optional[str] = Query(None, regex="^(CALL|PUT|call|put)$", description="Filter by option type"),
    strike_min: Optional[float] = Query(None, description="Minimum strike price"),
    strike_max: Optional[float] = Query(None, description="Maximum strike price"),
    strike_range: Optional[float] = Query(None, description="Strike range centered on spot (spot +/- range)")
):
    """Get filtered options chain."""
    return _filter_chain(
        ticker=ticker,
        expiration=expiration,
        opt_type=type,
        strike_min=strike_min,
        strike_max=strike_max,
        strike_range=strike_range
    )


@app.get("/spx/atm")
def get_atm_slice(
    ticker: str = Query("^SPX", description="Ticker symbol"),
    expiration: Optional[str] = Query(None, description="Expiration date YYYY-MM-DD (defaults to nearest)"),
    points: float = Query(50.0, description="Points around spot price (default +/- 50)")
):
    """Get near-the-money options slice around current spot price."""
    return _filter_chain(
        ticker=ticker,
        expiration=expiration,
        strike_range=points
    )


@app.get("/spx/gex")
def get_gex(
    ticker: str = Query("^SPX", description="Ticker symbol"),
    expiration: Optional[str] = Query(None, description="Expiration date YYYY-MM-DD (defaults to nearest)"),
    strike_range: float = Query(150.0, description="Points around spot price to compute GEX profile")
):
    """Calculate Net Gamma Exposure (GEX) by strike."""
    data = fetch_cboe_ticker_data(ticker)
    spot = data["spot_price"]
    target_exp = expiration or (data["expirations"][0] if data["expirations"] else "")

    contracts = [
        c for c in data["contracts"]
        if c["expiration"] == target_exp and abs(c["strike"] - spot) <= strike_range
    ]

    strike_map: Dict[float, Dict[str, Any]] = {}
    total_call_gex = 0.0
    total_put_gex = 0.0

    for c in contracts:
        strike = c["strike"]
        gamma = c["gamma"]
        oi = c["open_interest"]
        opt_type = c["type"]

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
            # Dealers are short the puts customers bought -> negative gamma, matching the
            # sign convention used by calculate_spx_gex in src/tools/api_cboe.py.
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
        "symbol": data["symbol"],
        "spot_price": spot,
        "expiration": target_exp,
        "total_net_gex_m": round(total_call_gex + total_put_gex, 4),
        "total_call_gex_m": round(total_call_gex, 4),
        "total_put_gex_m": round(total_put_gex, 4),
        "strikes_count": len(sorted_strikes),
        "strikes": sorted_strikes
    }
