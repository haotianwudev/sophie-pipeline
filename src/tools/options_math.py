"""Black-Scholes IV solver and greeks, shared across every local (non-Cboe) SPX
option chain source: the ThetaData gap backfill (spx-option-snapshot/backfill.py)
and the OptionsDX historical re-derivation (spx-option-snapshot/convert_optionsdx_unified.py).

Moved here from backfill.py so both consumers use one implementation on one
consistent basis (flat r=0.05 rate, no dividend yield) -- see the
spx-option-chain-unify skill's decision point 1/2 for why this matters: Cboe's
own live greeks (2026-08-21+) are intentionally left untouched and NOT run
through this module, to avoid silently changing what production already shows.
"""

import math
from typing import Dict

from scipy.stats import norm
from scipy.optimize import brentq


def bs_price(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, S - K) if option_type == "CALL" else max(0.0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "CALL":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def solve_iv(price: float, S: float, K: float, T: float, r: float, option_type: str) -> float:
    intrinsic = max(0.0, S - K) if option_type == "CALL" else max(0.0, K - S)
    if price <= intrinsic or price <= 0.01 or S <= 0 or K <= 0 or T <= 0.0001:
        return 0.0
    try:
        return float(brentq(lambda sig: bs_price(S, K, T, r, sig, option_type) - price, 0.005, 4.0, xtol=1e-4))
    except Exception:
        return 0.0


def compute_greeks(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> Dict[str, float]:
    """Returns delta/gamma/theta/vega only -- no rho. Neither this nor the
    ThetaData backfill computes rho; only Cboe's live feed provides a real one.
    Keep that asymmetry honest (null rho pre-2026-08-21) rather than adding a
    rho formula here that a second source wouldn't have gotten either."""
    if T <= 0.0001 or sigma <= 0.001 or S <= 0 or K <= 0:
        delta = (1.0 if S > K else 0.0) if option_type == "CALL" else (-1.0 if K > S else 0.0)
        return {"delta": delta, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    nd1 = norm.pdf(d1)

    gamma = nd1 / (S * sigma * sqrt_T)
    vega = S * sqrt_T * nd1 * 0.01

    if option_type == "CALL":
        delta = norm.cdf(d1)
        theta = (- (S * nd1 * sigma) / (2.0 * sqrt_T) - r * K * math.exp(-r * T) * norm.cdf(d2)) / 365.0
    else:
        delta = norm.cdf(d1) - 1.0
        theta = (- (S * nd1 * sigma) / (2.0 * sqrt_T) + r * K * math.exp(-r * T) * norm.cdf(-d2)) / 365.0

    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}
