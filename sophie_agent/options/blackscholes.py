"""Port of client/src/lib/black-scholes.ts — Black-Scholes-Merton with greeks.

Function-for-function identical to the TS (same formulas, same /100 and /365 scaling conventions)
so the two implementations can't quietly drift apart.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal


def standard_normal_cdf(x: float) -> float:
    t = 1 / (1 + 0.2316419 * abs(x))
    d = 0.3989423 * math.exp(-x * x / 2)
    prob = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))))
    return 1 - prob if x > 0 else prob


def standard_normal_pdf(x: float) -> float:
    return (1 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * x * x)


def inverse_standard_normal_cdf(p: float) -> float:
    """Acklam's algorithm — converts a probability back to a z-score."""
    if p <= 0 or p >= 1:
        raise ValueError("inverse_standard_normal_cdf: p must be in (0, 1)")
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    p_low, p_high = 0.02425, 1 - 0.02425

    if p < p_low:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
            ((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1
        )
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
        ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    )


@dataclass
class GreekResults:
    price: float
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float


def black_scholes(
    S: float, K: float, T: float, r: float, v: float, option_type: Literal["Call", "Put"], q: float = 0.0
) -> GreekResults:
    """`q` is the continuous dividend yield; pass it for index options (SPX ~1.3%)."""
    if T <= 0:
        intrinsic = max(S - K, 0.0) if option_type == "Call" else max(K - S, 0.0)
        if option_type == "Call":
            delta = 1.0 if S > K else 0.0
        else:
            delta = -1.0 if S < K else 0.0
        return GreekResults(price=intrinsic, delta=delta, gamma=0.0, vega=0.0, theta=0.0, rho=0.0)

    d1 = (math.log(S / K) + (r - q + v * v / 2) * T) / (v * math.sqrt(T))
    d2 = d1 - v * math.sqrt(T)
    discount_div = math.exp(-q * T)

    if option_type == "Call":
        price = S * discount_div * standard_normal_cdf(d1) - K * math.exp(-r * T) * standard_normal_cdf(d2)
        delta = discount_div * standard_normal_cdf(d1)
        gamma = discount_div * standard_normal_pdf(d1) / (S * v * math.sqrt(T))
        vega = S * discount_div * standard_normal_pdf(d1) * math.sqrt(T) / 100
        theta = (
            -(S * discount_div * standard_normal_pdf(d1) * v) / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * standard_normal_cdf(d2)
            + q * S * discount_div * standard_normal_cdf(d1)
        ) / 365
        rho = K * T * math.exp(-r * T) * standard_normal_cdf(d2) / 100
    else:
        price = K * math.exp(-r * T) * standard_normal_cdf(-d2) - S * discount_div * standard_normal_cdf(-d1)
        delta = discount_div * (standard_normal_cdf(d1) - 1)
        gamma = discount_div * standard_normal_pdf(d1) / (S * v * math.sqrt(T))
        vega = S * discount_div * standard_normal_pdf(d1) * math.sqrt(T) / 100
        theta = (
            -(S * discount_div * standard_normal_pdf(d1) * v) / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * standard_normal_cdf(-d2)
            - q * S * discount_div * standard_normal_cdf(-d1)
        ) / 365
        rho = -K * T * math.exp(-r * T) * standard_normal_cdf(-d2) / 100

    return GreekResults(price=price, delta=delta, gamma=gamma, vega=vega, theta=theta, rho=rho)
