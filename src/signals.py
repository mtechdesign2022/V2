# src/signals.py
from __future__ import annotations

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# Helpers (defensive: coerce strings to numbers, handle short data gracefully)
# -----------------------------------------------------------------------------

REQ_COLS = ["Open", "High", "Low", "Close", "Volume"]


def _coerce_numeric(series: pd.Series) -> pd.Series:
    """Coerce a Series (possibly containing strings) to numeric floats."""
    s = pd.to_numeric(series, errors="coerce")
    # If all NaN, just return as-is to let caller decide
    return s


def _ensure_ohlcv_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with OHLCV coerced to numeric; keep other cols untouched."""
    out = df.copy()
    for c in REQ_COLS:
        if c in out.columns:
            out[c] = _coerce_numeric(out[c])
    return out


def _has_cols(df: pd.DataFrame, cols: list[str]) -> bool:
    return all(c in df.columns for c in cols)


def _enough(series_or_df, n: int) -> bool:
    try:
        return len(series_or_df) >= n
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Core indicators
# -----------------------------------------------------------------------------

def sma(series: pd.Series, length: int) -> pd.Series:
    s = _coerce_numeric(series)
    return s.rolling(length, min_periods=length).mean()


def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    s = _coerce_numeric(series)
    delta = s.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(length, min_periods=length).mean()
    avg_loss = loss.rolling(length, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    # Avoid FutureWarning: use .bfill() instead of fillna(method="bfill")
    return out.bfill()


def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    if not _has_cols(df, ["High", "Low", "Close"]):
        return pd.Series(dtype=float)
    dfn = _ensure_ohlcv_numeric(df)
    high = dfn["High"]
    low = dfn["Low"]
    close = dfn["Close"]
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=length).mean()


# -----------------------------------------------------------------------------
# Relative Strength (vs index)
# -----------------------------------------------------------------------------

def _rs_series(sym_close: pd.Series, idx_close: pd.Series) -> pd.Series:
    s = _coerce_numeric(sym_close)
    i = _coerce_numeric(idx_close)
    rs = s / i.replace(0, np.nan)
    return rs


def relative_strength_ratio(sym_close: pd.Series, idx_close: pd.Series) -> pd.Series:
    """
    Raw relative strength line (symbol / index).
    """
    return _rs_series(sym_close, idx_close)


def rs_20d_high(sym_close: pd.Series, idx_close: pd.Series, window: int = 20) -> bool:
    """
    True if today's RS equals its rolling 20-day max.
    """
    rs = _rs_series(sym_close, idx_close)
    if not _enough(rs.dropna(), window):
        return False
    rs_max = rs.rolling(window, min_periods=window).max()
    return bool(np.isclose(rs.iloc[-1], rs_max.iloc[-1], equal_nan=False))


# -----------------------------------------------------------------------------
# Pattern/Filter style functions used by the scanner
# -----------------------------------------------------------------------------

def is_reclaim_setup(
    df: pd.DataFrame,
    lookback_days: int = 5,
    support_window: int = 20,
    require_green: bool = True,
) -> bool:
    """
    A simple 'false-breakdown then reclaim' heuristic:
      - Define support as the rolling `support_window`-day MIN of Close (shifted by 1).
      - If within the last `lookback_days` the Close dipped below support,
      - and today Close is back ABOVE support (i.e., reclaimed),
      - optionally also require today to be a green candle (Close > Open).

    Returns:
      bool for the latest bar.
    """
    if not _has_cols(df, ["Open", "Close"]):
        return False
    if not _enough(df, max(lookback_days + support_window, support_window + 1)):
        return False

    dfn = _ensure_ohlcv_numeric(df)
    close = dfn["Close"]
    open_ = dfn["Open"]

    # Prior support (shifted so the current day doesn't influence support level)
    prior_support = close.rolling(support_window, min_periods=support_window).min().shift(1)

    # Lookback window (including today for reclaim check)
    recent = pd.DataFrame({"Close": close, "Support": prior_support}).tail(lookback_days + 1)

    # Any breakdown in the last lookback_days BEFORE today?
    breakdown = (recent["Close"].iloc[:-1] < recent["Support"].iloc[:-1]).any()

    # Reclaim today: Close > prior support
    reclaim_today = recent["Close"].iloc[-1] > (recent["Support"].iloc[-1] if pd.notna(recent["Support"].iloc[-1]) else -np.inf)

    green_today = True if not require_green else bool(close.iloc[-1] > open_.iloc[-1])

    return bool(breakdown and reclaim_today and green_today)


def volume_thrust(df: pd.DataFrame, lookback: int = 20, multiple: float = 1.5) -> bool:
    """
    Today's Volume > multiple * average Volume of the previous `lookback` days.
    """
    if not _has_cols(df, ["Volume"]):
        return False
    dfn = _ensure_ohlcv_numeric(df)
    vol = dfn["Volume"]
    if not _enough(vol.dropna(), lookback + 1):
        return False
    prior_avg = vol.iloc[-(lookback + 1):-1].mean()
    return bool(vol.iloc[-1] > multiple * prior_avg)


def five_day_thrust(df: pd.DataFrame, short: int = 5, long: int = 50, ratio_min: float = 1.2) -> bool:
    """
    5-day avg volume vs 50-day avg volume. True if (5d / 50d) >= ratio_min.
    """
    if not _has_cols(df, ["Volume"]):
        return False
    dfn = _ensure_ohlcv_numeric(df)
    vol = dfn["Volume"]
    if not _enough(vol.dropna(), max(short, long)):
        return False
    v5 = vol.rolling(short, min_periods=short).mean().iloc[-1]
    v50 = vol.rolling(long, min_periods=long).mean().iloc[-1]
    if pd.isna(v5) or pd.isna(v50) or v50 == 0:
        return False
    return bool((v5 / v50) >= ratio_min)


def rising_rsi_band(
    close: pd.Series,
    low: float = 20.0,
    high: float = 38.0,
    lookback: int = 10,
    rsi_len: int = 14,
) -> bool:
    """
    RSI is inside [low, high] and generally rising over the last `lookback` bars.
    """
    r = rsi(close, rsi_len)
    r_valid = r.dropna()
    if not _enough(r_valid, lookback):
        return False

    r_tail = r.tail(lookback)
    if r_tail.isna().any():
        return False

    in_band = (r_tail.iloc[-1] >= low) and (r_tail.iloc[-1] <= high)

    # Rising: last value >= median of the last `lookback` and above its min
    rising = (r_tail.iloc[-1] >= r_tail.median()) and (r_tail.iloc[-1] > r_tail.min())
    return bool(in_band and rising)


# -----------------------------------------------------------------------------
# Exports (keep names consistent with imports in the app)
# -----------------------------------------------------------------------------

__all__ = [
    "sma",
    "rsi",
    "atr",
    "relative_strength_ratio",
    "rs_20d_high",
    "is_reclaim_setup",
    "volume_thrust",
    "five_day_thrust",
    "rising_rsi_band",
]
