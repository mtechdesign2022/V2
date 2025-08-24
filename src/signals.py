# src/signals.py
from __future__ import annotations
import pandas as pd
import numpy as np

# ---------- generic helpers ----------

def _to_num(s: pd.Series) -> pd.Series:
    """Coerce any series to float, NaN on bad values."""
    return pd.to_numeric(s, errors="coerce")

def _ok_len(series: pd.Series, n: int) -> bool:
    return isinstance(series, pd.Series) and len(series.dropna()) >= n

# ---------- classic indicators (hardened) ----------

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    s = _to_num(series)
    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss
    out = 100 - (100 / (1 + rs))
    return out.fillna(method="bfill")

def sma(series: pd.Series, length: int) -> pd.Series:
    s = _to_num(series)
    return s.rolling(length, min_periods=length).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = _to_num(df["High"])
    low = _to_num(df["Low"])
    close = _to_num(df["Close"])
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close  = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=length).mean()

# ---------- RS tools ----------

def relative_strength_ratio(stock_close: pd.Series, index_close: pd.Series) -> pd.Series:
    """
    RS = stock / index (aligned on index). Robust to NaNs and misaligned dates.
    """
    s = _to_num(stock_close)
    i = _to_num(index_close)
    rs = (s / i).replace([np.inf, -np.inf], np.nan)
    return rs.fillna(method="ffill").fillna(method="bfill")

def rs_20d_high(rs: pd.Series, lookback: int = 20) -> bool:
    """
    True if latest RS is at/near 20-day high.
    """
    if not _ok_len(rs, lookback):
        return False
    roll_max = rs.rolling(lookback, min_periods=lookback).max()
    try:
        return bool(rs.iloc[-1] >= roll_max.iloc[-1])
    except Exception:
        return False

# ---------- volume thrusts ----------

def volume_thrust(df: pd.DataFrame, lookback: int = 20, multiple: float = 1.5) -> bool:
    """
    Today's volume > multiple * rolling mean volume over lookback.
    """
    if "Volume" not in df.columns:
        return False
    v = _to_num(df["Volume"])
    if not _ok_len(v, max(lookback, 2)):
        return False
    ma = v.rolling(lookback, min_periods=lookback).mean()
    try:
        return bool(v.iloc[-1] > multiple * ma.iloc[-1])
    except Exception:
        return False

def five_day_thrust(df: pd.DataFrame, multiple: float = 1.3) -> bool:
    """
    Simple 5-day thrust: today's volume > multiple * average of last 5 volumes.
    """
    if "Volume" not in df.columns:
        return False
    v = _to_num(df["Volume"])
    if not _ok_len(v, 5):
        return False
    ma5 = v.rolling(5, min_periods=5).mean()
    try:
        return bool(v.iloc[-1] > multiple * ma5.iloc[-1])
    except Exception:
        return False

# ---------- RSI band/rising ----------

def rising_rsi_band(close: pd.Series, low: float = 20.0, high: float = 38.0, length: int = 14) -> bool:
    """
    RSI is inside [low, high] AND rising vs prior day.
    """
    r = rsi(close, length)
    if not _ok_len(r, length + 2):
        return False
    try:
        latest = float(r.iloc[-1])
        prev   = float(r.iloc[-2])
        return (low <= latest <= high) and (latest > prev)
    except Exception:
        return False

# ---------- Reclaim setup (false breakdown reclaim) ----------

def is_reclaim_setup(df: pd.DataFrame,
                     pivot_lookback: int = 20,
                     ma_len: int = 50) -> bool:
    """
    A pragmatic 'false breakdown reclaim' approximation:
      • Yesterday closed below the rolling pivot (recent lowest close or MA),
      • Today closed back above that level and above yesterday's high.
    This is intentionally forgiving and won’t crash on dirty data.
    """
    if not {"High","Low","Close"}.issubset(df.columns):
        return False

    c = _to_num(df["Close"])
    h = _to_num(df["High"])

    if len(df) < max(pivot_lookback, ma_len) + 2:
        return False

    try:
        # Define a reclaim level as the max of:
        # - rolling min close (recent pivot) and
        # - 50-DMA (to avoid obvious downtrends)
        pivot = c.rolling(pivot_lookback, min_periods=pivot_lookback).min()
        ma50  = sma(c, ma_len)
        level = np.maximum(pivot, ma50)

        y_close = float(c.iloc[-2])
        t_close = float(c.iloc[-1])
        y_high  = float(h.iloc[-2])
        lv_y    = float(level.iloc[-2])

        broke_below = y_close < lv_y
        reclaimed   = (t_close > lv_y) and (t_close > y_high)

        return bool(broke_below and reclaimed)
    except Exception:
        return False
