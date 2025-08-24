# src/signals.py
from __future__ import annotations
import pandas as pd

def _to_num(s: pd.Series) -> pd.Series:
    """Coerce any series to float, NaN on bad values."""
    return pd.to_numeric(s, errors="coerce")

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    s = _to_num(series)
    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(method="bfill")

def sma(series: pd.Series, length: int) -> pd.Series:
    s = _to_num(series)
    return s.rolling(length, min_periods=length).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = _to_num(df["High"])
    low = _to_num(df["Low"])
    close = _to_num(df["Close"])
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=length).mean()
