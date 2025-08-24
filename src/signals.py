import numpy as np
import pandas as pd

# ---- Helpers ----
def _to_num(series: pd.Series) -> pd.Series:
    """Convert series to numeric, coerce errors, drop NaNs later."""
    return pd.to_numeric(series, errors="coerce")

# ---- Core indicators ----
def sma(series: pd.Series, length: int) -> pd.Series:
    """Simple moving average."""
    s = _to_num(series)
    return s.rolling(length, min_periods=length).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    """Average True Range."""
    high = _to_num(df["High"])
    low = _to_num(df["Low"])
    close = _to_num(df["Close"])
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=length).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    """Relative Strength Index."""
    s = _to_num(series)
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.rolling(length, min_periods=length).mean()
    avg_loss = down.rolling(length, min_periods=length).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(method="bfill")

def volume_thrust(df: pd.DataFrame, lookback: int = 5, mult: float = 1.3) -> bool:
    """True if last day's volume > mult × avg of previous `lookback` days."""
    vol = _to_num(df["Volume"])
    if len(vol) < lookback + 1:
        return False
    recent_avg = vol.iloc[-(lookback+1):-1].mean()
    return bool(vol.iloc[-1] > mult * recent_avg)

def relative_strength_ratio(sym_close: pd.Series, idx_close: pd.Series) -> pd.Series:
    """RS ratio = symbol close / index close."""
    return _to_num(sym_close) / _to_num(idx_close)

def rising_rsi_band(series: pd.Series, lower: int = 20, upper: int = 38) -> bool:
    """Last 3 RSIs are rising and inside [lower, upper] band."""
    r = rsi(series)
    if len(r) < 3:
        return False
    last3 = r.iloc[-3:]
    rising = all(last3[i] < last3[i+1] for i in range(len(last3)-1))
    in_band = last3.between(lower, upper).all()
    return bool(rising and in_band)

# ---- Extra conditions ----
def five_day_thrust(df: pd.DataFrame) -> bool:
    """
    Returns True if today's volume is > 1.5x avg of last 5 days
    AND today's close > yesterday's close.
    """
    if len(df) < 6:
        return False
    vol = _to_num(df["Volume"])
    close = _to_num(df["Close"])
    avg5 = vol.iloc[-6:-1].mean()
    return bool(vol.iloc[-1] > 1.5 * avg5 and close.iloc[-1] > close.iloc[-2])

def rs_20d_high(sym_close: pd.Series, idx_close: pd.Series) -> bool:
    """
    Returns True if relative strength ratio is at a 20-day high.
    """
    rs = relative_strength_ratio(sym_close, idx_close)
    if len(rs) < 20:
        return False
    return bool(rs.iloc[-1] >= rs.rolling(20, min_periods=20).max().iloc[-1])

# ---- False breakdown reclaim ----
def is_reclaim_setup(
    df: pd.DataFrame,
    lookback_days: int | None = None,   # legacy alias
    pivot_lookback: int = 20,
    ma_len: int = 50,
    **_
) -> bool:
    """
    A pragmatic 'false breakdown reclaim' approximation:
      • Yesterday closed below the rolling pivot (recent lowest close or MA),
      • Today closed back above that level and above yesterday's high.
    """
    if lookback_days is not None:
        try:
            pivot_lookback = int(lookback_days)
        except Exception:
            pass

    if not {"High", "Low", "Close"}.issubset(df.columns):
        return False

    c = _to_num(df["Close"])
    h = _to_num(df["High"])

    if len(df) < max(pivot_lookback, ma_len) + 2:
        return False

    try:
        pivot = c.rolling(pivot_lookback, min_periods=pivot_lookback).min()
        ma50 = sma(c, ma_len)
        level = np.maximum(pivot, ma50)

        y_close = float(c.iloc[-2])
        t_close = float(c.iloc[-1])
        y_high = float(h.iloc[-2])
        lv_y = float(level.iloc[-2])

        broke_below = y_close < lv_y
        reclaimed = (t_close > lv_y) and (t_close > y_high)

        return bool(broke_below and reclaimed)
    except Exception:
        return False
