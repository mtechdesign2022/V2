import numpy as np
import pandas as pd

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain, index=series.index).ewm(alpha=1/length, adjust=False).mean()
    roll_down = pd.Series(loss, index=series.index).ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(method="bfill")

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - df["Close"].shift()).abs()
    low_close = (df["Low"] - df["Close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=1).mean()

def relative_strength_ratio(stock_close: pd.Series, index_close: pd.Series) -> pd.Series:
    aligned = pd.concat([stock_close, index_close], axis=1, join="inner").dropna()
    ratio = aligned.iloc[:,0] / aligned.iloc[:,1]
    ratio.index = aligned.index
    return ratio

def is_reclaim_setup(df: pd.DataFrame, lookback_days: int = 126, recent_window: int = 10) -> pd.Series:
    rolling_low = df["Low"].rolling(lookback_days, min_periods=lookback_days).min()
    is_new_low = df["Low"] == rolling_low
    # index of recent new low
    last_low_value = rolling_low.where(is_new_low).ffill()
    # approximate "age" since last new low
    grp = (is_new_low != is_new_low.shift()).cumsum()
    low_age = (~is_new_low).groupby(grp).cumcount().reindex(df.index).fillna(method="ffill")
    undercut = (df["Low"] >= last_low_value * 0.98) & (df["Low"] <= last_low_value * 1.00)
    close_above = df["Close"] > last_low_value
    within_window = low_age <= recent_window
    cond_recent = last_low_value.notna()
    return cond_recent & undercut & close_above & within_window

def volume_thrust(df: pd.DataFrame, mult: float = 1.8) -> pd.Series:
    v20 = df["Volume"].rolling(20, min_periods=20).mean()
    return df["Volume"] >= mult * v20

def five_day_thrust(df: pd.DataFrame, ratio: float = 1.3) -> pd.Series:
    v5 = df["Volume"].rolling(5, min_periods=5).sum()
    v20s = df["Volume"].rolling(20, min_periods=20).sum()
    return v5 >= ratio * v20s

def rising_rsi_band(df: pd.DataFrame, lower: int = 20, upper: int = 38, length: int = 14) -> pd.Series:
    r = rsi(df["Close"], length)
    rising = r > r.shift(1)
    in_band = (r >= lower) & (r <= upper)
    return rising & in_band

def rs_20d_high(rs_series: pd.Series, lookback: int = 20) -> pd.Series:
    rolling_max = rs_series.rolling(lookback, min_periods=lookback).max()
    return rs_series >= rolling_max
