# src/watchlist_builder.py
from __future__ import annotations
from typing import List, Dict
import pandas as pd
import numpy as np

from .signals import (
    atr,
    sma,
    rsi,
    volume_thrust,
    relative_strength_ratio,
    rising_rsi_band,
    is_reclaim_setup,
    five_day_thrust,
    rs_20d_high,
)
from .fundamentals import fundamentals_pass
from .io_utils import safe_read_eod
from . import config


# ---------- small utilities ----------

_NUMERIC_COLS = ["Open", "High", "Low", "Close", "Volume"]

def _coerce_ohlcv(df: pd.DataFrame) -> pd.DataFrame | None:
    """Ensure OHLCV numeric; return None if columns missing/empty after coercion."""
    if df is None:
        return None
    if not set(_NUMERIC_COLS).issubset(df.columns):
        return None
    out = df.copy()
    for c in _NUMERIC_COLS:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    # Drop rows that are completely nan across OHLCV
    mask_any = out[_NUMERIC_COLS].notna().any(axis=1)
    out = out.loc[mask_any]
    if len(out) == 0:
        return None
    return out

def _last(series: pd.Series, default=np.nan):
    try:
        return float(series.iloc[-1])
    except Exception:
        return default


# ---------- main API ----------

def build_watchlist(
    symbols: List[str],
    fund_df: pd.DataFrame,
    data_cache_dir: str,
    index_symbol: str,
) -> pd.DataFrame:
    """
    Scan the given symbols and return a DataFrame of candidates.

    A symbol is included when ALL of these hold:
      • has enough EOD data (>= 60 bars after cleaning),
      • false-breakdown reclaim pattern (is_reclaim_setup),
      • volume thrust (5-day) OR five_day_thrust helper,
      • RSI rising in the 20–38 band,
      • RS ratio (vs index) at 20D high,
      • fundamentals_pass == True.

    Output columns:
      Symbol, Close, ATR%, RSI, RS20DHigh, VolThrust, Reclaim, Fundamentals,
      Notes (comma-joined short flags for quick glance).
    """
    out_rows: List[Dict] = []

    # Load and clean index EOD
    idx_df = _coerce_ohlcv(safe_read_eod(data_cache_dir, index_symbol))
    if idx_df is None or len(idx_df) < 60:
        # We still try to scan but RS metrics may be unavailable
        idx_close = None
    else:
        idx_close = idx_df["Close"].astype(float)

    for sym in symbols:
        df = _coerce_ohlcv(safe_read_eod(data_cache_dir, sym))
        if df is None or len(df) < 60:
            continue

        close = df["Close"].astype(float)
        vol   = df["Volume"].astype(float)

        # Core indicators (safe)
        try:
            atr_val = _last(atr(df, length=14))
            atr_pct = (atr_val / _last(close)) * 100.0 if np.isfinite(atr_val) else np.nan
        except Exception:
            atr_pct = np.nan

        try:
            rsi_val = _last(rsi(close, length=14))
        except Exception:
            rsi_val = np.nan

        # Signals
        try:
            reclaim_ok = bool(is_reclaim_setup(df, lookback_days=20, ma_len=50))
        except Exception:
            reclaim_ok = False

        # Two flavors of volume thrust accepted (either passes)
        vt_a = False
        vt_b = False
        try:
            vt_a = bool(volume_thrust(df, lookback=5, mult=1.3))
        except Exception:
            pass
        try:
            vt_b = bool(five_day_thrust(df))
        except Exception:
            pass
        vol_ok = bool(vt_a or vt_b)

        try:
            rsi_band_ok = bool(rising_rsi_band(close, lower=20, upper=38))
        except Exception:
            rsi_band_ok = False

        # Relative Strength 20D high (needs index)
        if idx_close is not None and len(idx_close) >= 20 and len(close) >= 20:
            try:
                rs20_ok = bool(rs_20d_high(close.align(idx_close, join="inner")[0],
                                           idx_close.align(close, join="inner")[0]))
            except Exception:
                rs20_ok = False
        else:
            rs20_ok = False

        # Fundamentals
        try:
            fund_ok = bool(fundamentals_pass(fund_df, sym))
        except Exception:
            fund_ok = False

        # Decision
        if all([reclaim_ok, vol_ok, rsi_band_ok, rs20_ok, fund_ok]):
            notes = []
            if vt_a: notes.append("VolThrust5×1.3")
            if vt_b: notes.append("5DThrust")
            notes.append("RSI↑(20–38)")
            notes.append("RS 20D High")
            notes.append("Reclaim")
            notes.append("Fund OK")

            out_rows.append({
                "Symbol": sym,
                "Close": _last(close),
                "ATR%": round(atr_pct, 2) if np.isfinite(atr_pct) else np.nan,
                "RSI": round(rsi_val, 1) if np.isfinite(rsi_val) else np.nan,
                "RS20DHigh": rs20_ok,
                "VolThrust": vol_ok,
                "Reclaim": reclaim_ok,
                "Fundamentals": fund_ok,
                "Notes": ", ".join(notes),
            })

    if not out_rows:
        return pd.DataFrame(columns=[
            "Symbol", "Close", "ATR%", "RSI", "RS20DHigh", "VolThrust",
            "Reclaim", "Fundamentals", "Notes"
        ])

    df_out = pd.DataFrame(out_rows)

    # Nice ordering: strongest momentum-ish at top
    # Sort by: RS20DHigh desc, VolThrust desc, lower ATR% first (tighter risk), higher RSI last
    df_out = df_out.sort_values(
        by=["RS20DHigh", "VolThrust", "ATR%", "RSI"],
        ascending=[False, True, True, False],
        kind="mergesort"
    ).reset_index(drop=True)

    return df_out
