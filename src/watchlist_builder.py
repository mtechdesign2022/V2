from dataclasses import dataclass
from typing import List
import pandas as pd
from .signals import (atr, relative_strength_ratio, is_reclaim_setup, volume_thrust,
                      five_day_thrust, rising_rsi_band, rs_20d_high)
from .fundamentals import fundamentals_pass
from . import config
from .io_utils import safe_read_eod

@dataclass
class SignalParams:
    reclaim_lookback: int = config.RECLAIM_LOOKBACK_DAYS
    reclaim_recent: int = config.RECLAIM_RECENT_WINDOW_DAYS
    rsi_lower: int = 20
    rsi_upper: int = 38
    rsi_len: int = 14
    volume_mult: float = config.VOLUME_THRUST_MULTIPLIER
    thrust5_ratio: float = config.FIVE_DAY_THRUST_RATIO
    rs_lookback: int = config.RS_LOOKBACK_DAYS
    entry_buffer_pct: float = config.ENTRY_BUFFER_PCT
    stop_atr_mult: float = config.STOP_ATR_MULTIPLIER
    stop_min_buffer_pct: float = config.STOP_MIN_BUFFER_PCT

def build_watchlist(symbols: List[str], fundamentals_df: pd.DataFrame, cache_dir: str, index_symbol: str) -> pd.DataFrame:
    idx_df = safe_read_eod(cache_dir, index_symbol)
    if idx_df is None or len(idx_df) < 220:
        raise RuntimeError(f"Index EOD not found or insufficient: {index_symbol}")
    idx_close = idx_df[["Date","Close"]].set_index("Date")["Close"]

    rows = []
    for sym in symbols:
        df = safe_read_eod(cache_dir, sym)
        if df is None or len(df) < 200:
            continue
        if not is_reclaim_setup(df, lookback_days=config.RECLAIM_LOOKBACK_DAYS, recent_window=config.RECLAIM_RECENT_WINDOW_DAYS).iloc[-1]:
            continue
        rsi_ok = rising_rsi_band(df, lower=20, upper=38, length=14).iloc[-1]
        vol_ok = bool(volume_thrust(df, mult=config.VOLUME_THRUST_MULTIPLIER).iloc[-1] or five_day_thrust(df, ratio=config.FIVE_DAY_THRUST_RATIO).iloc[-1])
        rs_series = relative_strength_ratio(df.set_index("Date")["Close"], idx_close)
        rs_ok = rs_20d_high(rs_series, lookback=config.RS_LOOKBACK_DAYS).reindex(df["Date"]).fillna(False).iloc[-1]

        high = df["High"].iloc[-1]
        rolling_low = df["Low"].rolling(config.RECLAIM_LOOKBACK_DAYS, min_periods=config.RECLAIM_LOOKBACK_DAYS).min().iloc[-1]
        entry = round(high * (1 + config.ENTRY_BUFFER_PCT/100), 2)
        _atr = atr(df, length=14).iloc[-1]
        stop = round(min(rolling_low - config.STOP_ATR_MULTIPLIER * _atr,
                         rolling_low * (1 - config.STOP_MIN_BUFFER_PCT/100)), 2)
        risk_per_share = max(0.01, entry - stop)
        r1 = round(entry + risk_per_share * 1.0, 2)
        r2 = round(entry + risk_per_share * 2.0, 2)

        frow = fundamentals_df[fundamentals_df["Symbol"] == sym]
        if len(frow) == 1:
            fchecks = fundamentals_pass(frow.iloc[0], allow_unknown=config.ALLOW_UNKNOWN_FUNDAMENTALS)
        else:
            fchecks = {"de_le_1_5": None,"icr_ge_2_5": None,"pledge_le_20": None,"qoq_rev_or_eps_ge_1_of_3": None,"status": "PASS" if config.ALLOW_UNKNOWN_FUNDAMENTALS else "FAIL"}

        status = "Valid" if (rsi_ok and vol_ok and rs_ok and fchecks.get("status") == "PASS") else "Reject"

        rows.append({
            "Symbol": sym,
            "Status": status,
            "Signal": "False Breakdown Reclaim",
            "Entry": entry,
            "Stop": stop,
            "R1": r1,
            "R2": r2,
            "VolSpike": vol_ok,
            "RS_20D_High": rs_ok,
            "RSI_Rising_20_38": rsi_ok,
            "Fund_DE_le_1_5": fchecks.get("de_le_1_5"),
            "Fund_ICR_ge_2_5": fchecks.get("icr_ge_2_5"),
            "Fund_Pledge_le_20": fchecks.get("promoter_pledge_pct") if isinstance(fchecks, dict) and "promoter_pledge_pct" in fchecks else fchecks.get("pledge_le_20"),
            "Fund_QoQ_Pos_1of3": fchecks.get("qoq_rev_or_eps_ge_1_of_3"),
            "Fundamentals": fchecks.get("status"),
        })
    df_out = pd.DataFrame(rows)
    if not df_out.empty:
        df_out = df_out[["Symbol","Status","Signal","Entry","Stop","R1","R2","VolSpike","RS_20D_High","RSI_Rising_20_38",
                         "Fund_DE_le_1_5","Fund_ICR_ge_2_5","Fund_Pledge_le_20","Fund_QoQ_Pos_1of3","Fundamentals"]]
    return df_out
