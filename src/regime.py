# src/regime.py
from __future__ import annotations
import pandas as pd
from .signals import sma

def market_regime(idx_df: pd.DataFrame, universe_dfs: dict[str, pd.DataFrame], pct_above_50dma_for_on: float = 55.0) -> str:
    # Index above 200-DMA?
    try:
        idx_above_200 = bool(idx_df["Close"].iloc[-1] > sma(idx_df["Close"], 200).iloc[-1])
    except Exception:
        idx_above_200 = False

    total = above = 0
    for sym, df in universe_dfs.items():
        try:
            if df is None or len(df) < 50:
                continue
            total += 1
            above += int(df["Close"].iloc[-1] > sma(df["Close"], 50).iloc[-1])
        except Exception:
            # Skip symbols with dirty data
            continue

    pct = (above / total * 100.0) if total > 0 else 0.0
    on = idx_above_200 and (pct >= pct_above_50dma_for_on)
    return "ON" if on else "OFF"
