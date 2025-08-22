import pandas as pd
from .signals import sma

def market_regime(index_df: pd.DataFrame, universe_dfs: dict, pct_above_50dma_for_on: float = 45.0) -> str:
    if len(index_df) < 200:
        return "UNKNOWN"
    idx_above_200 = index_df["Close"].iloc[-1] > sma(index_df["Close"], 200).iloc[-1]
    total = 0
    above = 0
    for sym, df in universe_dfs.items():
        if df is None or len(df) < 50: 
            continue
        total += 1
        above += int(df["Close"].iloc[-1] > sma(df["Close"], 50).iloc[-1])
    pct = (above / total * 100.0) if total > 0 else 0.0
    on = idx_above_200 and (pct >= pct_above_50dma_for_on)
    if on: return "ON"
    if idx_above_200 or (pct >= pct_above_50dma_for_on):
        return "CAUTION"
    return "OFF"
