from pathlib import Path
import pandas as pd
from typing import Set

def load_universe_symbols(universe_csv: str) -> Set[str]:
    df = pd.read_csv(universe_csv)
    if "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == "EQ"]
    symbols = set(df["Symbol"].astype(str).str.strip().str.upper().tolist())
    if not symbols:
        raise ValueError("Universe symbols empty after filtering.")
    return symbols

def read_eod_csv(cache_dir: str, symbol: str) -> pd.DataFrame:
    p = Path(cache_dir) / f"{symbol}.csv"
    if not p.exists():
        raise FileNotFoundError(f"EOD file not found: {p}")
    df = pd.read_csv(p, parse_dates=["Date"])
    needed = {"Date","Open","High","Low","Close","Volume"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"{p} missing columns: {missing}")
    df = df.sort_values("Date").reset_index(drop=True)
    return df

def safe_read_eod(cache_dir: str, symbol: str):
    try:
        return read_eod_csv(cache_dir, symbol)
    except Exception:
        return None
