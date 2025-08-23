#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Update EOD OHLCV cache for V2.3 from the local universe CSV using yfinance.
- Reads: data/ind_nifty500list.csv (uses Series == EQ)
- Writes: cache/eod/{SYMBOL}.csv with columns: Date,Open,High,Low,Close,Volume
- Also updates index CSV: cache/eod/{INDEX_SYMBOL}.csv (default: NIFTY50 => ^NSEI)
Environment:
- UNIVERSE_CSV (default: data/ind_nifty500list.csv)
- DATA_CACHE_DIR (default: cache/eod)
- INDEX_SYMBOL (default: NIFTY50)
- DAYS_BACK (default: 1200)  # ~5 years
Dependencies: yfinance, pandas
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("ERROR: yfinance not installed. Add 'yfinance' to requirements.txt.", file=sys.stderr)
    raise

UNIVERSE_CSV = os.getenv("UNIVERSE_CSV", "data/ind_nifty500list.csv")
CACHE_DIR = Path(os.getenv("DATA_CACHE_DIR", "cache/eod"))
INDEX_SYMBOL = os.getenv("INDEX_SYMBOL", "NIFTY50")
DAYS_BACK = int(os.getenv("DAYS_BACK", "1200"))

def to_yf(sym: str) -> str:
    return f"{str(sym).strip().upper()}.NS"

def index_to_yf(index_symbol: str) -> str:
    mapping = {"NIFTY50": "^NSEI", "NIFTY_50": "^NSEI", "NIFTY500": "^CRSLDXN", "BANKNIFTY": "^NSEBANK"}
    return mapping.get(index_symbol.upper(), "^NSEI")

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename_axis("Date").reset_index()
    if "Adj Close" in df.columns and df["Close"].isna().all():
        df["Close"] = df["Adj Close"]
    keep = ["Date","Open","High","Low","Close","Volume"]
    df = df[keep].copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    return df

def merge_history(path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if path.exists():
        try:
            old = pd.read_csv(path, parse_dates=["Date"])
            old["Date"] = pd.to_datetime(old["Date"]).dt.date.astype(str)
            return (pd.concat([old, new_df], ignore_index=True)
                      .drop_duplicates(subset=["Date"])
                      .sort_values("Date"))
        except Exception:
            return new_df.sort_values("Date")
    return new_df.sort_values("Date")

def read_universe(csv_path: Path) -> list[str]:
    df = pd.read_csv(csv_path)
    if "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == "EQ"]
    return sorted(df["Symbol"].astype(str).str.strip().str.upper().unique().tolist())

def fetch_symbol(sym: str, period_days: int) -> pd.DataFrame | None:
    yf_sym = to_yf(sym)
    end = datetime.utcnow()
    start = end - timedelta(days=period_days)
    try:
        hist = yf.download(yf_sym, start=start.date(), end=end.date(), progress=False, auto_adjust=False, actions=False, timeout=20)
        if hist is None or hist.empty:
            return None
        return ensure_cols(hist)
    except Exception as e:
        print(f"WARN: fetch failed for {sym} ({yf_sym}): {e}", file=sys.stderr)
        return None

def fetch_index(index_symbol: str, period_days: int) -> pd.DataFrame | None:
    yf_sym = index_to_yf(index_symbol)
    end = datetime.utcnow()
    start = end - timedelta(days=period_days)
    try:
        hist = yf.download(yf_sym, start=start.date(), end=end.date(), progress=False, auto_adjust=False, actions=False, timeout=20)
        if hist is None or hist.empty:
            return None
        return ensure_cols(hist)
    except Exception as e:
        print(f"WARN: fetch failed for index {index_symbol} ({yf_sym}): {e}", file=sys.stderr)
        return None

def main():
    csv_path = Path(UNIVERSE_CSV)
    if not csv_path.exists():
        raise SystemExit(f"Universe CSV not found: {csv_path}")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    symbols = read_universe(csv_path)
    print(f"Universe size: {len(symbols)}")

    idx_df = fetch_index(INDEX_SYMBOL, DAYS_BACK)
    if idx_df is not None:
        idx_path = CACHE_DIR / f"{INDEX_SYMBOL}.csv"
        merge_history(idx_path, idx_df).to_csv(idx_path, index=False)
        print(f"Updated index: {idx_path}")
    else:
        print("WARN: index fetch failed")

    for i, sym in enumerate(symbols, 1):
        df = fetch_symbol(sym, DAYS_BACK)
        if df is None:
            print(f"WARN: no data for {sym}")
            continue
        out = CACHE_DIR / f"{sym}.csv"
        merge_history(out, df).to_csv(out, index=False)
        print(f"[{i}/{len(symbols)}] {sym}")
        time.sleep(0.2)

if __name__ == "__main__":
    main()
