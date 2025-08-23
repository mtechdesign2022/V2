#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updates EOD OHLCV cache into cache/eod/*.csv from your Universe CSV using yfinance.
"""
import os, sys, time
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

try:
    import yfinance as yf
except Exception:
    print("ERROR: yfinance not installed. Add 'yfinance' to requirements.txt.", file=sys.stderr)
    raise

UNIVERSE_CSV = os.getenv("UNIVERSE_CSV", "data/ind_nifty500list.csv")
CACHE_DIR = Path(os.getenv("DATA_CACHE_DIR", "cache/eod"))
INDEX_SYMBOL = os.getenv("INDEX_SYMBOL", "NIFTY50")
DAYS_BACK = int(os.getenv("DAYS_BACK", "1200"))

def to_yf(sym: str) -> str:
    return f"{str(sym).strip().upper()}.NS"

def index_to_yf(index_symbol: str) -> str:
    mapping = {
        "NIFTY50": "^NSEI",
        "NIFTY_50": "^NSEI",
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "NIFTY500": "^CRSLDXN",  # may be sparse
    }
    return mapping.get(index_symbol.upper(), "^NSEI")

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename_axis("Date").reset_index()
    if "Adj Close" in df.columns and "Close" in df.columns and df["Close"].isna().all():
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
            merged = pd.concat([old, new_df], ignore_index=True)\
                       .drop_duplicates(subset=["Date"])\
                       .sort_values("Date")
            return merged
        except Exception as e:
            print(f"WARN: could not merge {path}: {e}", file=sys.stderr)
            return new_df.sort_values("Date")
    return new_df.sort_values("Date")

def read_universe(csv_path: Path) -> list[str]:
    df = pd.read_csv(csv_path)
    if "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == "EQ"]
    syms = df["Symbol"].astype(str).str.strip().str.upper().dropna().unique().tolist()
    return sorted(syms)

def fetch_symbol(sym: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    yf_sym = to_yf(sym)
    hist = yf.download(
        yf_sym,
        start=start.date(), end=end.date(),
        progress=False, auto_adjust=False, actions=False, timeout=30
    )
    if hist is None or hist.empty:
        return None
    return ensure_cols(hist)

def fetch_index(index_symbol: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    yf_sym = index_to_yf(index_symbol)
    hist = yf.download(
        yf_sym,
        start=start.date(), end=end.date(),
        progress=False, auto_adjust=False, actions=False, timeout=30
    )
    if hist is None or hist.empty:
        return None
    return ensure_cols(hist)

def main():
    csv_path = Path(UNIVERSE_CSV)
    if not csv_path.exists():
        raise SystemExit(f"Universe CSV not found: {csv_path}")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    syms = read_universe(csv_path)
    print(f"[INFO] Universe size (EQ): {len(syms)}")

    end = datetime.utcnow()
    start = end - timedelta(days=DAYS_BACK)

    # Index first
    try:
        idx_df = fetch_index(INDEX_SYMBOL, start, end)
        if idx_df is None or idx_df.empty:
            print(f"[WARN] Index fetch empty for {INDEX_SYMBOL} -> {index_to_yf(INDEX_SYMBOL)}", file=sys.stderr)
        else:
            idx_path = CACHE_DIR / f"{INDEX_SYMBOL}.csv"
            merge_history(idx_path, idx_df).to_csv(idx_path, index=False)
            print(f"[OK] Updated index: {idx_path} rows={len(pd.read_csv(idx_path))}")
    except Exception as e:
        print(f"[ERR] Index update failed: {e}", file=sys.stderr)

    wrote = 0
    for i, s in enumerate(syms, 1):
        try:
            df = fetch_symbol(s, start, end)
            if df is None or df.empty:
                print(f"[SKIP] {s}: no data")
                continue
            out = CACHE_DIR / f"{s}.csv"
            merge_history(out, df).to_csv(out, index=False)
            wrote += 1
            if i % 25 == 0:
                print(f"[PROGRESS] {i}/{len(syms)} processed; wrote so far: {wrote}")
            time.sleep(0.2)
        except Exception as e:
            print(f"[WARN] {s}: {e}", file=sys.stderr)
    print(f"[DONE] wrote files for {wrote}/{len(syms)} symbols into {CACHE_DIR}")

if __name__ == "__main__":
    main()
