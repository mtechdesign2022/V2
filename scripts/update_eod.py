#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updates EOD OHLCV cache into cache/eod/*.csv from your Universe CSV using yfinance.
Robust to yfinance's recent column/index changes.
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
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0"))  # 0 = all

def to_yf(sym: str) -> str:
    return f"{str(sym).strip().str.upper() if hasattr(sym,'strip') else str(sym).upper()}.NS"

def index_to_yf(index_symbol: str) -> str:
    mapping = {
        "NIFTY50": "^NSEI",
        "NIFTY_50": "^NSEI",
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "NIFTY500": "^CRSLDXN",  # may be sparse
    }
    return mapping.get(str(index_symbol).upper(), "^NSEI")

def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    # If MultiIndex columns like ('Price','Close'), keep the last level -> 'Close'
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[-1] for c in df.columns]
    # Normalize capitalization and spacing just in case
    df.columns = [str(c).strip().title() for c in df.columns]
    return df

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = _flatten_cols(df)
    # yfinance returns columns: Open, High, Low, Close, Adj Close, Volume
    # make sure Close exists; fall back to Adj Close if needed
    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]
    needed = ["Open","High","Low","Close","Volume"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns from yfinance dataset: {missing}. Got: {list(df.columns)}")
    df = df[["Open","High","Low","Close","Volume"]].copy()
    df = df.rename_axis("Date").reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    return df

def merge_history(path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if path.exists():
        try:
            old = pd.read_csv(path, parse_dates=["Date"])
            old["Date"] = pd.to_datetime(old["Date"]).dt.date.astype(str)
            merged = (pd.concat([old, new_df], ignore_index=True)
                        .drop_duplicates(subset=["Date"])
                        .sort_values("Date"))
            return merged
        except Exception as e:
            print(f"[WARN] could not merge {path}: {e}", file=sys.stderr)
            return new_df.sort_values("Date")
    return new_df.sort_values("Date")

def read_universe(csv_path: Path) -> list[str]:
    df = pd.read_csv(csv_path)
    if "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == "EQ"]
    syms = (df["Symbol"]
            .astype(str).str.strip().str.upper()
            .dropna().unique().tolist())
    return sorted(syms)

def _dl(ticker: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    # Use explicit options to avoid surprises across yfinance versions
    return yf.download(
        ticker,
        start=start.date(), end=end.date(),
        progress=False,
        auto_adjust=False,   # we want raw OHLCV; adjust yourself later if needed
        actions=False,
        group_by="column",   # ensures single-level columns for single tickers
        timeout=60
    )

def fetch_symbol(sym: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    data = _dl(to_yf(sym), start, end)
    if data is None or data.empty:
        return None
    return ensure_cols(data)

def fetch_index(index_symbol: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    data = _dl(index_to_yf(index_symbol), start, end)
    if data is None or data.empty:
        return None
    return ensure_cols(data)

def main():
    csv_path = Path(UNIVERSE_CSV)
    if not csv_path.exists():
        raise SystemExit(f"Universe CSV not found: {csv_path}")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    syms = read_universe(csv_path)
    if MAX_SYMBOLS > 0:
        syms = syms[:MAX_SYMBOLS]
    print(f"[INFO] Universe size (EQ): {len(syms)} (MAX_SYMBOLS={MAX_SYMBOLS})")

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
            print(f"[OK] Updated index: {idx_path}")
    except Exception as e:
        print(f"[ERR] Index update failed: {e}", file=sys.stderr)

    wrote = 0
    total = len(syms)
    for i, s in enumerate(syms, 1):
        try:
            df = fetch_symbol(s, start, end)
            if df is None or df.empty:
                print(f"[SKIP] {s}: no data")
                continue
            out = CACHE_DIR / f"{s}.csv"
            merge_history(out, df).to_csv(out, index=False)
            wrote += 1
            if i % 25 == 0 or i == total:
                print(f"[PROGRESS] {i}/{total} processed; wrote so far: {wrote}")
            time.sleep(0.15)
        except Exception as e:
            print(f"[WARN] {s}: {e}", file=sys.stderr)
    print(f"[DONE] wrote files for {wrote}/{total} symbols into {CACHE_DIR}")

if __name__ == "__main__":
    main()
