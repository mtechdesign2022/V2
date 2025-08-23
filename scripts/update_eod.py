#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updates EOD OHLCV cache into cache/eod/*.csv from your Universe CSV using yfinance.
Robust to yfinance's many possible column layouts.
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
    """Convert NSE symbol -> Yahoo Finance ticker"""
    return f"{str(sym).strip().upper()}.NS"

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
    """Flatten MultiIndex columns from yfinance to standard OHLCV names"""
    KNOWN = {"open": "Open", "high": "High", "low": "Low",
             "close": "Close", "adj close": "Adj Close", "volume": "Volume"}
    if isinstance(df.columns, pd.MultiIndex):
        newcols = []
        for col in df.columns:
            parts = [str(x).strip() for x in col if x is not None]
            picked = None
            for p in parts:
                key = p.lower()
                if key in KNOWN:
                    picked = KNOWN[key]
                    break
            if picked is None and len(parts) >= 2 and parts[0].lower() == "price":
                picked = parts[1].title()
            if picked is None:
                picked = parts[-1].title()
            newcols.append(picked)
        df.columns = newcols
    else:
        df.columns = [str(c).strip().title() for c in df.columns]
    return df

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = _flatten_cols(df)
    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]
    needed = ["Open","High","Low","Close","Volume"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}. Got: {list(df.columns)}")
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
    syms = df["Symbol"].astype(str).str.strip().str.upper().dropna().unique().tolist()
    return sorted(syms)

def _dl(ticker: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    return yf.download(
        ticker,
        start=start.date(), end=end.date(),
        progress=False,
