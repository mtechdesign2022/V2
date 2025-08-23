#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, time
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("ERROR: yfinance not installed:", e, file=sys.stderr)
    raise

# ---- Config from environment (with defaults) ----
UNIVERSE_CSV = os.getenv("UNIVERSE_CSV", "data/ind_nifty500list.csv")
CACHE_DIR = Path(os.getenv("DATA_CACHE_DIR", "cache/eod"))
INDEX_SYMBOL = os.getenv("INDEX_SYMBOL", "NIFTY50")
DAYS_BACK = int(os.getenv("DAYS_BACK", "1200"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0"))  # 0 = all

# ---- Helpers ----
def to_yf(sym):
    return f"{str(sym).strip().upper()}.NS"

def index_to_yf(idx):
    m = {
        "NIFTY50": "^NSEI",
        "NIFTY_50": "^NSEI",
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "NIFTY500": "^CRSLDXN",
    }
    return m.get(str(idx).upper(), "^NSEI")

def _flatten_cols(df):
    """Flatten yfinance (possibly MultiIndex) to simple OHLCV names."""
    if isinstance(df.columns, pd.MultiIndex):
        newcols = []
        for col in df.columns:
            parts = [str(x).strip() for x in col if x is not None]
            picked = None
            # Look for canonical names anywhere in the tuple
            for p in parts:
                pl = p.lower()
                if pl == "open": picked = "Open"; break
                if pl == "high": picked = "High"; break
                if pl == "low": picked = "Low"; break
                if pl in ("close","adj close","adjclose"): picked = "Close" if pl=="close" else "Adj Close"; break
                if pl == "volume": picked = "Volume"; break
            if picked is None and len(parts) >= 2 and parts[0].lower() == "price":
                picked = parts[1].title()
            if picked is None:
                picked = parts[-1].title()
            newcols.append(picked)
        df.columns = newcols
    else:
        df.columns = [str(c).strip().title() for c in df.columns]
    return df

def ensure_cols(df):
    df = _flatten_cols(df)
    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]
    need = ["Open","High","Low","Close","Volume"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}. Got: {list(df.columns)}")
    out = df[["Open","High","Low","Close","Volume"]].copy()
    out = out.rename_axis("Date").reset_index()
    out["Date"] = pd.to_datetime(out["Date"]).dt.date.astype(str)
    return out

def merge_history(path, new_df):
    if path.exists():
        try:
            old = pd.read_csv(path)
            if "Date" in old.columns:
                old["Date"] = pd.to_datetime(old["Date"]).dt.date.astype(str)
            merged = (pd.concat([old, new_df], ignore_index=True)
                      .drop_duplicates(subset=["Date"])
                      .sort_values("Date"))
            return merged
        except Exception as e:
            print(f"[WARN] could not merge {path}: {e}", file=sys.stderr)
    return new_df.sort_values("Date")

def read_universe(csv_path):
    df = pd.read_csv(csv_path)
    if "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == "EQ"]
    syms = (df["Symbol"].astype(str).str.strip().str.upper()
            .dropna().unique().tolist())
    return sorted(syms)

def _dl(ticker, start, end):
    # Keep args explicit and close the paren properly!
    return yf.download(
        ticker,
        start=start.date(),
        end=end.date(),
        progress=False,
        auto_adjust=False,  # we want raw OHLC
        actions=False,
        timeout=60
    )

def fetch_symbol(sym, start, end):
    data = _dl(to_yf(sym), start, end)
    if data is None or data.empty:
        return None
    return ensure_cols(data)

def fetch_index(idx, start, end):
    data = _dl(index_to_yf(idx), start, end)
    if data is None or data.empty:
        return None
    return ensure_cols(data)

# ---- Main ----
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
            time.sleep(0.15)  # be polite to Yahoo endpoints
        except Exception as e:
            print(f"[WARN] {s}: {e}", file=sys.stderr)

    print(f"[DONE] wrote files for {wrote}/{total} symbols into {CACHE_DIR}")

if __name__ == "__main__":
    main()
