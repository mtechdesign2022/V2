#!/usr/bin/env python3
# Creates cache/eod/NIFTY50.csv from Yahoo (^NSEI)

import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    raise SystemExit(f"yfinance missing: {e}")

CACHE_DIR = Path("cache/eod")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

start = datetime.utcnow() - timedelta(days=1800)  # ~7 years
end = datetime.utcnow()

print("[INFO] downloading ^NSEI ...")
df = yf.download(
    "^NSEI",
    start=start.date(),
    end=end.date(),
    progress=False,
    auto_adjust=False,
    actions=False,
    timeout=60,
)

if df is None or df.empty:
    raise SystemExit("Got empty data for ^NSEI")

# Flatten yfinance columns → keep standard OHLCV
if isinstance(df.columns, pd.MultiIndex):
    newcols = []
    for col in df.columns:
        parts = [str(x).strip() for x in col if x is not None]
        # pick canonical name if present
        picked = None
        for p in parts:
            pl = p.lower()
            if pl == "open": picked = "Open"; break
            if pl == "high": picked = "High"; break
            if pl == "low": picked = "Low"; break
            if pl in ("close","adj close","adjclose"): picked = "Close" if pl=="close" else "Adj Close"; break
            if pl == "volume": picked = "Volume"; break
        if picked is None:
            picked = parts[-1].title()
        newcols.append(picked)
    df.columns = newcols
else:
    df.columns = [str(c).strip().title() for c in df.columns]

# Ensure Close exists (fallback to Adj Close)
if "Close" not in df.columns and "Adj Close" in df.columns:
    df["Close"] = df["Adj Close"]

need = ["Open", "High", "Low", "Close", "Volume"]
missing = [c for c in need if c not in df.columns]
if missing:
    raise SystemExit(f"Missing expected columns: {missing}. Got: {list(df.columns)}")

out = df[need].copy()
out = out.rename_axis("Date").reset_index()
out["Date"] = pd.to_datetime(out["Date"]).dt.date.astype(str)

out_path = CACHE_DIR / "NIFTY50.csv"
out.to_csv(out_path, index=False)
print(f"[OK] wrote {out_path} with {len(out)} rows")
