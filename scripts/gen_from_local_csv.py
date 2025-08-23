#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

CSV_PATH = Path("data/ind_nifty500list.csv")
OUT = Path("output"); OUT.mkdir(parents=True, exist_ok=True)

def main():
    if not CSV_PATH.exists():
        raise SystemExit(f"CSV not found: {CSV_PATH.resolve()}")
    df = pd.read_csv(CSV_PATH)
    for c in ["Company Name","Industry","Symbol","Series","ISIN Code"]:
        if c not in df.columns: raise SystemExit(f"Missing required column: {c}")
    df = df[df["Series"].astype(str).str.upper() == "EQ"].copy()
    df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
    symbols = sorted(df["Symbol"].dropna().unique().tolist())
    Path("output/nse500_universe.txt").write_text("\n".join(symbols), encoding="utf-8")
    pd.DataFrame({"Symbol": symbols}).to_csv("output/nse500_universe.csv", index=False)
    Path("output/nse500_tradingview.txt").write_text("\n".join([f"NSE:{s}" for s in symbols]), encoding="utf-8")
    print(f"Done. Symbols: {len(symbols)}")

if __name__ == "__main__":
    main()
