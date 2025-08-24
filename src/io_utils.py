# src/io_utils.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

NEEDED = ["Date", "Open", "High", "Low", "Close", "Volume"]

def _titleize(cols):
    return [str(c).strip().title() for c in cols]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = _titleize(df.columns)
    # Map Adj Close -> Close if Close is missing
    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]
    # Keep only the needed columns that exist
    keep = [c for c in NEEDED if c in df.columns]
    df = df[keep]
    # Ensure Date exists and is string yyyy-mm-dd
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date.astype(str)
    return df

def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ("Open", "High", "Low", "Close", "Volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Drop rows where Close is NaN or Date missing
    if "Close" in df.columns:
        df = df.dropna(subset=["Close"])
    if "Date" in df.columns:
        df = df.dropna(subset=["Date"])
    return df

def _sort_unique(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["Date"]) if "Date" in df.columns else df
    if "Date" in df.columns:
        return df.sort_values("Date").reset_index(drop=True)
    return df.reset_index(drop=True)

def safe_read_eod(cache_dir: str | Path, symbol: str) -> pd.DataFrame | None:
    """
    Load an OHLCV CSV and sanitize it:
      - normalize column names
      - coerce numeric columns
      - drop bad rows
      - sort by Date
    Returns None if file missing or cannot be made valid.
    """
    path = Path(cache_dir) / f"{symbol}.csv"
    if not path.exists():
        return None

    try:
        df = pd.read_csv(path)
        df = _normalize_columns(df)
        df = _coerce_numeric(df)
        df = _sort_unique(df)

        # Ensure we ended with the required columns
        if not set(("Close", "Date")).issubset(df.columns) or len(df) == 0:
            return None

        return df
    except Exception:
        # If anything weird (e.g., corrupted file), fail gracefully
        return None

def load_universe_symbols(csv_path: str | Path) -> list[str]:
    df = pd.read_csv(csv_path)
    if "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == "EQ"]
    return (
        df["Symbol"]
        .astype(str).str.strip().str.upper()
        .dropna().unique().tolist()
    )
