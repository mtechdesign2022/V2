# src/app.py
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# --- Use absolute imports from the src package ---
from src import config
from src.io_utils import load_universe_symbols, safe_read_eod
from src.watchlist_builder import build_watchlist
from src.fundamentals import load_fundamentals
from src.regime import market_regime

# If you're running locally, ensure an empty __init__.py exists in src/
# Streamlit Cloud runs from repo root, so "src" is importable as a package.

load_dotenv()

# ---------- Page config ----------
st.set_page_config(page_title="V2.3 Watchlist (Universe-only)", layout="wide")

# ---------- Simple access gate ----------
# Keep these in Streamlit Secrets or .env on Cloud:
# APP_PASSWORD=your-password
password = st.text_input("Enter app password", type="password")
if password != config.APP_PASSWORD:
    st.stop()

st.title("V2.3 – False Breakdown Reclaim Scanner (Universe-only)")

# ---------- Load Universe ----------
try:
    universe = sorted(load_universe_symbols(config.UNIVERSE_CSV))
except Exception as e:
    st.error(f"Universe load failed: {e}")
    st.stop()

# ---------- Sidebar ----------
st.sidebar.header("Paths & Settings")
st.sidebar.write(f"Universe CSV: `{config.UNIVERSE_CSV}`")
st.sidebar.write(f"EOD cache: `{config.DATA_CACHE_DIR}`")
st.sidebar.write(f"Fundamentals CSV: `{config.FUNDAMENTALS_CSV}`")
st.sidebar.write(f"Index symbol: `{config.INDEX_SYMBOL}`")

# Optional: filter to a smaller set for speed
subset = st.sidebar.multiselect("Limit to symbols", options=universe, default=[])
symbols = subset if subset else universe

# ---------- Load fundamentals ----------
fund_df = load_fundamentals(config.FUNDAMENTALS_CSV)

# ---------- Market regime (info only) ----------
idx_df = safe_read_eod(config.DATA_CACHE_DIR, config.INDEX_SYMBOL)
if idx_df is None:
    st.warning(
        f"Missing index EOD for {config.INDEX_SYMBOL}. "
        f"Place CSV at {config.DATA_CACHE_DIR}/{config.INDEX_SYMBOL}.csv "
        "(needs Date,Open,High,Low,Close,Volume)."
    )
else:
    # compute regime using a sample of the universe for speed
    sample_universe = symbols[:100]  # adjust if needed
    universe_dfs = {sym: safe_read_eod(config.DATA_CACHE_DIR, sym) for sym in sample_universe}
    state = market_regime(idx_df, universe_dfs, pct_above_50dma_for_on=config.PCT_ABOVE_50DMA_FOR_ON)
    st.info(f"**Market Regime:** {state}")

# ---------- Run Scan ----------
if st.button("Run Scan"):
    try:
        df_out = build_watchlist(symbols, fund_df, config.DATA_CACHE_DIR, config.INDEX_SYMBOL)
        if df_out.empty:
            st.warning("No valid candidates today (or missing/insufficient EOD data).")
        else:
            st.subheader("Actionable Watchlist")
            st.dataframe(df_out, use_container_width=True)

            # Export
            out_name = f"output/watchlist_{datetime.now().strftime('%Y%m%d')}.csv"
            os.makedirs(os.path.dirname(out_name), exist_ok=True)
            df_out.to_csv(out_name, index=False)

            st.success(f"Exported: {out_name}")
            st.download_button(
                "Download CSV",
                data=df_out.to_csv(index=False),
                file_name=os.path.basename(out_name),
                mime="text/csv",
            )

            st.caption(
                "Notes: "
                "• Universe strictly limited to Nifty 500 (Series == EQ). "
                "• 'Valid' requires reclaim + RSI rising in 20–38 band + volume thrust + RS 20D high "
                "+ Fundamentals PASS. "
                "• Fundamentals PASS needs D/E ≤ 1.5, ICR ≥ 2.5×, Pledge ≤ 20%, QoQ Rev or EPS positive in ≥1 of last 3."
            )
    except Exception as e:
        st.error(f"Scan failed: {e}")
