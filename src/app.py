import os
import streamlit as st
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from . import config
from .io_utils import load_universe_symbols
from .watchlist_builder import build_watchlist
from .fundamentals import load_fundamentals
from .regime import market_regime
from .io_utils import safe_read_eod

load_dotenv()

st.set_page_config(page_title="V2.3 Watchlist", layout="wide")

password = st.text_input("Enter app password", type="password")
if password != config.APP_PASSWORD:
    st.stop()

st.title("V2.3 – False Breakdown Reclaim Scanner (Universe-only)")

try:
    universe = sorted(load_universe_symbols(config.UNIVERSE_CSV))
except Exception as e:
    st.error(f"Universe load failed: {e}")
    st.stop()

st.sidebar.header("Paths & Settings")
st.sidebar.write(f"Universe CSV: `{config.UNIVERSE_CSV}`")
st.sidebar.write(f"EOD cache: `{config.DATA_CACHE_DIR}`")
st.sidebar.write(f"Fundamentals CSV: `{config.FUNDAMENTALS_CSV}`")
st.sidebar.write(f"Index symbol: `{config.INDEX_SYMBOL}`")

fund_df = load_fundamentals(config.FUNDAMENTALS_CSV)

subset = st.sidebar.multiselect("Limit to symbols", options=universe, default=[])
symbols = subset if subset else universe

idx_df = safe_read_eod(config.DATA_CACHE_DIR, config.INDEX_SYMBOL)
if idx_df is None:
    st.warning(f"Missing index EOD for {config.INDEX_SYMBOL}. Place CSV at cache/eod/{config.INDEX_SYMBOL}.csv")
else:
    universe_dfs = {}
    for sym in symbols[:100]:
        universe_dfs[sym] = safe_read_eod(config.DATA_CACHE_DIR, sym)
    state = market_regime(idx_df, universe_dfs, pct_above_50dma_for_on=config.PCT_ABOVE_50DMA_FOR_ON)
    st.info(f"Market Regime: **{state}**")

if st.button("Run Scan"):
    try:
        df_out = build_watchlist(symbols, fund_df, config.DATA_CACHE_DIR, config.INDEX_SYMBOL)
        if df_out.empty:
            st.warning("No valid candidates today (or missing EODs).")
        else:
            st.dataframe(df_out, use_container_width=True)
            out_name = f"output/watchlist_{datetime.now().strftime('%Y%m%d')}.csv"
            df_out.to_csv(out_name, index=False)
            st.success(f"Exported: {out_name}")
            st.download_button("Download CSV", data=df_out.to_csv(index=False), file_name=os.path.basename(out_name), mime="text/csv")
    except Exception as e:
        st.error(f"Scan failed: {e}")
