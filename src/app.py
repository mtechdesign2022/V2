# --- bootstrap: make project root importable ---
import os, sys
_PKG_DIR = os.path.dirname(__file__)          # /mount/src/v2/src
_ROOT_DIR = os.path.dirname(_PKG_DIR)         # /mount/src/v2
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)
# -----------------------------------------------

from datetime import datetime
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Local imports
from src import config
from src.io_utils import load_universe_symbols, safe_read_eod
from src.watchlist_builder import build_watchlist
from src.fundamentals import load_fundamentals
from src.regime import market_regime

# -------------------- App setup --------------------
load_dotenv()
st.set_page_config(page_title="V2.3 Watchlist (Universe-only)", layout="wide")

# ---------- Password helpers (login gate) ----------
def _expected_password() -> str:
    """
    Order of precedence:
      1) st.secrets["APP_PASSWORD"]
      2) env var APP_PASSWORD
      3) config.APP_PASSWORD (if present)
    """
    # 1) Streamlit secrets
    try:
        if "APP_PASSWORD" in st.secrets:
            return str(st.secrets["APP_PASSWORD"])
    except Exception:
        pass
    # 2) Environment
    env_pwd = os.getenv("APP_PASSWORD")
    if env_pwd:
        return env_pwd
    # 3) config fallback
    return str(getattr(config, "APP_PASSWORD", ""))

def _login_gate():
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if st.session_state.auth_ok:
        return True

    st.title("V2.3 – False Breakdown Reclaim Scanner (Universe-only)")
    st.subheader("Login")
    pwd = st.text_input("Enter app password", type="password", key="pwd", help="Press Enter or click Login")

    col1, col2 = st.columns([1, 6])
    with col1:
        submit = st.button("Login", type="primary")

    # Enter key also triggers a rerun; treat non-empty input as an attempt
    attempted = submit or (pwd != "")

    if attempted:
        if pwd == _expected_password():
            st.session_state.auth_ok = True
            st.success("Access granted ✅")
            st.rerun()
        else:
            st.error("Incorrect password ❌")

    # Stop rendering the rest of the app until authenticated
    st.stop()

# Gate the app
_login_gate()

# -------------------- Main App --------------------
st.title("V2.3 – False Breakdown Reclaim Scanner (Universe-only)")

# Sidebar: logout + paths
with st.sidebar:
    st.header("Session")
    if st.button("Logout"):
        st.session_state.clear()
        st.rerun()

st.sidebar.header("Paths & Settings")
st.sidebar.write(f"Universe CSV: `{config.UNIVERSE_CSV}`")
st.sidebar.write(f"EOD cache: `{config.DATA_CACHE_DIR}`")
st.sidebar.write(f"Fundamentals CSV: `{config.FUNDAMENTALS_CSV}`")
st.sidebar.write(f"Index symbol: `{config.INDEX_SYMBOL}`")

# Universe
try:
    universe = sorted(load_universe_symbols(config.UNIVERSE_CSV))
except Exception as e:
    st.error(f"Universe load failed: {e}")
    st.stop()

subset = st.sidebar.multiselect("Limit to symbols", options=universe, default=[])
symbols = subset if subset else universe

# Fundamentals
fund_df = load_fundamentals(config.FUNDAMENTALS_CSV)

# Regime
idx_df = safe_read_eod(config.DATA_CACHE_DIR, config.INDEX_SYMBOL)
if idx_df is None:
    st.warning(
        f"Missing index EOD for {config.INDEX_SYMBOL}. "
        f"Place CSV at {config.DATA_CACHE_DIR}/{config.INDEX_SYMBOL}.csv "
        "(needs Date,Open,High,Low,Close,Volume)."
    )
else:
    sample = symbols[:100]
    universe_dfs = {sym: safe_read_eod(config.DATA_CACHE_DIR, sym) for sym in sample}
    state = market_regime(idx_df, universe_dfs, pct_above_50dma_for_on=config.PCT_ABOVE_50DMA_FOR_ON)
    st.info(f"**Market Regime:** {state}")

# Scan
if st.button("Run Scan"):
    try:
        df_out = build_watchlist(symbols, fund_df, config.DATA_CACHE_DIR, config.INDEX_SYMBOL)
        if df_out.empty:
            st.warning("No valid candidates today (or missing/insufficient EOD data).")
        else:
            st.subheader("Actionable Watchlist")
            st.dataframe(df_out, use_container_width=True)

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
                "• Universe = Nifty 500 (Series == EQ) from local CSV. "
                "• 'Valid' requires reclaim + RSI rising 20–38 + volume thrust + RS 20D high + Fundamentals PASS. "
                "• Fundamentals PASS: D/E ≤ 1.5, ICR ≥ 2.5×, Pledge ≤ 20%, QoQ Rev or EPS positive in ≥1 of last 3."
            )
    except Exception as e:
        st.error(f"Scan failed: {e}")
