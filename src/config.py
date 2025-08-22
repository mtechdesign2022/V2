import os
from dotenv import load_dotenv

load_dotenv()

APP_PASSWORD = os.getenv("APP_PASSWORD", "change-me")
DATA_CACHE_DIR = os.getenv("DATA_CACHE_DIR", "cache/eod")
UNIVERSE_CSV = os.getenv("UNIVERSE_CSV", "data/ind_nifty500list.csv")
FUNDAMENTALS_CSV = os.getenv("FUNDAMENTALS_CSV", "data/fundamentals.csv")

ALLOW_UNKNOWN_FUNDAMENTALS = os.getenv("ALLOW_UNKNOWN_FUNDAMENTALS", "false").lower() == "true"
VOLUME_THRUST_MULTIPLIER = float(os.getenv("VOLUME_THRUST_MULTIPLIER", "1.8"))
FIVE_DAY_THRUST_RATIO = float(os.getenv("FIVE_DAY_THRUST_RATIO", "1.3"))
RS_LOOKBACK_DAYS = int(os.getenv("RS_LOOKBACK_DAYS", "20"))
RECLAIM_LOOKBACK_DAYS = int(os.getenv("RECLAIM_LOOKBACK_DAYS", "126"))
RECLAIM_RECENT_WINDOW_DAYS = int(os.getenv("RECLAIM_RECENT_WINDOW_DAYS", "10"))
ENTRY_BUFFER_PCT = float(os.getenv("ENTRY_BUFFER_PCT", "0.1"))
STOP_ATR_MULTIPLIER = float(os.getenv("STOP_ATR_MULTIPLIER", "1.0"))
STOP_MIN_BUFFER_PCT = float(os.getenv("STOP_MIN_BUFFER_PCT", "2.2"))
PCT_ABOVE_50DMA_FOR_ON = float(os.getenv("PCT_ABOVE_50DMA_FOR_ON", "45"))
INDEX_SYMBOL = os.getenv("INDEX_SYMBOL", "NIFTY50")
