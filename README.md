# Internal Trading Utilities

This repository contains internal tooling for proprietary research.
Access is restricted. For questions, contact the maintainer.

## Quick start (local only)
1) Create and populate a local EOD cache with OHLCV CSVs at `cache/eod/{SYMBOL}.csv`.
   - Required columns: Date, Open, High, Low, Close, Volume (Adj Close optional).
2) Place your NIFTY 500 CSV at `data/ind_nifty500list.csv`.
3) (Optional) Provide fundamentals at `data/fundamentals.csv` with columns:
   Symbol, debt_to_equity, interest_coverage, promoter_pledge_pct, qoq_rev_pos_last3, qoq_eps_pos_last3
4) Copy `.env.example` to `.env` and set paths/passwords.
5) Install: `pip install -r requirements.txt`
6) Run: `streamlit run src/app.py`

## Notes
- Universe is strictly limited to symbols in `data/ind_nifty500list.csv` (Series == EQ).
- Strategy V2.3 is technical; fundamentals are hygiene checks (PASS/FAIL).
- Do not distribute. All rights reserved.
