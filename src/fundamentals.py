import pandas as pd
from typing import Dict

REQUIRED = [
    "Symbol","debt_to_equity","interest_coverage",
    "promoter_pledge_pct","qoq_rev_pos_last3","qoq_eps_pos_last3"
]

def load_fundamentals(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED if c not in df.columns]
    for c in missing:
        df[c] = None
    df["Symbol"] = df["Symbol"].astype(str).str.upper()
    return df

def fundamentals_pass(row: pd.Series, allow_unknown: bool = False) -> Dict[str,bool|str]:
    def _to_float(x):
        try: return float(x)
        except: return None

    d_e = _to_float(row.get("debt_to_equity"))
    icr = _to_float(row.get("interest_coverage"))
    pledge = _to_float(row.get("promoter_pledge_pct"))
    qrev = _to_float(row.get("qoq_rev_pos_last3"))
    qeps = _to_float(row.get("qoq_eps_pos_last3"))

    checks = {
        "de_le_1_5": d_e is not None and d_e <= 1.5,
        "icr_ge_2_5": icr is not None and icr >= 2.5,
        "pledge_le_20": pledge is not None and pledge <= 20.0,
        "qoq_rev_or_eps_ge_1_of_3": (qrev is not None and qrev >= 1) or (qeps is not None and qeps >= 1),
    }
    if any(v is False for v in checks.values()):
        status = "FAIL"
    else:
        status = "PASS" if all(v is True for v in checks.values()) else ("UNKNOWN" if allow_unknown else "FAIL")
    checks["status"] = status
    return checks
