"""Utility helpers shared by several assets."""
import pandas as pd

SQM_TO_PING = 3.305785   # 1 坪 ≈ 3.305785 m²


def roc_to_ts(value) -> pd.Timestamp | pd.NaT:
    """Convert an ROC date integer/string (yyyymmdd or yyy/mm/dd) to Gregorian."""
    try:
        s = str(value).replace("/", "")
        year_len = 3 if len(s) == 7 else 2          # 1090101 vs 99/01/01
        year = int(s[:year_len]) + 1911
        month = int(s[year_len:year_len + 2])
        day = int(s[year_len + 2 :])
        return pd.Timestamp(year, month, day)
    except Exception:
        return pd.NaT
