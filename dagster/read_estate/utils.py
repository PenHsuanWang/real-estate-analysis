from __future__ import annotations

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


def chinese_number_to_int(chinese_number: str) -> int:
    """Convert simple Chinese numerals (一、二、三…) to integers."""
    numerals = {
        "一": 1,
        "二": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
        "十": 10,
    }

    if chinese_number in numerals:
        return numerals[chinese_number]
    if chinese_number.startswith("十"):
        # 十一 → 11, 十二 → 12
        return 10 + numerals.get(chinese_number[1], 0)
    if chinese_number.endswith("十"):
        # 二十 → 20
        return numerals.get(chinese_number[0], 0) * 10

    # 廿一 → 21 style numbers are not expected in the dataset
    if len(chinese_number) == 2:
        return numerals.get(chinese_number[0], 0) * 10 + numerals.get(
            chinese_number[1],
            0,
        )
    return 0


def chinese_floor_to_int(floor: str | int | None) -> int | None:
    """Parse Chinese floor designations like '五層' or '地下二層' to integers."""
    if not isinstance(floor, str) or "層" not in floor:
        return None

    num = floor.split("層", 1)[0]
    try:
        if num.startswith("地下"):
            return -chinese_number_to_int(num[2:])
        if num == "全":
            return 0
        return chinese_number_to_int(num)
    except Exception:
        return None
