from __future__ import annotations

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetCheckResult,
    asset_check,
)


# ─────────────────────────────────
# Data-QUALITY check
# ─────────────────────────────────
@asset_check(
    asset="enriched_transactions",
    description="price_per_ping 必須為正值且不可為 NaN"
)
def check_price_positive(
    context: AssetExecutionContext,
    enriched_transactions: pd.DataFrame,
) -> AssetCheckResult:
    bad = (enriched_transactions["price_per_ping"] <= 0) | (
        enriched_transactions["price_per_ping"].isna()
    )
    n_bad = int(bad.sum())
    passed = n_bad == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "invalid_rows": n_bad,
            "total_rows": len(enriched_transactions),
            "pass_rate": 1 - n_bad / max(len(enriched_transactions), 1),
        },
    )


# ─────────────────────────────────
# Data-FRESHNESS check
# ─────────────────────────────────
@asset_check(
    asset="enriched_transactions",
    description="最近一筆交易日期必須在 45 天內"
)
def check_freshness(
    context: AssetExecutionContext,
    enriched_transactions: pd.DataFrame,
) -> AssetCheckResult:
    most_recent = enriched_transactions["transaction_date"].max()
    days_since = (pd.Timestamp.utcnow().normalize() - most_recent).days
    passed = days_since <= 45

    return AssetCheckResult(
        passed=passed,
        metadata={
            "most_recent_date": str(most_recent.date()),
            "days_since": days_since,
            "threshold_days": 45,
        },
    )
