"""Entry-point for Dagster.

`defs` is the only object Dagster looks for; everything else
is imported here so Dagit can render the asset graph.
"""
from dagster import Definitions

# ─── bring assets & resources into scope ───
from .assets import (
    raw_transactions,
    filtered_transactions,
    enriched_transactions,
    tx_counts,
    avg_price_per_month,
    price_per_ping_plot,
)
from .resources import CsvPathResource

defs = Definitions(
    assets=[
        raw_transactions,
        filtered_transactions,
        enriched_transactions,
        tx_counts,
        avg_price_per_month,
        price_per_ping_plot,   # the image-producing asset
    ],
    resources={
        # 👇 update to the real path of your CSV
        "csv_path": CsvPathResource(
            path="../data/不動產實價登錄資訊-買賣案件-淡水區.csv"
        )
    },
)
