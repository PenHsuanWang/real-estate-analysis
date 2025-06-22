"""All ETL logic expressed as Dagster software-defined assets (SDAs)."""
import matplotlib.pyplot as plt
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetOut,
    MetadataValue,
    asset,
    multi_asset,
)

from .resources import CsvPathResource
from .utils import SQM_TO_PING, roc_to_ts


# ──────────────────────────── 1. Extract ────────────────────────────
@asset(required_resource_keys={"csv_path"})
def raw_transactions(context: AssetExecutionContext) -> pd.DataFrame:
    """Read raw CSV into Pandas."""
    path = context.resources.csv_path.path
    df = pd.read_csv(path, dtype={"rps14": str})
    context.log.info(f"Loaded {len(df):,} rows from {path}")
    return df


# ──────────────────────────── 2. Filter  ────────────────────────────
@asset(ins={"raw_transactions": AssetIn()})
def filtered_transactions(raw_transactions: pd.DataFrame) -> pd.DataFrame:
    """Keep only the two sale-type categories analysed in the notebook."""
    mask = raw_transactions["rps01"].isin(
        ["房地(土地+建物)", "房地(土地+建物)+車位"]
    )
    return raw_transactions[mask]


# ──────────────────────────── 3. Transform ─────────────────────────
@asset(ins={"filtered_transactions": AssetIn()})
def enriched_transactions(filtered_transactions: pd.DataFrame) -> pd.DataFrame:
    """All heavy transformations: date conversion, age, unit → 坪, ratios."""
    df = filtered_transactions.copy()

    # --- date fields & building age ---------------------------------------------------
    df["transaction_date"] = df["rps07"].apply(roc_to_ts)
    df["completion_date"] = df["rps14"].apply(roc_to_ts)
    df["building_age"] = (
        (df["transaction_date"] - df["completion_date"]).dt.days / 365.25
    )
    df = df[df["transaction_date"] >= pd.Timestamp(2021, 12, 1)]

    # --- area conversions -------------------------------------------------------------
    cols_to_ping = {
        "rps03": "land_area_ping",
        "rps15": "total_area_ping",
        "rps24": "parking_area_ping",
        "rps28": "main_building_area_ping",
        "rps29": "auxiliary_building_area_ping",
        "rps30": "balcony_area_ping",
    }
    for src, dst in cols_to_ping.items():
        df[dst] = df[src] / SQM_TO_PING

    df["price_per_ping"] = df["rps22"] * SQM_TO_PING

    # --- public-facility ratio --------------------------------------------------------
    df["public_facility_ratio"] = (
        df["total_area_ping"]
        - df["main_building_area_ping"]
        - df["auxiliary_building_area_ping"]
        - df["balcony_area_ping"]
        - df["parking_area_ping"]
    ) / df["total_area_ping"]

    ratio_cols = [
        "main_building_area_ping",
        "auxiliary_building_area_ping",
        "balcony_area_ping",
        "parking_area_ping",
        "public_facility_ratio",
    ]
    df[ratio_cols] = df[ratio_cols].clip(lower=0, upper=1).fillna(0)

    return df


# ───────────────── 4. Lightweight analytics as multi-asset ─────────────────
@multi_asset(
    outs={
        "tx_counts": AssetOut(metadata={"kind": "summary"}),
        "avg_price_per_month": AssetOut(metadata={"kind": "kpi"}),
    },
    ins={"enriched_transactions": AssetIn()},
)
def _analytics(enriched_transactions: pd.DataFrame):
    """Emit two tidy Pandas Series that downstream assets (e.g. plots) can reuse."""
    df = enriched_transactions.copy()
    df["year_month"] = df["transaction_date"].dt.to_period("M")

    tx_counts = (
        df.groupby("year_month")
        .size()
        .rename("tx_count")
        .sort_index()
    )

    avg_price = (
        df.groupby("year_month")["price_per_ping"]
        .mean()
        .rename("avg_price_per_ping")
        .sort_index()
    )

    yield tx_counts
    yield avg_price


# ───────────────── 5. Matplotlib plot as a first-class asset ───────────────
@asset(ins={"avg_price_per_month": AssetIn()})
def price_per_ping_plot(
    context: AssetExecutionContext, avg_price_per_month: pd.Series
):
    """Write a PNG line-chart and surface it as Dagster metadata."""
    fig, ax = plt.subplots(figsize=(12, 4))
    avg_price_per_month.plot(ax=ax, marker="o")
    ax.set_title("Average NT$/坪")
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("NT$ / Ping")
    ax.grid(True)

    plot_path = "dagster_artifacts/avg_price.png"
    fig.savefig(plot_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    context.add_output_metadata({"plot": MetadataValue.path(plot_path)})
