from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue
import os
import matplotlib.pyplot as plt
import pandas as pd

@asset(ins={"avg_price_per_month": AssetIn()})
def price_per_ping_plot(context: AssetExecutionContext, avg_price_per_month):
    fig, ax = plt.subplots(figsize=(12, 4))
    avg_price_per_month.sort_index().plot(ax=ax, marker="o")
    ax.set_title("Average NT$/坪")
    ax.grid(True)

    path = "dagster_artifacts/avg_price.png"
    plot_dir = os.path.dirname(path)
    if plot_dir and not os.path.exists(plot_dir):
        os.makedirs(plot_dir, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")

    context.add_output_metadata({"plot": MetadataValue.path(path)})


@asset(ins={"enriched_transactions": AssetIn()})
def analysis_plots(context: AssetExecutionContext, enriched_transactions):
    """Generate a suite of analysis plots and save them as PNG files."""
    df = enriched_transactions.copy()
    df["year_month"] = df["transaction_date"].dt.to_period("M")

    plot_dir = "dagster_artifacts"
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir, exist_ok=True)

    metadata = {}

    # --- Transaction volume by month -----------------------------------------
    fig, ax = plt.subplots(figsize=(15, 6))
    tx_counts = df.groupby("year_month").size()
    tx_counts.plot(kind="bar", ax=ax)
    ax.set_title("Transaction Volume by Year-Month")
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Number of Transactions")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y")
    plt.tight_layout()
    path_tx = os.path.join(plot_dir, "tx_volume.png")
    fig.savefig(path_tx, dpi=150, bbox_inches="tight")
    plt.close(fig)
    metadata["transaction_volume_plot"] = MetadataValue.path(path_tx)

    # --- Building age histogram ---------------------------------------------
    fig, ax = plt.subplots(figsize=(12, 6))
    df["building_age"].dropna().hist(bins=range(-5, 51), edgecolor="black", color="skyblue", ax=ax)
    ax.set_title("Distribution of Building Ages at Time of Transaction")
    ax.set_xlabel("Building Age (Years)")
    ax.set_ylabel("Number of Transactions")
    ax.set_xlim(-5, 50)
    ax.grid(axis="y")
    path_age = os.path.join(plot_dir, "building_age.png")
    fig.savefig(path_age, dpi=150, bbox_inches="tight")
    plt.close(fig)
    metadata["building_age_plot"] = MetadataValue.path(path_age)

    # --- Average price per ping since 2020 ----------------------------------
    fig, ax = plt.subplots(figsize=(15, 6))
    df_2020 = df[df["transaction_date"] >= pd.Timestamp(2020, 1, 1)]
    avg_price = df_2020.groupby("year_month")["price_per_ping"].mean()
    avg_price.plot(ax=ax, marker="o", color="teal")
    ax.set_title("Average Price per 坪 Over Time (Since 2020)")
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Average Price per 坪 (NT$)")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True)
    plt.tight_layout()
    path_price = os.path.join(plot_dir, "avg_price_since_2020.png")
    fig.savefig(path_price, dpi=150, bbox_inches="tight")
    plt.close(fig)
    metadata["avg_price_plot"] = MetadataValue.path(path_price)

    # --- Floor-level price analysis -----------------------------------------
    from .utils import chinese_floor_to_int

    df["transaction_floor"] = df["rps09"].apply(chinese_floor_to_int)
    try:
        df["total_floors"] = df["rps10"].astype(int)
    except Exception:
        df["total_floors"] = df["rps10"].apply(chinese_floor_to_int)

    def floor_cat(row):
        if row["transaction_floor"] == 4:
            return "4th Floor"
        if row["transaction_floor"] == 8:
            return "8th Floor"
        if row["transaction_floor"] is not None and row["total_floors"]:
            if row["transaction_floor"] > row["total_floors"] / 2:
                return "High Floor"
        return "Low Floor"

    df["floor_category"] = df.apply(floor_cat, axis=1)

    avg_price_floor = (
        df.groupby(["year_month", "floor_category"])["price_per_ping"].mean().unstack()
    )

    fig, ax1 = plt.subplots(figsize=(15, 6))
    tx_counts.index = tx_counts.index.astype(str)
    ax1.bar(tx_counts.index, tx_counts.values, color="b", alpha=0.5, label="Transactions")
    ax1.set_xlabel("Year-Month")
    ax1.set_ylabel("Number of Transactions", color="b")
    ax2 = ax1.twinx()
    for cat, series in avg_price_floor.items():
        ax2.plot(series.index.astype(str), series, label=cat)
    ax2.set_ylabel("Average Price per 坪 (NT$)", color="r")
    ax2.legend(loc="upper right")
    plt.title("Transactions & Average Prices by Floor Over Time")
    ax1.tick_params(axis="x", rotation=45)
    ax1.grid(axis="y")
    plt.tight_layout()
    path_floor = os.path.join(plot_dir, "floor_price.png")
    fig.savefig(path_floor, dpi=150, bbox_inches="tight")
    plt.close(fig)
    metadata["floor_price_plot"] = MetadataValue.path(path_floor)

    # --- Area ratio plots ----------------------------------------------------
    ratio_cols = [
        "main_building_area_ping",
        "auxiliary_building_area_ping",
        "balcony_area_ping",
        "parking_area_ping",
        "public_facility_ratio",
    ]
    rename_map = {
        "main_building_area_ping": "Main Building",
        "auxiliary_building_area_ping": "Auxiliary Building",
        "balcony_area_ping": "Balcony",
        "parking_area_ping": "Parking",
        "public_facility_ratio": "Public Facility",
    }

    average_ratios = df[ratio_cols].mean().rename(rename_map)
    fig, ax = plt.subplots(figsize=(10, 6))
    average_ratios.plot.bar(ax=ax, color="skyblue")
    ax.set_title("Average Area Ratios")
    ax.set_ylim(0, 1)
    path_ratio_bar = os.path.join(plot_dir, "area_ratio_avg.png")
    fig.savefig(path_ratio_bar, dpi=150, bbox_inches="tight")
    plt.close(fig)
    metadata["area_ratio_bar_plot"] = MetadataValue.path(path_ratio_bar)

    fig, ax = plt.subplots(figsize=(14, 7))
    df[ratio_cols].rename(columns=rename_map).boxplot(ax=ax)
    ax.set_title("Boxplot of Area Ratios")
    ax.set_ylabel("Ratio")
    ax.set_xlabel("Area Type")
    ax.grid(True)
    path_ratio_box = os.path.join(plot_dir, "area_ratio_box.png")
    fig.savefig(path_ratio_box, dpi=150, bbox_inches="tight")
    plt.close(fig)
    metadata["area_ratio_box_plot"] = MetadataValue.path(path_ratio_box)

    context.add_output_metadata(metadata)
