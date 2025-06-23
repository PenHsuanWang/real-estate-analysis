from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue
import os
import matplotlib.pyplot as plt

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
