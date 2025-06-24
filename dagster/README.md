This directory contains the Dagster implementation for the real estate
analysis project. The pipeline ingests CSV transaction data, performs a
series of transformations and emits several plots as PNG artifacts.

### Visual assets

* `price_per_ping_plot` – line chart of average NT$/坪
* `analysis_plots` – collection of Matplotlib-based summary plots
* `enhanced_area_ratio_plots` – Seaborn visualisations of area ratios,
  including box, violin, stacked bar and coloured average bar plots

The images are written to the `dagster_artifacts/` directory and exposed via
Dagster asset metadata.
