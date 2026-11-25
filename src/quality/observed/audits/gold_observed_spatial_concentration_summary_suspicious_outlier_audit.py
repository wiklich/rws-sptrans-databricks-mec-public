"""
Gold observed spatial concentration summary suspicious concentration spike audit pipeline.

Purpose:
    Identify analysis dates with unusually extreme concentration behavior in
    gold_observed_spatial_concentration_summary.

Audit focus:
    - very high top_cell_position_share_ratio
    - very high top_5_cells_position_share_ratio
    - very large max_positions_in_single_cell

Why this audit matters:
    This audit is useful because extreme concentration may indicate unusual
    data collection patterns, hotspot dominance, or weak spatial spread.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_concentration_summary"
OUTPUT_TABLE_NAME = (
    "gold.gold_observed_spatial_concentration_summary_suspicious_outlier_audit"
)

STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying daily rows with suspicious concentration spikes "
        "in gold_observed_spatial_concentration_summary, including unusually "
        "high top-cell concentration, unusually high top-5-cell concentration, "
        "and unusually large maximum positions in a single spatial cell."
    ),
)
def gold_observed_spatial_concentration_summary_suspicious_outlier_audit() -> DataFrame:
    """
    Materialize the suspicious concentration spike audit for
    gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit rows for daily summaries with suspiciously high concentration.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_spatial_concentration_summary_suspicious_outlier_audit(
        source_df
    )


def _build_gold_observed_spatial_concentration_summary_suspicious_outlier_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious concentration spike audit dataset.

    Processing steps:
        1. Read gold_observed_spatial_concentration_summary.
        2. Compute global reference statistics for concentration metrics.
        3. Compute high outlier thresholds.
        4. Flag daily rows with suspiciously high concentration behavior.
        5. Keep only rows that match at least one outlier condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit DataFrame containing daily rows with suspicious concentration spikes.
    """
    stats_df = source_df.agg(
        F.avg("top_cell_position_share_ratio").alias(
            "avg_top_cell_position_share_ratio"
        ),
        F.stddev_pop("top_cell_position_share_ratio").alias(
            "stddev_top_cell_position_share_ratio"
        ),
        F.min("top_cell_position_share_ratio").alias(
            "min_top_cell_position_share_ratio"
        ),
        F.max("top_cell_position_share_ratio").alias(
            "max_top_cell_position_share_ratio"
        ),
        F.avg("top_5_cells_position_share_ratio").alias(
            "avg_top_5_cells_position_share_ratio"
        ),
        F.stddev_pop("top_5_cells_position_share_ratio").alias(
            "stddev_top_5_cells_position_share_ratio"
        ),
        F.min("top_5_cells_position_share_ratio").alias(
            "min_top_5_cells_position_share_ratio"
        ),
        F.max("top_5_cells_position_share_ratio").alias(
            "max_top_5_cells_position_share_ratio"
        ),
        F.avg("max_positions_in_single_cell").alias(
            "avg_max_positions_in_single_cell"
        ),
        F.stddev_pop("max_positions_in_single_cell").alias(
            "stddev_max_positions_in_single_cell"
        ),
        F.min("max_positions_in_single_cell").alias(
            "min_max_positions_in_single_cell"
        ),
        F.max("max_positions_in_single_cell").alias(
            "max_max_positions_in_single_cell"
        ),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "high_top_cell_position_share_ratio_threshold",
            F.col("avg_top_cell_position_share_ratio")
            + (
                F.coalesce(
                    F.col("stddev_top_cell_position_share_ratio"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "high_top_5_cells_position_share_ratio_threshold",
            F.col("avg_top_5_cells_position_share_ratio")
            + (
                F.coalesce(
                    F.col("stddev_top_5_cells_position_share_ratio"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "high_max_positions_in_single_cell_threshold",
            F.col("avg_max_positions_in_single_cell")
            + (
                F.coalesce(
                    F.col("stddev_max_positions_in_single_cell"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "has_unusually_high_top_cell_position_share_ratio",
            F.col("top_cell_position_share_ratio").isNotNull()
            & (
                F.col("top_cell_position_share_ratio")
                > F.col("high_top_cell_position_share_ratio_threshold")
            ),
        )
        .withColumn(
            "has_unusually_high_top_5_cells_position_share_ratio",
            F.col("top_5_cells_position_share_ratio").isNotNull()
            & (
                F.col("top_5_cells_position_share_ratio")
                > F.col("high_top_5_cells_position_share_ratio_threshold")
            ),
        )
        .withColumn(
            "has_unusually_high_max_positions_in_single_cell",
            F.col("max_positions_in_single_cell").isNotNull()
            & (
                F.col("max_positions_in_single_cell")
                > F.col("high_max_positions_in_single_cell_threshold")
            ),
        )
        .withColumn(
            "has_suspicious_concentration_spike",
            F.col("has_unusually_high_top_cell_position_share_ratio")
            | F.col("has_unusually_high_top_5_cells_position_share_ratio")
            | F.col("has_unusually_high_max_positions_in_single_cell"),
        )
        .where(F.col("has_suspicious_concentration_spike") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "active_spatial_cells_count",
            "total_observed_position_records_count",
            "avg_positions_per_active_cell",
            "max_positions_in_single_cell",
            "max_distinct_observed_vehicles_in_single_cell",
            "max_distinct_observed_lines_in_single_cell",
            "top_cell_position_share_ratio",
            "top_5_cells_position_share_ratio",
            "avg_top_cell_position_share_ratio",
            "stddev_top_cell_position_share_ratio",
            "min_top_cell_position_share_ratio",
            "max_top_cell_position_share_ratio",
            "avg_top_5_cells_position_share_ratio",
            "stddev_top_5_cells_position_share_ratio",
            "min_top_5_cells_position_share_ratio",
            "max_top_5_cells_position_share_ratio",
            "avg_max_positions_in_single_cell",
            "stddev_max_positions_in_single_cell",
            "min_max_positions_in_single_cell",
            "max_max_positions_in_single_cell",
            "high_top_cell_position_share_ratio_threshold",
            "high_top_5_cells_position_share_ratio_threshold",
            "high_max_positions_in_single_cell_threshold",
            "has_unusually_high_top_cell_position_share_ratio",
            "has_unusually_high_top_5_cells_position_share_ratio",
            "has_unusually_high_max_positions_in_single_cell",
            "has_suspicious_concentration_spike",
            "record_created_at",
        )
    )

    return audit_df
