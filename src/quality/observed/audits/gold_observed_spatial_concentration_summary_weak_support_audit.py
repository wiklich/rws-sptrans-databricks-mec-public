"""
Gold observed spatial concentration summary weak spatial support audit pipeline.

Purpose:
    Identify analysis dates with too little active spatial support to sustain
    strong concentration interpretation in
    gold_observed_spatial_concentration_summary.

Audit focus:
    - very low active_spatial_cells_count
    - very low total_observed_position_records_count
    - very narrow observed spatial footprint

Why this audit matters:
    This audit is useful because concentration interpretation becomes weaker
    when the observed spatial support of the day is too limited.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_concentration_summary"
OUTPUT_TABLE_NAME = "gold.gold_observed_spatial_concentration_summary_weak_support_audit"

WEAK_ACTIVE_SPATIAL_CELLS_COUNT_THRESHOLD = 5
WEAK_TOTAL_OBSERVED_POSITION_RECORDS_COUNT_THRESHOLD = 100
NARROW_OBSERVED_SPATIAL_FOOTPRINT_THRESHOLD = 5
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying daily rows with weak spatial support in "
        "gold_observed_spatial_concentration_summary, including very low "
        "active spatial cell count, very low observed position support, and "
        "very narrow observed spatial footprint."
    ),
)
def gold_observed_spatial_concentration_summary_weak_support_audit() -> DataFrame:
    """
    Materialize the weak spatial support audit for
    gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit rows for daily summaries with weak support for strong
            concentration interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_spatial_concentration_summary_weak_support_audit(
        source_df
    )


def _build_gold_observed_spatial_concentration_summary_weak_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the weak spatial support audit dataset.

    Processing steps:
        1. Read gold_observed_spatial_concentration_summary.
        2. Compute global reference statistics for active spatial support and
           observed position support.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag daily rows with weak support.
        5. Keep only rows that match at least one weak-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit DataFrame containing daily rows with weak spatial support.
    """
    stats_df = source_df.agg(
        F.avg("active_spatial_cells_count").alias("avg_active_spatial_cells_count"),
        F.stddev_pop("active_spatial_cells_count").alias(
            "stddev_active_spatial_cells_count"
        ),
        F.min("active_spatial_cells_count").alias("min_active_spatial_cells_count"),
        F.max("active_spatial_cells_count").alias("max_active_spatial_cells_count"),
        F.avg("total_observed_position_records_count").alias(
            "avg_total_observed_position_records_count"
        ),
        F.stddev_pop("total_observed_position_records_count").alias(
            "stddev_total_observed_position_records_count"
        ),
        F.min("total_observed_position_records_count").alias(
            "min_total_observed_position_records_count"
        ),
        F.max("total_observed_position_records_count").alias(
            "max_total_observed_position_records_count"
        ),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_weak_active_spatial_cells_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_active_spatial_cells_count")
                - (
                    F.coalesce(
                        F.col("stddev_active_spatial_cells_count"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_total_observed_position_records_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_total_observed_position_records_count")
                - (
                    F.coalesce(
                        F.col("stddev_total_observed_position_records_count"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_active_spatial_cells_count",
            F.col("active_spatial_cells_count")
            <= F.lit(WEAK_ACTIVE_SPATIAL_CELLS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_active_spatial_cells_count",
            F.col("active_spatial_cells_count")
            < F.col("relative_weak_active_spatial_cells_count_threshold"),
        )
        .withColumn(
            "has_very_low_total_observed_position_records_count",
            F.col("total_observed_position_records_count")
            <= F.lit(WEAK_TOTAL_OBSERVED_POSITION_RECORDS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_total_observed_position_records_count",
            F.col("total_observed_position_records_count")
            < F.col(
                "relative_weak_total_observed_position_records_count_threshold"
            ),
        )
        .withColumn(
            "has_narrow_observed_spatial_footprint",
            F.col("active_spatial_cells_count")
            <= F.lit(NARROW_OBSERVED_SPATIAL_FOOTPRINT_THRESHOLD),
        )
        .withColumn(
            "has_weak_spatial_support",
            F.col("has_very_low_active_spatial_cells_count")
            | F.col("has_relative_weak_active_spatial_cells_count")
            | F.col("has_very_low_total_observed_position_records_count")
            | F.col("has_relative_weak_total_observed_position_records_count")
            | F.col("has_narrow_observed_spatial_footprint"),
        )
        .where(F.col("has_weak_spatial_support") == True)
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
            "avg_active_spatial_cells_count",
            "stddev_active_spatial_cells_count",
            "min_active_spatial_cells_count",
            "max_active_spatial_cells_count",
            "avg_total_observed_position_records_count",
            "stddev_total_observed_position_records_count",
            "min_total_observed_position_records_count",
            "max_total_observed_position_records_count",
            "relative_weak_active_spatial_cells_count_threshold",
            "relative_weak_total_observed_position_records_count_threshold",
            "has_very_low_active_spatial_cells_count",
            "has_relative_weak_active_spatial_cells_count",
            "has_very_low_total_observed_position_records_count",
            "has_relative_weak_total_observed_position_records_count",
            "has_narrow_observed_spatial_footprint",
            "has_weak_spatial_support",
            "record_created_at",
        )
    )

    return audit_df
