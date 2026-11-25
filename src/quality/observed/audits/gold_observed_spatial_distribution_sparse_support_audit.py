"""
Gold observed spatial distribution sparse spatial support audit pipeline.

Purpose:
    Identify spatial cells with too little support to sustain strong
    interpretation in gold_observed_spatial_distribution.

Audit focus:
    - very low observed_position_records_count
    - only one observed vehicle in a cell
    - extremely sparse daily cells

Why this audit matters:
    This audit is useful because spatial interpretation becomes weaker when
    a daily spatial cell has very limited observed support.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_distribution"
OUTPUT_TABLE_NAME = "gold.gold_observed_spatial_distribution_sparse_support_audit"

SPARSE_POSITION_RECORDS_THRESHOLD = 5
SPARSE_DISTINCT_VEHICLES_THRESHOLD = 1
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying spatial cells with weak support in "
        "gold_observed_spatial_distribution, including very low observed "
        "position volume and very low distinct observed vehicle support."
    ),
)
def gold_observed_spatial_distribution_sparse_support_audit() -> DataFrame:
    """
    Materialize the sparse spatial support audit for
    gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Audit rows for spatial cells with weak support for strong
            interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_spatial_distribution_sparse_support_audit(source_df)


def _build_gold_observed_spatial_distribution_sparse_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the sparse spatial support audit dataset.

    Processing steps:
        1. Read gold_observed_spatial_distribution.
        2. Compute global reference statistics for spatial support metrics.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag spatial cells with sparse support.
        5. Keep only rows that match at least one sparse-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Audit DataFrame containing spatial cells with sparse support.
    """
    stats_df = source_df.agg(
        F.avg("observed_position_records_count").alias(
            "avg_observed_position_records_count"
        ),
        F.stddev_pop("observed_position_records_count").alias(
            "stddev_observed_position_records_count"
        ),
        F.min("observed_position_records_count").alias(
            "min_observed_position_records_count"
        ),
        F.max("observed_position_records_count").alias(
            "max_observed_position_records_count"
        ),
        F.avg("distinct_observed_vehicles_count").alias(
            "avg_distinct_observed_vehicles_count"
        ),
        F.stddev_pop("distinct_observed_vehicles_count").alias(
            "stddev_distinct_observed_vehicles_count"
        ),
        F.min("distinct_observed_vehicles_count").alias(
            "min_distinct_observed_vehicles_count"
        ),
        F.max("distinct_observed_vehicles_count").alias(
            "max_distinct_observed_vehicles_count"
        ),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_sparse_position_records_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_observed_position_records_count")
                - (
                    F.coalesce(
                        F.col("stddev_observed_position_records_count"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_sparse_distinct_vehicles_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_observed_vehicles_count")
                - (
                    F.coalesce(
                        F.col("stddev_distinct_observed_vehicles_count"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_position_records_count",
            F.col("observed_position_records_count")
            <= F.lit(SPARSE_POSITION_RECORDS_THRESHOLD),
        )
        .withColumn(
            "has_relative_sparse_position_records_count",
            F.col("observed_position_records_count")
            < F.col("relative_sparse_position_records_threshold"),
        )
        .withColumn(
            "has_only_one_observed_vehicle",
            F.col("distinct_observed_vehicles_count")
            <= F.lit(SPARSE_DISTINCT_VEHICLES_THRESHOLD),
        )
        .withColumn(
            "has_relative_sparse_distinct_vehicles_count",
            F.col("distinct_observed_vehicles_count")
            < F.col("relative_sparse_distinct_vehicles_threshold"),
        )
        .withColumn(
            "has_sparse_spatial_support",
            F.col("has_very_low_position_records_count")
            | F.col("has_relative_sparse_position_records_count")
            | F.col("has_only_one_observed_vehicle")
            | F.col("has_relative_sparse_distinct_vehicles_count"),
        )
        .where(F.col("has_sparse_spatial_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "spatial_cell_id",
            "spatial_resolution",
            "cell_center_latitude",
            "cell_center_longitude",
            "observed_position_records_count",
            "distinct_observed_vehicles_count",
            "distinct_observed_lines_count",
            "min_captured_at_local",
            "max_captured_at_local",
            "avg_observed_position_records_count",
            "stddev_observed_position_records_count",
            "min_observed_position_records_count",
            "max_observed_position_records_count",
            "avg_distinct_observed_vehicles_count",
            "stddev_distinct_observed_vehicles_count",
            "min_distinct_observed_vehicles_count",
            "max_distinct_observed_vehicles_count",
            "relative_sparse_position_records_threshold",
            "relative_sparse_distinct_vehicles_threshold",
            "has_very_low_position_records_count",
            "has_relative_sparse_position_records_count",
            "has_only_one_observed_vehicle",
            "has_relative_sparse_distinct_vehicles_count",
            "has_sparse_spatial_support",
            "record_created_at",
        )
    )

    return audit_df
