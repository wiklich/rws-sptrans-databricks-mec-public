"""
Gold system-wide update behavior weak system support audit pipeline.

Purpose:
    Identify system-wide rows with too little support for strong interpretation
    in gold_system_wide_update_behavior.

Audit focus:
    - low vehicles_count
    - low total_measured_intervals_count
    - low total observation support

Why this audit matters:
    This audit is useful because system-wide interpretation becomes weaker when
    support is too limited.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_system_wide_update_behavior"
OUTPUT_TABLE_NAME = "gold.gold_system_wide_update_behavior_weak_support_audit"

WEAK_VEHICLES_COUNT_THRESHOLD = 10
WEAK_TOTAL_MEASURED_INTERVALS_COUNT_THRESHOLD = 100
WEAK_TOTAL_OBSERVATIONS_COUNT_THRESHOLD = 1000
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying system-wide rows with weak support in "
        "gold_system_wide_update_behavior, including low vehicle count, low "
        "measured interval support, and low total observation support."
    ),
)
def gold_system_wide_update_behavior_weak_support_audit() -> DataFrame:
    """
    Materialize the weak system support audit for
    gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Audit rows for system-wide summaries with weak support for strong
            interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_system_wide_update_behavior_weak_support_audit(source_df)


def _build_gold_system_wide_update_behavior_weak_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the weak system support audit dataset.

    Processing steps:
        1. Read gold_system_wide_update_behavior.
        2. Compute global reference statistics for support metrics.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag system-wide rows with weak support.
        5. Keep only rows that match at least one weak-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Audit DataFrame containing system-wide rows with weak support.
    """
    stats_df = source_df.agg(
        F.avg("vehicles_count").alias("avg_vehicles_count"),
        F.stddev_pop("vehicles_count").alias("stddev_vehicles_count"),
        F.min("vehicles_count").alias("min_vehicles_count"),
        F.max("vehicles_count").alias("max_vehicles_count"),
        F.avg("total_measured_intervals_count").alias(
            "avg_total_measured_intervals_count"
        ),
        F.stddev_pop("total_measured_intervals_count").alias(
            "stddev_total_measured_intervals_count"
        ),
        F.min("total_measured_intervals_count").alias(
            "min_total_measured_intervals_count"
        ),
        F.max("total_measured_intervals_count").alias(
            "max_total_measured_intervals_count"
        ),
        F.avg("total_observations_count").alias("avg_total_observations_count"),
        F.stddev_pop("total_observations_count").alias(
            "stddev_total_observations_count"
        ),
        F.min("total_observations_count").alias("min_total_observations_count"),
        F.max("total_observations_count").alias("max_total_observations_count"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_weak_vehicles_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_vehicles_count")
                - (
                    F.coalesce(F.col("stddev_vehicles_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_total_measured_intervals_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_total_measured_intervals_count")
                - (
                    F.coalesce(
                        F.col("stddev_total_measured_intervals_count"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_total_observations_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_total_observations_count")
                - (
                    F.coalesce(
                        F.col("stddev_total_observations_count"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_vehicles_count",
            F.col("vehicles_count") <= F.lit(WEAK_VEHICLES_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_vehicles_count",
            F.col("vehicles_count") < F.col("relative_weak_vehicles_count_threshold"),
        )
        .withColumn(
            "has_very_low_total_measured_intervals_count",
            F.col("total_measured_intervals_count")
            <= F.lit(WEAK_TOTAL_MEASURED_INTERVALS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_total_measured_intervals_count",
            F.col("total_measured_intervals_count")
            < F.col("relative_weak_total_measured_intervals_count_threshold"),
        )
        .withColumn(
            "has_very_low_total_observations_count",
            F.col("total_observations_count")
            <= F.lit(WEAK_TOTAL_OBSERVATIONS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_total_observations_count",
            F.col("total_observations_count")
            < F.col("relative_weak_total_observations_count_threshold"),
        )
        .withColumn(
            "has_weak_system_support",
            F.col("has_very_low_vehicles_count")
            | F.col("has_relative_weak_vehicles_count")
            | F.col("has_very_low_total_measured_intervals_count")
            | F.col("has_relative_weak_total_measured_intervals_count")
            | F.col("has_very_low_total_observations_count")
            | F.col("has_relative_weak_total_observations_count"),
        )
        .where(F.col("has_weak_system_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "aggregation_scope",
            "vehicles_count",
            "total_observations_count",
            "total_measured_intervals_count",
            "system_avg_update_interval_seconds",
            "system_min_update_interval_seconds",
            "system_max_update_interval_seconds",
            "system_avg_vehicle_stddev_interval_seconds",
            "total_distinct_minutes_with_data",
            "avg_continuity_support_ratio",
            "total_large_gaps_count",
            "avg_vehicles_count",
            "stddev_vehicles_count",
            "min_vehicles_count",
            "max_vehicles_count",
            "avg_total_measured_intervals_count",
            "stddev_total_measured_intervals_count",
            "min_total_measured_intervals_count",
            "max_total_measured_intervals_count",
            "avg_total_observations_count",
            "stddev_total_observations_count",
            "min_total_observations_count",
            "max_total_observations_count",
            "relative_weak_vehicles_count_threshold",
            "relative_weak_total_measured_intervals_count_threshold",
            "relative_weak_total_observations_count_threshold",
            "has_very_low_vehicles_count",
            "has_relative_weak_vehicles_count",
            "has_very_low_total_measured_intervals_count",
            "has_relative_weak_total_measured_intervals_count",
            "has_very_low_total_observations_count",
            "has_relative_weak_total_observations_count",
            "has_weak_system_support",
            "record_created_at",
        )
    )

    return audit_df
