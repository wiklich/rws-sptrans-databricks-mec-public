"""
Gold system-wide update behavior suspicious aggregate outlier audit pipeline.

Purpose:
    Identify unusually high or unusually low aggregate update behavior in
    gold_system_wide_update_behavior.

Audit focus:
    - very large system_avg_update_interval_seconds
    - unusually low system_avg_update_interval_seconds
    - unusually high system_avg_vehicle_stddev_interval_seconds
    - unusually high total_large_gaps_count

Why this audit matters:
    This audit is useful because it highlights aggregate system states that
    may indicate unusual data conditions or weak continuity.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_system_wide_update_behavior"
OUTPUT_TABLE_NAME = "gold.gold_system_wide_update_behavior_suspicious_outlier_audit"

STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying suspicious aggregate outlier behavior in "
        "gold_system_wide_update_behavior, including unusually high or low "
        "system average update intervals, unusually high average vehicle "
        "interval variability, and unusually high large-gap totals."
    ),
)
def gold_system_wide_update_behavior_suspicious_outlier_audit() -> DataFrame:
    """
    Materialize the suspicious aggregate outlier audit for
    gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Audit rows for system-wide summaries with suspicious aggregate
            outlier behavior.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_system_wide_update_behavior_suspicious_outlier_audit(
        source_df
    )


def _build_gold_system_wide_update_behavior_suspicious_outlier_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious aggregate outlier audit dataset.

    Processing steps:
        1. Read gold_system_wide_update_behavior.
        2. Compute global reference statistics for aggregate update behavior metrics.
        3. Compute high and low outlier thresholds.
        4. Flag system-wide rows with suspicious aggregate outlier behavior.
        5. Keep only rows that match at least one outlier condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Audit DataFrame containing system-wide rows with suspicious
            aggregate outlier behavior.
    """
    stats_df = source_df.agg(
        F.avg("system_avg_update_interval_seconds").alias(
            "avg_system_avg_update_interval_seconds"
        ),
        F.stddev_pop("system_avg_update_interval_seconds").alias(
            "stddev_system_avg_update_interval_seconds"
        ),
        F.min("system_avg_update_interval_seconds").alias(
            "min_system_avg_update_interval_seconds"
        ),
        F.max("system_avg_update_interval_seconds").alias(
            "max_system_avg_update_interval_seconds"
        ),
        F.avg("system_avg_vehicle_stddev_interval_seconds").alias(
            "avg_system_avg_vehicle_stddev_interval_seconds"
        ),
        F.stddev_pop("system_avg_vehicle_stddev_interval_seconds").alias(
            "stddev_system_avg_vehicle_stddev_interval_seconds"
        ),
        F.min("system_avg_vehicle_stddev_interval_seconds").alias(
            "min_system_avg_vehicle_stddev_interval_seconds"
        ),
        F.max("system_avg_vehicle_stddev_interval_seconds").alias(
            "max_system_avg_vehicle_stddev_interval_seconds"
        ),
        F.avg("total_large_gaps_count").alias("avg_total_large_gaps_count"),
        F.stddev_pop("total_large_gaps_count").alias(
            "stddev_total_large_gaps_count"
        ),
        F.min("total_large_gaps_count").alias("min_total_large_gaps_count"),
        F.max("total_large_gaps_count").alias("max_total_large_gaps_count"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "high_system_avg_update_interval_seconds_threshold",
            F.col("avg_system_avg_update_interval_seconds")
            + (
                F.coalesce(
                    F.col("stddev_system_avg_update_interval_seconds"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "low_system_avg_update_interval_seconds_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_system_avg_update_interval_seconds")
                - (
                    F.coalesce(
                        F.col("stddev_system_avg_update_interval_seconds"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "high_system_avg_vehicle_stddev_interval_seconds_threshold",
            F.col("avg_system_avg_vehicle_stddev_interval_seconds")
            + (
                F.coalesce(
                    F.col("stddev_system_avg_vehicle_stddev_interval_seconds"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "high_total_large_gaps_count_threshold",
            F.col("avg_total_large_gaps_count")
            + (
                F.coalesce(
                    F.col("stddev_total_large_gaps_count"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "has_unusually_high_system_avg_update_interval_seconds",
            F.col("system_avg_update_interval_seconds").isNotNull()
            & (
                F.col("system_avg_update_interval_seconds")
                > F.col("high_system_avg_update_interval_seconds_threshold")
            ),
        )
        .withColumn(
            "has_unusually_low_system_avg_update_interval_seconds",
            F.col("system_avg_update_interval_seconds").isNotNull()
            & (
                F.col("system_avg_update_interval_seconds")
                < F.col("low_system_avg_update_interval_seconds_threshold")
            ),
        )
        .withColumn(
            "has_unusually_high_system_avg_vehicle_stddev_interval_seconds",
            F.col("system_avg_vehicle_stddev_interval_seconds").isNotNull()
            & (
                F.col("system_avg_vehicle_stddev_interval_seconds")
                > F.col("high_system_avg_vehicle_stddev_interval_seconds_threshold")
            ),
        )
        .withColumn(
            "has_unusually_high_total_large_gaps_count",
            F.col("total_large_gaps_count").isNotNull()
            & (
                F.col("total_large_gaps_count")
                > F.col("high_total_large_gaps_count_threshold")
            ),
        )
        .withColumn(
            "has_suspicious_aggregate_outlier",
            F.col("has_unusually_high_system_avg_update_interval_seconds")
            | F.col("has_unusually_low_system_avg_update_interval_seconds")
            | F.col("has_unusually_high_system_avg_vehicle_stddev_interval_seconds")
            | F.col("has_unusually_high_total_large_gaps_count"),
        )
        .where(F.col("has_suspicious_aggregate_outlier") == True)
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
            "avg_system_avg_update_interval_seconds",
            "stddev_system_avg_update_interval_seconds",
            "min_system_avg_update_interval_seconds",
            "max_system_avg_update_interval_seconds",
            "avg_system_avg_vehicle_stddev_interval_seconds",
            "stddev_system_avg_vehicle_stddev_interval_seconds",
            "min_system_avg_vehicle_stddev_interval_seconds",
            "max_system_avg_vehicle_stddev_interval_seconds",
            "avg_total_large_gaps_count",
            "stddev_total_large_gaps_count",
            "min_total_large_gaps_count",
            "max_total_large_gaps_count",
            "high_system_avg_update_interval_seconds_threshold",
            "low_system_avg_update_interval_seconds_threshold",
            "high_system_avg_vehicle_stddev_interval_seconds_threshold",
            "high_total_large_gaps_count_threshold",
            "has_unusually_high_system_avg_update_interval_seconds",
            "has_unusually_low_system_avg_update_interval_seconds",
            "has_unusually_high_system_avg_vehicle_stddev_interval_seconds",
            "has_unusually_high_total_large_gaps_count",
            "has_suspicious_aggregate_outlier",
            "record_created_at",
        )
    )

    return audit_df
