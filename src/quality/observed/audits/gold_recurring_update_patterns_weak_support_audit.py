"""
Gold recurring update patterns weak recurring support audit pipeline.

Purpose:
    Identify recurring temporal buckets with too little support to sustain
    strong recurring interpretation in gold_recurring_update_patterns.

Audit focus:
    - very low distinct_dates_count
    - very low distinct_vehicles_count
    - very low measured_intervals_count

Why this audit matters:
    This audit is useful because recurring-pattern interpretation becomes
    weaker when support is too limited.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_recurring_update_patterns"
OUTPUT_TABLE_NAME = "gold.gold_recurring_update_patterns_weak_support_audit"

WEAK_DISTINCT_DATES_COUNT_THRESHOLD = 2
WEAK_DISTINCT_VEHICLES_COUNT_THRESHOLD = 3
WEAK_MEASURED_INTERVALS_COUNT_THRESHOLD = 10
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying recurring temporal buckets with weak support "
        "in gold_recurring_update_patterns, including very low distinct date "
        "count, very low distinct vehicle count, and very low measurable "
        "interval count."
    ),
)
def gold_recurring_update_patterns_weak_support_audit() -> DataFrame:
    """
    Materialize the weak recurring support audit for
    gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Audit rows for recurring buckets with weak support for strong
            recurring interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_recurring_update_patterns_weak_support_audit(source_df)


def _build_gold_recurring_update_patterns_weak_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the weak recurring support audit dataset.

    Processing steps:
        1. Read gold_recurring_update_patterns.
        2. Compute global reference statistics for recurring support metrics.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag recurring buckets with weak support.
        5. Keep only rows that match at least one weak-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Audit DataFrame containing recurring buckets with weak support.
    """
    stats_df = source_df.agg(
        F.avg("distinct_dates_count").alias("avg_distinct_dates_count"),
        F.stddev_pop("distinct_dates_count").alias("stddev_distinct_dates_count"),
        F.min("distinct_dates_count").alias("min_distinct_dates_count"),
        F.max("distinct_dates_count").alias("max_distinct_dates_count"),
        F.avg("distinct_vehicles_count").alias("avg_distinct_vehicles_count"),
        F.stddev_pop("distinct_vehicles_count").alias("stddev_distinct_vehicles_count"),
        F.min("distinct_vehicles_count").alias("min_distinct_vehicles_count"),
        F.max("distinct_vehicles_count").alias("max_distinct_vehicles_count"),
        F.avg("measured_intervals_count").alias("avg_measured_intervals_count"),
        F.stddev_pop("measured_intervals_count").alias(
            "stddev_measured_intervals_count"
        ),
        F.min("measured_intervals_count").alias("min_measured_intervals_count"),
        F.max("measured_intervals_count").alias("max_measured_intervals_count"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_weak_distinct_dates_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_dates_count")
                - (
                    F.coalesce(F.col("stddev_distinct_dates_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_distinct_vehicles_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_vehicles_count")
                - (
                    F.coalesce(F.col("stddev_distinct_vehicles_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_measured_intervals_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_measured_intervals_count")
                - (
                    F.coalesce(F.col("stddev_measured_intervals_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_distinct_dates_count",
            F.col("distinct_dates_count")
            <= F.lit(WEAK_DISTINCT_DATES_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_distinct_dates_count",
            F.col("distinct_dates_count")
            < F.col("relative_weak_distinct_dates_count_threshold"),
        )
        .withColumn(
            "has_very_low_distinct_vehicles_count",
            F.col("distinct_vehicles_count")
            <= F.lit(WEAK_DISTINCT_VEHICLES_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_distinct_vehicles_count",
            F.col("distinct_vehicles_count")
            < F.col("relative_weak_distinct_vehicles_count_threshold"),
        )
        .withColumn(
            "has_very_low_measured_intervals_count",
            F.col("measured_intervals_count")
            <= F.lit(WEAK_MEASURED_INTERVALS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_measured_intervals_count",
            F.col("measured_intervals_count")
            < F.col("relative_weak_measured_intervals_count_threshold"),
        )
        .withColumn(
            "has_weak_recurring_support",
            F.col("has_very_low_distinct_dates_count")
            | F.col("has_relative_weak_distinct_dates_count")
            | F.col("has_very_low_distinct_vehicles_count")
            | F.col("has_relative_weak_distinct_vehicles_count")
            | F.col("has_very_low_measured_intervals_count")
            | F.col("has_relative_weak_measured_intervals_count"),
        )
        .where(F.col("has_weak_recurring_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "day_of_week",
            "hour_of_day",
            "observations_count",
            "measured_intervals_count",
            "avg_update_interval_seconds",
            "min_update_interval_seconds",
            "max_update_interval_seconds",
            "stddev_update_interval_seconds",
            "distinct_vehicles_count",
            "distinct_dates_count",
            "avg_intervals_per_vehicle",
            "avg_intervals_per_date",
            "avg_distinct_dates_count",
            "stddev_distinct_dates_count",
            "min_distinct_dates_count",
            "max_distinct_dates_count",
            "avg_distinct_vehicles_count",
            "stddev_distinct_vehicles_count",
            "min_distinct_vehicles_count",
            "max_distinct_vehicles_count",
            "avg_measured_intervals_count",
            "stddev_measured_intervals_count",
            "min_measured_intervals_count",
            "max_measured_intervals_count",
            "relative_weak_distinct_dates_count_threshold",
            "relative_weak_distinct_vehicles_count_threshold",
            "relative_weak_measured_intervals_count_threshold",
            "has_very_low_distinct_dates_count",
            "has_relative_weak_distinct_dates_count",
            "has_very_low_distinct_vehicles_count",
            "has_relative_weak_distinct_vehicles_count",
            "has_very_low_measured_intervals_count",
            "has_relative_weak_measured_intervals_count",
            "has_weak_recurring_support",
            "record_created_at",
        )
    )

    return audit_df
