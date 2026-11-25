"""
Gold observed daily activity sparse-day support audit pipeline.

Purpose:
    Identify days with unusually weak temporal coverage in
    gold_observed_daily_activity.

Audit focus:
    - very low distinct_observed_hours_count
    - very low distinct_observed_minutes_count
    - days with narrow observation windows

Why this audit matters:
    This audit is useful because the strength of daily interpretation depends
    on how complete the observed time coverage is.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_daily_activity"
OUTPUT_TABLE_NAME = "gold.gold_observed_daily_activity_sparse_day_support_audit"

SPARSE_HOURS_THRESHOLD = 6
SPARSE_MINUTES_THRESHOLD = 120
STDDEV_MULTIPLIER = 2.0
NARROW_WINDOW_THRESHOLD_SECONDS = 4 * 60 * 60


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying days with unusually weak temporal coverage in "
        "gold_observed_daily_activity."
    ),
)
def gold_observed_daily_activity_sparse_day_support_audit() -> DataFrame:
    """
    Materialize the sparse-day support audit for gold_observed_daily_activity.

    Returns:
        DataFrame:
            Audit rows for days with weak temporal support.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_daily_activity_sparse_day_support_audit(source_df)


def _build_gold_observed_daily_activity_sparse_day_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the sparse-day support audit dataset.

    Processing steps:
        1. Read gold_observed_daily_activity.
        2. Compute global reference statistics for distinct observed hours and minutes.
        3. Compute the observed window span of each day.
        4. Compute absolute and relative weak-support thresholds.
        5. Flag days with sparse temporal coverage.
        6. Keep only rows that match at least one sparse-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_daily_activity.

    Returns:
        DataFrame:
            Audit DataFrame containing days with weak temporal coverage.
    """
    stats_df = source_df.agg(
        F.avg("distinct_observed_hours_count").alias("avg_distinct_observed_hours_count"),
        F.stddev_pop("distinct_observed_hours_count").alias(
            "stddev_distinct_observed_hours_count"
        ),
        F.min("distinct_observed_hours_count").alias("min_distinct_observed_hours_count"),
        F.max("distinct_observed_hours_count").alias("max_distinct_observed_hours_count"),
        F.avg("distinct_observed_minutes_count").alias(
            "avg_distinct_observed_minutes_count"
        ),
        F.stddev_pop("distinct_observed_minutes_count").alias(
            "stddev_distinct_observed_minutes_count"
        ),
        F.min("distinct_observed_minutes_count").alias(
            "min_distinct_observed_minutes_count"
        ),
        F.max("distinct_observed_minutes_count").alias(
            "max_distinct_observed_minutes_count"
        ),
    )

    audit_df = (
        source_df.withColumn(
            "observed_window_seconds",
            F.when(
                F.col("min_captured_at_local").isNotNull()
                & F.col("max_captured_at_local").isNotNull(),
                F.unix_timestamp("max_captured_at_local")
                - F.unix_timestamp("min_captured_at_local"),
            ).otherwise(F.lit(None)),
        )
        .crossJoin(stats_df)
        .withColumn(
            "relative_sparse_hours_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_observed_hours_count")
                - (
                    F.coalesce(F.col("stddev_distinct_observed_hours_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_sparse_minutes_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_observed_minutes_count")
                - (
                    F.coalesce(
                        F.col("stddev_distinct_observed_minutes_count"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_hour_support",
            F.col("distinct_observed_hours_count") <= F.lit(SPARSE_HOURS_THRESHOLD),
        )
        .withColumn(
            "has_very_low_minute_support",
            F.col("distinct_observed_minutes_count") <= F.lit(SPARSE_MINUTES_THRESHOLD),
        )
        .withColumn(
            "has_relative_sparse_hour_support",
            F.col("distinct_observed_hours_count")
            < F.col("relative_sparse_hours_threshold"),
        )
        .withColumn(
            "has_relative_sparse_minute_support",
            F.col("distinct_observed_minutes_count")
            < F.col("relative_sparse_minutes_threshold"),
        )
        .withColumn(
            "has_narrow_observation_window",
            F.col("observed_window_seconds").isNotNull()
            & (F.col("observed_window_seconds") <= F.lit(NARROW_WINDOW_THRESHOLD_SECONDS)),
        )
        .withColumn(
            "has_sparse_day_support",
            F.col("has_very_low_hour_support")
            | F.col("has_very_low_minute_support")
            | F.col("has_relative_sparse_hour_support")
            | F.col("has_relative_sparse_minute_support")
            | F.col("has_narrow_observation_window"),
        )
        .where(F.col("has_sparse_day_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "observed_position_records_count",
            "distinct_observed_vehicles_count",
            "distinct_observed_lines_count",
            "distinct_observed_hours_count",
            "distinct_observed_minutes_count",
            "min_captured_at_local",
            "max_captured_at_local",
            "observed_window_seconds",
            "avg_distinct_observed_hours_count",
            "stddev_distinct_observed_hours_count",
            "min_distinct_observed_hours_count",
            "max_distinct_observed_hours_count",
            "avg_distinct_observed_minutes_count",
            "stddev_distinct_observed_minutes_count",
            "min_distinct_observed_minutes_count",
            "max_distinct_observed_minutes_count",
            "relative_sparse_hours_threshold",
            "relative_sparse_minutes_threshold",
            "has_very_low_hour_support",
            "has_very_low_minute_support",
            "has_relative_sparse_hour_support",
            "has_relative_sparse_minute_support",
            "has_narrow_observation_window",
            "has_sparse_day_support",
            "record_created_at",
        )
    )

    return audit_df
