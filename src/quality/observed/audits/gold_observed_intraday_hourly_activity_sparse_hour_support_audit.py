"""
Gold observed intraday hourly activity sparse-hour support audit pipeline.

Purpose:
    Identify hours with unusually weak minute-level support in
    gold_observed_intraday_hourly_activity.

Audit focus:
    - very low distinct_minutes_with_data
    - hours with only a few observed minutes
    - weak hourly support relative to the overall observed distribution

Why this audit matters:
    This audit is useful because the strength of intraday interpretation depends
    on minute-level support inside the hour.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_intraday_hourly_activity"
OUTPUT_TABLE_NAME = "gold.gold_observed_intraday_hourly_activity_sparse_hour_support_audit"

SPARSE_MINUTES_THRESHOLD = 10
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying hours with unusually weak minute-level support "
        "in gold_observed_intraday_hourly_activity."
    ),
)
def gold_observed_intraday_hourly_activity_sparse_hour_support_audit() -> DataFrame:
    """
    Materialize the sparse-hour support audit for
    gold_observed_intraday_hourly_activity.

    Returns:
        DataFrame:
            Audit rows for hours with weak minute-level support.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_intraday_hourly_activity_sparse_hour_support_audit(
        source_df
    )


def _build_gold_observed_intraday_hourly_activity_sparse_hour_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the sparse-hour support audit dataset.

    Processing steps:
        1. Read gold_observed_intraday_hourly_activity.
        2. Compute global reference statistics for distinct_minutes_with_data.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag hours with sparse minute coverage.
        5. Keep only rows that match at least one sparse-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_intraday_hourly_activity.

    Returns:
        DataFrame:
            Audit DataFrame containing hours with weak minute-level support.
    """
    stats_df = source_df.agg(
        F.avg("distinct_minutes_with_data").alias("avg_distinct_minutes_with_data"),
        F.stddev_pop("distinct_minutes_with_data").alias(
            "stddev_distinct_minutes_with_data"
        ),
        F.min("distinct_minutes_with_data").alias("min_distinct_minutes_with_data"),
        F.max("distinct_minutes_with_data").alias("max_distinct_minutes_with_data"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_sparse_minutes_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_minutes_with_data")
                - (
                    F.coalesce(F.col("stddev_distinct_minutes_with_data"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_minute_support",
            F.col("distinct_minutes_with_data") <= F.lit(SPARSE_MINUTES_THRESHOLD),
        )
        .withColumn(
            "has_relative_sparse_minute_support",
            F.col("distinct_minutes_with_data")
            < F.col("relative_sparse_minutes_threshold"),
        )
        .withColumn(
            "has_sparse_hour_support",
            F.col("has_very_low_minute_support")
            | F.col("has_relative_sparse_minute_support"),
        )
        .where(F.col("has_sparse_hour_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "analysis_hour_local",
            "observed_position_records_count",
            "distinct_observed_vehicles_count",
            "distinct_observed_lines_count",
            "distinct_minutes_with_data",
            "min_captured_at_local",
            "max_captured_at_local",
            "avg_distinct_minutes_with_data",
            "stddev_distinct_minutes_with_data",
            "min_distinct_minutes_with_data",
            "max_distinct_minutes_with_data",
            "relative_sparse_minutes_threshold",
            "has_very_low_minute_support",
            "has_relative_sparse_minute_support",
            "has_sparse_hour_support",
            "record_created_at",
        )
    )

    return audit_df
