"""
Gold recurring update patterns suspicious recurring outlier audit pipeline.

Purpose:
    Identify recurring temporal buckets with unusually high or unusually low
    update intervals or unusually high variability in
    gold_recurring_update_patterns.

Audit focus:
    - very large avg_update_interval_seconds
    - very large stddev_update_interval_seconds
    - recurring periods that strongly deviate from the broader distribution

Why this audit matters:
    This audit is useful because it highlights recurring periods whose
    behavior is unusually different from the overall system pattern.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_recurring_update_patterns"
OUTPUT_TABLE_NAME = "gold.gold_recurring_update_patterns_suspicious_outlier_audit"

STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying recurring temporal buckets with suspicious "
        "outlier behavior in gold_recurring_update_patterns, including "
        "unusually high average update intervals, unusually low average update "
        "intervals, and unusually high interval variability."
    ),
)
def gold_recurring_update_patterns_suspicious_outlier_audit() -> DataFrame:
    """
    Materialize the suspicious recurring outlier audit for
    gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Audit rows for recurring buckets with suspicious outlier behavior.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_recurring_update_patterns_suspicious_outlier_audit(source_df)


def _build_gold_recurring_update_patterns_suspicious_outlier_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious recurring outlier audit dataset.

    Processing steps:
        1. Read gold_recurring_update_patterns.
        2. Compute global reference statistics for average interval and
           interval variability.
        3. Compute high and low outlier thresholds.
        4. Flag recurring buckets with suspicious outlier behavior.
        5. Keep only rows that match at least one outlier condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Audit DataFrame containing recurring buckets with suspicious
            outlier behavior.
    """
    stats_df = source_df.agg(
        F.avg("avg_update_interval_seconds").alias("avg_avg_update_interval_seconds"),
        F.stddev_pop("avg_update_interval_seconds").alias(
            "stddev_avg_update_interval_seconds"
        ),
        F.min("avg_update_interval_seconds").alias("min_avg_update_interval_seconds"),
        F.max("avg_update_interval_seconds").alias("max_avg_update_interval_seconds"),
        F.avg("stddev_update_interval_seconds").alias(
            "avg_stddev_update_interval_seconds"
        ),
        F.stddev_pop("stddev_update_interval_seconds").alias(
            "stddev_stddev_update_interval_seconds"
        ),
        F.min("stddev_update_interval_seconds").alias(
            "min_stddev_update_interval_seconds"
        ),
        F.max("stddev_update_interval_seconds").alias(
            "max_stddev_update_interval_seconds"
        ),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "high_avg_update_interval_threshold",
            F.col("avg_avg_update_interval_seconds")
            + (
                F.coalesce(
                    F.col("stddev_avg_update_interval_seconds"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "low_avg_update_interval_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_avg_update_interval_seconds")
                - (
                    F.coalesce(
                        F.col("stddev_avg_update_interval_seconds"),
                        F.lit(0.0),
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "high_stddev_update_interval_threshold",
            F.col("avg_stddev_update_interval_seconds")
            + (
                F.coalesce(
                    F.col("stddev_stddev_update_interval_seconds"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "has_unusually_high_avg_update_interval",
            F.col("avg_update_interval_seconds").isNotNull()
            & (
                F.col("avg_update_interval_seconds")
                > F.col("high_avg_update_interval_threshold")
            ),
        )
        .withColumn(
            "has_unusually_low_avg_update_interval",
            F.col("avg_update_interval_seconds").isNotNull()
            & (
                F.col("avg_update_interval_seconds")
                < F.col("low_avg_update_interval_threshold")
            ),
        )
        .withColumn(
            "has_unusually_high_stddev_update_interval",
            F.col("stddev_update_interval_seconds").isNotNull()
            & (
                F.col("stddev_update_interval_seconds")
                > F.col("high_stddev_update_interval_threshold")
            ),
        )
        .withColumn(
            "has_suspicious_recurring_outlier",
            F.col("has_unusually_high_avg_update_interval")
            | F.col("has_unusually_low_avg_update_interval")
            | F.col("has_unusually_high_stddev_update_interval"),
        )
        .where(F.col("has_suspicious_recurring_outlier") == True)
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
            "avg_avg_update_interval_seconds",
            "stddev_avg_update_interval_seconds",
            "min_avg_update_interval_seconds",
            "max_avg_update_interval_seconds",
            "avg_stddev_update_interval_seconds",
            "stddev_stddev_update_interval_seconds",
            "min_stddev_update_interval_seconds",
            "max_stddev_update_interval_seconds",
            "high_avg_update_interval_threshold",
            "low_avg_update_interval_threshold",
            "high_stddev_update_interval_threshold",
            "has_unusually_high_avg_update_interval",
            "has_unusually_low_avg_update_interval",
            "has_unusually_high_stddev_update_interval",
            "has_suspicious_recurring_outlier",
            "record_created_at",
        )
    )

    return audit_df
