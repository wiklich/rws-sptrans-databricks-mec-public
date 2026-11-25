"""
Gold vehicle update frequency suspicious interval outlier audit pipeline.

Purpose:
    Identify vehicles with unusually high or unusually low update intervals in
    gold_vehicle_update_frequency.

Audit focus:
    - very large average update interval
    - extremely high maximum interval
    - unusually low minimum interval

Why this audit matters:
    This audit is useful because extreme values may indicate unusual
    observation behavior, timestamp issues, or weak continuity.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_update_frequency"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_update_frequency_suspicious_interval_outlier_audit"

STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying vehicles with suspicious interval outliers in "
        "gold_vehicle_update_frequency, including unusually high average update "
        "intervals, extremely high maximum intervals, and unusually low "
        "minimum intervals."
    ),
)
def gold_vehicle_update_frequency_suspicious_interval_outlier_audit() -> DataFrame:
    """
    Materialize the suspicious interval outlier audit for
    gold_vehicle_update_frequency.

    Returns:
        DataFrame:
            Audit rows for vehicles with suspicious interval behavior.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_update_frequency_suspicious_interval_outlier_audit(
        source_df
    )


def _build_gold_vehicle_update_frequency_suspicious_interval_outlier_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious interval outlier audit dataset.

    Processing steps:
        1. Read gold_vehicle_update_frequency.
        2. Compute global reference statistics for average, maximum, and minimum
           update intervals.
        3. Compute high and low outlier thresholds.
        4. Flag vehicles with suspicious interval values.
        5. Keep only rows that match at least one outlier condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.

    Returns:
        DataFrame:
            Audit DataFrame containing vehicles with suspicious interval outliers.
    """
    stats_df = source_df.agg(
        F.avg("avg_update_interval_seconds").alias("avg_avg_update_interval_seconds"),
        F.stddev_pop("avg_update_interval_seconds").alias(
            "stddev_avg_update_interval_seconds"
        ),
        F.min("avg_update_interval_seconds").alias("min_avg_update_interval_seconds"),
        F.max("avg_update_interval_seconds").alias("max_avg_update_interval_seconds"),
        F.avg("max_update_interval_seconds").alias("avg_max_update_interval_seconds"),
        F.stddev_pop("max_update_interval_seconds").alias(
            "stddev_max_update_interval_seconds"
        ),
        F.min("max_update_interval_seconds").alias("min_max_update_interval_seconds"),
        F.max("max_update_interval_seconds").alias("max_max_update_interval_seconds"),
        F.avg("min_update_interval_seconds").alias("avg_min_update_interval_seconds"),
        F.stddev_pop("min_update_interval_seconds").alias(
            "stddev_min_update_interval_seconds"
        ),
        F.min("min_update_interval_seconds").alias("min_min_update_interval_seconds"),
        F.max("min_update_interval_seconds").alias("max_min_update_interval_seconds"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "high_avg_interval_threshold",
            F.col("avg_avg_update_interval_seconds")
            + (
                F.coalesce(F.col("stddev_avg_update_interval_seconds"), F.lit(0.0))
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "high_max_interval_threshold",
            F.col("avg_max_update_interval_seconds")
            + (
                F.coalesce(F.col("stddev_max_update_interval_seconds"), F.lit(0.0))
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "low_min_interval_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_min_update_interval_seconds")
                - (
                    F.coalesce(F.col("stddev_min_update_interval_seconds"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_unusually_high_avg_interval",
            F.col("avg_update_interval_seconds").isNotNull()
            & (
                F.col("avg_update_interval_seconds")
                > F.col("high_avg_interval_threshold")
            ),
        )
        .withColumn(
            "has_unusually_high_max_interval",
            F.col("max_update_interval_seconds").isNotNull()
            & (
                F.col("max_update_interval_seconds")
                > F.col("high_max_interval_threshold")
            ),
        )
        .withColumn(
            "has_unusually_low_min_interval",
            F.col("min_update_interval_seconds").isNotNull()
            & (
                F.col("min_update_interval_seconds")
                < F.col("low_min_interval_threshold")
            ),
        )
        .withColumn(
            "has_suspicious_interval_outlier",
            F.col("has_unusually_high_avg_interval")
            | F.col("has_unusually_high_max_interval")
            | F.col("has_unusually_low_min_interval"),
        )
        .where(F.col("has_suspicious_interval_outlier") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "vehicle_prefix",
            "updates_count",
            "measured_intervals_count",
            "avg_update_interval_seconds",
            "min_update_interval_seconds",
            "max_update_interval_seconds",
            "stddev_update_interval_seconds",
            "first_captured_at_local",
            "last_captured_at_local",
            "observed_span_seconds",
            "avg_avg_update_interval_seconds",
            "stddev_avg_update_interval_seconds",
            "min_avg_update_interval_seconds",
            "max_avg_update_interval_seconds",
            "avg_max_update_interval_seconds",
            "stddev_max_update_interval_seconds",
            "min_max_update_interval_seconds",
            "max_max_update_interval_seconds",
            "avg_min_update_interval_seconds",
            "stddev_min_update_interval_seconds",
            "min_min_update_interval_seconds",
            "max_min_update_interval_seconds",
            "high_avg_interval_threshold",
            "high_max_interval_threshold",
            "low_min_interval_threshold",
            "has_unusually_high_avg_interval",
            "has_unusually_high_max_interval",
            "has_unusually_low_min_interval",
            "has_suspicious_interval_outlier",
            "record_created_at",
        )
    )

    return audit_df
