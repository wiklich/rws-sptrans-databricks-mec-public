"""
Gold vehicle update frequency weak interval support audit pipeline.

Purpose:
    Identify vehicles with too little measurable interval support to sustain
    stable update-frequency interpretation in gold_vehicle_update_frequency.

Audit focus:
    - very low measured_intervals_count
    - vehicles with only one or two measurable gaps
    - vehicles with short observed span

Why this audit matters:
    This audit is useful because average interval interpretation becomes weaker
    when support is very low.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_update_frequency"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_update_frequency_weak_interval_support_audit"

WEAK_INTERVAL_COUNT_THRESHOLD = 2
WEAK_SPAN_SECONDS_THRESHOLD = 300
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying vehicles with weak interval support in "
        "gold_vehicle_update_frequency, including very low measurable interval "
        "counts and short observed spans."
    ),
)
def gold_vehicle_update_frequency_weak_interval_support_audit() -> DataFrame:
    """
    Materialize the weak interval support audit for gold_vehicle_update_frequency.

    Returns:
        DataFrame:
            Audit rows for vehicles with weak support for stable interval
            interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_update_frequency_weak_interval_support_audit(
        source_df
    )


def _build_gold_vehicle_update_frequency_weak_interval_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the weak interval support audit dataset.

    Processing steps:
        1. Read gold_vehicle_update_frequency.
        2. Compute global reference statistics for measured interval count and
           observed span.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag vehicles with weak interval support.
        5. Keep only rows that match at least one weak-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.

    Returns:
        DataFrame:
            Audit DataFrame containing vehicles with weak interval support.
    """
    stats_df = source_df.agg(
        F.avg("measured_intervals_count").alias("avg_measured_intervals_count"),
        F.stddev_pop("measured_intervals_count").alias(
            "stddev_measured_intervals_count"
        ),
        F.min("measured_intervals_count").alias("min_measured_intervals_count"),
        F.max("measured_intervals_count").alias("max_measured_intervals_count"),
        F.avg("observed_span_seconds").alias("avg_observed_span_seconds"),
        F.stddev_pop("observed_span_seconds").alias("stddev_observed_span_seconds"),
        F.min("observed_span_seconds").alias("min_observed_span_seconds"),
        F.max("observed_span_seconds").alias("max_observed_span_seconds"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_weak_interval_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_measured_intervals_count")
                - (
                    F.coalesce(
                        F.col("stddev_measured_intervals_count"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_observed_span_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_observed_span_seconds")
                - (
                    F.coalesce(
                        F.col("stddev_observed_span_seconds"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_measured_intervals_count",
            F.col("measured_intervals_count") <= F.lit(WEAK_INTERVAL_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_measured_intervals_count",
            F.col("measured_intervals_count")
            < F.col("relative_weak_interval_count_threshold"),
        )
        .withColumn(
            "has_short_observed_span",
            F.col("observed_span_seconds") <= F.lit(WEAK_SPAN_SECONDS_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_observed_span",
            F.col("observed_span_seconds")
            < F.col("relative_weak_observed_span_threshold"),
        )
        .withColumn(
            "has_weak_interval_support",
            F.col("has_very_low_measured_intervals_count")
            | F.col("has_relative_weak_measured_intervals_count")
            | F.col("has_short_observed_span")
            | F.col("has_relative_weak_observed_span"),
        )
        .where(F.col("has_weak_interval_support") == True)
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
            "avg_measured_intervals_count",
            "stddev_measured_intervals_count",
            "min_measured_intervals_count",
            "max_measured_intervals_count",
            "avg_observed_span_seconds",
            "stddev_observed_span_seconds",
            "min_observed_span_seconds",
            "max_observed_span_seconds",
            "relative_weak_interval_count_threshold",
            "relative_weak_observed_span_threshold",
            "has_very_low_measured_intervals_count",
            "has_relative_weak_measured_intervals_count",
            "has_short_observed_span",
            "has_relative_weak_observed_span",
            "has_weak_interval_support",
            "record_created_at",
        )
    )

    return audit_df
