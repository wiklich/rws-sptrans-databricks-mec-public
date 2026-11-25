"""
Gold vehicle observation continuity weak continuity support audit pipeline.

Purpose:
    Identify vehicles with too little temporal support to sustain strong
    continuity interpretation in gold_vehicle_observation_continuity.

Audit focus:
    - very low distinct_minutes_with_data
    - very short observed_span_seconds
    - very low measured_gaps_count

Why this audit matters:
    This audit is useful because continuity interpretation becomes weaker when
    observed support is very limited.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observation_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observation_continuity_weak_support_audit"

WEAK_MINUTES_WITH_DATA_THRESHOLD = 5
WEAK_OBSERVED_SPAN_SECONDS_THRESHOLD = 300
WEAK_MEASURED_GAPS_COUNT_THRESHOLD = 2
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying vehicles with weak temporal support in "
        "gold_vehicle_observation_continuity, including very low minute-level "
        "support, very short observed span, and very low measurable gap count."
    ),
)
def gold_vehicle_observation_continuity_weak_support_audit() -> DataFrame:
    """
    Materialize the weak continuity support audit for
    gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Audit rows for vehicles with weak support for strong continuity
            interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observation_continuity_weak_support_audit(source_df)


def _build_gold_vehicle_observation_continuity_weak_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the weak continuity support audit dataset.

    Processing steps:
        1. Read gold_vehicle_observation_continuity.
        2. Compute global reference statistics for minute support, observed span,
           and measurable gap count.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag vehicles with weak continuity support.
        5. Keep only rows that match at least one weak-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Audit DataFrame containing vehicles with weak continuity support.
    """
    stats_df = source_df.agg(
        F.avg("distinct_minutes_with_data").alias("avg_distinct_minutes_with_data"),
        F.stddev_pop("distinct_minutes_with_data").alias(
            "stddev_distinct_minutes_with_data"
        ),
        F.min("distinct_minutes_with_data").alias("min_distinct_minutes_with_data"),
        F.max("distinct_minutes_with_data").alias("max_distinct_minutes_with_data"),
        F.avg("observed_span_seconds").alias("avg_observed_span_seconds"),
        F.stddev_pop("observed_span_seconds").alias("stddev_observed_span_seconds"),
        F.min("observed_span_seconds").alias("min_observed_span_seconds"),
        F.max("observed_span_seconds").alias("max_observed_span_seconds"),
        F.avg("measured_gaps_count").alias("avg_measured_gaps_count"),
        F.stddev_pop("measured_gaps_count").alias("stddev_measured_gaps_count"),
        F.min("measured_gaps_count").alias("min_measured_gaps_count"),
        F.max("measured_gaps_count").alias("max_measured_gaps_count"),
    )

    audit_df = (
        source_df.crossJoin(stats_df)
        .withColumn(
            "relative_weak_minutes_with_data_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_minutes_with_data")
                - (
                    F.coalesce(
                        F.col("stddev_distinct_minutes_with_data"), F.lit(0.0)
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
                    F.coalesce(F.col("stddev_observed_span_seconds"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_measured_gaps_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_measured_gaps_count")
                - (
                    F.coalesce(F.col("stddev_measured_gaps_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_minutes_with_data",
            F.col("distinct_minutes_with_data")
            <= F.lit(WEAK_MINUTES_WITH_DATA_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_minutes_with_data",
            F.col("distinct_minutes_with_data")
            < F.col("relative_weak_minutes_with_data_threshold"),
        )
        .withColumn(
            "has_very_short_observed_span",
            F.col("observed_span_seconds")
            <= F.lit(WEAK_OBSERVED_SPAN_SECONDS_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_observed_span",
            F.col("observed_span_seconds")
            < F.col("relative_weak_observed_span_threshold"),
        )
        .withColumn(
            "has_very_low_measured_gaps_count",
            F.col("measured_gaps_count")
            <= F.lit(WEAK_MEASURED_GAPS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_measured_gaps_count",
            F.col("measured_gaps_count")
            < F.col("relative_weak_measured_gaps_threshold"),
        )
        .withColumn(
            "has_weak_continuity_support",
            F.col("has_very_low_minutes_with_data")
            | F.col("has_relative_weak_minutes_with_data")
            | F.col("has_very_short_observed_span")
            | F.col("has_relative_weak_observed_span")
            | F.col("has_very_low_measured_gaps_count")
            | F.col("has_relative_weak_measured_gaps_count"),
        )
        .where(F.col("has_weak_continuity_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "vehicle_prefix",
            "observations_count",
            "measured_gaps_count",
            "first_captured_at_local",
            "last_captured_at_local",
            "observed_span_seconds",
            "distinct_minutes_with_data",
            "distinct_hours_with_data",
            "avg_gap_seconds",
            "max_gap_seconds",
            "large_gaps_count",
            "continuity_support_ratio",
            "avg_distinct_minutes_with_data",
            "stddev_distinct_minutes_with_data",
            "min_distinct_minutes_with_data",
            "max_distinct_minutes_with_data",
            "avg_observed_span_seconds",
            "stddev_observed_span_seconds",
            "min_observed_span_seconds",
            "max_observed_span_seconds",
            "avg_measured_gaps_count",
            "stddev_measured_gaps_count",
            "min_measured_gaps_count",
            "max_measured_gaps_count",
            "relative_weak_minutes_with_data_threshold",
            "relative_weak_observed_span_threshold",
            "relative_weak_measured_gaps_threshold",
            "has_very_low_minutes_with_data",
            "has_relative_weak_minutes_with_data",
            "has_very_short_observed_span",
            "has_relative_weak_observed_span",
            "has_very_low_measured_gaps_count",
            "has_relative_weak_measured_gaps_count",
            "has_weak_continuity_support",
            "record_created_at",
        )
    )

    return audit_df
