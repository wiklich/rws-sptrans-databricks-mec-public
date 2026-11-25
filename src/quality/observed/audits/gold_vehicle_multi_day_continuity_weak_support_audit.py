"""
Gold vehicle multi-day continuity weak multi-day support audit pipeline.

Purpose:
    Identify vehicles with too little multi-day support to sustain strong
    continuity interpretation in gold_vehicle_multi_day_continuity.

Audit focus:
    - very low observed_dates_count
    - very short observed_date_span_days
    - very low total_observations_count

Why this audit matters:
    This audit is useful because multi-day continuity interpretation becomes
    weaker when support is too limited.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_multi_day_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_multi_day_continuity_weak_support_audit"

WEAK_OBSERVED_DATES_COUNT_THRESHOLD = 2
WEAK_OBSERVED_DATE_SPAN_DAYS_THRESHOLD = 2
WEAK_TOTAL_OBSERVATIONS_COUNT_THRESHOLD = 10
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying vehicles with weak multi-day support in "
        "gold_vehicle_multi_day_continuity, including very low observed date "
        "count, very short observed date span, and very low total observation count."
    ),
)
def gold_vehicle_multi_day_continuity_weak_support_audit() -> DataFrame:
    """
    Materialize the weak multi-day support audit for
    gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Audit rows for vehicles with weak support for strong multi-day
            continuity interpretation.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_multi_day_continuity_weak_support_audit(source_df)


def _build_gold_vehicle_multi_day_continuity_weak_support_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the weak multi-day support audit dataset.

    Processing steps:
        1. Read gold_vehicle_multi_day_continuity.
        2. Compute global reference statistics for observed date count,
           observed date span, and total observation count.
        3. Compute absolute and relative weak-support thresholds.
        4. Flag vehicles with weak multi-day support.
        5. Keep only rows that match at least one weak-support condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Audit DataFrame containing vehicles with weak multi-day support.
    """
    stats_df = source_df.agg(
        F.avg("observed_dates_count").alias("avg_observed_dates_count"),
        F.stddev_pop("observed_dates_count").alias("stddev_observed_dates_count"),
        F.min("observed_dates_count").alias("min_observed_dates_count"),
        F.max("observed_dates_count").alias("max_observed_dates_count"),
        F.avg("observed_date_span_days").alias("avg_observed_date_span_days"),
        F.stddev_pop("observed_date_span_days").alias(
            "stddev_observed_date_span_days"
        ),
        F.min("observed_date_span_days").alias("min_observed_date_span_days"),
        F.max("observed_date_span_days").alias("max_observed_date_span_days"),
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
            "relative_weak_observed_dates_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_observed_dates_count")
                - (
                    F.coalesce(F.col("stddev_observed_dates_count"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "relative_weak_observed_date_span_days_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_observed_date_span_days")
                - (
                    F.coalesce(
                        F.col("stddev_observed_date_span_days"), F.lit(0.0)
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
                        F.col("stddev_total_observations_count"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_low_observed_dates_count",
            F.col("observed_dates_count")
            <= F.lit(WEAK_OBSERVED_DATES_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_observed_dates_count",
            F.col("observed_dates_count")
            < F.col("relative_weak_observed_dates_count_threshold"),
        )
        .withColumn(
            "has_very_short_observed_date_span_days",
            F.col("observed_date_span_days")
            <= F.lit(WEAK_OBSERVED_DATE_SPAN_DAYS_THRESHOLD),
        )
        .withColumn(
            "has_relative_weak_observed_date_span_days",
            F.col("observed_date_span_days")
            < F.col("relative_weak_observed_date_span_days_threshold"),
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
            "has_weak_multi_day_support",
            F.col("has_very_low_observed_dates_count")
            | F.col("has_relative_weak_observed_dates_count")
            | F.col("has_very_short_observed_date_span_days")
            | F.col("has_relative_weak_observed_date_span_days")
            | F.col("has_very_low_total_observations_count")
            | F.col("has_relative_weak_total_observations_count"),
        )
        .where(F.col("has_weak_multi_day_support") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "vehicle_prefix",
            "observed_dates_count",
            "first_observed_date",
            "last_observed_date",
            "observed_date_span_days",
            "missing_dates_within_span_count",
            "date_presence_ratio",
            "first_captured_at_local",
            "last_captured_at_local",
            "total_observations_count",
            "avg_observations_per_observed_date",
            "max_consecutive_observed_dates",
            "max_consecutive_missing_dates_within_span",
            "avg_observed_dates_count",
            "stddev_observed_dates_count",
            "min_observed_dates_count",
            "max_observed_dates_count",
            "avg_observed_date_span_days",
            "stddev_observed_date_span_days",
            "min_observed_date_span_days",
            "max_observed_date_span_days",
            "avg_total_observations_count",
            "stddev_total_observations_count",
            "min_total_observations_count",
            "max_total_observations_count",
            "relative_weak_observed_dates_count_threshold",
            "relative_weak_observed_date_span_days_threshold",
            "relative_weak_total_observations_count_threshold",
            "has_very_low_observed_dates_count",
            "has_relative_weak_observed_dates_count",
            "has_very_short_observed_date_span_days",
            "has_relative_weak_observed_date_span_days",
            "has_very_low_total_observations_count",
            "has_relative_weak_total_observations_count",
            "has_weak_multi_day_support",
            "record_created_at",
        )
    )

    return audit_df
