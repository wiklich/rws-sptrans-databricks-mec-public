"""
Gold vehicle multi-day continuity fragmented continuity audit pipeline.

Purpose:
    Identify vehicles with weak cross-day continuity inside their own observed
    span in gold_vehicle_multi_day_continuity.

Audit focus:
    - very high missing_dates_within_span_count
    - very low date_presence_ratio
    - very short maximum consecutive observed-date streak compared to total span

Why this audit matters:
    This audit is useful because it highlights vehicles whose day-level
    presence is fragmented rather than continuous.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_multi_day_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_multi_day_continuity_fragmented_continuity_audit"

MISSING_DATES_THRESHOLD = 5
DATE_PRESENCE_RATIO_THRESHOLD = 0.50
OBSERVED_STREAK_RATIO_THRESHOLD = 0.30
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying vehicles with fragmented multi-day continuity "
        "in gold_vehicle_multi_day_continuity, including high missing-date "
        "counts, low date presence ratios, and short consecutive observed-date "
        "streaks relative to the observed span."
    ),
)
def gold_vehicle_multi_day_continuity_fragmented_continuity_audit() -> DataFrame:
    """
    Materialize the fragmented multi-day continuity audit for
    gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Audit rows for vehicles with fragmented multi-day continuity behavior.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_multi_day_continuity_fragmented_continuity_audit(
        source_df
    )


def _build_gold_vehicle_multi_day_continuity_fragmented_continuity_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the fragmented multi-day continuity audit dataset.

    Processing steps:
        1. Read gold_vehicle_multi_day_continuity.
        2. Compute global reference statistics for fragmentation-related metrics.
        3. Compute relative fragmentation thresholds.
        4. Derive observed streak ratio relative to total span.
        5. Flag vehicles with fragmented multi-day continuity.
        6. Keep only rows that match at least one fragmentation condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Audit DataFrame containing vehicles with fragmented multi-day
            continuity behavior.
    """
    stats_df = source_df.agg(
        F.avg("missing_dates_within_span_count").alias(
            "avg_missing_dates_within_span_count"
        ),
        F.stddev_pop("missing_dates_within_span_count").alias(
            "stddev_missing_dates_within_span_count"
        ),
        F.min("missing_dates_within_span_count").alias(
            "min_missing_dates_within_span_count"
        ),
        F.max("missing_dates_within_span_count").alias(
            "max_missing_dates_within_span_count"
        ),
        F.avg("date_presence_ratio").alias("avg_date_presence_ratio"),
        F.stddev_pop("date_presence_ratio").alias("stddev_date_presence_ratio"),
        F.min("date_presence_ratio").alias("min_date_presence_ratio"),
        F.max("date_presence_ratio").alias("max_date_presence_ratio"),
        F.avg("max_consecutive_observed_dates").alias(
            "avg_max_consecutive_observed_dates"
        ),
        F.stddev_pop("max_consecutive_observed_dates").alias(
            "stddev_max_consecutive_observed_dates"
        ),
        F.min("max_consecutive_observed_dates").alias(
            "min_max_consecutive_observed_dates"
        ),
        F.max("max_consecutive_observed_dates").alias(
            "max_max_consecutive_observed_dates"
        ),
    )

    audit_df = (
        source_df.withColumn(
            "observed_streak_ratio",
            F.when(
                F.col("observed_date_span_days").isNotNull()
                & (F.col("observed_date_span_days") > 0),
                (
                    F.col("max_consecutive_observed_dates").cast("double")
                    / F.col("observed_date_span_days").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .crossJoin(stats_df)
        .withColumn(
            "relative_high_missing_dates_threshold",
            F.col("avg_missing_dates_within_span_count")
            + (
                F.coalesce(
                    F.col("stddev_missing_dates_within_span_count"), F.lit(0.0)
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "relative_low_date_presence_ratio_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_date_presence_ratio")
                - (
                    F.coalesce(F.col("stddev_date_presence_ratio"), F.lit(0.0))
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_high_missing_dates_within_span_count",
            F.col("missing_dates_within_span_count") >= F.lit(MISSING_DATES_THRESHOLD),
        )
        .withColumn(
            "has_relative_high_missing_dates_within_span_count",
            F.col("missing_dates_within_span_count")
            > F.col("relative_high_missing_dates_threshold"),
        )
        .withColumn(
            "has_very_low_date_presence_ratio",
            F.col("date_presence_ratio").isNotNull()
            & (F.col("date_presence_ratio") <= F.lit(DATE_PRESENCE_RATIO_THRESHOLD)),
        )
        .withColumn(
            "has_relative_low_date_presence_ratio",
            F.col("date_presence_ratio").isNotNull()
            & (
                F.col("date_presence_ratio")
                < F.col("relative_low_date_presence_ratio_threshold")
            ),
        )
        .withColumn(
            "has_short_max_consecutive_observed_streak_relative_to_span",
            F.col("observed_streak_ratio").isNotNull()
            & (F.col("observed_streak_ratio") <= F.lit(OBSERVED_STREAK_RATIO_THRESHOLD)),
        )
        .withColumn(
            "has_fragmented_multi_day_continuity",
            F.col("has_very_high_missing_dates_within_span_count")
            | F.col("has_relative_high_missing_dates_within_span_count")
            | F.col("has_very_low_date_presence_ratio")
            | F.col("has_relative_low_date_presence_ratio")
            | F.col("has_short_max_consecutive_observed_streak_relative_to_span"),
        )
        .where(F.col("has_fragmented_multi_day_continuity") == True)
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
            "observed_streak_ratio",
            "avg_missing_dates_within_span_count",
            "stddev_missing_dates_within_span_count",
            "min_missing_dates_within_span_count",
            "max_missing_dates_within_span_count",
            "avg_date_presence_ratio",
            "stddev_date_presence_ratio",
            "min_date_presence_ratio",
            "max_date_presence_ratio",
            "avg_max_consecutive_observed_dates",
            "stddev_max_consecutive_observed_dates",
            "min_max_consecutive_observed_dates",
            "max_max_consecutive_observed_dates",
            "relative_high_missing_dates_threshold",
            "relative_low_date_presence_ratio_threshold",
            "has_very_high_missing_dates_within_span_count",
            "has_relative_high_missing_dates_within_span_count",
            "has_very_low_date_presence_ratio",
            "has_relative_low_date_presence_ratio",
            "has_short_max_consecutive_observed_streak_relative_to_span",
            "has_fragmented_multi_day_continuity",
            "record_created_at",
        )
    )

    return audit_df
