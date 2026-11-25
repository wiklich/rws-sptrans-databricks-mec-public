"""
Gold vehicle observation continuity fragmented observation audit pipeline.

Purpose:
    Identify vehicles with weak continuity support or excessive large gaps in
    gold_vehicle_observation_continuity.

Audit focus:
    - very high large_gaps_count
    - very low continuity_support_ratio
    - large max_gap_seconds relative to observed span

Why this audit matters:
    This audit is useful because it highlights vehicles whose observed
    presence is fragmented rather than continuous.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observation_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observation_continuity_fragmented_observation_audit"

LARGE_GAPS_COUNT_THRESHOLD = 5
CONTINUITY_SUPPORT_RATIO_THRESHOLD = 0.25
MAX_GAP_SPAN_RATIO_THRESHOLD = 0.50
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying vehicles with fragmented observation behavior "
        "in gold_vehicle_observation_continuity, including high large-gap "
        "counts, low continuity support ratios, and large maximum gaps "
        "relative to observed span."
    ),
)
def gold_vehicle_observation_continuity_fragmented_observation_audit() -> DataFrame:
    """
    Materialize the fragmented observation audit for
    gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Audit rows for vehicles with fragmented observation behavior.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observation_continuity_fragmented_observation_audit(
        source_df
    )


def _build_gold_vehicle_observation_continuity_fragmented_observation_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the fragmented observation audit dataset.

    Processing steps:
        1. Read gold_vehicle_observation_continuity.
        2. Compute global reference statistics for fragmentation-related metrics.
        3. Compute absolute and relative fragmentation thresholds.
        4. Derive max-gap-to-span ratio.
        5. Flag vehicles with fragmented observation behavior.
        6. Keep only rows that match at least one fragmentation condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Audit DataFrame containing vehicles with fragmented observation behavior.
    """
    stats_df = source_df.agg(
        F.avg("large_gaps_count").alias("avg_large_gaps_count"),
        F.stddev_pop("large_gaps_count").alias("stddev_large_gaps_count"),
        F.min("large_gaps_count").alias("min_large_gaps_count"),
        F.max("large_gaps_count").alias("max_large_gaps_count"),
        F.avg("continuity_support_ratio").alias("avg_continuity_support_ratio"),
        F.stddev_pop("continuity_support_ratio").alias(
            "stddev_continuity_support_ratio"
        ),
        F.min("continuity_support_ratio").alias("min_continuity_support_ratio"),
        F.max("continuity_support_ratio").alias("max_continuity_support_ratio"),
    )

    audit_df = (
        source_df.withColumn(
            "max_gap_span_ratio",
            F.when(
                F.col("observed_span_seconds").isNotNull()
                & (F.col("observed_span_seconds") > 0)
                & F.col("max_gap_seconds").isNotNull(),
                F.col("max_gap_seconds") / F.col("observed_span_seconds"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .crossJoin(stats_df)
        .withColumn(
            "relative_high_large_gaps_threshold",
            F.col("avg_large_gaps_count")
            + (
                F.coalesce(F.col("stddev_large_gaps_count"), F.lit(0.0))
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "relative_low_continuity_support_ratio_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_continuity_support_ratio")
                - (
                    F.coalesce(
                        F.col("stddev_continuity_support_ratio"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_very_high_large_gaps_count",
            F.col("large_gaps_count") >= F.lit(LARGE_GAPS_COUNT_THRESHOLD),
        )
        .withColumn(
            "has_relative_high_large_gaps_count",
            F.col("large_gaps_count") > F.col("relative_high_large_gaps_threshold"),
        )
        .withColumn(
            "has_very_low_continuity_support_ratio",
            F.col("continuity_support_ratio").isNotNull()
            & (
                F.col("continuity_support_ratio")
                <= F.lit(CONTINUITY_SUPPORT_RATIO_THRESHOLD)
            ),
        )
        .withColumn(
            "has_relative_low_continuity_support_ratio",
            F.col("continuity_support_ratio").isNotNull()
            & (
                F.col("continuity_support_ratio")
                < F.col("relative_low_continuity_support_ratio_threshold")
            ),
        )
        .withColumn(
            "has_large_max_gap_relative_to_span",
            F.col("max_gap_span_ratio").isNotNull()
            & (F.col("max_gap_span_ratio") >= F.lit(MAX_GAP_SPAN_RATIO_THRESHOLD)),
        )
        .withColumn(
            "has_fragmented_observation",
            F.col("has_very_high_large_gaps_count")
            | F.col("has_relative_high_large_gaps_count")
            | F.col("has_very_low_continuity_support_ratio")
            | F.col("has_relative_low_continuity_support_ratio")
            | F.col("has_large_max_gap_relative_to_span"),
        )
        .where(F.col("has_fragmented_observation") == True)
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
            "max_gap_span_ratio",
            "avg_large_gaps_count",
            "stddev_large_gaps_count",
            "min_large_gaps_count",
            "max_large_gaps_count",
            "avg_continuity_support_ratio",
            "stddev_continuity_support_ratio",
            "min_continuity_support_ratio",
            "max_continuity_support_ratio",
            "relative_high_large_gaps_threshold",
            "relative_low_continuity_support_ratio_threshold",
            "has_very_high_large_gaps_count",
            "has_relative_high_large_gaps_count",
            "has_very_low_continuity_support_ratio",
            "has_relative_low_continuity_support_ratio",
            "has_large_max_gap_relative_to_span",
            "has_fragmented_observation",
            "record_created_at",
        )
    )

    return audit_df
