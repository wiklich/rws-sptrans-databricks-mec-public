"""
Gold vehicle observed spatial traces suspicious trace gap audit pipeline.

Purpose:
    Identify unusually large time gaps between consecutive observed points in
    gold_vehicle_observed_spatial_traces.

Audit focus:
    - very large time_since_previous_seconds
    - fragmented observed traces
    - abrupt breaks in the observed sequence

Why this audit matters:
    This audit is useful because unusually large gaps weaken trace continuity
    interpretation and may indicate fragmented observation support or abrupt
    breaks in the observed sequence.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observed_spatial_traces"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observed_spatial_traces_suspicious_gap_audit"

ABSOLUTE_LARGE_GAP_THRESHOLD_SECONDS = 300.0
FRAGMENTED_TRACE_LARGE_GAPS_COUNT_THRESHOLD = 2
FRAGMENTED_TRACE_LARGE_GAP_RATIO_THRESHOLD = 0.20
STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying suspicious time gaps in "
        "gold_vehicle_observed_spatial_traces, including very large gaps "
        "between consecutive observed points, fragmented observed traces, "
        "and abrupt breaks in the observed sequence."
    ),
)
def gold_vehicle_observed_spatial_traces_suspicious_gap_audit() -> DataFrame:
    """
    Materialize the suspicious trace gap audit for
    gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit rows for suspiciously large consecutive-point gaps.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observed_spatial_traces_suspicious_gap_audit(
        source_df
    )


def _build_gold_vehicle_observed_spatial_traces_suspicious_gap_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious trace gap audit dataset.

    Processing steps:
        1. Read gold_vehicle_observed_spatial_traces.
        2. Calculate time_since_previous_seconds for each trace row.
        3. Compute global reference statistics for measurable gaps.
        4. Compute trace-level continuity support indicators.
        5. Flag rows with suspiciously large gaps, fragmented traces, or
           abrupt breaks.
        6. Keep only rows that match at least one suspicious-gap condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit DataFrame containing suspicious trace-gap rows.
    """
    trace_partition_window = Window.partitionBy("analysis_date", "vehicle_prefix")
    trace_order_window = trace_partition_window.orderBy(F.col("trace_sequence").asc())

    base_df = (
        source_df.withColumn(
            "time_since_previous_seconds",
            F.when(
                F.col("previous_captured_at_local").isNotNull(),
                (
                    F.unix_timestamp("captured_at_local")
                    - F.unix_timestamp("previous_captured_at_local")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "has_measurable_gap",
            F.col("time_since_previous_seconds").isNotNull()
            & (F.col("time_since_previous_seconds") > 0),
        )
        .withColumn(
            "previous_trace_sequence",
            F.lag("trace_sequence").over(trace_order_window),
        )
    )

    gap_stats_df = base_df.where(F.col("has_measurable_gap") == True).agg(
        F.avg("time_since_previous_seconds").alias("avg_time_since_previous_seconds"),
        F.stddev_pop("time_since_previous_seconds").alias(
            "stddev_time_since_previous_seconds"
        ),
        F.min("time_since_previous_seconds").alias("min_time_since_previous_seconds"),
        F.max("time_since_previous_seconds").alias("max_time_since_previous_seconds"),
    )

    enriched_df = (
        base_df.crossJoin(gap_stats_df)
        .withColumn(
            "relative_large_gap_threshold_seconds",
            F.col("avg_time_since_previous_seconds")
            + (
                F.coalesce(
                    F.col("stddev_time_since_previous_seconds"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "is_absolute_large_gap",
            F.col("has_measurable_gap")
            & (
                F.col("time_since_previous_seconds")
                >= F.lit(ABSOLUTE_LARGE_GAP_THRESHOLD_SECONDS)
            ),
        )
        .withColumn(
            "is_relative_large_gap",
            F.col("has_measurable_gap")
            & (
                F.col("time_since_previous_seconds")
                > F.col("relative_large_gap_threshold_seconds")
            ),
        )
    )

    trace_support_df = (
        enriched_df.groupBy("analysis_date", "vehicle_prefix")
        .agg(
            F.count(F.lit(1)).cast("long").alias("trace_points_count"),
            F.sum(F.when(F.col("has_measurable_gap"), F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("measurable_gaps_count"),
            F.avg(
                F.when(F.col("has_measurable_gap"), F.col("time_since_previous_seconds"))
            ).cast("double").alias("avg_gap_seconds_in_trace"),
            F.max(
                F.when(F.col("has_measurable_gap"), F.col("time_since_previous_seconds"))
            ).cast("double").alias("max_gap_seconds_in_trace"),
            F.sum(
                F.when(
                    F.col("is_absolute_large_gap") | F.col("is_relative_large_gap"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("large_gaps_in_trace_count"),
        )
        .withColumn(
            "large_gap_ratio_in_trace",
            F.when(
                F.col("measurable_gaps_count") > 0,
                (
                    F.col("large_gaps_in_trace_count").cast("double")
                    / F.col("measurable_gaps_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "has_fragmented_observed_trace",
            (F.col("large_gaps_in_trace_count") >= F.lit(FRAGMENTED_TRACE_LARGE_GAPS_COUNT_THRESHOLD))
            | (
                F.col("large_gap_ratio_in_trace").isNotNull()
                & (
                    F.col("large_gap_ratio_in_trace")
                    >= F.lit(FRAGMENTED_TRACE_LARGE_GAP_RATIO_THRESHOLD)
                )
            ),
        )
    )

    audit_df = (
        enriched_df.join(
            trace_support_df,
            on=["analysis_date", "vehicle_prefix"],
            how="left",
        )
        .withColumn(
            "has_very_large_time_since_previous_seconds",
            F.col("is_absolute_large_gap") | F.col("is_relative_large_gap"),
        )
        .withColumn(
            "has_abrupt_break_in_observed_sequence",
            F.col("has_measurable_gap")
            & F.col("max_gap_seconds_in_trace").isNotNull()
            & (
                F.col("time_since_previous_seconds")
                == F.col("max_gap_seconds_in_trace")
            )
            & (
                F.col("is_absolute_large_gap") | F.col("is_relative_large_gap")
            ),
        )
        .withColumn(
            "has_suspicious_trace_gap",
            F.col("has_very_large_time_since_previous_seconds")
            | F.col("has_fragmented_observed_trace")
            | F.col("has_abrupt_break_in_observed_sequence"),
        )
        .where(F.col("has_suspicious_trace_gap") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "vehicle_prefix",
            "trace_sequence",
            "captured_at_local",
            "latitude",
            "longitude",
            "line_sign",
            "direction",
            "direction_description",
            "previous_captured_at_local",
            "previous_latitude",
            "previous_longitude",
            "time_since_previous_seconds",
            "avg_time_since_previous_seconds",
            "stddev_time_since_previous_seconds",
            "min_time_since_previous_seconds",
            "max_time_since_previous_seconds",
            "relative_large_gap_threshold_seconds",
            "trace_points_count",
            "measurable_gaps_count",
            "avg_gap_seconds_in_trace",
            "max_gap_seconds_in_trace",
            "large_gaps_in_trace_count",
            "large_gap_ratio_in_trace",
            "is_absolute_large_gap",
            "is_relative_large_gap",
            "has_very_large_time_since_previous_seconds",
            "has_fragmented_observed_trace",
            "has_abrupt_break_in_observed_sequence",
            "has_suspicious_trace_gap",
            "record_created_at",
        )
    )

    return audit_df
