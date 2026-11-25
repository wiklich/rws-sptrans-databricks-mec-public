"""
Gold vehicle observation continuity pipeline.

Purpose:
    Build the canonical vehicle-level observation continuity layer used by the
    Observed Update Frequency and Continuity Analysis component.

Business role:
    This dataset represents a business-ready view of observation continuity
    per vehicle, derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_vehicle_observation_continuity

Declared grain:
    - one row per vehicle_prefix

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per vehicle_prefix is allowed
    - continuity metrics must be calculated only within the same vehicle
    - negative or zero intervals must not be treated as valid measurable gaps
    - large-gap logic must use an explicitly documented threshold
    - the table must be interpreted only within the captured observation window
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.gold_observed.schemas.schema_gold_vehicle_observation_continuity import (
    get_gold_vehicle_observation_continuity_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observation_continuity"

LARGE_GAP_THRESHOLD_SECONDS = 300.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical vehicle-level observation continuity table derived from "
        "silver_sptrans_positions. Contains one row per vehicle_prefix with "
        "observation counts, measurable gap counts, timestamp bounds, "
        "observed span, minute/hour support, large-gap counts, and "
        "continuity support ratio."
    ),
    schema=get_gold_vehicle_observation_continuity_schema(),
)
@dp.expect_or_drop(
    "valid_vehicle_prefix",
    "vehicle_prefix IS NOT NULL AND TRIM(vehicle_prefix) <> ''",
)
@dp.expect_or_drop(
    "valid_observations_count",
    "observations_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_measured_gaps_count",
    "measured_gaps_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_first_captured_at_local",
    "first_captured_at_local IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_last_captured_at_local",
    "last_captured_at_local IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observed_span_seconds",
    "observed_span_seconds IS NOT NULL AND observed_span_seconds >= 0",
)
@dp.expect_or_drop(
    "valid_distinct_minutes_with_data",
    "distinct_minutes_with_data IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_hours_with_data",
    "distinct_hours_with_data IS NOT NULL",
)
def gold_vehicle_observation_continuity() -> DataFrame:
    """
    Materialize the canonical Gold vehicle observation continuity table.

    Returns:
        DataFrame:
            Vehicle-level observation continuity output derived from Silver
            SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_vehicle_observation_continuity(source_df)


def _prepare_vehicle_position_stream(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for vehicle-level continuity
    calculation.

    Processing steps:
        1. Keep only vehicle_prefix and captured_at_local.
        2. Cast fields to the expected analytical types.
        3. Filter rows with invalid vehicle identifiers or timestamps.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Prepared vehicle/timestamp stream ready for continuity calculation.
    """
    return (
        source_df.select(
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
        )
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
        .where(F.col("captured_at_local").isNotNull())
    )


def _build_vehicle_gaps(prepared_df: DataFrame) -> DataFrame:
    """
    Build consecutive timestamp gaps within each vehicle stream.

    Processing steps:
        1. Order observations by captured_at_local within each vehicle.
        2. Get the previous observed timestamp.
        3. Calculate the raw gap in seconds.
        4. Keep only valid positive gaps.

    Args:
        prepared_df:
            Prepared vehicle/timestamp stream.

    Returns:
        DataFrame:
            Vehicle stream enriched with previous timestamp and valid gap.
    """
    window_spec = Window.partitionBy("vehicle_prefix").orderBy("captured_at_local")

    return (
        prepared_df.withColumn(
            "previous_captured_at_local",
            F.lag("captured_at_local").over(window_spec),
        )
        .withColumn(
            "raw_gap_seconds",
            F.when(
                F.col("previous_captured_at_local").isNotNull(),
                (
                    F.unix_timestamp("captured_at_local")
                    - F.unix_timestamp("previous_captured_at_local")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "valid_gap_seconds",
            F.when(
                F.col("raw_gap_seconds") > 0,
                F.col("raw_gap_seconds"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def _build_gold_vehicle_observation_continuity(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold vehicle observation continuity dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions stream.
        2. Calculate consecutive valid gaps within each vehicle.
        3. Aggregate vehicle-level continuity metrics.
        4. Preserve first and last observed local timestamps.
        5. Derive observed span, minute/hour support, large-gap counts, and
           continuity support ratio.
        6. Materialize the final Gold output.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical vehicle-level observation continuity output with one row
            per vehicle_prefix.
    """
    prepared_df = _prepare_vehicle_position_stream(source_df)
    gaps_df = _build_vehicle_gaps(prepared_df)

    aggregated_df = (
        gaps_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).cast("long").alias("observations_count"),
            F.count("valid_gap_seconds").cast("long").alias("measured_gaps_count"),
            F.min("captured_at_local").alias("first_captured_at_local"),
            F.max("captured_at_local").alias("last_captured_at_local"),
            F.countDistinct(
                F.date_trunc("minute", F.col("captured_at_local"))
            ).cast("long").alias("distinct_minutes_with_data"),
            F.countDistinct(
                F.date_trunc("hour", F.col("captured_at_local"))
            ).cast("long").alias("distinct_hours_with_data"),
            F.avg("valid_gap_seconds").cast("double").alias("avg_gap_seconds"),
            F.max("valid_gap_seconds").cast("double").alias("max_gap_seconds"),
            F.sum(
                F.when(
                    F.col("valid_gap_seconds") > F.lit(LARGE_GAP_THRESHOLD_SECONDS),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).cast("long").alias("large_gaps_count"),
        )
        .withColumn(
            "observed_span_seconds",
            (
                F.unix_timestamp("last_captured_at_local")
                - F.unix_timestamp("first_captured_at_local")
            ).cast("long"),
        )
        .withColumn(
            "continuity_support_ratio",
            F.when(
                F.col("observed_span_seconds").isNotNull()
                & (F.col("observed_span_seconds") >= 0),
                (
                    F.col("distinct_minutes_with_data").cast("double")
                    / ((F.col("observed_span_seconds").cast("double") / F.lit(60.0)) + F.lit(1.0))
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
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
        "record_created_at",
    )
