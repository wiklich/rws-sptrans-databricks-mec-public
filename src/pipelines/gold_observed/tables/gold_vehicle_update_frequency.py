"""
Gold vehicle update frequency pipeline.

Purpose:
    Build the canonical vehicle-level update frequency layer used by the
    Observed Update Frequency and Continuity Analysis component.

Business role:
    This dataset represents a business-ready view of update frequency behavior
    per vehicle, derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_vehicle_update_frequency

Declared grain:
    - one row per vehicle_prefix

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per vehicle_prefix is allowed
    - update intervals must be calculated only within the same vehicle
    - negative or zero intervals must not be treated as valid update intervals
    - the table must be interpreted only within the captured observation window
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.gold_observed.schemas.schema_gold_vehicle_update_frequency import (
    get_gold_vehicle_update_frequency_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_update_frequency"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical vehicle-level update frequency table derived from "
        "silver_sptrans_positions. Contains one row per vehicle_prefix with "
        "update counts, measurable interval counts, interval statistics, "
        "timestamp bounds, and observed span."
    ),
    schema=get_gold_vehicle_update_frequency_schema(),
)
@dp.expect_or_drop(
    "valid_vehicle_prefix",
    "vehicle_prefix IS NOT NULL AND TRIM(vehicle_prefix) <> ''",
)
@dp.expect_or_drop(
    "valid_updates_count",
    "updates_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_measured_intervals_count",
    "measured_intervals_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observed_span_seconds",
    "observed_span_seconds IS NOT NULL AND observed_span_seconds >= 0",
)
def gold_vehicle_update_frequency() -> DataFrame:
    """
    Materialize the canonical Gold vehicle update frequency table.

    Returns:
        DataFrame:
            Vehicle-level update frequency output derived from Silver SPTrans
            positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_vehicle_update_frequency(source_df)


def _prepare_vehicle_position_stream(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for vehicle-level interval
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
            Prepared vehicle/timestamp stream ready for interval calculation.
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


def _build_vehicle_intervals(prepared_df: DataFrame) -> DataFrame:
    """
    Build consecutive timestamp intervals within each vehicle stream.

    Processing steps:
        1. Order observations by captured_at_local within each vehicle.
        2. Get the previous observed timestamp.
        3. Calculate the raw interval in seconds.
        4. Keep only valid positive intervals.

    Args:
        prepared_df:
            Prepared vehicle/timestamp stream.

    Returns:
        DataFrame:
            Vehicle stream enriched with previous timestamp and valid interval.
    """
    window_spec = Window.partitionBy("vehicle_prefix").orderBy("captured_at_local")

    return (
        prepared_df.withColumn(
            "previous_captured_at_local",
            F.lag("captured_at_local").over(window_spec),
        )
        .withColumn(
            "raw_interval_seconds",
            F.when(
                F.col("previous_captured_at_local").isNotNull(),
                (
                    F.unix_timestamp("captured_at_local")
                    - F.unix_timestamp("previous_captured_at_local")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "valid_interval_seconds",
            F.when(
                F.col("raw_interval_seconds") > 0,
                F.col("raw_interval_seconds"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def _build_gold_vehicle_update_frequency(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold vehicle update frequency dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions stream.
        2. Calculate consecutive valid intervals within each vehicle.
        3. Aggregate vehicle-level update frequency metrics.
        4. Preserve first and last observed local timestamps.
        5. Derive observed span in seconds.
        6. Materialize the final Gold output.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical vehicle-level update frequency output with one row per
            vehicle_prefix.
    """
    prepared_df = _prepare_vehicle_position_stream(source_df)
    intervals_df = _build_vehicle_intervals(prepared_df)

    aggregated_df = (
        intervals_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).cast("long").alias("updates_count"),
            F.count("valid_interval_seconds").cast("long").alias(
                "measured_intervals_count"
            ),
            F.avg("valid_interval_seconds").cast("double").alias(
                "avg_update_interval_seconds"
            ),
            F.min("valid_interval_seconds").cast("double").alias(
                "min_update_interval_seconds"
            ),
            F.max("valid_interval_seconds").cast("double").alias(
                "max_update_interval_seconds"
            ),
            F.stddev_pop("valid_interval_seconds").cast("double").alias(
                "stddev_update_interval_seconds"
            ),
            F.min("captured_at_local").alias("first_captured_at_local"),
            F.max("captured_at_local").alias("last_captured_at_local"),
        )
        .withColumn(
            "observed_span_seconds",
            (
                F.unix_timestamp("last_captured_at_local")
                - F.unix_timestamp("first_captured_at_local")
            ).cast("long"),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
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
        "record_created_at",
    )
