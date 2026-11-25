"""
Gold recurring update patterns pipeline.

Purpose:
    Build the canonical recurring-pattern layer used by the
    Observed Update Frequency and Continuity Analysis component.

Business role:
    This dataset represents a business-ready view of recurring update behavior
    across comparable temporal buckets, derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_recurring_update_patterns

Declared grain:
    - one row per day_of_week and hour_of_day

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per day_of_week and hour_of_day is allowed
    - update intervals must be calculated only within the same vehicle
    - intervals must be based on ordered local timestamps
    - negative or zero intervals must not be treated as valid update intervals
    - recurring metrics must be interpreted only when sufficient support exists
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.gold_observed.schemas.schema_gold_recurring_update_patterns import (
    get_gold_recurring_update_patterns_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_recurring_update_patterns"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical recurring update patterns table derived from "
        "silver_sptrans_positions. Contains one row per day_of_week and "
        "hour_of_day with interval statistics, support metrics, and "
        "recurring update behavior indicators."
    ),
    schema=get_gold_recurring_update_patterns_schema(),
)
@dp.expect_or_drop(
    "valid_day_of_week",
    "day_of_week IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_hour_of_day",
    "hour_of_day IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observations_count",
    "observations_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_measured_intervals_count",
    "measured_intervals_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_vehicles_count",
    "distinct_vehicles_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_dates_count",
    "distinct_dates_count IS NOT NULL",
)
def gold_recurring_update_patterns() -> DataFrame:
    """
    Materialize the canonical Gold recurring update patterns table.

    Returns:
        DataFrame:
            Recurring-pattern output derived from Silver SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_recurring_update_patterns(source_df)


def _prepare_vehicle_position_stream(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for recurring interval analysis.

    Processing steps:
        1. Keep vehicle_prefix, local timestamp, and local observed date.
        2. Cast fields to expected analytical types.
        3. Filter rows with invalid vehicle identifiers, timestamps, or dates.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Prepared vehicle/timestamp/date stream ready for interval calculation.
    """
    return (
        source_df.select(
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.coalesce(
                F.col("captured_date_local").cast("date"),
                F.to_date(F.col("captured_at_local")),
            ).alias("captured_date_local"),
        )
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
        .where(F.col("captured_at_local").isNotNull())
        .where(F.col("captured_date_local").isNotNull())
    )


def _build_vehicle_intervals(prepared_df: DataFrame) -> DataFrame:
    """
    Build consecutive timestamp intervals within each vehicle stream.

    Processing steps:
        1. Order observations by captured_at_local within each vehicle.
        2. Get the previous observed timestamp.
        3. Calculate the raw interval in seconds.
        4. Keep only valid positive intervals.
        5. Derive recurring temporal buckets from the current local timestamp.

    Args:
        prepared_df:
            Prepared vehicle/timestamp/date stream.

    Returns:
        DataFrame:
            Vehicle stream enriched with valid intervals and recurring buckets.
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
        .where(F.col("valid_interval_seconds").isNotNull())
        .withColumn("day_of_week", F.dayofweek(F.col("captured_at_local")).cast("int"))
        .withColumn("hour_of_day", F.hour(F.col("captured_at_local")).cast("int"))
    )


def _build_gold_recurring_update_patterns(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold recurring update patterns dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions stream.
        2. Calculate valid consecutive intervals within each vehicle.
        3. Assign each valid interval to day_of_week and hour_of_day.
        4. Aggregate recurring-pattern metrics by the declared grain.
        5. Derive support ratios per vehicle and per date.
        6. Materialize the final Gold output.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical recurring-pattern output with one row per
            day_of_week and hour_of_day.
    """
    prepared_df = _prepare_vehicle_position_stream(source_df)
    intervals_df = _build_vehicle_intervals(prepared_df)

    aggregated_df = (
        intervals_df.groupBy("day_of_week", "hour_of_day")
        .agg(
            F.count(F.lit(1)).cast("long").alias("observations_count"),
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
            F.countDistinct("vehicle_prefix").cast("long").alias(
                "distinct_vehicles_count"
            ),
            F.countDistinct("captured_date_local").cast("long").alias(
                "distinct_dates_count"
            ),
        )
        .withColumn(
            "avg_intervals_per_vehicle",
            F.when(
                F.col("distinct_vehicles_count") > 0,
                (
                    F.col("measured_intervals_count").cast("double")
                    / F.col("distinct_vehicles_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "avg_intervals_per_date",
            F.when(
                F.col("distinct_dates_count") > 0,
                (
                    F.col("measured_intervals_count").cast("double")
                    / F.col("distinct_dates_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
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
        "record_created_at",
    )
