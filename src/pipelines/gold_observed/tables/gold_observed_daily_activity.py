"""
Gold observed daily activity pipeline.

Purpose:
    Build the canonical daily observed activity layer used by the
    Observed Temporal Activity Analysis component.

Business role:
    This dataset represents a business-ready daily view of observed
    operational activity derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_observed_daily_activity

Declared grain:
    - one row per analysis_date

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per analysis_date is allowed
    - distinct observed lines should use line_sign
    - the table must be interpreted according to the real captured
      observation coverage of each day
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_observed.schemas.schema_gold_observed_daily_activity import (
    get_gold_observed_daily_activity_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_observed_daily_activity"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical daily observed activity table derived from "
        "silver_sptrans_positions. Contains one row per analysis_date with "
        "observed position volume, distinct vehicles, distinct lines, "
        "hour-level support, minute-level support, and local timestamp bounds."
    ),
    schema=get_gold_observed_daily_activity_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observed_position_records_count",
    "observed_position_records_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_observed_vehicles_count",
    "distinct_observed_vehicles_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_observed_lines_count",
    "distinct_observed_lines_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_observed_hours_count",
    "distinct_observed_hours_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_observed_minutes_count",
    "distinct_observed_minutes_count IS NOT NULL",
)
def gold_observed_daily_activity() -> DataFrame:
    """
    Materialize the canonical Gold observed daily activity table.

    Returns:
        DataFrame:
            Daily observed activity output derived from Silver SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_observed_daily_activity(source_df)


def _build_gold_observed_daily_activity(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold observed daily activity dataset.

    Processing steps:
        1. Read the Silver SPTrans positions table.
        2. Preserve or derive the local analysis date.
        3. Preserve or derive the local analysis hour.
        4. Keep only records with valid temporal fields.
        5. Aggregate records by analysis_date.
        6. Derive observed position volume, distinct vehicles, distinct lines,
           hour-level support, minute-level support, and local timestamp bounds.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical daily observed activity output with one row per
            analysis_date.
    """
    prepared_df = (
        source_df.select(
            F.coalesce(
                F.col("captured_date_local").cast("date"),
                F.to_date(F.col("captured_at_local")),
            ).alias("analysis_date"),
            F.coalesce(
                F.col("captured_hour_local").cast("int"),
                F.hour(F.col("captured_at_local")),
            ).alias("captured_hour_local"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("line_sign").cast("string").alias("line_sign"),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("captured_at_local").isNotNull())
    )

    aggregated_df = (
        prepared_df.groupBy("analysis_date")
        .agg(
            F.count(F.lit(1)).cast("long").alias("observed_position_records_count"),
            F.countDistinct(
                F.when(
                    F.col("vehicle_prefix").isNotNull()
                    & (F.trim(F.col("vehicle_prefix")) != ""),
                    F.col("vehicle_prefix"),
                )
            ).cast("long").alias("distinct_observed_vehicles_count"),
            F.countDistinct(
                F.when(
                    F.col("line_sign").isNotNull()
                    & (F.trim(F.col("line_sign")) != ""),
                    F.col("line_sign"),
                )
            ).cast("long").alias("distinct_observed_lines_count"),
            F.countDistinct("captured_hour_local").cast("long").alias(
                "distinct_observed_hours_count"
            ),
            F.countDistinct(
                F.date_trunc("minute", F.col("captured_at_local"))
            ).cast("long").alias("distinct_observed_minutes_count"),
            F.min("captured_at_local").alias("min_captured_at_local"),
            F.max("captured_at_local").alias("max_captured_at_local"),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "analysis_date",
        "observed_position_records_count",
        "distinct_observed_vehicles_count",
        "distinct_observed_lines_count",
        "distinct_observed_hours_count",
        "distinct_observed_minutes_count",
        "min_captured_at_local",
        "max_captured_at_local",
        "record_created_at",
    )
