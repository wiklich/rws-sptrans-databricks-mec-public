"""
Gold observed intraday hourly activity pipeline.

Purpose:
    Build the canonical intraday observed activity layer used by the
    Observed Temporal Activity Analysis component.

Business role:
    This dataset represents a business-ready hourly view of observed
    operational activity derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_observed_intraday_hourly_activity

Declared grain:
    - one row per analysis_date and analysis_hour_local

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per analysis_date and analysis_hour_local is allowed
    - distinct observed lines should use line_sign
    - the table must be interpreted only within the actually captured
      observation window
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_observed.schemas.schema_gold_observed_intraday_hourly_activity import (
    get_gold_observed_intraday_hourly_activity_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_observed_intraday_hourly_activity"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical intraday observed activity table derived from "
        "silver_sptrans_positions. Contains one row per analysis_date and "
        "analysis_hour_local with observed position volume, distinct vehicles, "
        "distinct lines, minute-level support, and local timestamp bounds."
    ),
    schema=get_gold_observed_intraday_hourly_activity_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_analysis_hour_local",
    "analysis_hour_local IS NOT NULL",
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
    "valid_distinct_minutes_with_data",
    "distinct_minutes_with_data IS NOT NULL",
)
def gold_observed_intraday_hourly_activity() -> DataFrame:
    """
    Materialize the canonical Gold observed intraday hourly activity table.

    Returns:
        DataFrame:
            Hourly observed activity output derived from Silver SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_observed_intraday_hourly_activity(source_df)


def _build_gold_observed_intraday_hourly_activity(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold observed intraday hourly activity dataset.

    Processing steps:
        1. Read the Silver SPTrans positions table.
        2. Preserve or derive the local analysis date and local analysis hour.
        3. Keep only records with valid temporal fields and valid vehicle identifiers.
        4. Aggregate records by analysis_date and analysis_hour_local.
        5. Derive observed position volume, distinct vehicles, distinct lines,
           minute-level support, and local timestamp bounds.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical intraday observed activity output with one row per
            analysis_date and analysis_hour_local.
    """
    prepared_df = (
        source_df.select(
            F.col("captured_date_local").cast("date").alias("analysis_date"),
            F.col("captured_hour_local").cast("int").alias("analysis_hour_local"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("line_sign").cast("string").alias("line_sign"),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("analysis_hour_local").isNotNull())
        .where(F.col("captured_at_local").isNotNull())
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
    )

    aggregated_df = (
        prepared_df.groupBy("analysis_date", "analysis_hour_local")
        .agg(
            F.count(F.lit(1)).cast("long").alias("observed_position_records_count"),
            F.countDistinct("vehicle_prefix").cast("long").alias(
                "distinct_observed_vehicles_count"
            ),
            F.countDistinct(
                F.when(
                    F.col("line_sign").isNotNull()
                    & (F.trim(F.col("line_sign")) != ""),
                    F.col("line_sign"),
                )
            ).cast("long").alias("distinct_observed_lines_count"),
            F.countDistinct(
                F.date_trunc("minute", F.col("captured_at_local"))
            ).cast("long").alias("distinct_minutes_with_data"),
            F.min("captured_at_local").alias("min_captured_at_local"),
            F.max("captured_at_local").alias("max_captured_at_local"),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "analysis_date",
        "analysis_hour_local",
        "observed_position_records_count",
        "distinct_observed_vehicles_count",
        "distinct_observed_lines_count",
        "distinct_minutes_with_data",
        "min_captured_at_local",
        "max_captured_at_local",
        "record_created_at",
    )
