"""
Silver GTFS stop_times pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS stop_times
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Cast stop_sequence to integer

Main validations:
    - trip_id must be present
    - arrival_time must be present and match GTFS time format
    - departure_time must be present and match GTFS time format
    - stop_id must be present
    - stop_sequence must be present and must be a non-negative integer
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_stop_times import (
    get_silver_gtfs_stop_times_schema,
)

from utils.string_utils import with_trimmed_nullable_strings
from utils.constants_utils import GTFS_TIME_REGEX
from utils.time_utils import with_time_to_seconds_columns


@dp.table(
    name="silver.silver_gtfs_stop_times",
    comment="Times that a vehicle arrives at and departs from stops for each trip.",
    schema=get_silver_gtfs_stop_times_schema(),
)
@dp.expect_or_drop("valid_trip_id", "trip_id IS NOT NULL")
@dp.expect_or_drop("valid_arrival_time", f"arrival_time IS NOT NULL AND arrival_time RLIKE '{GTFS_TIME_REGEX}'")
@dp.expect_or_drop("valid_departure_time", f"departure_time IS NOT NULL AND departure_time RLIKE '{GTFS_TIME_REGEX}'")
@dp.expect(
    "valid_arrival_departure_order",
    "arrival_time_seconds IS NOT NULL AND departure_time_seconds IS NOT NULL AND arrival_time_seconds <= departure_time_seconds"
)
@dp.expect_or_drop("valid_stop_id", "stop_id IS NOT NULL")
@dp.expect_or_drop("valid_stop_sequence", "stop_sequence IS NOT NULL AND stop_sequence >= 0")
def silver_gtfs_stop_times():
    """
    Builds the silver-layer GTFS stop_times table from the bronze raw ingestion.

    This pipeline:
      - Cleans and standardizes textual fields (trim + empty → null)
      - Converts stop_sequence to integer
      - Adds arrival/departure time in seconds (handling >24:00:00 GTFS times)
      - Computes a data quality flag for arrival ≤ departure
      - Applies strict validation rules (dropping invalid records)

    Returns:
        pyspark.sql.DataFrame:
            Streaming DataFrame ready to be written to the silver table
            with cleaned, typed and validated GTFS stop_times records.

    Main transformations:
        - Trim whitespace and convert empty strings to NULL on key fields
        - Cast stop_sequence to integer
        - Convert GTFS time strings (HH:MM:SS) to seconds since midnight
        - Create is_valid_time_order flag (true if arrival ≤ departure)
        - Select only the final silver schema columns

    Data quality enforcements (via @dp.expect* decorators):
        - trip_id must be present
        - arrival_time and departure_time must be present and match GTFS time pattern
        - stop_id must be present
        - stop_sequence must be present and ≥ 0
        - arrival_time_seconds ≤ departure_time_seconds (soft validation – warning only)

    Notes:
        - Records failing mandatory validations (except time order) are dropped
        - Time order violations are flagged but not dropped (useful for analysis)
        - Designed as a streaming pipeline (readStream → writeStream)
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_stop_times")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "trip_id",
                    "arrival_time",
                    "departure_time",
                    "stop_id",
                    "stop_sequence",
                ],
            )
        )
        .transform(
            lambda dataframe: with_time_to_seconds_columns(
                dataframe,
                {
                    "arrival_time": "arrival_time_seconds",
                    "departure_time": "departure_time_seconds",
                },
            )
        )
        .withColumn(
            "is_valid_time_order",
            F.when(
                (F.col("arrival_time_seconds").isNotNull())
                & (F.col("departure_time_seconds").isNotNull())
                & (F.col("arrival_time_seconds") <= F.col("departure_time_seconds")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
        .select(
            "trip_id",
            "arrival_time",
            "departure_time",
            "arrival_time_seconds",
            "departure_time_seconds",
            "is_valid_time_order",
            "stop_id",
            "stop_sequence",
            "gtfs_load_date",
        )
    )

    return cleaned_df
