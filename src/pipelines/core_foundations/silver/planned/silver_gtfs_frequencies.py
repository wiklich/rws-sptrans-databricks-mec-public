"""
Silver GTFS frequencies pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS frequencies
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Keep start_time and end_time as GTFS time strings
    - Convert start_time and end_time to total seconds
    - Cast headway_secs to integer

Main validations:
    - trip_id must be present
    - start_time must be present and match GTFS time format
    - end_time must be present and match GTFS time format
    - headway_secs must be present and must be greater than 0
    - end_time_seconds must be greater than start_time_seconds
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_frequencies import (
    get_silver_gtfs_frequencies_schema,
)
from utils.string_utils import with_trimmed_nullable_strings
from utils.time_utils import (
    with_time_to_seconds_columns,
    with_seconds_to_minutes_columns,
)

from utils.constants_utils import GTFS_TIME_REGEX


@dp.table(
    name="silver.silver_gtfs_frequencies",
    comment="Headway (time between trips) for headway-based service or a compressed representation of fixed-schedule service.",
    schema=get_silver_gtfs_frequencies_schema(),
)
@dp.expect_or_drop("valid_trip_id", "trip_id IS NOT NULL")
@dp.expect_or_drop("valid_start_time", f"start_time IS NOT NULL AND start_time RLIKE '{GTFS_TIME_REGEX}'")
@dp.expect_or_drop("valid_end_time", f"end_time IS NOT NULL AND end_time RLIKE '{GTFS_TIME_REGEX}'")
@dp.expect_or_drop("valid_headway_secs", "headway_secs IS NOT NULL AND headway_secs > 0")
@dp.expect_or_drop(
    "valid_time_range",
    "start_time_seconds IS NOT NULL AND end_time_seconds IS NOT NULL AND end_time_seconds > start_time_seconds",
)
def silver_gtfs_frequencies():
    """
    Create the Silver GTFS frequencies table from the Bronze GTFS frequencies table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS
            frequencies records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS frequencies streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Convert GTFS time strings to total seconds.
        4. Cast headway_secs to integer.
        5. Cast metadata columns to their final analytical types.
        6. Select the final Silver output columns.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_frequencies")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "trip_id",
                    "start_time",
                    "end_time",
                    "headway_secs",
                ],
            )
        )
        .transform(
            lambda dataframe: with_time_to_seconds_columns(
                dataframe,
                {
                    "start_time": "start_time_seconds",
                    "end_time": "end_time_seconds",
                },
            )
        )
        .withColumn(
            "service_window_seconds",
            F.when(
                F.col("start_time_seconds").isNotNull() & F.col("end_time_seconds").isNotNull(),
                F.col("end_time_seconds") - F.col("start_time_seconds"),
            ).otherwise(F.lit(None).cast("int"))
        )
        .transform(
            lambda dataframe: with_seconds_to_minutes_columns(
                dataframe,
                {
                    "headway_secs": "headway_minutes",
                    "start_time_seconds": "start_time_minutes",
                    "end_time_seconds": "end_time_minutes",
                    "service_window_seconds": "service_window_minutes",
                },
            )
        )
        .withColumn("headway_secs", F.col("headway_secs").cast("int"))
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))
        .select(
            "trip_id",
            "start_time",
            "start_time_seconds",
            "start_time_minutes",
            "end_time",
            "end_time_seconds",
            "end_time_minutes",
            "service_window_seconds",
            "service_window_minutes",
            "headway_secs",
            "headway_minutes",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
    )

    return cleaned_df