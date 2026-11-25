"""
Silver GTFS calendar pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS calendar
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Cast weekday availability flags to integer
    - Convert weekday availability flags to boolean helper columns
    - Parse GTFS dates from yyyyMMdd format into date columns
    - Create service date interval helper columns

Main validations:
    - service_id must be present
    - weekday flags must be present and must be either 0 or 1
    - start_date must be present and valid
    - end_date must be present and valid
    - end_date must be greater than or equal to start_date
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_calendar import (
    get_silver_gtfs_calendar_schema
)

from utils.string_utils import with_trimmed_nullable_strings
from utils.boolean_utils import with_nullable_boolean_from_int_columns



@dp.table(
    name="silver.silver_gtfs_calendar",
    comment=(
        "Service dates specified using a weekly schedule with start and end dates. "
        "Conditionally required unless all service dates are defined in calendar_dates.txt."
    ),
    schema=get_silver_gtfs_calendar_schema(),
)
@dp.expect_or_drop("valid_service_id", "service_id IS NOT NULL")
@dp.expect_or_drop("valid_monday_active", "monday_active IS NOT NULL")
@dp.expect_or_drop("valid_tuesday_active", "tuesday_active IS NOT NULL")
@dp.expect_or_drop("valid_wednesday_active", "wednesday_active IS NOT NULL")
@dp.expect_or_drop("valid_thursday_active", "thursday_active IS NOT NULL")
@dp.expect_or_drop("valid_friday_active", "friday_active IS NOT NULL")
@dp.expect_or_drop("valid_saturday_active", "saturday_active IS NOT NULL")
@dp.expect_or_drop("valid_sunday_active", "sunday_active IS NOT NULL")
@dp.expect_or_drop("valid_start_date", "start_date IS NOT NULL")
@dp.expect_or_drop("valid_end_date", "end_date IS NOT NULL")
@dp.expect_or_drop("valid_service_interval", "end_date >= start_date")
def silver_gtfs_calendar():
    """
    Create the Silver GTFS calendar table from the Bronze GTFS calendar table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS calendar
            records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS calendar streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Cast weekday availability flags to integer.
        4. Parse GTFS service interval dates from yyyyMMdd format.
        5. Create boolean helper columns for weekday activity.
        6. Create analytical helper columns for service interval duration and
           active weekday count.
        7. Apply data quality expectations through dp.expect_or_drop decorators.
        8. Select the final Silver output columns.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_calendar")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "service_id",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                    "start_date",
                    "end_date",
                ],
            )
        )
        .withColumn("monday", F.col("monday").cast("int"))
        .withColumn("tuesday", F.col("tuesday").cast("int"))
        .withColumn("wednesday", F.col("wednesday").cast("int"))
        .withColumn("thursday", F.col("thursday").cast("int"))
        .withColumn("friday", F.col("friday").cast("int"))
        .withColumn("saturday", F.col("saturday").cast("int"))
        .withColumn("sunday", F.col("sunday").cast("int"))
        .transform(
            lambda dataframe: with_nullable_boolean_from_int_columns(
                dataframe,
                {
                    "monday": "monday_active",
                    "tuesday": "tuesday_active",
                    "wednesday": "wednesday_active",
                    "thursday": "thursday_active",
                    "friday": "friday_active",
                    "saturday": "saturday_active",
                    "sunday": "sunday_active",
                },
            )
        )
        .withColumn("start_date", F.to_date(F.col("start_date"), "yyyyMMdd"))
        .withColumn("end_date", F.to_date(F.col("end_date"), "yyyyMMdd"))
        .withColumn(
            "service_duration_days",
            F.when(
                F.col("start_date").isNotNull() & F.col("end_date").isNotNull(),
                F.datediff(F.col("end_date"), F.col("start_date")) + F.lit(1),
            ).otherwise(F.lit(None).cast("int"))
        )
        .withColumn(
            "active_weekdays_count",
            F.coalesce(F.col("monday"), F.lit(0))
            + F.coalesce(F.col("tuesday"), F.lit(0))
            + F.coalesce(F.col("wednesday"), F.lit(0))
            + F.coalesce(F.col("thursday"), F.lit(0))
            + F.coalesce(F.col("friday"), F.lit(0))
            + F.coalesce(F.col("saturday"), F.lit(0))
            + F.coalesce(F.col("sunday"), F.lit(0))
        )
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))
        .select(
            "service_id",
            "monday_active",
            "tuesday_active",
            "wednesday_active",
            "thursday_active",
            "friday_active",
            "saturday_active",
            "sunday_active",
            "start_date",
            "end_date",
            "service_duration_days",
            "active_weekdays_count",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
    )

    return cleaned_df
