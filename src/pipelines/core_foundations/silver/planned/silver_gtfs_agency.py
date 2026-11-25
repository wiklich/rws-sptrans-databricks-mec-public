"""
Silver GTFS agency pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS agency
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Preserve technical lineage metadata from Bronze

Main validations:
    - agency_id must be present
    - agency_name must be present
    - agency_url must be present and must start with http:// or https://
    - agency_timezone must be present
    - agency_lang, when present, must follow a basic language code pattern
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_agency import (
    get_silver_gtfs_agency_schema
)

from utils.string_utils import (
    with_trimmed_nullable_strings,
    with_uppercase_strings
)


@dp.table(
    name="silver.silver_gtfs_agency",
    comment="Transit agencies with service represented in this dataset.",
    schema=get_silver_gtfs_agency_schema(),
)
@dp.expect_or_drop("valid_agency_id", "agency_id IS NOT NULL")
@dp.expect_or_drop("valid_agency_name", "agency_name IS NOT NULL")
@dp.expect_or_drop("valid_agency_url", "agency_url IS NOT NULL AND agency_url RLIKE '^https?://.+'")
@dp.expect_or_drop("valid_agency_timezone", "agency_timezone IS NOT NULL")
@dp.expect_or_drop(
    "valid_agency_lang",
    "agency_lang IS NULL OR agency_lang RLIKE '^[A-Za-z]{2,3}([_-][A-Za-z0-9]+)*$'",
)
def silver_gtfs_agency():
    """
    Create the Silver GTFS agency table from the Bronze GTFS agency table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS agency
            records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS agency streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Preserve identifier casing and canonical timezone values.
        4. Cast metadata columns to their final analytical types.
        5. Apply data quality expectations through dp.expect_or_drop decorators.
        6. Select the final Silver output columns.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_agency")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "agency_id",
                    "agency_name",
                    "agency_url",
                    "agency_timezone",
                    "agency_lang",
                ],
            )
        )
        .transform(
            lambda dataframe: with_uppercase_strings(
                dataframe,
                [
                    "agency_name",
                    "agency_timezone",
                    "agency_lang",
                ],
            )
        )
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))
        .select(
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "agency_lang",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
    )

    return cleaned_df