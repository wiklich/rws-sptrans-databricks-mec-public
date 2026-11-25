"""
Silver GTFS stops pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS stops
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Uppercase descriptive text fields
    - Cast stop_lat and stop_lon to double

Main validations:
    - stop_id must be present
    - stop_name must be present
    - stop_lat must be present and must be a valid latitude
    - stop_lon must be present and must be a valid longitude
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_stops import (
    get_silver_gtfs_stops_schema,
)

from utils.string_utils import (
    with_trimmed_nullable_strings,
    with_uppercase_strings,
)

from utils.constants_utils import (
    BOUNDING_BOX_MIN_LAT,
    BOUNDING_BOX_MAX_LAT,
    BOUNDING_BOX_MIN_LON,
    BOUNDING_BOX_MAX_LON,
)


@dp.table(
    name="silver.silver_gtfs_stops",
    comment="Stops where vehicles pick up or drop off riders. Also defines stations and station entrances.",
    schema=get_silver_gtfs_stops_schema(),
)
@dp.expect_or_drop("valid_stop_id", "stop_id IS NOT NULL")
@dp.expect_or_drop("valid_stop_name", "stop_name IS NOT NULL")
@dp.expect_or_drop("valid_stop_lat", "stop_lat IS NOT NULL AND stop_lat BETWEEN -90 AND 90")
@dp.expect_or_drop("valid_stop_lon", "stop_lon IS NOT NULL AND stop_lon BETWEEN -180 AND 180")
@dp.expect_or_drop(
    "within_sptrans_bounding_box",
    f"""
    stop_lat BETWEEN {BOUNDING_BOX_MIN_LAT} AND {BOUNDING_BOX_MAX_LAT}
    AND stop_lon BETWEEN {BOUNDING_BOX_MIN_LON} AND {BOUNDING_BOX_MAX_LON}
    """
)
def silver_gtfs_stops():
    """
    Create the Silver GTFS stops table from the Bronze GTFS stops table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS stop
            records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS stops streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Uppercase descriptive text fields.
        4. Cast latitude and longitude columns to double.
        5. Cast metadata columns to their final analytical types.
        6. Select the final Silver output columns.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_stops")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "stop_id",
                    "stop_name",
                    "stop_desc",
                    "stop_lat",
                    "stop_lon",
                ],
            )
        )
        .transform(
            lambda dataframe: with_uppercase_strings(
                dataframe,
                [
                    "stop_name",
                    "stop_desc",
                ],
            )
        )
        .withColumn("stop_lat", F.col("stop_lat").cast("double"))
        .withColumn("stop_lon", F.col("stop_lon").cast("double"))
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))
        .select(
            "stop_id",
            "stop_name",
            "stop_desc",
            "stop_lat",
            "stop_lon",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
    )

    return cleaned_df