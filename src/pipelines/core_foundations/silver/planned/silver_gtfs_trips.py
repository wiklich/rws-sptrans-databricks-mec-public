"""
Silver GTFS trips pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS trips
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Cast direction_id to integer
    - Create direction_description helper column

Main validations:
    - route_id must be present
    - service_id must be present
    - trip_id must be present
    - shape_id must be present
    - direction_id, when present, must be either 0 or 1
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_trips import (
    get_silver_gtfs_trips_schema,
)
from utils.string_utils import (
    with_trimmed_nullable_strings,
    with_uppercase_strings,
)


@dp.table(
    name="silver.silver_gtfs_trips",
    comment="Trips for each route. A trip is a sequence of two or more stops that occur during a specific time period.",
    schema=get_silver_gtfs_trips_schema(),
)
@dp.expect_or_drop("valid_route_id", "route_id IS NOT NULL")
@dp.expect_or_drop("valid_service_id", "service_id IS NOT NULL")
@dp.expect_or_drop("valid_trip_id", "trip_id IS NOT NULL")
@dp.expect_or_drop("valid_shape_id", "shape_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_direction_id",
    "direction_id IS NULL OR direction_id IN (0, 1)",
)
def silver_gtfs_trips():
    """
    Create the Silver GTFS trips table from the Bronze GTFS trips table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS trip
            records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS trips streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Cast direction_id to integer.
        4. Create a human-readable direction_description helper column.
        5. Cast metadata columns to their final analytical types.
        6. Select the final Silver output columns.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_trips")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "route_id",
                    "service_id",
                    "trip_id",
                    "trip_headsign",
                    "direction_id",
                    "shape_id",
                ],
            )
        )
        .withColumn("direction_id", F.col("direction_id").cast("int"))
        .withColumn(
            "direction_description",
            F.when(F.col("direction_id") == 0, F.lit("TRAVEL IN ONE DIRECTION (E.G. OUTBOUND TRAVEL)."))
            .when(F.col("direction_id") == 1, F.lit("TRAVEL IN THE OPPOSITE DIRECTION (E.G. INBOUND TRAVEL)."))
            .otherwise(F.lit(None))
        )
        .transform(
            lambda dataframe: with_uppercase_strings(
                dataframe,
                [
                    "trip_headsign"
                ],
            )
        )
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))
        .select(
            "route_id",
            "service_id",
            "trip_id",
            "trip_headsign",
            "direction_id",
            "direction_description",
            "shape_id",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
    )

    return cleaned_df