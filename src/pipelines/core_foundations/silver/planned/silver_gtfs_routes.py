"""
Silver GTFS routes pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS routes
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Cast route_type to integer
    - Normalize route color fields to uppercase
    - Create route_name_display for downstream consumption

Main validations:
    - route_id must be present
    - agency_id must be present
    - route_type must be one of the GTFS allowed values
    - at least one of route_short_name or route_long_name must be present
    - route_color, when present, must be a valid 6-character hexadecimal color
    - route_text_color, when present, must be a valid 6-character hexadecimal color
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_routes import (
    get_silver_gtfs_routes_schema
)
from utils.color_utils import with_color_description
from utils.string_utils import (
    with_trimmed_nullable_strings,
    with_uppercase_strings
)


VALID_ROUTE_TYPE_VALUES = (0, 1, 2, 3, 4, 5, 6, 7, 11, 12)


@dp.table(
    name="silver.silver_gtfs_routes",
    comment="Cleaned, validated, and standardized GTFS routes",
    schema=get_silver_gtfs_routes_schema()
)
@dp.expect_or_drop("valid_route_id", "route_id IS NOT NULL")
@dp.expect_or_drop("valid_agency_id", "agency_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_route_name",
    "route_short_name IS NOT NULL OR route_long_name IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_route_type",
    f"route_type IN ({','.join(str(value) for value in VALID_ROUTE_TYPE_VALUES)})"
)
@dp.expect_or_drop(
    "valid_route_color",
    "route_color IS NULL OR route_color RLIKE '^[0-9A-F]{6}$'"
)
@dp.expect_or_drop(
    "valid_route_text_color",
    "route_text_color IS NULL OR route_text_color RLIKE '^[0-9A-F]{6}$'"
)
def silver_gtfs_routes():
    """
    Create the Silver GTFS routes table from the Bronze GTFS routes table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS route
            records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS routes streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Cast route_type to integer.
        4. Standardize color columns to uppercase.
        5. Create route_name_display for downstream analytical usage.
        6. Preserve technical lineage and ingestion metadata.
        7. Apply data quality expectations through dp.expect_or_drop decorators.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_routes")

    cleaned_df = (
        df
        .transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "route_id",
                    "agency_id",
                    "route_short_name",
                    "route_long_name",
                    "route_color",
                    "route_text_color",
                ],
            )
        )
        .withColumn("route_type", F.col("route_type").cast("int"))
        .withColumn(
            "route_type_description",
            F.when(F.col("route_type") == 0, F.lit("TRAM, STREETCAR, LIGHT RAIL. ANY LIGHT RAIL OR STREET LEVEL SYSTEM WITHIN A METROPOLITAN AREA."))
             .when(F.col("route_type") == 1, F.lit("SUBWAY, METRO. ANY UNDERGROUND RAIL SYSTEM WITHIN A METROPOLITAN AREA."))
             .when(F.col("route_type") == 2, F.lit("RAIL. USED FOR INTERCITY OR LONG-DISTANCE TRAVEL."))
             .when(F.col("route_type") == 3, F.lit("BUS. USED FOR SHORT- AND LONG-DISTANCE BUS ROUTES."))
             .when(F.col("route_type") == 4, F.lit("FERRY. USED FOR SHORT- AND LONG-DISTANCE BOAT SERVICE."))
             .when(F.col("route_type") == 5, F.lit("CABLE TRAM. USED FOR STREET-LEVEL RAIL CARS WHERE THE CABLE RUNS BENEATH THE VEHICLE (E.G., CABLE CAR IN SAN FRANCISCO)."))
             .when(F.col("route_type") == 6, F.lit("AERIAL LIFT, SUSPENDED CABLE CAR (E.G., GONDOLA LIFT, AERIAL TRAMWAY). CABLE TRANSPORT WHERE CABINS, CARS, GONDOLAS OR OPEN CHAIRS ARE SUSPENDED BY MEANS OF ONE OR MORE CABLES."))
             .when(F.col("route_type") == 7, F.lit("FUNICULAR. ANY RAIL SYSTEM DESIGNED FOR STEEP INCLINES."))
             .when(F.col("route_type") == 11, F.lit("TROLLEYBUS. ELECTRIC BUSES THAT DRAW POWER FROM OVERHEAD WIRES USING POLES."))
             .when(F.col("route_type") == 12, F.lit("MONORAIL. RAILWAY IN WHICH THE TRACK CONSISTS OF A SINGLE RAIL OR A BEAM."))
            .otherwise(F.lit(None))
        )
        .transform(lambda dataframe: with_color_description(
            dataframe,
            "route_color",
            "route_color_name",
        ))
        .transform(lambda dataframe: with_color_description(
            dataframe,
            "route_text_color",
            "route_text_color_name",
        ))
        .transform(
            lambda dataframe: with_uppercase_strings(
                dataframe,
                [
                    "route_long_name",
                    "route_color",
                    "route_text_color",
                ],
            )
        )
        .withColumn(
            "route_name_display",
            F.concat(F.col("route_short_name"), F.lit(" "), F.col("route_long_name"))
        )
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))        
        .select(
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_name_display",
            "route_type",
            "route_type_description",
            "route_color",
            "route_color_name",
            "route_text_color",
            "route_text_color_name",
            "gtfs_load_date",
            "gtfs_loaded_at",            
            "record_hash",
        )
    )

    return cleaned_df
