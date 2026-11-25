"""
Silver GTFS shapes pipeline.

Purpose:
    Build a cleaned, validated, and standardized Silver table for GTFS shapes
    based on the Bronze raw ingestion table.

Main transformations:
    - Trim string fields
    - Convert empty strings to null
    - Preserve identifier casing
    - Cast latitude and longitude to double
    - Cast shape_pt_sequence to integer
    - Cast shape_dist_traveled to double

Main validations:
    - shape_id must be present
    - shape_pt_lat must be present and must be a valid latitude
    - shape_pt_lon must be present and must be a valid longitude
    - shape_pt_sequence must be present and must be a non-negative integer
    - shape_dist_traveled must be present and must be a non-negative float
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_gtfs_shapes import (
    get_silver_gtfs_shapes_schema,
)
from utils.string_utils import with_trimmed_nullable_strings
from utils.constants_utils import (
    BOUNDING_BOX_MIN_LAT,
    BOUNDING_BOX_MAX_LAT,
    BOUNDING_BOX_MIN_LON,
    BOUNDING_BOX_MAX_LON,
)


@dp.table(
    name="silver.silver_gtfs_shapes",
    comment="Rules for mapping vehicle travel paths, sometimes referred to as route alignments.",
    schema=get_silver_gtfs_shapes_schema(),
)
@dp.expect_or_drop("valid_shape_id", "shape_id IS NOT NULL")
@dp.expect_or_drop("valid_shape_pt_lat", "shape_pt_lat IS NOT NULL AND shape_pt_lat BETWEEN -90 AND 90")
@dp.expect_or_drop("valid_shape_pt_lon", "shape_pt_lon IS NOT NULL AND shape_pt_lon BETWEEN -180 AND 180")
@dp.expect_or_drop("valid_shape_pt_sequence", "shape_pt_sequence IS NOT NULL AND shape_pt_sequence >= 0")
@dp.expect_or_drop("valid_shape_dist_traveled", "shape_dist_traveled IS NOT NULL AND shape_dist_traveled >= 0")
@dp.expect_or_drop(
    "within_sptrans_bounding_box",
    f"""
    shape_pt_lat BETWEEN {BOUNDING_BOX_MIN_LAT} AND {BOUNDING_BOX_MAX_LAT}
    AND shape_pt_lon BETWEEN {BOUNDING_BOX_MIN_LON} AND {BOUNDING_BOX_MAX_LON}
    """
)
def silver_gtfs_shapes():
    """
    Create the Silver GTFS shapes table from the Bronze GTFS shapes table.

    Returns:
        pyspark.sql.DataFrame:
            A cleaned and validated DataFrame containing standardized GTFS shape
            records and preserved technical metadata from Bronze.

    Processing steps:
        1. Read the Bronze GTFS shapes streaming table.
        2. Clean textual columns by trimming whitespace and converting empty
           strings to null.
        3. Cast latitude and longitude columns to double.
        4. Cast shape_pt_sequence to integer.
        5. Cast shape_dist_traveled to double.
        6. Cast metadata columns to their final analytical types.
        7. Select the final Silver output columns.
    """
    df = spark.readStream.table("main.bronze.bronze_gtfs_shapes")

    cleaned_df = (
        df.transform(
            lambda dataframe: with_trimmed_nullable_strings(
                dataframe,
                [
                    "shape_id",
                    "shape_pt_lat",
                    "shape_pt_lon",
                    "shape_pt_sequence",
                    "shape_dist_traveled",
                ],
            )
        )
        .withColumn("shape_pt_lat", F.col("shape_pt_lat").cast("double"))
        .withColumn("shape_pt_lon", F.col("shape_pt_lon").cast("double"))
        .withColumn("shape_pt_sequence", F.col("shape_pt_sequence").cast("int"))
        .withColumn("shape_dist_traveled", F.col("shape_dist_traveled").cast("double"))
        .withColumn("gtfs_load_date", F.col("gtfs_load_date").cast("date"))
        .withColumn("gtfs_loaded_at", F.col("gtfs_loaded_at").cast("timestamp"))
        .select(
            "shape_id",
            "shape_pt_lat",
            "shape_pt_lon",
            "shape_pt_sequence",
            "shape_dist_traveled",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
    )

    return cleaned_df