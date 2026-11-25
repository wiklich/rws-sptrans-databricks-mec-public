"""
Audit pipeline for logical inconsistencies in
gold.gold_route_geometry_reference.

Purpose:
    Detect violations in core Gold route geometry logic.

Audit scope:
    - shape_usage_rank <> 1 for a selected representative shape
    - geometry_point_count < 2
    - trip_count_using_shape <= 0

Important:
    This is a non-blocking audit table.
    It exposes inconsistent Gold route geometry records for investigation
    without dropping records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_geometry_reference"
TABLE_NAME = (
    "gold.gold_route_geometry_reference_logical_inconsistencies_audit"
)


def _representative_shape_rank_not_one(df: DataFrame) -> DataFrame:
    """
    Detect rows where the selected representative shape does not have
    shape_usage_rank = 1.
    """
    return (
        df.filter(F.col("shape_usage_rank") != 1)
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("representative_shape_id").alias("representative_shape_id"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("geometry_point_count").alias("geometry_point_count"),
            F.col("geometry_wkt").alias("geometry_wkt"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("REPRESENTATIVE_SHAPE_RANK_NOT_ONE").alias("audit_rule"),
            F.lit(
                "A selected representative shape has shape_usage_rank different "
                "from 1, which violates the expected representative shape logic."
            ).alias("audit_message"),
        )
    )


def _geometry_point_count_too_low(df: DataFrame) -> DataFrame:
    """
    Detect rows where geometry_point_count is less than 2.
    """
    return (
        df.filter(F.col("geometry_point_count") < 2)
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("representative_shape_id").alias("representative_shape_id"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("geometry_point_count").alias("geometry_point_count"),
            F.col("geometry_wkt").alias("geometry_wkt"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("GEOMETRY_POINT_COUNT_TOO_LOW").alias("audit_rule"),
            F.lit(
                "geometry_point_count is less than 2, which is not sufficient "
                "for a valid route LINESTRING geometry."
            ).alias("audit_message"),
        )
    )


def _trip_count_using_shape_non_positive(df: DataFrame) -> DataFrame:
    """
    Detect rows where trip_count_using_shape is less than or equal to 0.
    """
    return (
        df.filter(F.col("trip_count_using_shape") <= 0)
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("representative_shape_id").alias("representative_shape_id"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("geometry_point_count").alias("geometry_point_count"),
            F.col("geometry_wkt").alias("geometry_wkt"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("TRIP_COUNT_USING_SHAPE_NON_POSITIVE").alias("audit_rule"),
            F.lit(
                "trip_count_using_shape is less than or equal to 0, which "
                "violates the expected representative shape usage logic."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "gold.gold_route_geometry_reference, including representative shape "
        "rank issues, invalid geometry point counts, and non-positive trip "
        "usage counts."
    ),
)
def gold_route_geometry_reference_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    Gold route geometry reference table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    representative_shape_rank_not_one_df = _representative_shape_rank_not_one(
        source_df
    )

    geometry_point_count_too_low_df = _geometry_point_count_too_low(source_df)

    trip_count_using_shape_non_positive_df = _trip_count_using_shape_non_positive(
        source_df
    )

    audit_df = (
        representative_shape_rank_not_one_df.unionByName(
            geometry_point_count_too_low_df
        )
        .unionByName(trip_count_using_shape_non_positive_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
