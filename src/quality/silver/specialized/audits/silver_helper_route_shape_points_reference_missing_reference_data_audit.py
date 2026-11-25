"""
Audit pipeline for missing reference data in
silver.silver_helper_route_shape_points_reference.

Purpose:
    Detect missing descriptive enrichment fields that do not always block
    downstream usage, but should remain visible for review.

Audit scope:
    - route_long_name IS NULL
    - direction_description IS NULL
    - trip_headsign_reference IS NULL

Important:
    This is a non-blocking audit table.
    It exposes rows with missing descriptive reference data without dropping
    records from the main Silver helper table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_helper_route_shape_points_reference"
TABLE_NAME = (
    "silver.silver_helper_route_shape_points_reference_"
    "missing_reference_data_audit"
)


def _route_long_name_missing(df: DataFrame) -> DataFrame:
    """
    Detect rows where route_long_name is null.
    """
    return (
        df.filter(F.col("route_long_name").isNull())
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("shape_id").alias("shape_id"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("shape_pt_sequence").alias("shape_pt_sequence"),
            F.col("shape_pt_lat").alias("shape_pt_lat"),
            F.col("shape_pt_lon").alias("shape_pt_lon"),
            F.col("shape_dist_traveled").alias("shape_dist_traveled"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("ROUTE_LONG_NAME_MISSING").alias("audit_rule"),
            F.lit(
                "route_long_name is null. This does not always block downstream "
                "usage, but route descriptive enrichment is incomplete."
            ).alias("audit_message"),
        )
    )


def _direction_description_missing(df: DataFrame) -> DataFrame:
    """
    Detect rows where direction_description is null.
    """
    return (
        df.filter(F.col("direction_description").isNull())
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("shape_id").alias("shape_id"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("shape_pt_sequence").alias("shape_pt_sequence"),
            F.col("shape_pt_lat").alias("shape_pt_lat"),
            F.col("shape_pt_lon").alias("shape_pt_lon"),
            F.col("shape_dist_traveled").alias("shape_dist_traveled"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("DIRECTION_DESCRIPTION_MISSING").alias("audit_rule"),
            F.lit(
                "direction_description is null. This does not always block "
                "downstream usage, but direction descriptive enrichment is incomplete."
            ).alias("audit_message"),
        )
    )


def _trip_headsign_reference_missing(df: DataFrame) -> DataFrame:
    """
    Detect rows where trip_headsign_reference is null.
    """
    return (
        df.filter(F.col("trip_headsign_reference").isNull())
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("shape_id").alias("shape_id"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("shape_pt_sequence").alias("shape_pt_sequence"),
            F.col("shape_pt_lat").alias("shape_pt_lat"),
            F.col("shape_pt_lon").alias("shape_pt_lon"),
            F.col("shape_dist_traveled").alias("shape_dist_traveled"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("TRIP_HEADSIGN_REFERENCE_MISSING").alias("audit_rule"),
            F.lit(
                "trip_headsign_reference is null. This does not always block "
                "downstream usage, but trip descriptive enrichment is incomplete."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for missing descriptive reference data in "
        "silver.silver_helper_route_shape_points_reference, including missing "
        "route_long_name, direction_description, and trip_headsign_reference."
    ),
)
def silver_helper_route_shape_points_reference_missing_reference_data_audit() -> DataFrame:
    """
    Create a non-blocking audit table for rows with missing descriptive
    enrichment fields in the route-shape point helper table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    route_long_name_missing_df = _route_long_name_missing(source_df)
    direction_description_missing_df = _direction_description_missing(source_df)
    trip_headsign_reference_missing_df = _trip_headsign_reference_missing(source_df)

    audit_df = (
        route_long_name_missing_df.unionByName(direction_description_missing_df)
        .unionByName(trip_headsign_reference_missing_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
