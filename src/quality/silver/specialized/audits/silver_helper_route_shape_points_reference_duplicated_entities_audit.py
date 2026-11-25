"""
Audit pipeline for duplicated entities in
silver.silver_helper_route_shape_points_reference.

Purpose:
    Detect unexpected duplication at the declared helper grain.

Declared grain:
    One row per:
        line_sign + gtfs_direction_id + shape_id + shape_pt_sequence

Audit rule:
    - COUNT(*) > 1 grouped by line_sign, gtfs_direction_id, shape_id, shape_pt_sequence

Important:
    This is a non-blocking audit table.
    It exposes duplicated helper-grain records for investigation without
    dropping records from the main Silver helper table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_helper_route_shape_points_reference"
TABLE_NAME = (
    "silver.silver_helper_route_shape_points_reference_"
    "duplicated_entities_audit"
)


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for duplicated entities in "
        "silver.silver_helper_route_shape_points_reference. Detects cases "
        "where more than one row exists for the same line_sign, "
        "gtfs_direction_id, shape_id, and shape_pt_sequence."
    ),
)
def silver_helper_route_shape_points_reference_duplicated_entities_audit() -> DataFrame:
    """
    Create a non-blocking audit table for duplicated entities at the declared
    helper grain.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy(
            "line_sign",
            "gtfs_direction_id",
            "shape_id",
            "shape_pt_sequence",
        )
        .agg(F.count(F.lit(1)).alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
    )

    audit_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=[
                "line_sign",
                "gtfs_direction_id",
                "shape_id",
                "shape_pt_sequence",
            ],
            how="inner",
        )
        .select(
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.route_id").alias("route_id"),
            F.col("src.route_long_name").alias("route_long_name"),
            F.col("src.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.direction_description").alias("direction_description"),
            F.col("src.shape_id").alias("shape_id"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("src.shape_usage_rank").alias("shape_usage_rank"),
            F.col("src.is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("src.shape_point_count").alias("shape_point_count"),
            F.col("src.shape_pt_sequence").alias("shape_pt_sequence"),
            F.col("src.shape_pt_lat").alias("shape_pt_lat"),
            F.col("src.shape_pt_lon").alias("shape_pt_lon"),
            F.col("src.shape_dist_traveled").alias("shape_dist_traveled"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("dup.duplicate_count").alias("duplicate_count"),
            F.lit("DUPLICATED_HELPER_GRAIN_ENTITY").alias("audit_rule"),
            F.lit(
                "More than one row exists for the same line_sign, "
                "gtfs_direction_id, shape_id, and shape_pt_sequence, which "
                "violates the declared helper grain."
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
