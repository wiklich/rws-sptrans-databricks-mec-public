"""
Audit pipeline for duplicate grain violations in
gold.gold_route_geometry_reference.

Purpose:
    Detect unexpected duplication at the declared Gold grain.

Declared grain:
    One row per:
        line_sign + gtfs_direction_id

Audit rule:
    - COUNT(*) > 1 grouped by line_sign, gtfs_direction_id

Important:
    This is a non-blocking audit table.
    It exposes duplicate-grain Gold records for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_geometry_reference"
TABLE_NAME = "gold.gold_route_geometry_reference_duplicate_grain_violations_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for duplicate grain violations in "
        "gold.gold_route_geometry_reference. Detects cases where more than one "
        "row exists for the same line_sign and gtfs_direction_id."
    ),
)
def gold_route_geometry_reference_duplicate_grain_violations_audit() -> DataFrame:
    """
    Create a non-blocking audit table for duplicate rows at the declared
    Gold grain.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy(
            "line_sign",
            "gtfs_direction_id",
        )
        .agg(F.count(F.lit(1)).alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
    )

    audit_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .select(
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.route_id").alias("route_id"),
            F.col("src.route_long_name").alias("route_long_name"),
            F.col("src.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.direction_description").alias("direction_description"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.representative_shape_id").alias("representative_shape_id"),
            F.col("src.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("src.shape_usage_rank").alias("shape_usage_rank"),
            F.col("src.shape_point_count").alias("shape_point_count"),
            F.col("src.geometry_point_count").alias("geometry_point_count"),
            F.col("src.geometry_wkt").alias("geometry_wkt"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("dup.duplicate_count").alias("duplicate_count"),
            F.lit("DUPLICATE_GOLD_GRAIN_ENTITY").alias("audit_rule"),
            F.lit(
                "More than one row exists for the same line_sign and "
                "gtfs_direction_id, which violates the declared Gold grain."
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
