"""
Audit pipeline for duplicate grain violations in
gold.gold_line_explorer_map_points.

Purpose:
    Detect unexpected duplication at the declared Gold serving grain.

Declared grain:
    One row per:
        service_date
        + captured_at_local
        + line_sign
        + gtfs_direction_id
        + vehicle_prefix
        + latitude
        + longitude

Audit rule:
    - COUNT(*) > 1 grouped by the declared event-level serving grain

Important:
    This is a non-blocking audit table.
    It exposes duplicate-grain Gold records for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_line_explorer_map_points"
TABLE_NAME = "gold.gold_line_explorer_map_points_duplicate_grain_violations_audit"

SOURCE_COLUMNS = [
    "event_id",
    "service_date",
    "captured_at_local",
    "hour",
    "line_sign",
    "observed_direction",
    "gtfs_direction_id",
    "direction_description",
    "vehicle_prefix",
    "latitude",
    "longitude",
    "planned_headway_minutes",
    "observed_headway_avg_minutes",
    "headway_deviation_minutes",
    "headway_deviation_pct",
    "missed_service_flag",
    "adherence_band",
    "record_created_at",
    "record_hash",
]


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for duplicate grain violations in "
        "gold.gold_line_explorer_map_points. Detects cases where more than one "
        "row exists for the same service_date, captured_at_local, line_sign, "
        "gtfs_direction_id, vehicle_prefix, latitude, and longitude."
    ),
)
def gold_line_explorer_map_points_duplicate_grain_violations_audit() -> DataFrame:
    """
    Create a non-blocking audit table for duplicate rows at the declared
    Gold line explorer map point grain.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy(
            "service_date",
            "captured_at_local",
            "line_sign",
            "gtfs_direction_id",
            "vehicle_prefix",
            "latitude",
            "longitude",
        )
        .agg(F.count(F.lit(1)).cast("long").alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
    )

    audit_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=[
                "service_date",
                "captured_at_local",
                "line_sign",
                "gtfs_direction_id",
                "vehicle_prefix",
                "latitude",
                "longitude",
            ],
            how="inner",
        )
        .select(
            *[F.col(f"src.{column_name}").alias(column_name) for column_name in SOURCE_COLUMNS],
            F.col("dup.duplicate_count").alias("duplicate_count"),
            F.lit("DUPLICATE_GOLD_POINT_GRAIN_ENTITY").alias("audit_rule"),
            F.lit(
                "More than one row exists for the same service_date, "
                "captured_at_local, line_sign, gtfs_direction_id, vehicle_prefix, "
                "latitude, and longitude, which violates the declared Gold point grain."
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
