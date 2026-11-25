"""
Audit pipeline for duplicate grain violations in
gold.gold_terminal_risk_daily.

Purpose:
    Detect unexpected duplication at the declared Gold grain.

Declared grain:
    One row per:
        service_date + line_sign + gtfs_direction_id + terminal_role

Audit rule:
    - COUNT(*) > 1 grouped by service_date, line_sign, gtfs_direction_id, terminal_role

Important:
    This is a non-blocking audit table.
    It exposes duplicate-grain Gold records for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_terminal_risk_daily"
TABLE_NAME = "gold.gold_terminal_risk_daily_duplicate_grain_violations_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for duplicate grain violations in "
        "gold.gold_terminal_risk_daily. Detects cases where more than one row "
        "exists for the same service_date, line_sign, gtfs_direction_id, and "
        "terminal_role."
    ),
)
def gold_terminal_risk_daily_duplicate_grain_violations_audit() -> DataFrame:
    """
    Create a non-blocking audit table for duplicate rows at the declared
    Gold grain.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "terminal_role",
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
                "line_sign",
                "gtfs_direction_id",
                "terminal_role",
            ],
            how="inner",
        )
        .select(
            F.col("src.service_date").alias("service_date"),
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.route_id").alias("route_id"),
            F.col("src.route_long_name").alias("route_long_name"),
            F.col("src.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.direction_description").alias("direction_description"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.terminal_role").alias("terminal_role"),
            F.col("src.stop_id").alias("stop_id"),
            F.col("src.stop_name").alias("stop_name"),
            F.col("src.latitude").alias("latitude"),
            F.col("src.longitude").alias("longitude"),
            F.col("src.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("src.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("src.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("src.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("src.planned_service_hours").alias("planned_service_hours"),
            F.col("src.observed_service_hours").alias("observed_service_hours"),
            F.col("src.missed_service_windows").alias("missed_service_windows"),
            F.col("src.missed_service_rate").alias("missed_service_rate"),
            F.col("src.avg_planned_headway_minutes").alias(
                "avg_planned_headway_minutes"
            ),
            F.col("src.avg_observed_headway_avg_minutes").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.col("src.avg_headway_deviation_minutes").alias(
                "avg_headway_deviation_minutes"
            ),
            F.col("src.avg_headway_deviation_pct").alias(
                "avg_headway_deviation_pct"
            ),
            F.col("src.terminal_risk_band").alias("terminal_risk_band"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("dup.duplicate_count").alias("duplicate_count"),
            F.lit("DUPLICATE_GOLD_GRAIN_ENTITY").alias("audit_rule"),
            F.lit(
                "More than one row exists for the same service_date, line_sign, "
                "gtfs_direction_id, and terminal_role, which violates the "
                "declared Gold grain."
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
