"""
Audit pipeline for duplicate grain violations in
gold.gold_planned_vs_observed_service_adherence_hourly.

Purpose:
    Detect unexpected duplication at the declared Gold grain.

Declared grain:
    One row per:
        service_date + line_sign + gtfs_direction_id + hour

Audit rule:
    - COUNT(*) > 1 grouped by service_date, line_sign, gtfs_direction_id, hour

Important:
    This is a non-blocking audit table.
    It exposes duplicate-grain violations for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_planned_vs_observed_service_adherence_hourly"
TABLE_NAME = (
    "gold.gold_planned_vs_observed_service_adherence_hourly_"
    "duplicate_grain_violations_audit"
)


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for duplicate grain violations in "
        "gold.gold_planned_vs_observed_service_adherence_hourly. Detects cases "
        "where more than one row exists for the same service_date, line_sign, "
        "gtfs_direction_id, and hour."
    ),
)
def gold_planned_vs_observed_service_adherence_hourly_duplicate_grain_violations_audit() -> DataFrame:
    """
    Create a non-blocking audit table for duplicate grain violations in the
    Gold planned versus observed service adherence table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
        )
        .agg(F.count(F.lit(1)).alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
    )

    audit_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="inner",
        )
        .select(
            F.col("src.service_date").alias("service_date"),
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.hour").alias("hour"),
            F.col("src.planned_headway_minutes").alias("planned_headway_minutes"),
            F.col("src.observed_event_count_proxy").alias("observed_event_count_proxy"),
            F.col("src.observed_vehicle_count").alias("observed_vehicle_count"),
            F.col("src.observed_headway_avg_minutes").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("src.observed_headway_p50_minutes").alias(
                "observed_headway_p50_minutes"
            ),
            F.col("src.observed_headway_p90_minutes").alias(
                "observed_headway_p90_minutes"
            ),
            F.col("src.headway_deviation_minutes").alias(
                "headway_deviation_minutes"
            ),
            F.col("src.headway_deviation_pct").alias("headway_deviation_pct"),
            F.col("src.planned_service_present_flag").alias(
                "planned_service_present_flag"
            ),
            F.col("src.observed_service_present_flag").alias(
                "observed_service_present_flag"
            ),
            F.col("src.missed_service_flag").alias("missed_service_flag"),
            F.col("src.adherence_type").alias("adherence_type"),
            F.col("src.adherence_note").alias("adherence_note"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("dup.duplicate_count").alias("duplicate_count"),
            F.lit("DUPLICATE_GRAIN_VIOLATION").alias("audit_rule"),
            F.lit(
                "More than one row exists for the same service_date, "
                "line_sign, gtfs_direction_id, and hour, which violates "
                "the declared Gold grain."
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
