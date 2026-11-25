"""
Gold terminal risk daily pipeline.

Purpose:
    Build a daily terminal-level risk table by combining:
    - gold.gold_route_terminal_reference
    - gold.gold_planned_vs_observed_service_adherence_hourly

Grain:
    One row per:
        service_date + line_sign + gtfs_direction_id + terminal_role
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_terminal_risk_daily import (
    GOLD_TERMINAL_RISK_DAILY_SCHEMA
)

TABLE_NAME = "gold.gold_terminal_risk_daily"

TERMINAL_REFERENCE_SOURCE = "main.gold.gold_route_terminal_reference"
SERVICE_ADHERENCE_SOURCE = "main.gold.gold_planned_vs_observed_service_adherence_hourly"

APPROVED_TERMINAL_ROLES = ["ORIGIN", "DESTINATION"]


def _build_terminal_reference() -> DataFrame:
    """
    Read the authoritative terminal reference source.
    """
    return (
        spark.read.table(TERMINAL_REFERENCE_SOURCE)
        .select(
            F.col("line_sign"),
            F.col("route_id"),
            F.col("route_long_name"),
            F.col("route_name_display"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("trip_headsign_reference"),
            F.col("terminal_role"),
            F.col("stop_id"),
            F.col("stop_name"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("representative_first_stop_id"),
            F.col("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").cast("long").alias(
                "endpoint_pair_trip_count"
            ),
            F.col("endpoint_pair_rank").cast("int").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
        )
        .filter(
            F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("terminal_role").isin(APPROVED_TERMINAL_ROLES)
            & F.col("stop_id").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .dropDuplicates(["line_sign", "gtfs_direction_id", "terminal_role"])
    )


def _build_daily_route_direction_metrics() -> DataFrame:
    """
    Aggregate hourly service adherence metrics to daily route-direction grain.
    """
    return (
        spark.read.table(SERVICE_ADHERENCE_SOURCE)
        .select(
            F.col("service_date").cast("date").alias("service_date"),
            F.col("line_sign"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("planned_service_present_flag").cast("boolean").alias(
                "planned_service_present_flag"
            ),
            F.col("observed_service_present_flag").cast("boolean").alias(
                "observed_service_present_flag"
            ),
            F.col("missed_service_flag").cast("boolean").alias("missed_service_flag"),
            F.col("planned_headway_minutes").cast("double").alias(
                "planned_headway_minutes"
            ),
            F.col("observed_headway_avg_minutes").cast("double").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("headway_deviation_minutes").cast("double").alias(
                "headway_deviation_minutes"
            ),
            F.col("headway_deviation_pct").cast("double").alias(
                "headway_deviation_pct"
            ),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("gtfs_direction_id").isNotNull()
        )
        .groupBy("service_date", "line_sign", "gtfs_direction_id")
        .agg(
            F.sum(
                F.when(F.col("planned_service_present_flag") == F.lit(True), F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("planned_service_hours"),
            F.sum(
                F.when(F.col("observed_service_present_flag") == F.lit(True), F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("observed_service_hours"),
            F.sum(
                F.when(F.col("missed_service_flag") == F.lit(True), F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("missed_service_windows"),
            F.avg("planned_headway_minutes").cast("double").alias(
                "avg_planned_headway_minutes"
            ),
            F.avg("observed_headway_avg_minutes").cast("double").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.avg("headway_deviation_minutes").cast("double").alias(
                "avg_headway_deviation_minutes"
            ),
            F.avg("headway_deviation_pct").cast("double").alias(
                "avg_headway_deviation_pct"
            ),
        )
        .withColumn(
            "missed_service_rate",
            F.when(
                F.col("planned_service_hours") > 0,
                F.col("missed_service_windows") / F.col("planned_service_hours"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Gold daily terminal risk table that combines the authoritative "
        "terminal reference with daily planned-vs-observed service adherence "
        "metrics for downstream terminal maps and analytics."
    ),
    schema=GOLD_TERMINAL_RISK_DAILY_SCHEMA,
)
@dp.expect_or_drop("valid_service_date", "service_date IS NOT NULL")
@dp.expect_or_drop("valid_line_sign", "line_sign IS NOT NULL")
@dp.expect_or_drop("valid_gtfs_direction_id", "gtfs_direction_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_terminal_role",
    "terminal_role IN ('ORIGIN', 'DESTINATION')",
)
@dp.expect_or_drop("valid_stop_id", "stop_id IS NOT NULL")
@dp.expect_or_drop("valid_latitude", "latitude IS NOT NULL")
@dp.expect_or_drop("valid_longitude", "longitude IS NOT NULL")
def gold_terminal_risk_daily() -> DataFrame:
    """
    Create a daily terminal risk Gold table.

    Final output:
        One row per service_date + line_sign + gtfs_direction_id + terminal_role
    """
    terminal_reference = _build_terminal_reference()
    daily_metrics = _build_daily_route_direction_metrics()

    final_df = (
        daily_metrics.alias("m")
        .join(
            terminal_reference.alias("t"),
            on=["line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .withColumn(
            "terminal_risk_band",
            F.when(
                (F.col("missed_service_windows") >= F.lit(20))
                | (F.col("missed_service_rate") >= F.lit(0.40))
                | (F.col("avg_headway_deviation_minutes") >= F.lit(10.0)),
                F.lit("High"),
            )
            .when(
                (F.col("missed_service_windows") >= F.lit(5))
                | (F.col("missed_service_rate") >= F.lit(0.15))
                | (F.col("avg_headway_deviation_minutes") >= F.lit(3.0)),
                F.lit("Medium"),
            )
            .otherwise(F.lit("Low")),
        )
        .withColumn("record_created_at", F.current_timestamp())
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("service_date").cast("string"),
                    F.col("line_sign"),
                    F.col("gtfs_direction_id").cast("string"),
                    F.col("terminal_role"),
                    F.col("stop_id"),
                    F.coalesce(
                        F.col("missed_service_windows").cast("string"),
                        F.lit("NULL"),
                    ),
                    F.coalesce(
                        F.col("missed_service_rate").cast("string"),
                        F.lit("NULL"),
                    ),
                    F.coalesce(
                        F.col("avg_headway_deviation_minutes").cast("string"),
                        F.lit("NULL"),
                    ),
                ),
                256,
            ),
        )
        .select(
            F.col("service_date").cast("date").alias("service_date"),
            F.col("line_sign").cast("string").alias("line_sign"),
            F.col("route_id").cast("string").alias("route_id"),
            F.col("route_long_name").cast("string").alias("route_long_name"),
            F.col("route_name_display").cast("string").alias("route_name_display"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("direction_description").cast("string").alias(
                "direction_description"
            ),
            F.col("trip_headsign_reference").cast("string").alias(
                "trip_headsign_reference"
            ),
            F.col("terminal_role").cast("string").alias("terminal_role"),
            F.col("stop_id").cast("string").alias("stop_id"),
            F.col("stop_name").cast("string").alias("stop_name"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("representative_first_stop_id").cast("string").alias(
                "representative_first_stop_id"
            ),
            F.col("representative_last_stop_id").cast("string").alias(
                "representative_last_stop_id"
            ),
            F.col("endpoint_pair_trip_count").cast("long").alias(
                "endpoint_pair_trip_count"
            ),
            F.col("endpoint_pair_rank").cast("int").alias("endpoint_pair_rank"),
            F.col("planned_service_hours").cast("long").alias("planned_service_hours"),
            F.col("observed_service_hours").cast("long").alias(
                "observed_service_hours"
            ),
            F.col("missed_service_windows").cast("long").alias(
                "missed_service_windows"
            ),
            F.col("missed_service_rate").cast("double").alias("missed_service_rate"),
            F.col("avg_planned_headway_minutes").cast("double").alias(
                "avg_planned_headway_minutes"
            ),
            F.col("avg_observed_headway_avg_minutes").cast("double").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.col("avg_headway_deviation_minutes").cast("double").alias(
                "avg_headway_deviation_minutes"
            ),
            F.col("avg_headway_deviation_pct").cast("double").alias(
                "avg_headway_deviation_pct"
            ),
            F.col("terminal_risk_band").cast("string").alias("terminal_risk_band"),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
            F.col("record_created_at").cast("timestamp").alias("record_created_at"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )
    )

    return final_df
