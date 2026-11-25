"""
Audit pipeline for logical inconsistencies in
gold.gold_terminal_risk_daily.

Purpose:
    Detect violations in core Gold terminal risk daily logic.

Audit scope:
    - missed_service_rate < 0
    - missed_service_rate > 1
    - planned_service_hours < observed_service_hours
    - terminal_risk_band inconsistent with the approved classification logic

Important:
    This is a non-blocking audit table.
    It exposes inconsistent Gold terminal risk daily records for investigation
    without dropping records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_terminal_risk_daily"
TABLE_NAME = "gold.gold_terminal_risk_daily_logical_inconsistencies_audit"


def _missed_service_rate_negative(df: DataFrame) -> DataFrame:
    """
    Detect rows where missed_service_rate is negative.
    """
    return (
        df.filter(F.col("missed_service_rate") < F.lit(0.0))
        .select(
            F.col("service_date").alias("service_date"),
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("planned_service_hours").alias("planned_service_hours"),
            F.col("observed_service_hours").alias("observed_service_hours"),
            F.col("missed_service_windows").alias("missed_service_windows"),
            F.col("missed_service_rate").alias("missed_service_rate"),
            F.col("avg_planned_headway_minutes").alias("avg_planned_headway_minutes"),
            F.col("avg_observed_headway_avg_minutes").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.col("avg_headway_deviation_minutes").alias(
                "avg_headway_deviation_minutes"
            ),
            F.col("avg_headway_deviation_pct").alias("avg_headway_deviation_pct"),
            F.col("terminal_risk_band").alias("terminal_risk_band"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("MISSED_SERVICE_RATE_NEGATIVE").alias("audit_rule"),
            F.lit(
                "missed_service_rate is negative, which violates the expected "
                "daily terminal risk metric bounds."
            ).alias("audit_message"),
        )
    )


def _missed_service_rate_above_one(df: DataFrame) -> DataFrame:
    """
    Detect rows where missed_service_rate is greater than 1.
    """
    return (
        df.filter(F.col("missed_service_rate") > F.lit(1.0))
        .select(
            F.col("service_date").alias("service_date"),
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("planned_service_hours").alias("planned_service_hours"),
            F.col("observed_service_hours").alias("observed_service_hours"),
            F.col("missed_service_windows").alias("missed_service_windows"),
            F.col("missed_service_rate").alias("missed_service_rate"),
            F.col("avg_planned_headway_minutes").alias("avg_planned_headway_minutes"),
            F.col("avg_observed_headway_avg_minutes").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.col("avg_headway_deviation_minutes").alias(
                "avg_headway_deviation_minutes"
            ),
            F.col("avg_headway_deviation_pct").alias("avg_headway_deviation_pct"),
            F.col("terminal_risk_band").alias("terminal_risk_band"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("MISSED_SERVICE_RATE_ABOVE_ONE").alias("audit_rule"),
            F.lit(
                "missed_service_rate is greater than 1, which violates the expected "
                "daily terminal risk metric bounds."
            ).alias("audit_message"),
        )
    )


def _planned_service_hours_below_observed_service_hours(df: DataFrame) -> DataFrame:
    """
    Detect suspicious cases where observed_service_hours exceeds planned_service_hours.
    """
    return (
        df.filter(
            F.col("planned_service_hours").isNotNull()
            & F.col("observed_service_hours").isNotNull()
            & (F.col("planned_service_hours") < F.col("observed_service_hours"))
        )
        .select(
            F.col("service_date").alias("service_date"),
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("planned_service_hours").alias("planned_service_hours"),
            F.col("observed_service_hours").alias("observed_service_hours"),
            F.col("missed_service_windows").alias("missed_service_windows"),
            F.col("missed_service_rate").alias("missed_service_rate"),
            F.col("avg_planned_headway_minutes").alias("avg_planned_headway_minutes"),
            F.col("avg_observed_headway_avg_minutes").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.col("avg_headway_deviation_minutes").alias(
                "avg_headway_deviation_minutes"
            ),
            F.col("avg_headway_deviation_pct").alias("avg_headway_deviation_pct"),
            F.col("terminal_risk_band").alias("terminal_risk_band"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("PLANNED_SERVICE_HOURS_BELOW_OBSERVED_SERVICE_HOURS").alias(
                "audit_rule"
            ),
            F.lit(
                "observed_service_hours exceeds planned_service_hours, which is a "
                "suspicious daily terminal risk condition that should be reviewed."
            ).alias("audit_message"),
        )
    )


def _terminal_risk_band_inconsistent_with_logic(df: DataFrame) -> DataFrame:
    """
    Detect rows where terminal_risk_band is inconsistent with the approved
    classification logic.

    Approved logic:
        High:
            - missed_service_windows >= 20
            - OR missed_service_rate >= 0.40
            - OR avg_headway_deviation_minutes >= 10.0

        Medium:
            - missed_service_windows >= 5
            - OR missed_service_rate >= 0.15
            - OR avg_headway_deviation_minutes >= 3.0

        Low:
            - otherwise
    """
    expected_terminal_risk_band = (
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
        .otherwise(F.lit("Low"))
    )

    return (
        df.filter(~F.col("terminal_risk_band").eqNullSafe(expected_terminal_risk_band))
        .select(
            F.col("service_date").alias("service_date"),
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("planned_service_hours").alias("planned_service_hours"),
            F.col("observed_service_hours").alias("observed_service_hours"),
            F.col("missed_service_windows").alias("missed_service_windows"),
            F.col("missed_service_rate").alias("missed_service_rate"),
            F.col("avg_planned_headway_minutes").alias("avg_planned_headway_minutes"),
            F.col("avg_observed_headway_avg_minutes").alias(
                "avg_observed_headway_avg_minutes"
            ),
            F.col("avg_headway_deviation_minutes").alias(
                "avg_headway_deviation_minutes"
            ),
            F.col("avg_headway_deviation_pct").alias("avg_headway_deviation_pct"),
            F.col("terminal_risk_band").alias("terminal_risk_band"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("TERMINAL_RISK_BAND_INCONSISTENT_WITH_LOGIC").alias("audit_rule"),
            F.lit(
                "terminal_risk_band is inconsistent with the approved daily terminal "
                "risk classification logic."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "gold.gold_terminal_risk_daily, including invalid missed_service_rate "
        "bounds, suspicious planned versus observed service hour relationships, "
        "and terminal_risk_band classification inconsistencies."
    ),
)
def gold_terminal_risk_daily_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    Gold terminal risk daily table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    missed_service_rate_negative_df = _missed_service_rate_negative(source_df)
    missed_service_rate_above_one_df = _missed_service_rate_above_one(source_df)
    planned_service_hours_below_observed_df = (
        _planned_service_hours_below_observed_service_hours(source_df)
    )
    terminal_risk_band_inconsistent_df = _terminal_risk_band_inconsistent_with_logic(
        source_df
    )

    audit_df = (
        missed_service_rate_negative_df.unionByName(missed_service_rate_above_one_df)
        .unionByName(planned_service_hours_below_observed_df)
        .unionByName(terminal_risk_band_inconsistent_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
