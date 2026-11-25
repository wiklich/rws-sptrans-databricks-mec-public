"""
Audit pipeline for edge cases in
gold.gold_planned_vs_observed_service_adherence_hourly.

Purpose:
    Detect unusual but possible scenarios that should be monitored in the
    Gold planned versus observed service adherence table.

Audit scope:
    - planned service exists but observed service does not
    - observed service exists but planned service does not
    - extremely low observed headway values that may indicate proxy overcount
    - extremely high observed headway values that may indicate sparse coverage
      or telemetry loss

Important:
    This is a non-blocking audit table.
    It exposes edge cases for investigation without dropping records from the
    main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_planned_vs_observed_service_adherence_hourly"
TABLE_NAME = "gold.gold_planned_vs_observed_service_adherence_hourly_edge_cases_audit"

EXTREMELY_LOW_OBSERVED_HEADWAY_MINUTES = 1.0
EXTREMELY_HIGH_OBSERVED_HEADWAY_MINUTES = 60.0


def _planned_exists_observed_missing(df: DataFrame) -> DataFrame:
    """
    Detect rows where planned service exists but observed service does not.
    """
    return (
        df.filter(
            (F.col("planned_service_present_flag") == F.lit(True))
            & (F.col("observed_service_present_flag") == F.lit(False))
        )
        .withColumn("audit_rule", F.lit("PLANNED_SERVICE_EXISTS_OBSERVED_MISSING"))
        .withColumn(
            "audit_message",
            F.lit(
                "Planned service exists for the hour, but no observed service proxy "
                "was detected."
            ),
        )
    )


def _observed_exists_planned_missing(df: DataFrame) -> DataFrame:
    """
    Detect rows where observed service exists but planned service does not.
    """
    return (
        df.filter(
            (F.col("planned_service_present_flag") == F.lit(False))
            & (F.col("observed_service_present_flag") == F.lit(True))
        )
        .withColumn("audit_rule", F.lit("OBSERVED_SERVICE_EXISTS_PLANNED_MISSING"))
        .withColumn(
            "audit_message",
            F.lit(
                "Observed service proxy exists for the hour, but no planned service "
                "was expected."
            ),
        )
    )


def _extremely_low_observed_headway(df: DataFrame) -> DataFrame:
    """
    Detect rows where observed_headway_avg_minutes is extremely low and may
    indicate proxy overcount or clustered telemetry behavior.
    """
    return (
        df.filter(
            F.col("observed_headway_avg_minutes").isNotNull()
            & (
                F.col("observed_headway_avg_minutes")
                < F.lit(EXTREMELY_LOW_OBSERVED_HEADWAY_MINUTES)
            )
        )
        .withColumn("audit_rule", F.lit("EXTREMELY_LOW_OBSERVED_HEADWAY"))
        .withColumn(
            "audit_message",
            F.lit(
                "observed_headway_avg_minutes is extremely low and may indicate "
                "proxy overcount or clustered telemetry behavior."
            ),
        )
    )


def _extremely_high_observed_headway(df: DataFrame) -> DataFrame:
    """
    Detect rows where observed_headway_avg_minutes is extremely high and may
    indicate sparse observed coverage or telemetry loss.
    """
    return (
        df.filter(
            F.col("observed_headway_avg_minutes").isNotNull()
            & (
                F.col("observed_headway_avg_minutes")
                > F.lit(EXTREMELY_HIGH_OBSERVED_HEADWAY_MINUTES)
            )
        )
        .withColumn("audit_rule", F.lit("EXTREMELY_HIGH_OBSERVED_HEADWAY"))
        .withColumn(
            "audit_message",
            F.lit(
                "observed_headway_avg_minutes is extremely high and may indicate "
                "sparse observed coverage or telemetry loss."
            ),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "gold.gold_planned_vs_observed_service_adherence_hourly, including "
        "planned-versus-observed presence mismatches and extreme observed "
        "headway values that may indicate proxy sensitivity or telemetry issues."
    ),
)
def gold_planned_vs_observed_service_adherence_hourly_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for edge cases in the Gold planned
    versus observed service adherence table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    base_columns = [
        "service_date",
        "line_sign",
        "gtfs_direction_id",
        "hour",
        "planned_headway_minutes",
        "observed_event_count_proxy",
        "observed_vehicle_count",
        "observed_headway_avg_minutes",
        "observed_headway_p50_minutes",
        "observed_headway_p90_minutes",
        "headway_deviation_minutes",
        "headway_deviation_pct",
        "planned_service_present_flag",
        "observed_service_present_flag",
        "missed_service_flag",
        "adherence_type",
        "adherence_note",
        "record_created_at",
    ]

    planned_exists_observed_missing_df = _planned_exists_observed_missing(
        source_df
    ).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    observed_exists_planned_missing_df = _observed_exists_planned_missing(
        source_df
    ).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    extremely_low_observed_headway_df = _extremely_low_observed_headway(
        source_df
    ).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    extremely_high_observed_headway_df = _extremely_high_observed_headway(
        source_df
    ).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    audit_df = (
        planned_exists_observed_missing_df.unionByName(
            observed_exists_planned_missing_df
        )
        .unionByName(extremely_low_observed_headway_df)
        .unionByName(extremely_high_observed_headway_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
