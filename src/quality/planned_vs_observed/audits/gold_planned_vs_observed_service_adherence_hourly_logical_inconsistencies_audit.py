"""
Audit pipeline for logical inconsistencies in
gold.gold_planned_vs_observed_service_adherence_hourly.

Purpose:
    Detect business-logic violations in the Gold planned versus observed
    service adherence table.

Audit scope:
    - planned_headway_minutes <= 0
    - observed_headway_avg_minutes < 0
    - headway_deviation_pct outside reasonable analytical bounds
    - missed_service_flag = true while observed_service_present_flag = true

Important:
    This is a non-blocking audit table.
    It exposes inconsistent rows for investigation without dropping records
    from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_planned_vs_observed_service_adherence_hourly"
TABLE_NAME = "gold.gold_planned_vs_observed_service_adherence_hourly_logical_inconsistencies_audit"

MIN_REASONABLE_HEADWAY_DEVIATION_PCT = -1.0
MAX_REASONABLE_HEADWAY_DEVIATION_PCT = 10.0


def _planned_headway_non_positive(df: DataFrame) -> DataFrame:
    """
    Detect rows where planned_headway_minutes is less than or equal to zero.
    """
    return (
        df.filter(
            F.col("planned_headway_minutes").isNotNull()
            & (F.col("planned_headway_minutes") <= 0)
        )
        .withColumn("audit_rule", F.lit("PLANNED_HEADWAY_MINUTES_NON_POSITIVE"))
        .withColumn(
            "audit_message",
            F.lit(
                "planned_headway_minutes is less than or equal to zero, "
                "which is not logically valid for planned service frequency."
            ),
        )
    )


def _observed_headway_negative(df: DataFrame) -> DataFrame:
    """
    Detect rows where observed_headway_avg_minutes is negative.
    """
    return (
        df.filter(
            F.col("observed_headway_avg_minutes").isNotNull()
            & (F.col("observed_headway_avg_minutes") < 0)
        )
        .withColumn("audit_rule", F.lit("OBSERVED_HEADWAY_AVG_MINUTES_NEGATIVE"))
        .withColumn(
            "audit_message",
            F.lit(
                "observed_headway_avg_minutes is negative, which is not logically valid."
            ),
        )
    )


def _headway_deviation_pct_out_of_bounds(df: DataFrame) -> DataFrame:
    """
    Detect rows where headway_deviation_pct is outside reasonable analytical bounds.
    """
    return (
        df.filter(
            F.col("headway_deviation_pct").isNotNull()
            & (
                (F.col("headway_deviation_pct") < F.lit(MIN_REASONABLE_HEADWAY_DEVIATION_PCT))
                | (F.col("headway_deviation_pct") > F.lit(MAX_REASONABLE_HEADWAY_DEVIATION_PCT))
            )
        )
        .withColumn("audit_rule", F.lit("HEADWAY_DEVIATION_PCT_OUT_OF_BOUNDS"))
        .withColumn(
            "audit_message",
            F.lit(
                "headway_deviation_pct is outside the configured reasonable analytical bounds."
            ),
        )
    )


def _missed_service_and_observed_present(df: DataFrame) -> DataFrame:
    """
    Detect rows where missed_service_flag is true while observed_service_present_flag is true.
    """
    return (
        df.filter(
            (F.col("missed_service_flag") == F.lit(True))
            & (F.col("observed_service_present_flag") == F.lit(True))
        )
        .withColumn(
            "audit_rule",
            F.lit("MISSED_SERVICE_TRUE_WHILE_OBSERVED_SERVICE_PRESENT_TRUE"),
        )
        .withColumn(
            "audit_message",
            F.lit(
                "missed_service_flag is true while observed_service_present_flag is also true, "
                "which is logically inconsistent."
            ),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "gold.gold_planned_vs_observed_service_adherence_hourly, including "
        "invalid planned or observed headway values, unreasonable deviation "
        "percentages, and contradictory service presence flags."
    ),
)
def gold_planned_vs_observed_service_adherence_hourly_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    Gold planned versus observed service adherence table.
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

    planned_headway_non_positive_df = _planned_headway_non_positive(source_df).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    observed_headway_negative_df = _observed_headway_negative(source_df).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    headway_deviation_pct_out_of_bounds_df = _headway_deviation_pct_out_of_bounds(
        source_df
    ).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    missed_service_and_observed_present_df = _missed_service_and_observed_present(
        source_df
    ).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    audit_df = (
        planned_headway_non_positive_df.unionByName(observed_headway_negative_df)
        .unionByName(headway_deviation_pct_out_of_bounds_df)
        .unionByName(missed_service_and_observed_present_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
