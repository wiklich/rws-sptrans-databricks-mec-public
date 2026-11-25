"""
Audit pipeline for edge cases in
gold.gold_terminal_risk_daily.

Purpose:
    Detect unusual but possible daily terminal-risk scenarios that should be monitored.

Audit scope:
    - a daily terminal row exists with very low planned service coverage
    - unusually high missed_service_rate
    - terminal rows with no matching daily adherence signal
    - origin and destination risk patterns becoming identical for long periods

Important:
    This is a non-blocking audit table.
    It exposes unusual Gold terminal risk daily cases for investigation
    without dropping records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_terminal_risk_daily"
SERVICE_ADHERENCE_SOURCE = "main.gold.gold_planned_vs_observed_service_adherence_hourly"
TABLE_NAME = "gold.gold_terminal_risk_daily_edge_cases_audit"

VERY_LOW_PLANNED_SERVICE_HOURS_THRESHOLD = 2
UNUSUALLY_HIGH_MISSED_SERVICE_RATE_THRESHOLD = 0.80
LONG_IDENTICAL_RISK_PATTERN_DAYS_THRESHOLD = 14

SOURCE_COLUMNS = [
    "service_date",
    "line_sign",
    "route_id",
    "route_long_name",
    "route_name_display",
    "gtfs_direction_id",
    "direction_description",
    "trip_headsign_reference",
    "terminal_role",
    "stop_id",
    "stop_name",
    "latitude",
    "longitude",
    "representative_first_stop_id",
    "representative_last_stop_id",
    "endpoint_pair_trip_count",
    "endpoint_pair_rank",
    "planned_service_hours",
    "observed_service_hours",
    "missed_service_windows",
    "missed_service_rate",
    "avg_planned_headway_minutes",
    "avg_observed_headway_avg_minutes",
    "avg_headway_deviation_minutes",
    "avg_headway_deviation_pct",
    "terminal_risk_band",
    "gtfs_load_date",
    "gtfs_loaded_at",
    "record_created_at",
    "record_hash",
]


def _source_projection(alias_name: str) -> list:
    """
    Build a reusable projection of source columns from the given alias.
    """
    return [
        F.col(f"{alias_name}.{column_name}").alias(column_name)
        for column_name in SOURCE_COLUMNS
    ]


def _build_daily_adherence_signal() -> DataFrame:
    """
    Build the expected daily adherence signal at:
        service_date + line_sign + gtfs_direction_id
    """
    return (
        spark.read.table(SERVICE_ADHERENCE_SOURCE)
        .select(
            F.col("service_date").cast("date").alias("service_date"),
            F.col("line_sign"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("gtfs_direction_id").isNotNull()
        )
        .groupBy("service_date", "line_sign", "gtfs_direction_id")
        .agg(F.count(F.lit(1)).cast("long").alias("daily_adherence_hour_count"))
    )


def _very_low_planned_service_coverage(df: DataFrame) -> DataFrame:
    """
    Detect daily terminal rows with very low planned service coverage.
    """
    return (
        df.filter(
            F.col("planned_service_hours").isNotNull()
            & (F.col("planned_service_hours") <= F.lit(VERY_LOW_PLANNED_SERVICE_HOURS_THRESHOLD))
        )
        .select(
            *[F.col(column_name).alias(column_name) for column_name in SOURCE_COLUMNS],
            F.col("planned_service_hours").cast("long").alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.col("planned_service_hours").cast("long").alias(
                        "planned_service_hours"
                    ),
                    F.lit(VERY_LOW_PLANNED_SERVICE_HOURS_THRESHOLD).alias(
                        "very_low_planned_service_hours_threshold"
                    ),
                )
            ).alias("edge_case_metric_values"),
            F.lit("VERY_LOW_PLANNED_SERVICE_COVERAGE").alias("audit_rule"),
            F.lit(
                "A daily terminal row exists with very low planned service coverage."
            ).alias("audit_message"),
        )
    )


def _unusually_high_missed_service_rate(df: DataFrame) -> DataFrame:
    """
    Detect daily terminal rows with unusually high missed_service_rate.
    """
    return (
        df.filter(
            F.col("missed_service_rate").isNotNull()
            & (F.col("missed_service_rate") >= F.lit(UNUSUALLY_HIGH_MISSED_SERVICE_RATE_THRESHOLD))
        )
        .select(
            *[F.col(column_name).alias(column_name) for column_name in SOURCE_COLUMNS],
            F.col("missed_service_rate").cast("double").alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.col("missed_service_rate").cast("double").alias(
                        "missed_service_rate"
                    ),
                    F.lit(UNUSUALLY_HIGH_MISSED_SERVICE_RATE_THRESHOLD).alias(
                        "unusually_high_missed_service_rate_threshold"
                    ),
                )
            ).alias("edge_case_metric_values"),
            F.lit("UNUSUALLY_HIGH_MISSED_SERVICE_RATE").alias("audit_rule"),
            F.lit(
                "A daily terminal row has an unusually high missed_service_rate."
            ).alias("audit_message"),
        )
    )


def _terminal_rows_without_matching_daily_adherence_signal(df: DataFrame) -> DataFrame:
    """
    Detect terminal rows that do not have a matching daily adherence signal.

    This should normally be zero because the Gold table is built from the
    daily adherence aggregation. It is still kept as a defensive monitoring rule.
    """
    daily_signal = _build_daily_adherence_signal()

    return (
        df.alias("src")
        .join(
            daily_signal.alias("sig"),
            on=["service_date", "line_sign", "gtfs_direction_id"],
            how="left",
        )
        .filter(F.col("sig.daily_adherence_hour_count").isNull())
        .select(
            *_source_projection("src"),
            F.lit(None).cast("long").alias("edge_case_metric_count"),
            F.lit(None).cast("string").alias("edge_case_metric_values"),
            F.lit("NO_MATCHING_DAILY_ADHERENCE_SIGNAL").alias("audit_rule"),
            F.lit(
                "A terminal risk daily row exists without a matching daily adherence "
                "signal in gold.gold_planned_vs_observed_service_adherence_hourly."
            ).alias("audit_message"),
        )
    )


def _origin_destination_identical_risk_patterns_for_long_periods(
    df: DataFrame,
) -> DataFrame:
    """
    Detect route-directions where origin and destination show identical risk
    patterns for long periods.

    Important note:
        In the current model, route-direction daily risk metrics are propagated
        to both terminal roles. Therefore identical patterns may be expected.
        This rule is treated as monitoring context rather than a hard anomaly.
    """
    origin_df = df.filter(F.col("terminal_role") == F.lit("ORIGIN")).alias("o")
    destination_df = df.filter(F.col("terminal_role") == F.lit("DESTINATION")).alias("d")

    matching_days = (
        origin_df.join(
            destination_df,
            on=[
                F.col("o.service_date") == F.col("d.service_date"),
                F.col("o.line_sign") == F.col("d.line_sign"),
                F.col("o.gtfs_direction_id") == F.col("d.gtfs_direction_id"),
            ],
            how="inner",
        )
        .filter(
            F.col("o.planned_service_hours").eqNullSafe(F.col("d.planned_service_hours"))
            & F.col("o.observed_service_hours").eqNullSafe(F.col("d.observed_service_hours"))
            & F.col("o.missed_service_windows").eqNullSafe(F.col("d.missed_service_windows"))
            & F.col("o.missed_service_rate").eqNullSafe(F.col("d.missed_service_rate"))
            & F.col("o.avg_planned_headway_minutes").eqNullSafe(
                F.col("d.avg_planned_headway_minutes")
            )
            & F.col("o.avg_observed_headway_avg_minutes").eqNullSafe(
                F.col("d.avg_observed_headway_avg_minutes")
            )
            & F.col("o.avg_headway_deviation_minutes").eqNullSafe(
                F.col("d.avg_headway_deviation_minutes")
            )
            & F.col("o.avg_headway_deviation_pct").eqNullSafe(
                F.col("d.avg_headway_deviation_pct")
            )
            & F.col("o.terminal_risk_band").eqNullSafe(F.col("d.terminal_risk_band"))
        )
        .select(
            F.col("o.service_date").alias("service_date"),
            F.col("o.line_sign").alias("line_sign"),
            F.col("o.gtfs_direction_id").alias("gtfs_direction_id"),
        )
        .dropDuplicates()
    )

    long_period_groups = (
        matching_days.groupBy("line_sign", "gtfs_direction_id")
        .agg(
            F.countDistinct("service_date").cast("long").alias("identical_risk_day_count"),
            F.min("service_date").alias("first_identical_service_date"),
            F.max("service_date").alias("last_identical_service_date"),
        )
        .filter(
            F.col("identical_risk_day_count")
            >= F.lit(LONG_IDENTICAL_RISK_PATTERN_DAYS_THRESHOLD)
        )
    )

    qualifying_days = matching_days.alias("md").join(
        long_period_groups.alias("grp"),
        on=["line_sign", "gtfs_direction_id"],
        how="inner",
    )

    return (
        df.alias("src")
        .join(
            qualifying_days.alias("q"),
            on=["service_date", "line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .select(
            *_source_projection("src"),
            F.col("q.identical_risk_day_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(
                F.struct(
                    F.col("q.identical_risk_day_count").cast("long").alias(
                        "identical_risk_day_count"
                    ),
                    F.col("q.first_identical_service_date").alias(
                        "first_identical_service_date"
                    ),
                    F.col("q.last_identical_service_date").alias(
                        "last_identical_service_date"
                    ),
                    F.lit(LONG_IDENTICAL_RISK_PATTERN_DAYS_THRESHOLD).alias(
                        "long_identical_risk_pattern_days_threshold"
                    ),
                )
            ).alias("edge_case_metric_values"),
            F.lit("LONG_IDENTICAL_ORIGIN_DESTINATION_RISK_PATTERNS").alias(
                "audit_rule"
            ),
            F.lit(
                "Origin and destination show identical risk patterns for a long period. "
                "This may be expected in the current model, but should remain visible."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "gold.gold_terminal_risk_daily, including very low planned service "
        "coverage, unusually high missed service rate, missing daily adherence "
        "signal, and long identical origin-destination risk patterns."
    ),
)
def gold_terminal_risk_daily_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for unusual but possible edge cases
    in the Gold terminal risk daily table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    very_low_planned_service_coverage_df = _very_low_planned_service_coverage(source_df)
    unusually_high_missed_service_rate_df = _unusually_high_missed_service_rate(
        source_df
    )
    no_matching_daily_adherence_signal_df = (
        _terminal_rows_without_matching_daily_adherence_signal(source_df)
    )
    long_identical_origin_destination_risk_patterns_df = (
        _origin_destination_identical_risk_patterns_for_long_periods(source_df)
    )

    audit_df = (
        very_low_planned_service_coverage_df.unionByName(
            unusually_high_missed_service_rate_df
        )
        .unionByName(no_matching_daily_adherence_signal_df)
        .unionByName(long_identical_origin_destination_risk_patterns_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
