"""
Validation pipeline for gold.gold_terminal_risk_daily.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    gold.gold_terminal_risk_daily is acceptable for downstream terminal-risk use.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_terminal_risk_daily"
TABLE_NAME = "gold.gold_terminal_risk_daily_validation"

TERMINAL_REFERENCE_SOURCE = "main.gold.gold_route_terminal_reference"
SERVICE_ADHERENCE_SOURCE = "main.gold.gold_planned_vs_observed_service_adherence_hourly"

APPROVED_TERMINAL_ROLES = ["ORIGIN", "DESTINATION"]
ALLOWED_GTFS_DIRECTIONS = [0, 1]


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
                F.when(
                    F.col("planned_service_present_flag") == F.lit(True),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("planned_service_hours"),
            F.sum(
                F.when(
                    F.col("observed_service_present_flag") == F.lit(True),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("observed_service_hours"),
            F.sum(
                F.when(
                    F.col("missed_service_flag") == F.lit(True),
                    F.lit(1),
                ).otherwise(F.lit(0))
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


def _build_expected_gold_rows() -> DataFrame:
    """
    Rebuild the expected Gold rows from upstream logic for reconciliation.
    """
    terminal_reference = _build_terminal_reference()
    daily_metrics = _build_daily_route_direction_metrics()

    return (
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
        .select(
            F.col("service_date"),
            F.col("line_sign"),
            F.col("route_id"),
            F.col("route_long_name"),
            F.col("route_name_display"),
            F.col("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("trip_headsign_reference"),
            F.col("terminal_role"),
            F.col("stop_id"),
            F.col("stop_name"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("representative_first_stop_id"),
            F.col("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank"),
            F.col("planned_service_hours"),
            F.col("observed_service_hours"),
            F.col("missed_service_windows"),
            F.col("missed_service_rate"),
            F.col("avg_planned_headway_minutes"),
            F.col("avg_observed_headway_avg_minutes"),
            F.col("avg_headway_deviation_minutes"),
            F.col("avg_headway_deviation_pct"),
            F.col("terminal_risk_band"),
            F.col("gtfs_load_date"),
            F.col("gtfs_loaded_at"),
        )
    )


def _append_result(
    results: list,
    validation_rule: str,
    validation_description: str,
    severity: str,
    validation_passed: bool,
    violation_count: int,
) -> None:
    results.append(
        {
            "source_table": SOURCE_TABLE,
            "validation_rule": validation_rule,
            "validation_description": validation_description,
            "severity": severity,
            "validation_status": "PASS" if validation_passed else "FAIL",
            "violation_count": int(violation_count),
        }
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for gold.gold_terminal_risk_daily. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations for downstream terminal-risk use."
    ),
)
def gold_terminal_risk_daily_validation() -> DataFrame:
    """
    Create a formal validation table for gold.gold_terminal_risk_daily.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    terminal_reference = _build_terminal_reference()
    daily_metrics = _build_daily_route_direction_metrics()
    expected_df = _build_expected_gold_rows()

    results = []
    columns = set(source_df.columns)

    required_columns = [
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

    required_not_null_columns = [
        "service_date",
        "line_sign",
        "route_id",
        "gtfs_direction_id",
        "terminal_role",
        "stop_id",
        "latitude",
        "longitude",
        "representative_first_stop_id",
        "representative_last_stop_id",
        "endpoint_pair_trip_count",
        "endpoint_pair_rank",
        "planned_service_hours",
        "observed_service_hours",
        "missed_service_windows",
        "terminal_risk_band",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_created_at",
        "record_hash",
    ]

    # ============================================================
    # A. STRUCTURAL VALIDATION
    # ============================================================

    missing_columns = sorted(set(required_columns) - columns)

    _append_result(
        results=results,
        validation_rule="REQUIRED_COLUMNS_PRESENT",
        validation_description="All required columns are present in the Gold terminal risk daily table.",
        severity="ERROR",
        validation_passed=len(missing_columns) == 0,
        violation_count=len(missing_columns),
    )

    for column_name in required_not_null_columns:
        if column_name in columns:
            null_count = source_df.filter(F.col(column_name).isNull()).count()

            _append_result(
                results=results,
                validation_rule=f"{column_name.upper()}_NOT_NULL",
                validation_description=f"Column {column_name} must not contain null values.",
                severity="ERROR",
                validation_passed=null_count == 0,
                violation_count=null_count,
            )

    if "line_sign" in columns:
        blank_line_sign_count = source_df.filter(
            F.trim(F.col("line_sign")) == ""
        ).count()

        _append_result(
            results=results,
            validation_rule="LINE_SIGN_NOT_BLANK",
            validation_description="line_sign must not be blank.",
            severity="ERROR",
            validation_passed=blank_line_sign_count == 0,
            violation_count=blank_line_sign_count,
        )

    if "terminal_role" in columns:
        invalid_terminal_role_count = source_df.filter(
            ~F.col("terminal_role").isin(APPROVED_TERMINAL_ROLES)
        ).count()

        _append_result(
            results=results,
            validation_rule="TERMINAL_ROLE_ALLOWED_DOMAIN",
            validation_description="terminal_role must belong to the approved domain.",
            severity="ERROR",
            validation_passed=invalid_terminal_role_count == 0,
            violation_count=invalid_terminal_role_count,
        )

    if "gtfs_direction_id" in columns:
        invalid_gtfs_direction_count = source_df.filter(
            ~F.col("gtfs_direction_id").isin(ALLOWED_GTFS_DIRECTIONS)
        ).count()

        _append_result(
            results=results,
            validation_rule="GTFS_DIRECTION_ID_ALLOWED_DOMAIN",
            validation_description="gtfs_direction_id must belong to the approved domain.",
            severity="ERROR",
            validation_passed=invalid_gtfs_direction_count == 0,
            violation_count=invalid_gtfs_direction_count,
        )

    if "latitude" in columns:
        null_latitude_count = source_df.filter(F.col("latitude").isNull()).count()

        _append_result(
            results=results,
            validation_rule="LATITUDE_POPULATED",
            validation_description="latitude must be populated.",
            severity="ERROR",
            validation_passed=null_latitude_count == 0,
            violation_count=null_latitude_count,
        )

    if "longitude" in columns:
        null_longitude_count = source_df.filter(F.col("longitude").isNull()).count()

        _append_result(
            results=results,
            validation_rule="LONGITUDE_POPULATED",
            validation_description="longitude must be populated.",
            severity="ERROR",
            validation_passed=null_longitude_count == 0,
            violation_count=null_longitude_count,
        )

    if "service_date" in columns:
        null_service_date_count = source_df.filter(
            F.col("service_date").isNull()
        ).count()

        _append_result(
            results=results,
            validation_rule="SERVICE_DATE_POPULATED",
            validation_description="service_date must be populated.",
            severity="ERROR",
            validation_passed=null_service_date_count == 0,
            violation_count=null_service_date_count,
        )

    # ============================================================
    # B. GRAIN VALIDATION
    # ============================================================

    duplicate_grain_count = (
        source_df.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "terminal_role",
        )
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DUPLICATE_ROWS_AT_GOLD_GRAIN",
        validation_description="No duplicate rows exist at service_date + line_sign + gtfs_direction_id + terminal_role.",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        violation_count=duplicate_grain_count,
    )

    # ============================================================
    # C. BUSINESS-RULE VALIDATION
    # ============================================================

    terminal_reference_reconciliation_count = (
        source_df.alias("g")
        .join(
            terminal_reference.alias("t"),
            on=["line_sign", "gtfs_direction_id", "terminal_role"],
            how="full_outer",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("t.line_sign").isNull()
            | ~F.col("g.stop_id").eqNullSafe(F.col("t.stop_id"))
            | ~F.col("g.route_id").eqNullSafe(F.col("t.route_id"))
            | ~F.col("g.route_long_name").eqNullSafe(F.col("t.route_long_name"))
            | ~F.col("g.route_name_display").eqNullSafe(F.col("t.route_name_display"))
            | ~F.col("g.direction_description").eqNullSafe(
                F.col("t.direction_description")
            )
            | ~F.col("g.trip_headsign_reference").eqNullSafe(
                F.col("t.trip_headsign_reference")
            )
            | ~F.col("g.latitude").eqNullSafe(F.col("t.latitude"))
            | ~F.col("g.longitude").eqNullSafe(F.col("t.longitude"))
            | ~F.col("g.representative_first_stop_id").eqNullSafe(
                F.col("t.representative_first_stop_id")
            )
            | ~F.col("g.representative_last_stop_id").eqNullSafe(
                F.col("t.representative_last_stop_id")
            )
            | ~F.col("g.endpoint_pair_trip_count").eqNullSafe(
                F.col("t.endpoint_pair_trip_count")
            )
            | ~F.col("g.endpoint_pair_rank").eqNullSafe(
                F.col("t.endpoint_pair_rank")
            )
            | ~F.col("g.gtfs_load_date").eqNullSafe(F.col("t.gtfs_load_date"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="TERMINAL_ROWS_RECONCILE_WITH_GOLD_ROUTE_TERMINAL_REFERENCE",
        validation_description="Terminal reference attributes must reconcile with gold.gold_route_terminal_reference.",
        severity="ERROR",
        validation_passed=terminal_reference_reconciliation_count == 0,
        violation_count=terminal_reference_reconciliation_count,
    )

    terminal_risk_band_mismatch_count = source_df.filter(
        ~F.col("terminal_risk_band").eqNullSafe(
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
    ).count()

    _append_result(
        results=results,
        validation_rule="TERMINAL_RISK_BAND_CONSISTENT_WITH_APPROVED_LOGIC",
        validation_description="terminal_risk_band must be consistent with the approved daily terminal risk classification logic.",
        severity="ERROR",
        validation_passed=terminal_risk_band_mismatch_count == 0,
        violation_count=terminal_risk_band_mismatch_count,
    )

    no_recompute_needed_count = (
        source_df.alias("g")
        .join(
            expected_df.alias("e"),
            on=[
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "terminal_role",
            ],
            how="full_outer",
        )
        .filter(
            F.col("g.service_date").isNull()
            | F.col("e.service_date").isNull()
            | ~F.col("g.stop_id").eqNullSafe(F.col("e.stop_id"))
            | ~F.col("g.route_id").eqNullSafe(F.col("e.route_id"))
            | ~F.col("g.route_long_name").eqNullSafe(F.col("e.route_long_name"))
            | ~F.col("g.route_name_display").eqNullSafe(F.col("e.route_name_display"))
            | ~F.col("g.direction_description").eqNullSafe(
                F.col("e.direction_description")
            )
            | ~F.col("g.trip_headsign_reference").eqNullSafe(
                F.col("e.trip_headsign_reference")
            )
            | ~F.col("g.latitude").eqNullSafe(F.col("e.latitude"))
            | ~F.col("g.longitude").eqNullSafe(F.col("e.longitude"))
            | ~F.col("g.representative_first_stop_id").eqNullSafe(
                F.col("e.representative_first_stop_id")
            )
            | ~F.col("g.representative_last_stop_id").eqNullSafe(
                F.col("e.representative_last_stop_id")
            )
            | ~F.col("g.endpoint_pair_trip_count").eqNullSafe(
                F.col("e.endpoint_pair_trip_count")
            )
            | ~F.col("g.endpoint_pair_rank").eqNullSafe(
                F.col("e.endpoint_pair_rank")
            )
            | ~F.col("g.planned_service_hours").eqNullSafe(
                F.col("e.planned_service_hours")
            )
            | ~F.col("g.observed_service_hours").eqNullSafe(
                F.col("e.observed_service_hours")
            )
            | ~F.col("g.missed_service_windows").eqNullSafe(
                F.col("e.missed_service_windows")
            )
            | ~F.col("g.missed_service_rate").eqNullSafe(
                F.col("e.missed_service_rate")
            )
            | ~F.col("g.avg_planned_headway_minutes").eqNullSafe(
                F.col("e.avg_planned_headway_minutes")
            )
            | ~F.col("g.avg_observed_headway_avg_minutes").eqNullSafe(
                F.col("e.avg_observed_headway_avg_minutes")
            )
            | ~F.col("g.avg_headway_deviation_minutes").eqNullSafe(
                F.col("e.avg_headway_deviation_minutes")
            )
            | ~F.col("g.avg_headway_deviation_pct").eqNullSafe(
                F.col("e.avg_headway_deviation_pct")
            )
            | ~F.col("g.terminal_risk_band").eqNullSafe(
                F.col("e.terminal_risk_band")
            )
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DOWNSTREAM_RECOMPUTE_NEEDED_FROM_HOURLY_ADHERENCE",
        validation_description="The Gold table must already contain the approved daily risk aggregation and classification so downstream consumers do not need to recompute daily risk from hourly adherence.",
        severity="ERROR",
        validation_passed=no_recompute_needed_count == 0,
        violation_count=no_recompute_needed_count,
    )

    # ============================================================
    # D. METRIC VALIDATION
    # ============================================================

    negative_planned_service_hours_count = source_df.filter(
        F.col("planned_service_hours") < F.lit(0)
    ).count()

    _append_result(
        results=results,
        validation_rule="PLANNED_SERVICE_HOURS_NON_NEGATIVE",
        validation_description="planned_service_hours must be greater than or equal to 0.",
        severity="ERROR",
        validation_passed=negative_planned_service_hours_count == 0,
        violation_count=negative_planned_service_hours_count,
    )

    negative_observed_service_hours_count = source_df.filter(
        F.col("observed_service_hours") < F.lit(0)
    ).count()

    _append_result(
        results=results,
        validation_rule="OBSERVED_SERVICE_HOURS_NON_NEGATIVE",
        validation_description="observed_service_hours must be greater than or equal to 0.",
        severity="ERROR",
        validation_passed=negative_observed_service_hours_count == 0,
        violation_count=negative_observed_service_hours_count,
    )

    negative_missed_service_windows_count = source_df.filter(
        F.col("missed_service_windows") < F.lit(0)
    ).count()

    _append_result(
        results=results,
        validation_rule="MISSED_SERVICE_WINDOWS_NON_NEGATIVE",
        validation_description="missed_service_windows must be greater than or equal to 0.",
        severity="ERROR",
        validation_passed=negative_missed_service_windows_count == 0,
        violation_count=negative_missed_service_windows_count,
    )

    invalid_missed_service_rate_count = source_df.filter(
        (F.col("missed_service_rate") < F.lit(0.0))
        | (F.col("missed_service_rate") > F.lit(1.0))
    ).count()

    _append_result(
        results=results,
        validation_rule="MISSED_SERVICE_RATE_WITHIN_EXPECTED_BOUNDS",
        validation_description="missed_service_rate must be within expected bounds [0, 1] when populated.",
        severity="ERROR",
        validation_passed=invalid_missed_service_rate_count == 0,
        violation_count=invalid_missed_service_rate_count,
    )

    incoherent_daily_averages_count = source_df.filter(
        (
            F.col("avg_headway_deviation_pct").isNotNull()
            & F.col("avg_headway_deviation_minutes").isNull()
        )
        | (
            F.col("avg_headway_deviation_pct").isNotNull()
            & F.col("avg_planned_headway_minutes").isNull()
        )
        | (
            F.col("avg_headway_deviation_pct").isNotNull()
            & (F.col("avg_planned_headway_minutes") <= F.lit(0.0))
        )
    ).count()

    _append_result(
        results=results,
        validation_rule="DAILY_AVERAGES_NUMERICALLY_COHERENT",
        validation_description="Daily average deviation metrics must be numerically coherent when populated.",
        severity="WARNING",
        validation_passed=incoherent_daily_averages_count == 0,
        violation_count=incoherent_daily_averages_count,
    )

    # ============================================================
    # E. RECONCILIATION VALIDATION
    # ============================================================

    daily_metrics_reconciliation_count = (
        source_df.alias("g")
        .select(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "planned_service_hours",
            "observed_service_hours",
            "missed_service_windows",
            "missed_service_rate",
            "avg_planned_headway_minutes",
            "avg_observed_headway_avg_minutes",
            "avg_headway_deviation_minutes",
            "avg_headway_deviation_pct",
        )
        .dropDuplicates()
        .join(
            daily_metrics.alias("m"),
            on=["service_date", "line_sign", "gtfs_direction_id"],
            how="full_outer",
        )
        .filter(
            F.col("g.service_date").isNull()
            | F.col("m.service_date").isNull()
            | ~F.col("g.planned_service_hours").eqNullSafe(
                F.col("m.planned_service_hours")
            )
            | ~F.col("g.observed_service_hours").eqNullSafe(
                F.col("m.observed_service_hours")
            )
            | ~F.col("g.missed_service_windows").eqNullSafe(
                F.col("m.missed_service_windows")
            )
            | ~F.col("g.missed_service_rate").eqNullSafe(
                F.col("m.missed_service_rate")
            )
            | ~F.col("g.avg_planned_headway_minutes").eqNullSafe(
                F.col("m.avg_planned_headway_minutes")
            )
            | ~F.col("g.avg_observed_headway_avg_minutes").eqNullSafe(
                F.col("m.avg_observed_headway_avg_minutes")
            )
            | ~F.col("g.avg_headway_deviation_minutes").eqNullSafe(
                F.col("m.avg_headway_deviation_minutes")
            )
            | ~F.col("g.avg_headway_deviation_pct").eqNullSafe(
                F.col("m.avg_headway_deviation_pct")
            )
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="DAILY_ROUTE_DIRECTION_METRICS_RECONCILE_WITH_HOURLY_ADHERENCE_GOLD",
        validation_description="Daily route-direction metrics must reconcile with the hourly adherence Gold aggregation.",
        severity="ERROR",
        validation_passed=daily_metrics_reconciliation_count == 0,
        violation_count=daily_metrics_reconciliation_count,
    )

    terminal_reference_attribute_reconciliation_count = (
        source_df.alias("g")
        .join(
            terminal_reference.alias("t"),
            on=["line_sign", "gtfs_direction_id", "terminal_role"],
            how="full_outer",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("t.line_sign").isNull()
            | ~F.col("g.stop_id").eqNullSafe(F.col("t.stop_id"))
            | ~F.col("g.route_id").eqNullSafe(F.col("t.route_id"))
            | ~F.col("g.route_long_name").eqNullSafe(F.col("t.route_long_name"))
            | ~F.col("g.route_name_display").eqNullSafe(F.col("t.route_name_display"))
            | ~F.col("g.direction_description").eqNullSafe(
                F.col("t.direction_description")
            )
            | ~F.col("g.trip_headsign_reference").eqNullSafe(
                F.col("t.trip_headsign_reference")
            )
            | ~F.col("g.latitude").eqNullSafe(F.col("t.latitude"))
            | ~F.col("g.longitude").eqNullSafe(F.col("t.longitude"))
            | ~F.col("g.representative_first_stop_id").eqNullSafe(
                F.col("t.representative_first_stop_id")
            )
            | ~F.col("g.representative_last_stop_id").eqNullSafe(
                F.col("t.representative_last_stop_id")
            )
            | ~F.col("g.endpoint_pair_trip_count").eqNullSafe(
                F.col("t.endpoint_pair_trip_count")
            )
            | ~F.col("g.endpoint_pair_rank").eqNullSafe(
                F.col("t.endpoint_pair_rank")
            )
            | ~F.col("g.gtfs_load_date").eqNullSafe(F.col("t.gtfs_load_date"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="TERMINAL_REFERENCE_ATTRIBUTES_RECONCILE_WITH_GOLD_ROUTE_TERMINAL_REFERENCE",
        validation_description="Terminal reference attributes must reconcile with gold.gold_route_terminal_reference.",
        severity="ERROR",
        validation_passed=terminal_reference_attribute_reconciliation_count == 0,
        violation_count=terminal_reference_attribute_reconciliation_count,
    )

    expected_route_direction_coverage_count = daily_metrics.select(
        "service_date",
        "line_sign",
        "gtfs_direction_id",
    ).dropDuplicates().count()

    actual_route_direction_coverage_count = source_df.select(
        "service_date",
        "line_sign",
        "gtfs_direction_id",
    ).dropDuplicates().count()

    coverage_violation_count = abs(
        expected_route_direction_coverage_count - actual_route_direction_coverage_count
    )

    _append_result(
        results=results,
        validation_rule="ROUTE_DIRECTION_COVERAGE_RECONCILES_WITH_AVAILABLE_DAILY_ADHERENCE_SOURCE",
        validation_description="Gold route-direction coverage must reconcile with the available daily adherence source.",
        severity="ERROR",
        validation_passed=coverage_violation_count == 0,
        violation_count=coverage_violation_count,
    )

    missing_expected_terminal_rows_count = (
        expected_df.select(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "terminal_role",
        )
        .dropDuplicates()
        .alias("e")
        .join(
            source_df.select(
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "terminal_role",
            )
            .dropDuplicates()
            .alias("g"),
            on=[
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "terminal_role",
            ],
            how="left_anti",
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_MISSING_TERMINAL_ROWS_FROM_EXPECTED_DAILY_COVERAGE",
        validation_description="Every expected terminal row derived from terminal reference and daily adherence coverage must exist in Gold.",
        severity="ERROR",
        validation_passed=missing_expected_terminal_rows_count == 0,
        violation_count=missing_expected_terminal_rows_count,
    )

    validation_df = (
        spark.createDataFrame(results)
        .withColumn("executed_at", F.current_timestamp())
        .select(
            "source_table",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "executed_at",
        )
    )

    return validation_df
