"""
Validation pipeline for gold.gold_route_terminal_reference.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    gold.gold_route_terminal_reference is acceptable for downstream terminal-map use.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation

Important:
    - Validation is different from auditing.
    - Audit tables expose ambiguity, conflicts, edge cases, and unusual patterns.
    - This validation table provides an explicit readiness view.
    - ERROR rows should be treated as blockers for downstream terminal-map usage.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_terminal_reference"
TABLE_NAME = "gold.gold_route_terminal_reference_validation"

GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_STOP_TIMES_SOURCE = "main.silver.silver_gtfs_stop_times"
GTFS_STOPS_SOURCE = "main.silver.silver_gtfs_stops"

APPROVED_TERMINAL_ROLES = ["ORIGIN", "DESTINATION"]
ALLOWED_GTFS_DIRECTIONS = [0, 1]


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for gold.gold_route_terminal_reference. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations for downstream terminal-map use."
    ),
)
def gold_route_terminal_reference_validation() -> DataFrame:
    """
    Create a formal validation table for gold.gold_route_terminal_reference.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
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
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_created_at",
        "record_hash",
    ]

    required_not_null_columns = [
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
        validation_description="All required columns are present in the Gold route terminal table.",
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

    if "stop_id" in columns:
        blank_stop_id_count = source_df.filter(
            F.trim(F.col("stop_id")) == ""
        ).count()

        _append_result(
            results=results,
            validation_rule="STOP_ID_NOT_BLANK",
            validation_description="stop_id must not be blank.",
            severity="ERROR",
            validation_passed=blank_stop_id_count == 0,
            violation_count=blank_stop_id_count,
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

    # ============================================================
    # B. GRAIN VALIDATION
    # ============================================================

    duplicate_grain_count = (
        source_df.groupBy("line_sign", "gtfs_direction_id", "terminal_role")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DUPLICATE_ROWS_AT_GOLD_GRAIN",
        validation_description="No duplicate rows exist at line_sign + gtfs_direction_id + terminal_role.",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        violation_count=duplicate_grain_count,
    )

    # ============================================================
    # C. BUSINESS-RULE VALIDATION
    # ============================================================

    selected_endpoint_pair_rank_inconsistent_count = source_df.filter(
        F.col("endpoint_pair_rank") != 1
    ).count()

    _append_result(
        results=results,
        validation_rule="SELECTED_ENDPOINT_PAIR_IS_RANK_ONE",
        validation_description="Selected representative endpoint pair must have endpoint_pair_rank = 1.",
        severity="ERROR",
        validation_passed=selected_endpoint_pair_rank_inconsistent_count == 0,
        violation_count=selected_endpoint_pair_rank_inconsistent_count,
    )

    selected_endpoint_pair_uniqueness_violation_count = (
        source_df.groupBy("line_sign", "gtfs_direction_id")
        .agg(
            F.countDistinct(
                F.concat_ws(
                    "||",
                    F.col("representative_first_stop_id"),
                    F.col("representative_last_stop_id"),
                )
            ).alias("distinct_endpoint_pair_count")
        )
        .filter(F.col("distinct_endpoint_pair_count") != 1)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="ENDPOINT_PAIR_UNIQUE_PER_ROUTE_DIRECTION",
        validation_description="Exactly one representative endpoint pair must exist per line_sign + gtfs_direction_id.",
        severity="ERROR",
        validation_passed=selected_endpoint_pair_uniqueness_violation_count == 0,
        violation_count=selected_endpoint_pair_uniqueness_violation_count,
    )

    expected_terminals = _build_expected_terminals()

    no_rerank_needed_count = (
        source_df.alias("g")
        .join(
            expected_terminals.alias("e"),
            on=["line_sign", "gtfs_direction_id", "terminal_role", "gtfs_load_date"],
            how="inner",
        )
        .filter(
            (F.col("g.stop_id") != F.col("e.stop_id"))
            | (
                F.col("g.representative_first_stop_id")
                != F.col("e.representative_first_stop_id")
            )
            | (
                F.col("g.representative_last_stop_id")
                != F.col("e.representative_last_stop_id")
            )
            | (
                F.col("g.endpoint_pair_trip_count")
                != F.col("e.endpoint_pair_trip_count")
            )
            | (F.col("g.endpoint_pair_rank") != F.col("e.endpoint_pair_rank"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DOWNSTREAM_RERANK_NEEDED",
        validation_description="Selected endpoint pair and terminal rows must match the deterministic upstream endpoint selection outcome.",
        severity="ERROR",
        validation_passed=no_rerank_needed_count == 0,
        violation_count=no_rerank_needed_count,
    )

    # ============================================================
    # D. METRIC VALIDATION
    # ============================================================

    invalid_endpoint_pair_trip_count = source_df.filter(
        F.col("endpoint_pair_trip_count") <= 0
    ).count()

    _append_result(
        results=results,
        validation_rule="ENDPOINT_PAIR_TRIP_COUNT_POSITIVE",
        validation_description="endpoint_pair_trip_count must be greater than 0.",
        severity="ERROR",
        validation_passed=invalid_endpoint_pair_trip_count == 0,
        violation_count=invalid_endpoint_pair_trip_count,
    )

    invalid_endpoint_pair_rank = source_df.filter(
        F.col("endpoint_pair_rank") < 1
    ).count()

    _append_result(
        results=results,
        validation_rule="ENDPOINT_PAIR_RANK_AT_LEAST_ONE",
        validation_description="endpoint_pair_rank must be greater than or equal to 1.",
        severity="ERROR",
        validation_passed=invalid_endpoint_pair_rank == 0,
        violation_count=invalid_endpoint_pair_rank,
    )

    same_first_last_stop_count = source_df.filter(
        F.col("representative_first_stop_id") == F.col("representative_last_stop_id")
    ).count()

    _append_result(
        results=results,
        validation_rule="REPRESENTATIVE_FIRST_LAST_STOP_NOT_EQUAL",
        validation_description="representative_first_stop_id and representative_last_stop_id should normally not be equal.",
        severity="WARNING",
        validation_passed=same_first_last_stop_count == 0,
        violation_count=same_first_last_stop_count,
    )

    # ============================================================
    # E. RECONCILIATION VALIDATION
    # ============================================================

    representative_terminals_reconciliation_failures = (
        source_df.alias("g")
        .join(
            expected_terminals.alias("e"),
            on=["line_sign", "gtfs_direction_id", "terminal_role", "gtfs_load_date"],
            how="full_outer",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("e.line_sign").isNull()
            | (F.col("g.stop_id") != F.col("e.stop_id"))
            | (
                F.col("g.representative_first_stop_id")
                != F.col("e.representative_first_stop_id")
            )
            | (
                F.col("g.representative_last_stop_id")
                != F.col("e.representative_last_stop_id")
            )
            | (
                F.col("g.endpoint_pair_trip_count")
                != F.col("e.endpoint_pair_trip_count")
            )
            | (F.col("g.endpoint_pair_rank") != F.col("e.endpoint_pair_rank"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="FINAL_TERMINALS_RECONCILE_WITH_UPSTREAM_ENDPOINT_ROWS",
        validation_description="Final representative terminal rows must reconcile with upstream trip endpoint logic.",
        severity="ERROR",
        validation_passed=representative_terminals_reconciliation_failures == 0,
        violation_count=representative_terminals_reconciliation_failures,
    )

    origin_destination_mapping_failures = source_df.filter(
        (
            (F.col("terminal_role") == F.lit("ORIGIN"))
            & (F.col("stop_id") != F.col("representative_first_stop_id"))
        )
        | (
            (F.col("terminal_role") == F.lit("DESTINATION"))
            & (F.col("stop_id") != F.col("representative_last_stop_id"))
        )
    ).count()

    _append_result(
        results=results,
        validation_rule="ORIGIN_DESTINATION_ROWS_RECONCILE_WITH_SELECTED_ENDPOINT_PAIR",
        validation_description="ORIGIN and DESTINATION rows must reconcile with the selected representative endpoint pair.",
        severity="ERROR",
        validation_passed=origin_destination_mapping_failures == 0,
        violation_count=origin_destination_mapping_failures,
    )

    expected_route_direction_coverage_count = (
        expected_terminals.select(
            "line_sign",
            "gtfs_direction_id",
            "gtfs_load_date",
        )
        .dropDuplicates()
        .count()
    )

    actual_route_direction_coverage_count = (
        source_df.select(
            "line_sign",
            "gtfs_direction_id",
            "gtfs_load_date",
        )
        .dropDuplicates()
        .count()
    )

    coverage_violation_count = abs(
        expected_route_direction_coverage_count - actual_route_direction_coverage_count
    )

    _append_result(
        results=results,
        validation_rule="ROUTE_DIRECTION_COVERAGE_RECONCILES_WITH_GTFS_ENDPOINT_COVERAGE",
        validation_description="Gold route-direction coverage must reconcile with available GTFS endpoint coverage.",
        severity="ERROR",
        validation_passed=coverage_violation_count == 0,
        violation_count=coverage_violation_count,
    )

    missing_gold_route_direction_terminal_count = (
        expected_terminals.select(
            "line_sign",
            "gtfs_direction_id",
            "terminal_role",
            "gtfs_load_date",
        )
        .dropDuplicates()
        .alias("e")
        .join(
            source_df.select(
                "line_sign",
                "gtfs_direction_id",
                "terminal_role",
                "gtfs_load_date",
            )
            .dropDuplicates()
            .alias("g"),
            on=["line_sign", "gtfs_direction_id", "terminal_role", "gtfs_load_date"],
            how="left_anti",
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_MISSING_GOLD_TERMINAL_FROM_EXPECTED_UPSTREAM_COVERAGE",
        validation_description="Every expected route-direction-terminal from upstream coverage must exist in Gold.",
        severity="ERROR",
        validation_passed=missing_gold_route_direction_terminal_count == 0,
        violation_count=missing_gold_route_direction_terminal_count,
    )

    distinct_terminal_role_count_violation = (
        source_df.groupBy("line_sign", "gtfs_direction_id", "gtfs_load_date")
        .agg(F.countDistinct("terminal_role").alias("distinct_terminal_role_count"))
        .filter(F.col("distinct_terminal_role_count") != 2)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="EXACTLY_TWO_TERMINAL_ROLES_PER_ROUTE_DIRECTION",
        validation_description="Each route-direction should normally contain exactly two terminal roles: ORIGIN and DESTINATION.",
        severity="WARNING",
        validation_passed=distinct_terminal_role_count_violation == 0,
        violation_count=distinct_terminal_role_count_violation,
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


def _build_latest_common_snapshot() -> DataFrame:
    """
    Build a one-row DataFrame containing the latest common GTFS snapshot date
    shared across routes, trips, stop_times, and stops.
    """
    routes_dates = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    trips_dates = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    stop_times_dates = (
        spark.read.table(GTFS_STOP_TIMES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    stops_dates = (
        spark.read.table(GTFS_STOPS_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    return (
        routes_dates.join(trips_dates, on="gtfs_load_date", how="inner")
        .join(stop_times_dates, on="gtfs_load_date", how="inner")
        .join(stops_dates, on="gtfs_load_date", how="inner")
        .agg(F.max("gtfs_load_date").alias("gtfs_load_date"))
        .filter(F.col("gtfs_load_date").isNotNull())
    )


def _build_routes_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("route_id"),
            F.col("route_short_name").alias("line_sign"),
            F.col("route_long_name"),
            F.col("route_name_display"),
            F.col("gtfs_loaded_at").alias("route_gtfs_loaded_at"),
        )
        .filter(
            F.col("route_id").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
        )
    )


def _build_trips_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("route_id"),
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("trip_headsign"),
            F.col("gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
    )


def _build_stop_times_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_STOP_TIMES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("stop_id"),
            F.col("stop_sequence").cast("int").alias("stop_sequence"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("stop_id").isNotNull()
            & F.col("stop_sequence").isNotNull()
        )
    )


def _build_stops_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_STOPS_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("stop_id"),
            F.col("stop_name"),
            F.col("stop_lat").alias("latitude"),
            F.col("stop_lon").alias("longitude"),
            F.col("gtfs_loaded_at").alias("stop_gtfs_loaded_at"),
        )
    )


def _build_route_trip_base() -> DataFrame:
    routes_latest = _build_routes_latest()
    trips_latest = _build_trips_latest()

    return routes_latest.join(
        trips_latest,
        on=["gtfs_load_date", "route_id"],
        how="inner",
    ).select(
        F.col("gtfs_load_date"),
        F.col("line_sign"),
        F.col("route_id"),
        F.col("route_long_name"),
        F.col("route_name_display"),
        F.col("trip_id"),
        F.col("gtfs_direction_id"),
        F.col("direction_description"),
        F.col("trip_headsign"),
        F.col("route_gtfs_loaded_at"),
        F.col("trip_gtfs_loaded_at"),
    )


def _build_first_trip_stops() -> DataFrame:
    stop_times_latest = _build_stop_times_latest()

    first_stop_window = Window.partitionBy("gtfs_load_date", "trip_id").orderBy(
        F.col("stop_sequence").asc()
    )

    return (
        stop_times_latest.withColumn(
            "first_stop_row_number",
            F.row_number().over(first_stop_window),
        )
        .filter(F.col("first_stop_row_number") == 1)
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("stop_id").alias("first_stop_id"),
        )
    )


def _build_last_trip_stops() -> DataFrame:
    stop_times_latest = _build_stop_times_latest()

    last_stop_window = Window.partitionBy("gtfs_load_date", "trip_id").orderBy(
        F.col("stop_sequence").desc()
    )

    return (
        stop_times_latest.withColumn(
            "last_stop_row_number",
            F.row_number().over(last_stop_window),
        )
        .filter(F.col("last_stop_row_number") == 1)
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("stop_id").alias("last_stop_id"),
        )
    )


def _build_trip_endpoints() -> DataFrame:
    route_trip_base = _build_route_trip_base()
    first_trip_stops = _build_first_trip_stops()
    last_trip_stops = _build_last_trip_stops()

    return (
        route_trip_base.join(
            first_trip_stops,
            on=["gtfs_load_date", "trip_id"],
            how="inner",
        )
        .join(
            last_trip_stops,
            on=["gtfs_load_date", "trip_id"],
            how="inner",
        )
        .select(
            F.col("gtfs_load_date"),
            F.col("line_sign"),
            F.col("route_id"),
            F.col("route_long_name"),
            F.col("route_name_display"),
            F.col("trip_id"),
            F.col("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("trip_headsign"),
            F.col("first_stop_id"),
            F.col("last_stop_id"),
            F.col("route_gtfs_loaded_at"),
            F.col("trip_gtfs_loaded_at"),
        )
    )


def _build_endpoint_pair_counts() -> DataFrame:
    trip_endpoints = _build_trip_endpoints()

    return (
        trip_endpoints.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "first_stop_id",
            "last_stop_id",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias("endpoint_pair_trip_count"),
        )
    )


def _build_endpoint_pair_reference_metadata() -> DataFrame:
    """
    Choose deterministic representative route metadata for each endpoint pair.
    """
    trip_endpoints = _build_trip_endpoints()

    pair_reference_candidates = (
        trip_endpoints.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "first_stop_id",
            "last_stop_id",
            "route_id",
            "route_long_name",
            "route_name_display",
            "direction_description",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias(
                "route_trip_count_using_endpoint_pair"
            ),
            F.min("trip_headsign").alias("trip_headsign_reference"),
            F.max("route_gtfs_loaded_at").alias("route_gtfs_loaded_at"),
            F.max("trip_gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
    )

    pair_reference_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
        "first_stop_id",
        "last_stop_id",
    ).orderBy(
        F.col("route_trip_count_using_endpoint_pair").desc(),
        F.col("route_id").asc(),
    )

    return (
        pair_reference_candidates.withColumn(
            "pair_reference_rank",
            F.row_number().over(pair_reference_window),
        )
        .filter(F.col("pair_reference_rank") == 1)
        .drop("pair_reference_rank")
        .withColumn(
            "route_trip_gtfs_loaded_at",
            F.greatest(
                F.col("route_gtfs_loaded_at"),
                F.col("trip_gtfs_loaded_at"),
            ),
        )
        .drop("route_gtfs_loaded_at", "trip_gtfs_loaded_at")
    )


def _build_representative_endpoint_pairs() -> DataFrame:
    """
    Select one representative endpoint pair per line_sign + gtfs_direction_id.
    """
    endpoint_pair_counts = _build_endpoint_pair_counts()

    pair_rank_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
    ).orderBy(
        F.col("endpoint_pair_trip_count").desc(),
        F.col("first_stop_id").asc(),
        F.col("last_stop_id").asc(),
    )

    return (
        endpoint_pair_counts.withColumn(
            "endpoint_pair_rank",
            F.row_number().over(pair_rank_window).cast("int"),
        )
        .filter(F.col("endpoint_pair_rank") == 1)
    )


def _build_expected_terminals() -> DataFrame:
    """
    Build expected representative terminals from upstream logic.
    """
    representative_pairs = _build_representative_endpoint_pairs()
    pair_reference_metadata = _build_endpoint_pair_reference_metadata()
    stops_latest = _build_stops_latest()

    selected_pairs = (
        representative_pairs.alias("pair")
        .join(
            pair_reference_metadata.alias("meta"),
            on=[
                "gtfs_load_date",
                "line_sign",
                "gtfs_direction_id",
                "first_stop_id",
                "last_stop_id",
            ],
            how="inner",
        )
        .select(
            F.col("pair.gtfs_load_date").alias("gtfs_load_date"),
            F.col("pair.line_sign").alias("line_sign"),
            F.col("meta.route_id").alias("route_id"),
            F.col("meta.route_long_name").alias("route_long_name"),
            F.col("meta.route_name_display").alias("route_name_display"),
            F.col("pair.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("meta.direction_description").alias("direction_description"),
            F.col("meta.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("pair.first_stop_id").alias("representative_first_stop_id"),
            F.col("pair.last_stop_id").alias("representative_last_stop_id"),
            F.col("pair.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("pair.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("meta.route_trip_gtfs_loaded_at").alias("route_trip_gtfs_loaded_at"),
        )
    )

    origin_terminals = (
        selected_pairs.alias("sel")
        .join(
            stops_latest.alias("stp"),
            on=[
                F.col("sel.gtfs_load_date") == F.col("stp.gtfs_load_date"),
                F.col("sel.representative_first_stop_id") == F.col("stp.stop_id"),
            ],
            how="left",
        )
        .select(
            F.col("sel.line_sign").alias("line_sign"),
            F.col("sel.route_id").alias("route_id"),
            F.col("sel.route_long_name").alias("route_long_name"),
            F.col("sel.route_name_display").alias("route_name_display"),
            F.col("sel.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("sel.direction_description").alias("direction_description"),
            F.col("sel.trip_headsign_reference").alias("trip_headsign_reference"),
            F.lit("ORIGIN").alias("terminal_role"),
            F.col("sel.representative_first_stop_id").alias("stop_id"),
            F.col("stp.stop_name").alias("stop_name"),
            F.col("stp.latitude").alias("latitude"),
            F.col("stp.longitude").alias("longitude"),
            F.col("sel.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("sel.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("sel.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("sel.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("sel.gtfs_load_date").alias("gtfs_load_date"),
            F.greatest(
                F.col("sel.route_trip_gtfs_loaded_at"),
                F.col("stp.stop_gtfs_loaded_at"),
            ).alias("gtfs_loaded_at"),
        )
    )

    destination_terminals = (
        selected_pairs.alias("sel")
        .join(
            stops_latest.alias("stp"),
            on=[
                F.col("sel.gtfs_load_date") == F.col("stp.gtfs_load_date"),
                F.col("sel.representative_last_stop_id") == F.col("stp.stop_id"),
            ],
            how="left",
        )
        .select(
            F.col("sel.line_sign").alias("line_sign"),
            F.col("sel.route_id").alias("route_id"),
            F.col("sel.route_long_name").alias("route_long_name"),
            F.col("sel.route_name_display").alias("route_name_display"),
            F.col("sel.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("sel.direction_description").alias("direction_description"),
            F.col("sel.trip_headsign_reference").alias("trip_headsign_reference"),
            F.lit("DESTINATION").alias("terminal_role"),
            F.col("sel.representative_last_stop_id").alias("stop_id"),
            F.col("stp.stop_name").alias("stop_name"),
            F.col("stp.latitude").alias("latitude"),
            F.col("stp.longitude").alias("longitude"),
            F.col("sel.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("sel.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("sel.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("sel.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("sel.gtfs_load_date").alias("gtfs_load_date"),
            F.greatest(
                F.col("sel.route_trip_gtfs_loaded_at"),
                F.col("stp.stop_gtfs_loaded_at"),
            ).alias("gtfs_loaded_at"),
        )
    )

    return origin_terminals.unionByName(destination_terminals)


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
