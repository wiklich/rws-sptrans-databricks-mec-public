"""
Validation pipeline for gold.gold_planned_vs_observed_service_adherence_hourly.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    gold.gold_planned_vs_observed_service_adherence_hourly is acceptable
    for downstream analytical use.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation

Important:
    - Validation is different from auditing.
    - Audit tables expose ambiguity, conflicts, and unusual patterns.
    - This validation table provides an explicit readiness view.
    - ERROR rows should be treated as blockers for formal acceptance.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_planned_vs_observed_service_adherence_hourly"
TABLE_NAME = "gold.gold_planned_vs_observed_service_adherence_hourly_validation"

OBSERVED_SOURCE = "main.silver.silver_sptrans_positions"
GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_FREQUENCIES_SOURCE = "main.silver.silver_gtfs_frequencies"
GTFS_CALENDAR_SOURCE = "main.silver.silver_gtfs_calendar"
DIRECTION_MAPPING_SOURCE = "main.gold.gold_direction_mapping_reference"

OBSERVED_EVENT_MIN_GAP_MINUTES = 15.0

ALLOWED_GTFS_DIRECTIONS = [0, 1]
ALLOWED_HOUR_MIN = 0
ALLOWED_HOUR_MAX = 23
ALLOWED_ADHERENCE_TYPE = "PROXY_BASED_SERVICE_ADHERENCE"


def _gtfs_time_to_seconds(column_name: str):
    """
    Convert GTFS HH:MM:SS-like values to total seconds.

    Supports extended GTFS hour values such as 25:30:00.
    """
    parts = F.split(F.col(column_name), ":")

    return (
        parts.getItem(0).cast("int") * F.lit(3600)
        + parts.getItem(1).cast("int") * F.lit(60)
        + parts.getItem(2).cast("int")
    )


def _build_valid_service_dates() -> DataFrame:
    """
    Build valid GTFS service_ids for the observed local dates.
    """
    observed_dates = (
        spark.read.table(OBSERVED_SOURCE)
        .select(F.col("captured_date_local").alias("service_date"))
        .filter(F.col("service_date").isNotNull())
        .distinct()
    )

    calendar = (
        spark.read.table(GTFS_CALENDAR_SOURCE)
        .select(
            F.col("service_id"),
            F.col("start_date").cast("date").alias("start_date"),
            F.col("end_date").cast("date").alias("end_date"),
            F.col("monday_active").cast("boolean").alias("monday_active"),
            F.col("tuesday_active").cast("boolean").alias("tuesday_active"),
            F.col("wednesday_active").cast("boolean").alias("wednesday_active"),
            F.col("thursday_active").cast("boolean").alias("thursday_active"),
            F.col("friday_active").cast("boolean").alias("friday_active"),
            F.col("saturday_active").cast("boolean").alias("saturday_active"),
            F.col("sunday_active").cast("boolean").alias("sunday_active"),
        )
        .filter(
            F.col("service_id").isNotNull()
            & F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
        )
    )

    weekday_active = (
        F.when(F.dayofweek(F.col("obs.service_date")) == 1, F.col("cal.sunday_active"))
        .when(F.dayofweek(F.col("obs.service_date")) == 2, F.col("cal.monday_active"))
        .when(F.dayofweek(F.col("obs.service_date")) == 3, F.col("cal.tuesday_active"))
        .when(
            F.dayofweek(F.col("obs.service_date")) == 4,
            F.col("cal.wednesday_active"),
        )
        .when(
            F.dayofweek(F.col("obs.service_date")) == 5,
            F.col("cal.thursday_active"),
        )
        .when(F.dayofweek(F.col("obs.service_date")) == 6, F.col("cal.friday_active"))
        .when(
            F.dayofweek(F.col("obs.service_date")) == 7,
            F.col("cal.saturday_active"),
        )
        .otherwise(F.lit(False))
    )

    return (
        observed_dates.alias("obs")
        .join(
            calendar.alias("cal"),
            (
                F.col("obs.service_date").between(
                    F.col("cal.start_date"),
                    F.col("cal.end_date"),
                )
            )
            & weekday_active,
            "inner",
        )
        .select(
            F.col("obs.service_date").alias("service_date"),
            F.col("cal.service_id").alias("service_id"),
        )
        .distinct()
    )


def _build_expected_planned_hourly() -> DataFrame:
    """
    Rebuild the expected planned hourly dataset for reconciliation validation.
    """
    valid_service_dates = _build_valid_service_dates()

    routes = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select(
            F.col("route_id"),
            F.col("route_short_name").alias("line_sign"),
        )
        .filter(F.col("route_id").isNotNull() & F.col("line_sign").isNotNull())
    )

    trips = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select(
            F.col("trip_id"),
            F.col("route_id"),
            F.col("service_id"),
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("service_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
    )

    frequencies = (
        spark.read.table(GTFS_FREQUENCIES_SOURCE)
        .select(
            F.col("trip_id"),
            F.col("start_time"),
            F.col("end_time"),
            F.col("headway_secs").cast("double").alias("headway_secs"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("start_time").isNotNull()
            & F.col("end_time").isNotNull()
            & F.col("headway_secs").isNotNull()
            & (F.col("headway_secs") > 0)
        )
    )

    planned_windows = (
        trips.join(routes, on="route_id", how="inner")
        .join(frequencies, on="trip_id", how="inner")
        .join(valid_service_dates, on="service_id", how="inner")
        .withColumn("start_seconds", _gtfs_time_to_seconds("start_time"))
        .withColumn("end_seconds", _gtfs_time_to_seconds("end_time"))
        .filter(F.col("end_seconds") > F.col("start_seconds"))
        .withColumn("planned_headway_minutes", F.col("headway_secs") / F.lit(60.0))
        .withColumn("service_midnight_ts", F.to_timestamp(F.col("service_date")))
        .withColumn(
            "start_ts",
            F.to_timestamp(
                F.from_unixtime(
                    F.unix_timestamp(F.col("service_midnight_ts"))
                    + F.col("start_seconds")
                )
            ),
        )
        .withColumn(
            "end_ts",
            F.to_timestamp(
                F.from_unixtime(
                    F.unix_timestamp(F.col("service_midnight_ts"))
                    + F.col("end_seconds")
                )
            ),
        )
        .withColumn(
            "hour_bucket_ts",
            F.explode(
                F.sequence(
                    F.date_trunc("hour", F.col("start_ts")),
                    F.date_trunc("hour", F.expr("end_ts - INTERVAL 1 SECOND")),
                    F.expr("INTERVAL 1 HOUR"),
                )
            ),
        )
        .select(
            F.to_date("hour_bucket_ts").alias("service_date"),
            F.col("line_sign"),
            F.col("gtfs_direction_id"),
            F.hour("hour_bucket_ts").cast("int").alias("hour"),
            F.col("planned_headway_minutes").cast("double"),
        )
    )

    return (
        planned_windows.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
        )
        .agg(F.min("planned_headway_minutes").alias("planned_headway_minutes"))
        .withColumn("planned_service_present_flag", F.lit(True))
    )


def _build_valid_direction_mapping() -> DataFrame:
    """
    Read the production-safe direction mapping reference.
    """
    return (
        spark.read.table(DIRECTION_MAPPING_SOURCE)
        .filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("review_required_flag") == F.lit(False))
        )
        .select(
            F.col("line_sign"),
            F.col("observed_direction").cast("int").alias("observed_direction"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
        )
        .dropDuplicates(["line_sign", "observed_direction"])
    )


def _build_expected_observed_hourly() -> DataFrame:
    """
    Rebuild the expected observed hourly proxy dataset for reconciliation validation.
    """
    direction_mapping = _build_valid_direction_mapping()

    observed_positions = (
        spark.read.table(OBSERVED_SOURCE)
        .select(
            F.col("captured_date_local").alias("service_date"),
            F.col("line_sign"),
            F.col("direction").cast("int").alias("observed_direction"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_id"),
            F.col("captured_at_local").alias("event_timestamp"),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & F.col("observed_direction").isNotNull()
            & F.col("vehicle_id").isNotNull()
            & F.col("event_timestamp").isNotNull()
        )
    )

    observed_mapped = observed_positions.join(
        direction_mapping,
        on=["line_sign", "observed_direction"],
        how="inner",
    )

    vehicle_window = Window.partitionBy(
        "service_date",
        "line_sign",
        "observed_direction",
        "vehicle_id",
    ).orderBy("event_timestamp")

    observed_event_candidates = (
        observed_mapped.withColumn(
            "previous_vehicle_timestamp",
            F.lag("event_timestamp").over(vehicle_window),
        )
        .withColumn(
            "minutes_since_previous_vehicle_ping",
            (
                F.col("event_timestamp").cast("long")
                - F.col("previous_vehicle_timestamp").cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "is_observed_event_proxy",
            F.when(F.col("previous_vehicle_timestamp").isNull(), F.lit(True))
            .when(
                F.col("minutes_since_previous_vehicle_ping")
                >= F.lit(OBSERVED_EVENT_MIN_GAP_MINUTES),
                F.lit(True),
            )
            .otherwise(F.lit(False)),
        )
        .filter(F.col("is_observed_event_proxy") == F.lit(True))
        .withColumn("hour", F.hour("event_timestamp").cast("int"))
    )

    headway_window = Window.partitionBy(
        "service_date",
        "line_sign",
        "gtfs_direction_id",
        "hour",
    ).orderBy("event_timestamp", "vehicle_id")

    observed_events_with_intervals = (
        observed_event_candidates.withColumn(
            "previous_event_timestamp",
            F.lag("event_timestamp").over(headway_window),
        )
        .withColumn(
            "observed_headway_avg_minutes_component",
            (
                F.col("event_timestamp").cast("long")
                - F.col("previous_event_timestamp").cast("long")
            )
            / F.lit(60.0),
        )
    )

    return (
        observed_events_with_intervals.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("observed_event_count_proxy"),
            F.countDistinct("vehicle_id").cast("long").alias("observed_vehicle_count"),
            F.avg("observed_headway_avg_minutes_component")
            .cast("double")
            .alias("observed_headway_avg_minutes"),
            F.expr("percentile_approx(observed_headway_avg_minutes_component, 0.5)")
            .cast("double")
            .alias("observed_headway_p50_minutes"),
            F.expr("percentile_approx(observed_headway_avg_minutes_component, 0.9)")
            .cast("double")
            .alias("observed_headway_p90_minutes"),
        )
        .withColumn("observed_service_present_flag", F.lit(True))
    )


def _append_result(
    results: list[dict],
    validation_category: str,
    validation_rule: str,
    severity: str,
    validation_passed: bool,
    failed_row_count: int | None,
    validation_message: str,
) -> None:
    """
    Append one validation result row to the output list.
    """
    results.append(
        {
            "source_table": SOURCE_TABLE,
            "validation_category": validation_category,
            "validation_rule": validation_rule,
            "severity": severity,
            "validation_passed": validation_passed,
            "failed_row_count": failed_row_count,
            "validation_message": validation_message,
        }
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for gold.gold_planned_vs_observed_service_adherence_hourly. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations."
    ),
)
def gold_planned_vs_observed_service_adherence_hourly_validation() -> DataFrame:
    """
    Create a formal validation table for
    gold.gold_planned_vs_observed_service_adherence_hourly.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results: list[dict] = []

    columns = set(source_df.columns)

    required_columns = [
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

    required_not_null_columns = [
        "service_date",
        "line_sign",
        "gtfs_direction_id",
        "hour",
        "planned_service_present_flag",
        "observed_service_present_flag",
        "missed_service_flag",
        "adherence_type",
        "adherence_note",
        "record_created_at",
    ]

    missing_columns = sorted(set(required_columns) - columns)

    _append_result(
        results=results,
        validation_category="STRUCTURAL",
        validation_rule="REQUIRED_COLUMNS_PRESENT",
        severity="ERROR",
        validation_passed=len(missing_columns) == 0,
        failed_row_count=len(missing_columns),
        validation_message=(
            "All required columns are present."
            if len(missing_columns) == 0
            else f"Missing required columns: {', '.join(missing_columns)}."
        ),
    )

    for column_name in required_not_null_columns:
        if column_name in columns:
            null_count = source_df.filter(F.col(column_name).isNull()).count()
            _append_result(
                results=results,
                validation_category="STRUCTURAL",
                validation_rule=f"{column_name.upper()}_NOT_NULL",
                severity="ERROR",
                validation_passed=null_count == 0,
                failed_row_count=null_count,
                validation_message=(
                    f"Column {column_name} has no null values."
                    if null_count == 0
                    else f"Column {column_name} contains null values."
                ),
            )

    if "hour" in columns:
        invalid_hour_count = source_df.filter(
            (F.col("hour") < F.lit(ALLOWED_HOUR_MIN))
            | (F.col("hour") > F.lit(ALLOWED_HOUR_MAX))
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="HOUR_ALLOWED_DOMAIN",
            severity="ERROR",
            validation_passed=invalid_hour_count == 0,
            failed_row_count=invalid_hour_count,
            validation_message=(
                "All hour values belong to the approved domain."
                if invalid_hour_count == 0
                else "Some hour values are outside the approved domain."
            ),
        )

    if "gtfs_direction_id" in columns:
        invalid_gtfs_direction_count = source_df.filter(
            ~F.col("gtfs_direction_id").isin(ALLOWED_GTFS_DIRECTIONS)
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="GTFS_DIRECTION_ID_ALLOWED_DOMAIN",
            severity="ERROR",
            validation_passed=invalid_gtfs_direction_count == 0,
            failed_row_count=invalid_gtfs_direction_count,
            validation_message=(
                "All gtfs_direction_id values belong to the approved domain."
                if invalid_gtfs_direction_count == 0
                else "Some gtfs_direction_id values are outside the approved domain."
            ),
        )

    if all(
        col in columns
        for col in ["service_date", "line_sign", "gtfs_direction_id", "hour"]
    ):
        duplicate_key_count = (
            source_df.groupBy("service_date", "line_sign", "gtfs_direction_id", "hour")
            .agg(F.count(F.lit(1)).alias("row_count"))
            .filter(F.col("row_count") > 1)
            .count()
        )

        _append_result(
            results=results,
            validation_category="GRAIN",
            validation_rule="NO_DUPLICATE_ROWS_AT_DECLARED_GRAIN",
            severity="ERROR",
            validation_passed=duplicate_key_count == 0,
            failed_row_count=duplicate_key_count,
            validation_message=(
                "No duplicate rows exist at the declared Gold grain."
                if duplicate_key_count == 0
                else "Duplicate rows exist at the declared Gold grain."
            ),
        )

    if all(col in columns for col in ["adherence_type", "adherence_note"]):
        invalid_adherence_type_count = source_df.filter(
            F.col("adherence_type") != F.lit(ALLOWED_ADHERENCE_TYPE)
        ).count()

        _append_result(
            results=results,
            validation_category="BUSINESS_RULE",
            validation_rule="ADHERENCE_TYPE_IS_PROXY_BASED_SERVICE_ADHERENCE",
            severity="ERROR",
            validation_passed=invalid_adherence_type_count == 0,
            failed_row_count=invalid_adherence_type_count,
            validation_message=(
                "All rows use the approved adherence_type."
                if invalid_adherence_type_count == 0
                else "Some rows use an unexpected adherence_type."
            ),
        )

        official_trip_delay_language_count = source_df.filter(
            F.lower(F.col("adherence_note")).contains("official trip delay")
            | F.lower(F.col("adherence_note")).contains("exact trip delay")
            | F.lower(F.col("adherence_note")).contains("official punctuality")
        ).count()

        _append_result(
            results=results,
            validation_category="BUSINESS_RULE",
            validation_rule="NO_ROW_PRESENTED_AS_OFFICIAL_TRIP_DELAY",
            severity="ERROR",
            validation_passed=official_trip_delay_language_count == 0,
            failed_row_count=official_trip_delay_language_count,
            validation_message=(
                "No row is presented as official trip delay."
                if official_trip_delay_language_count == 0
                else "Some rows contain prohibited official trip delay language."
            ),
        )

    if all(
        col in columns
        for col in [
            "missed_service_flag",
            "planned_service_present_flag",
            "observed_service_present_flag",
        ]
    ):
        missed_service_logic_violation_count = source_df.filter(
            (F.col("missed_service_flag") == F.lit(True))
            & (
                (F.col("planned_service_present_flag") != F.lit(True))
                | (F.col("observed_service_present_flag") != F.lit(False))
            )
        ).count()

        _append_result(
            results=results,
            validation_category="BUSINESS_RULE",
            validation_rule="MISSED_SERVICE_FLAG_LOGIC_IS_CONSISTENT",
            severity="ERROR",
            validation_passed=missed_service_logic_violation_count == 0,
            failed_row_count=missed_service_logic_violation_count,
            validation_message=(
                "missed_service_flag is logically consistent."
                if missed_service_logic_violation_count == 0
                else "Some rows violate missed_service_flag logic."
            ),
        )

    if "planned_headway_minutes" in columns:
        invalid_planned_headway_count = source_df.filter(
            F.col("planned_headway_minutes").isNotNull()
            & (F.col("planned_headway_minutes") <= 0)
        ).count()

        _append_result(
            results=results,
            validation_category="METRIC",
            validation_rule="PLANNED_HEADWAY_MINUTES_POSITIVE_WHEN_PRESENT",
            severity="ERROR",
            validation_passed=invalid_planned_headway_count == 0,
            failed_row_count=invalid_planned_headway_count,
            validation_message=(
                "All non-null planned_headway_minutes values are positive."
                if invalid_planned_headway_count == 0
                else "Some planned_headway_minutes values are non-positive."
            ),
        )

    if "observed_headway_avg_minutes" in columns:
        invalid_observed_headway_count = source_df.filter(
            F.col("observed_headway_avg_minutes").isNotNull()
            & (F.col("observed_headway_avg_minutes") < 0)
        ).count()

        _append_result(
            results=results,
            validation_category="METRIC",
            validation_rule="OBSERVED_HEADWAY_AVG_MINUTES_NON_NEGATIVE_WHEN_PRESENT",
            severity="ERROR",
            validation_passed=invalid_observed_headway_count == 0,
            failed_row_count=invalid_observed_headway_count,
            validation_message=(
                "All non-null observed_headway_avg_minutes values are non-negative."
                if invalid_observed_headway_count == 0
                else "Some observed_headway_avg_minutes values are negative."
            ),
        )

    if all(col in columns for col in ["planned_headway_minutes", "headway_deviation_pct"]):
        division_by_zero_protection_violation_count = source_df.filter(
            F.col("headway_deviation_pct").isNotNull()
            & (
                F.col("planned_headway_minutes").isNull()
                | (F.col("planned_headway_minutes") <= 0)
            )
        ).count()

        _append_result(
            results=results,
            validation_category="METRIC",
            validation_rule="DIVISION_BY_ZERO_PROTECTION_APPLIED_TO_HEADWAY_DEVIATION_PCT",
            severity="ERROR",
            validation_passed=division_by_zero_protection_violation_count == 0,
            failed_row_count=division_by_zero_protection_violation_count,
            validation_message=(
                "Division-by-zero protection is correctly applied to headway_deviation_pct."
                if division_by_zero_protection_violation_count == 0
                else "Some headway_deviation_pct values exist where planned_headway_minutes is null or non-positive."
            ),
        )

    if all(
        col in columns
        for col in ["observed_headway_p50_minutes", "observed_headway_p90_minutes"]
    ):
        percentile_ordering_violation_count = source_df.filter(
            F.col("observed_headway_p50_minutes").isNotNull()
            & F.col("observed_headway_p90_minutes").isNotNull()
            & (F.col("observed_headway_p50_minutes") > F.col("observed_headway_p90_minutes"))
        ).count()

        _append_result(
            results=results,
            validation_category="METRIC",
            validation_rule="PERCENTILE_ORDERING_IS_RESPECTED",
            severity="ERROR",
            validation_passed=percentile_ordering_violation_count == 0,
            failed_row_count=percentile_ordering_violation_count,
            validation_message=(
                "Observed headway percentile ordering is respected."
                if percentile_ordering_violation_count == 0
                else "Some rows violate observed headway percentile ordering."
            ),
        )

    validated_mapping_reference = (
        spark.read.table(DIRECTION_MAPPING_SOURCE)
        .filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("review_required_flag") == F.lit(False))
        )
        .select("line_sign", "gtfs_direction_id")
        .distinct()
    )

    if all(col in columns for col in ["line_sign", "gtfs_direction_id"]):
        invalid_mapping_usage_count = (
            source_df.select("line_sign", "gtfs_direction_id")
            .distinct()
            .join(
                validated_mapping_reference,
                on=["line_sign", "gtfs_direction_id"],
                how="left_anti",
            )
            .count()
        )

        _append_result(
            results=results,
            validation_category="BUSINESS_RULE",
            validation_rule="ONLY_VALIDATED_DIRECTION_MAPPINGS_USED",
            severity="ERROR",
            validation_passed=invalid_mapping_usage_count == 0,
            failed_row_count=invalid_mapping_usage_count,
            validation_message=(
                "All mapped line-direction combinations reconcile with validated direction mappings."
                if invalid_mapping_usage_count == 0
                else "Some mapped line-direction combinations do not reconcile with validated direction mappings."
            ),
        )

    expected_planned_hourly = _build_expected_planned_hourly()

    if all(
        col in columns
        for col in [
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
            "planned_headway_minutes",
            "planned_service_present_flag",
        ]
    ):
        final_planned_subset = (
            source_df.filter(F.col("planned_service_present_flag") == F.lit(True))
            .select(
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "hour",
                "planned_headway_minutes",
            )
            .distinct()
        )

        missing_or_mismatched_planned_in_final_count = (
            expected_planned_hourly.alias("exp")
            .join(
                final_planned_subset.alias("fin"),
                on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
                how="left",
            )
            .filter(
                F.col("fin.planned_headway_minutes").isNull()
                | (
                    F.col("exp.planned_headway_minutes")
                    != F.col("fin.planned_headway_minutes")
                )
            )
            .count()
        )

        unexpected_planned_in_final_count = (
            final_planned_subset.alias("fin")
            .join(
                expected_planned_hourly.alias("exp"),
                on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
                how="left",
            )
            .filter(
                F.col("exp.planned_headway_minutes").isNull()
                | (
                    F.col("exp.planned_headway_minutes")
                    != F.col("fin.planned_headway_minutes")
                )
            )
            .count()
        )

        planned_reconciliation_failed_count = (
            missing_or_mismatched_planned_in_final_count
            + unexpected_planned_in_final_count
        )

        _append_result(
            results=results,
            validation_category="RECONCILIATION",
            validation_rule="PLANNED_HOURLY_SERVICE_RECONCILES_WITH_GTFS_FREQUENCY_EXPANSION",
            severity="ERROR",
            validation_passed=planned_reconciliation_failed_count == 0,
            failed_row_count=planned_reconciliation_failed_count,
            validation_message=(
                "Planned hourly service reconciles with GTFS frequency expansion."
                if planned_reconciliation_failed_count == 0
                else "Planned hourly service does not fully reconcile with GTFS frequency expansion."
            ),
        )

    expected_observed_hourly = _build_expected_observed_hourly()

    if all(
        col in columns
        for col in [
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
            "observed_service_present_flag",
            "observed_event_count_proxy",
            "observed_vehicle_count",
        ]
    ):
        final_observed_subset = (
            source_df.filter(F.col("observed_service_present_flag") == F.lit(True))
            .select(
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "hour",
                "observed_event_count_proxy",
                "observed_vehicle_count",
            )
            .distinct()
        )

        missing_or_mismatched_observed_in_final_count = (
            expected_observed_hourly.alias("exp")
            .join(
                final_observed_subset.alias("fin"),
                on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
                how="left",
            )
            .filter(
                F.col("fin.observed_event_count_proxy").isNull()
                | F.col("fin.observed_vehicle_count").isNull()
                | (
                    F.col("exp.observed_event_count_proxy")
                    != F.col("fin.observed_event_count_proxy")
                )
                | (
                    F.col("exp.observed_vehicle_count")
                    != F.col("fin.observed_vehicle_count")
                )
            )
            .count()
        )

        unexpected_observed_in_final_count = (
            final_observed_subset.alias("fin")
            .join(
                expected_observed_hourly.alias("exp"),
                on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
                how="left",
            )
            .filter(
                F.col("exp.observed_event_count_proxy").isNull()
                | F.col("exp.observed_vehicle_count").isNull()
                | (
                    F.col("exp.observed_event_count_proxy")
                    != F.col("fin.observed_event_count_proxy")
                )
                | (
                    F.col("exp.observed_vehicle_count")
                    != F.col("fin.observed_vehicle_count")
                )
            )
            .count()
        )

        observed_reconciliation_failed_count = (
            missing_or_mismatched_observed_in_final_count
            + unexpected_observed_in_final_count
        )

        _append_result(
            results=results,
            validation_category="RECONCILIATION",
            validation_rule="HOURLY_OBSERVED_COUNTS_RECONCILE_WITH_APPROVED_OBSERVED_EVENT_PROXY",
            severity="ERROR",
            validation_passed=observed_reconciliation_failed_count == 0,
            failed_row_count=observed_reconciliation_failed_count,
            validation_message=(
                "Hourly observed counts reconcile with the approved observed event proxy logic."
                if observed_reconciliation_failed_count == 0
                else "Hourly observed counts do not fully reconcile with the approved observed event proxy logic."
            ),
        )

    validation_df = (
        spark.createDataFrame(results)
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "source_table",
            "validation_category",
            "validation_rule",
            "severity",
            "validation_passed",
            "failed_row_count",
            "validation_message",
            "validated_at",
        )
    )

    return validation_df
