"""
Validation pipeline for silver.silver_gtfs_stop_times.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_stop_times is acceptable for downstream planned,
    specialized, and planned-vs-observed use cases.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Profiling validation
    F. Reconciliation validation

Important:
    - Validation is different from auditing.
    - Audit tables expose duplicates, anomalies, edge cases, and suspicious patterns.
    - This validation table provides an explicit readiness view.
    - ERROR rows should be treated as blockers for downstream analytical usage.
    - WARNING rows should be treated as review signals rather than blockers.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.silver_gtfs_stop_times"
TABLE_NAME = "silver.silver_gtfs_stop_times_validation"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_stop_times. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, profiling, and reconciliation validations."
    ),
)
def silver_gtfs_stop_times_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_stop_times.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "trip_id",
        "arrival_time",
        "departure_time",
        "arrival_time_seconds",
        "departure_time_seconds",
        "is_valid_time_order",
        "stop_id",
        "stop_sequence",
        "gtfs_load_date",
    ]

    required_not_null_columns = [
        "trip_id",
        "stop_id",
        "stop_sequence",
        "gtfs_load_date",
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

    source_row_count = source_df.count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="TABLE_CONTAINS_ROWS",
        severity="WARNING",
        validation_passed=source_row_count > 0,
        failed_row_count=0 if source_row_count > 0 else 1,
        validation_message=(
            "The table contains rows."
            if source_row_count > 0
            else "The table is empty."
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

    if "trip_id" in columns:
        blank_trip_id_count = source_df.filter(
            F.col("trip_id").isNotNull() & (F.trim(F.col("trip_id")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="TRIP_ID_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_trip_id_count == 0,
            failed_row_count=blank_trip_id_count,
            validation_message=(
                "All non-null trip_id values are non-blank."
                if blank_trip_id_count == 0
                else "Some non-null trip_id values are blank."
            ),
        )

    if "stop_id" in columns:
        blank_stop_id_count = source_df.filter(
            F.col("stop_id").isNotNull() & (F.trim(F.col("stop_id")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="STOP_ID_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_stop_id_count == 0,
            failed_row_count=blank_stop_id_count,
            validation_message=(
                "All non-null stop_id values are non-blank."
                if blank_stop_id_count == 0
                else "Some non-null stop_id values are blank."
            ),
        )

    duplicate_grain_count = (
        source_df.groupBy("trip_id", "stop_sequence")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_TRIP_ID_AND_STOP_SEQUENCE_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared (trip_id, stop_sequence) grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared (trip_id, stop_sequence) grain."
        ),
    )

    invalid_stop_sequence_count = source_df.filter(
        F.col("stop_sequence") < 0
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="STOP_SEQUENCE_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=invalid_stop_sequence_count == 0,
        failed_row_count=invalid_stop_sequence_count,
        validation_message=(
            "All stop_sequence values are non-negative."
            if invalid_stop_sequence_count == 0
            else "Some stop_sequence values are negative."
        ),
    )

    negative_arrival_seconds_count = source_df.filter(
        F.col("arrival_time_seconds").isNotNull()
        & (F.col("arrival_time_seconds") < 0)
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="ARRIVAL_TIME_SECONDS_NON_NEGATIVE_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=negative_arrival_seconds_count == 0,
        failed_row_count=negative_arrival_seconds_count,
        validation_message=(
            "All non-null arrival_time_seconds values are non-negative."
            if negative_arrival_seconds_count == 0
            else "Some non-null arrival_time_seconds values are negative."
        ),
    )

    negative_departure_seconds_count = source_df.filter(
        F.col("departure_time_seconds").isNotNull()
        & (F.col("departure_time_seconds") < 0)
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="DEPARTURE_TIME_SECONDS_NON_NEGATIVE_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=negative_departure_seconds_count == 0,
        failed_row_count=negative_departure_seconds_count,
        validation_message=(
            "All non-null departure_time_seconds values are non-negative."
            if negative_departure_seconds_count == 0
            else "Some non-null departure_time_seconds values are negative."
        ),
    )

    arrival_seconds_missing_when_arrival_present_count = source_df.filter(
        F.col("arrival_time").isNotNull() & F.col("arrival_time_seconds").isNull()
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="ARRIVAL_TIME_SECONDS_PRESENT_WHEN_ARRIVAL_TIME_PRESENT",
        severity="ERROR",
        validation_passed=arrival_seconds_missing_when_arrival_present_count == 0,
        failed_row_count=arrival_seconds_missing_when_arrival_present_count,
        validation_message=(
            "arrival_time_seconds is populated whenever arrival_time is populated."
            if arrival_seconds_missing_when_arrival_present_count == 0
            else "Some rows have arrival_time populated but arrival_time_seconds missing."
        ),
    )

    departure_seconds_missing_when_departure_present_count = source_df.filter(
        F.col("departure_time").isNotNull() & F.col("departure_time_seconds").isNull()
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="DEPARTURE_TIME_SECONDS_PRESENT_WHEN_DEPARTURE_TIME_PRESENT",
        severity="ERROR",
        validation_passed=departure_seconds_missing_when_departure_present_count == 0,
        failed_row_count=departure_seconds_missing_when_departure_present_count,
        validation_message=(
            "departure_time_seconds is populated whenever departure_time is populated."
            if departure_seconds_missing_when_departure_present_count == 0
            else "Some rows have departure_time populated but departure_time_seconds missing."
        ),
    )

    arrival_time_missing_when_arrival_seconds_present_count = source_df.filter(
        F.col("arrival_time").isNull() & F.col("arrival_time_seconds").isNotNull()
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="ARRIVAL_TIME_PRESENT_WHEN_ARRIVAL_TIME_SECONDS_PRESENT",
        severity="ERROR",
        validation_passed=arrival_time_missing_when_arrival_seconds_present_count == 0,
        failed_row_count=arrival_time_missing_when_arrival_seconds_present_count,
        validation_message=(
            "arrival_time is populated whenever arrival_time_seconds is populated."
            if arrival_time_missing_when_arrival_seconds_present_count == 0
            else "Some rows have arrival_time_seconds populated but arrival_time missing."
        ),
    )

    departure_time_missing_when_departure_seconds_present_count = source_df.filter(
        F.col("departure_time").isNull() & F.col("departure_time_seconds").isNotNull()
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="DEPARTURE_TIME_PRESENT_WHEN_DEPARTURE_TIME_SECONDS_PRESENT",
        severity="ERROR",
        validation_passed=departure_time_missing_when_departure_seconds_present_count == 0,
        failed_row_count=departure_time_missing_when_departure_seconds_present_count,
        validation_message=(
            "departure_time is populated whenever departure_time_seconds is populated."
            if departure_time_missing_when_departure_seconds_present_count == 0
            else "Some rows have departure_time_seconds populated but departure_time missing."
        ),
    )

    partial_string_time_profile_count = source_df.filter(
        (F.col("arrival_time").isNull() & F.col("departure_time").isNotNull())
        | (F.col("arrival_time").isNotNull() & F.col("departure_time").isNull())
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="NO_PARTIAL_STRING_TIME_PROFILE_WITHIN_ROW",
        severity="WARNING",
        validation_passed=partial_string_time_profile_count == 0,
        failed_row_count=partial_string_time_profile_count,
        validation_message=(
            "No rows contain partial string time profiles."
            if partial_string_time_profile_count == 0
            else "Some rows contain only one of arrival_time or departure_time."
        ),
    )

    partial_seconds_profile_count = source_df.filter(
        (F.col("arrival_time_seconds").isNull() & F.col("departure_time_seconds").isNotNull())
        | (F.col("arrival_time_seconds").isNotNull() & F.col("departure_time_seconds").isNull())
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="NO_PARTIAL_SECONDS_PROFILE_WITHIN_ROW",
        severity="WARNING",
        validation_passed=partial_seconds_profile_count == 0,
        failed_row_count=partial_seconds_profile_count,
        validation_message=(
            "No rows contain partial seconds-based time profiles."
            if partial_seconds_profile_count == 0
            else "Some rows contain only one of arrival_time_seconds or departure_time_seconds."
        ),
    )

    invalid_direct_time_order_count = source_df.filter(
        F.col("arrival_time_seconds").isNotNull()
        & F.col("departure_time_seconds").isNotNull()
        & (F.col("arrival_time_seconds") > F.col("departure_time_seconds"))
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="ARRIVAL_TIME_SECONDS_LE_DEPARTURE_TIME_SECONDS_WHEN_BOTH_PRESENT",
        severity="ERROR",
        validation_passed=invalid_direct_time_order_count == 0,
        failed_row_count=invalid_direct_time_order_count,
        validation_message=(
            "arrival_time_seconds is less than or equal to departure_time_seconds whenever both are present."
            if invalid_direct_time_order_count == 0
            else "Some rows have arrival_time_seconds greater than departure_time_seconds."
        ),
    )

    invalid_time_order_flag_reconciliation_count = source_df.filter(
        F.col("arrival_time_seconds").isNotNull()
        & F.col("departure_time_seconds").isNotNull()
        & (
            F.col("is_valid_time_order")
            != (F.col("arrival_time_seconds") <= F.col("departure_time_seconds"))
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="IS_VALID_TIME_ORDER_RECONCILES_WITH_SECONDS_FIELDS",
        severity="ERROR",
        validation_passed=invalid_time_order_flag_reconciliation_count == 0,
        failed_row_count=invalid_time_order_flag_reconciliation_count,
        validation_message=(
            "is_valid_time_order reconciles with arrival_time_seconds and departure_time_seconds."
            if invalid_time_order_flag_reconciliation_count == 0
            else "Some rows have is_valid_time_order inconsistent with the seconds-based time comparison."
        ),
    )

    trip_time_window = Window.partitionBy("trip_id").orderBy("stop_sequence")

    arrival_monotonic_violation_count = (
        source_df.withColumn(
            "next_arrival_time_seconds",
            F.lead("arrival_time_seconds").over(trip_time_window),
        )
        .filter(
            F.col("arrival_time_seconds").isNotNull()
            & F.col("next_arrival_time_seconds").isNotNull()
            & (F.col("arrival_time_seconds") > F.col("next_arrival_time_seconds"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="ARRIVAL_TIME_SECONDS_MONOTONIC_WITHIN_TRIP",
        severity="ERROR",
        validation_passed=arrival_monotonic_violation_count == 0,
        failed_row_count=arrival_monotonic_violation_count,
        validation_message=(
            "arrival_time_seconds is monotonic within each trip_id."
            if arrival_monotonic_violation_count == 0
            else "Some trips have decreasing arrival_time_seconds across stop_sequence."
        ),
    )

    departure_monotonic_violation_count = (
        source_df.withColumn(
            "next_departure_time_seconds",
            F.lead("departure_time_seconds").over(trip_time_window),
        )
        .filter(
            F.col("departure_time_seconds").isNotNull()
            & F.col("next_departure_time_seconds").isNotNull()
            & (F.col("departure_time_seconds") > F.col("next_departure_time_seconds"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="DEPARTURE_TIME_SECONDS_MONOTONIC_WITHIN_TRIP",
        severity="ERROR",
        validation_passed=departure_monotonic_violation_count == 0,
        failed_row_count=departure_monotonic_violation_count,
        validation_message=(
            "departure_time_seconds is monotonic within each trip_id."
            if departure_monotonic_violation_count == 0
            else "Some trips have decreasing departure_time_seconds across stop_sequence."
        ),
    )

    single_stop_trip_count = (
        source_df.groupBy("trip_id")
        .agg(F.count(F.lit(1)).alias("stop_count"))
        .filter(F.col("stop_count") < 2)
        .count()
    )

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="TRIP_HAS_AT_LEAST_TWO_STOP_TIMES",
        severity="WARNING",
        validation_passed=single_stop_trip_count == 0,
        failed_row_count=single_stop_trip_count,
        validation_message=(
            "All trips contain at least two stop_times rows."
            if single_stop_trip_count == 0
            else "Some trips contain fewer than two stop_times rows."
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


def _append_result(
    results: list,
    validation_category: str,
    validation_rule: str,
    severity: str,
    validation_passed: bool,
    failed_row_count: int,
    validation_message: str,
) -> None:
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
