"""
Validation pipeline for silver.silver_gtfs_frequencies.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_frequencies is acceptable for downstream planned,
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

SOURCE_TABLE = "silver.silver_gtfs_frequencies"
TABLE_NAME = "silver.silver_gtfs_frequencies_validation"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_frequencies. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, profiling, and reconciliation validations."
    ),
)
def silver_gtfs_frequencies_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_frequencies.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "trip_id",
        "start_time",
        "start_time_seconds",
        "start_time_minutes",
        "end_time",
        "end_time_seconds",
        "end_time_minutes",
        "service_window_seconds",
        "service_window_minutes",
        "headway_secs",
        "headway_minutes",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "trip_id",
        "start_time",
        "start_time_seconds",
        "end_time",
        "end_time_seconds",
        "headway_secs",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
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

    for column_name in ["trip_id", "start_time", "end_time", "record_hash"]:
        if column_name in columns:
            blank_count = source_df.filter(
                F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) == "")
            ).count()

            _append_result(
                results=results,
                validation_category="STRUCTURAL",
                validation_rule=f"{column_name.upper()}_NOT_BLANK",
                severity="ERROR",
                validation_passed=blank_count == 0,
                failed_row_count=blank_count,
                validation_message=(
                    f"All non-null {column_name} values are non-blank."
                    if blank_count == 0
                    else f"Some non-null {column_name} values are blank."
                ),
            )

    duplicate_grain_count = (
        source_df.groupBy("trip_id", "start_time", "end_time")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_TRIP_AND_WINDOW_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared (trip_id, start_time, end_time) grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared (trip_id, start_time, end_time) grain."
        ),
    )

    negative_start_time_seconds_count = source_df.filter(
        F.col("start_time_seconds").isNotNull()
        & (F.col("start_time_seconds") < 0)
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="START_TIME_SECONDS_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=negative_start_time_seconds_count == 0,
        failed_row_count=negative_start_time_seconds_count,
        validation_message=(
            "All non-null start_time_seconds values are non-negative."
            if negative_start_time_seconds_count == 0
            else "Some start_time_seconds values are negative."
        ),
    )

    negative_end_time_seconds_count = source_df.filter(
        F.col("end_time_seconds").isNotNull()
        & (F.col("end_time_seconds") < 0)
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="END_TIME_SECONDS_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=negative_end_time_seconds_count == 0,
        failed_row_count=negative_end_time_seconds_count,
        validation_message=(
            "All non-null end_time_seconds values are non-negative."
            if negative_end_time_seconds_count == 0
            else "Some end_time_seconds values are negative."
        ),
    )

    negative_headway_secs_count = source_df.filter(
        F.col("headway_secs").isNotNull()
        & (F.col("headway_secs") <= 0)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="HEADWAY_SECS_POSITIVE",
        severity="ERROR",
        validation_passed=negative_headway_secs_count == 0,
        failed_row_count=negative_headway_secs_count,
        validation_message=(
            "All non-null headway_secs values are positive."
            if negative_headway_secs_count == 0
            else "Some headway_secs values are zero or negative."
        ),
    )

    invalid_time_order_count = source_df.filter(
        F.col("start_time_seconds").isNotNull()
        & F.col("end_time_seconds").isNotNull()
        & (F.col("start_time_seconds") > F.col("end_time_seconds"))
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="START_TIME_SECONDS_LE_END_TIME_SECONDS",
        severity="ERROR",
        validation_passed=invalid_time_order_count == 0,
        failed_row_count=invalid_time_order_count,
        validation_message=(
            "start_time_seconds is less than or equal to end_time_seconds."
            if invalid_time_order_count == 0
            else "Some rows have start_time_seconds greater than end_time_seconds."
        ),
    )

    invalid_service_window_count = source_df.filter(
        F.col("service_window_seconds").isNotNull()
        & (F.col("service_window_seconds") <= 0)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="SERVICE_WINDOW_SECONDS_POSITIVE_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=invalid_service_window_count == 0,
        failed_row_count=invalid_service_window_count,
        validation_message=(
            "All non-null service_window_seconds values are positive."
            if invalid_service_window_count == 0
            else "Some service_window_seconds values are zero or negative."
        ),
    )

    service_window_reconciliation_count = source_df.filter(
        F.col("start_time_seconds").isNotNull()
        & F.col("end_time_seconds").isNotNull()
        & F.col("service_window_seconds").isNotNull()
        & (
            F.col("service_window_seconds")
            != (F.col("end_time_seconds") - F.col("start_time_seconds"))
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="SERVICE_WINDOW_SECONDS_RECONCILES_WITH_START_AND_END_SECONDS",
        severity="ERROR",
        validation_passed=service_window_reconciliation_count == 0,
        failed_row_count=service_window_reconciliation_count,
        validation_message=(
            "service_window_seconds reconciles with end_time_seconds - start_time_seconds."
            if service_window_reconciliation_count == 0
            else "Some service_window_seconds values do not reconcile with end_time_seconds - start_time_seconds."
        ),
    )

    service_window_minutes_reconciliation_count = source_df.filter(
        F.col("service_window_seconds").isNotNull()
        & F.col("service_window_minutes").isNotNull()
        & (
            F.abs(F.col("service_window_minutes") - (F.col("service_window_seconds") / F.lit(60.0)))
            > F.lit(0.000001)
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="SERVICE_WINDOW_MINUTES_RECONCILES_WITH_SERVICE_WINDOW_SECONDS",
        severity="ERROR",
        validation_passed=service_window_minutes_reconciliation_count == 0,
        failed_row_count=service_window_minutes_reconciliation_count,
        validation_message=(
            "service_window_minutes reconciles with service_window_seconds."
            if service_window_minutes_reconciliation_count == 0
            else "Some service_window_minutes values do not reconcile with service_window_seconds."
        ),
    )

    headway_minutes_reconciliation_count = source_df.filter(
        F.col("headway_secs").isNotNull()
        & F.col("headway_minutes").isNotNull()
        & (
            F.abs(F.col("headway_minutes") - (F.col("headway_secs") / F.lit(60.0)))
            > F.lit(0.000001)
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="HEADWAY_MINUTES_RECONCILES_WITH_HEADWAY_SECS",
        severity="ERROR",
        validation_passed=headway_minutes_reconciliation_count == 0,
        failed_row_count=headway_minutes_reconciliation_count,
        validation_message=(
            "headway_minutes reconciles with headway_secs."
            if headway_minutes_reconciliation_count == 0
            else "Some headway_minutes values do not reconcile with headway_secs."
        ),
    )

    start_time_minutes_reconciliation_count = source_df.filter(
        F.col("start_time_seconds").isNotNull()
        & F.col("start_time_minutes").isNotNull()
        & (
            F.abs(F.col("start_time_minutes") - (F.col("start_time_seconds") / F.lit(60.0)))
            > F.lit(0.000001)
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="START_TIME_MINUTES_RECONCILES_WITH_START_TIME_SECONDS",
        severity="ERROR",
        validation_passed=start_time_minutes_reconciliation_count == 0,
        failed_row_count=start_time_minutes_reconciliation_count,
        validation_message=(
            "start_time_minutes reconciles with start_time_seconds."
            if start_time_minutes_reconciliation_count == 0
            else "Some start_time_minutes values do not reconcile with start_time_seconds."
        ),
    )

    end_time_minutes_reconciliation_count = source_df.filter(
        F.col("end_time_seconds").isNotNull()
        & F.col("end_time_minutes").isNotNull()
        & (
            F.abs(F.col("end_time_minutes") - (F.col("end_time_seconds") / F.lit(60.0)))
            > F.lit(0.000001)
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="END_TIME_MINUTES_RECONCILES_WITH_END_TIME_SECONDS",
        severity="ERROR",
        validation_passed=end_time_minutes_reconciliation_count == 0,
        failed_row_count=end_time_minutes_reconciliation_count,
        validation_message=(
            "end_time_minutes reconciles with end_time_seconds."
            if end_time_minutes_reconciliation_count == 0
            else "Some end_time_minutes values do not reconcile with end_time_seconds."
        ),
    )

    headway_larger_than_window_count = source_df.filter(
        F.col("headway_secs").isNotNull()
        & F.col("service_window_seconds").isNotNull()
        & (F.col("headway_secs") > F.col("service_window_seconds"))
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="HEADWAY_SECS_NOT_GREATER_THAN_SERVICE_WINDOW_SECONDS",
        severity="WARNING",
        validation_passed=headway_larger_than_window_count == 0,
        failed_row_count=headway_larger_than_window_count,
        validation_message=(
            "No rows have headway_secs greater than service_window_seconds."
            if headway_larger_than_window_count == 0
            else "Some rows have headway_secs greater than service_window_seconds."
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
