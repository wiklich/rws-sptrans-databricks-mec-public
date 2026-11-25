"""
Validation pipeline for silver.silver_gtfs_calendar.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_calendar is acceptable for downstream planned,
    specialized, and planned-vs-observed use cases.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Profiling validation
    E. Reconciliation validation

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

SOURCE_TABLE = "silver.silver_gtfs_calendar"
TABLE_NAME = "silver.silver_gtfs_calendar_validation"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_calendar. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "profiling, and reconciliation validations."
    ),
)
def silver_gtfs_calendar_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_calendar.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "service_id",
        "monday_active",
        "tuesday_active",
        "wednesday_active",
        "thursday_active",
        "friday_active",
        "saturday_active",
        "sunday_active",
        "start_date",
        "end_date",
        "service_duration_days",
        "active_weekdays_count",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "service_id",
        "start_date",
        "end_date",
        "service_duration_days",
        "active_weekdays_count",
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

    for column_name in ["service_id", "record_hash"]:
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
        source_df.groupBy("service_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_SERVICE_ID_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared service_id grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared service_id grain."
        ),
    )

    invalid_service_window_order_count = source_df.filter(
        F.col("start_date").isNotNull()
        & F.col("end_date").isNotNull()
        & (F.col("start_date") > F.col("end_date"))
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="START_DATE_LE_END_DATE",
        severity="ERROR",
        validation_passed=invalid_service_window_order_count == 0,
        failed_row_count=invalid_service_window_order_count,
        validation_message=(
            "start_date is less than or equal to end_date."
            if invalid_service_window_order_count == 0
            else "Some rows have start_date greater than end_date."
        ),
    )

    invalid_service_duration_count = source_df.filter(
        F.col("service_duration_days").isNotNull()
        & (F.col("service_duration_days") <= 0)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="SERVICE_DURATION_DAYS_POSITIVE",
        severity="ERROR",
        validation_passed=invalid_service_duration_count == 0,
        failed_row_count=invalid_service_duration_count,
        validation_message=(
            "All non-null service_duration_days values are positive."
            if invalid_service_duration_count == 0
            else "Some service_duration_days values are zero or negative."
        ),
    )

    weekday_count_out_of_domain = source_df.filter(
        F.col("active_weekdays_count").isNotNull()
        & (
            (F.col("active_weekdays_count") < 0)
            | (F.col("active_weekdays_count") > 5)
        )
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="ACTIVE_WEEKDAYS_COUNT_BETWEEN_0_AND_5",
        severity="ERROR",
        validation_passed=weekday_count_out_of_domain == 0,
        failed_row_count=weekday_count_out_of_domain,
        validation_message=(
            "All non-null active_weekdays_count values are between 0 and 5."
            if weekday_count_out_of_domain == 0
            else "Some active_weekdays_count values are outside the range 0 to 5."
        ),
    )

    duration_reconciliation_count = source_df.filter(
        F.col("start_date").isNotNull()
        & F.col("end_date").isNotNull()
        & F.col("service_duration_days").isNotNull()
        & (
            F.col("service_duration_days")
            != (F.datediff(F.col("end_date"), F.col("start_date")) + F.lit(1))
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="SERVICE_DURATION_DAYS_RECONCILES_WITH_START_AND_END_DATE",
        severity="ERROR",
        validation_passed=duration_reconciliation_count == 0,
        failed_row_count=duration_reconciliation_count,
        validation_message=(
            "service_duration_days reconciles with start_date and end_date."
            if duration_reconciliation_count == 0
            else "Some service_duration_days values do not reconcile with start_date and end_date."
        ),
    )

    weekday_flags_sum_expr = (
        F.when(F.col("monday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("tuesday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("wednesday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("thursday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("friday_active") == True, F.lit(1)).otherwise(F.lit(0))
    )

    active_weekdays_reconciliation_count = source_df.filter(
        F.col("active_weekdays_count").isNotNull()
        & (F.col("active_weekdays_count") != weekday_flags_sum_expr)
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="ACTIVE_WEEKDAYS_COUNT_RECONCILES_WITH_WEEKDAY_FLAGS",
        severity="ERROR",
        validation_passed=active_weekdays_reconciliation_count == 0,
        failed_row_count=active_weekdays_reconciliation_count,
        validation_message=(
            "active_weekdays_count reconciles with monday_active through friday_active."
            if active_weekdays_reconciliation_count == 0
            else "Some active_weekdays_count values do not reconcile with weekday flags."
        ),
    )

    no_active_day_count = source_df.filter(
        ~F.coalesce(F.col("monday_active"), F.lit(False))
        & ~F.coalesce(F.col("tuesday_active"), F.lit(False))
        & ~F.coalesce(F.col("wednesday_active"), F.lit(False))
        & ~F.coalesce(F.col("thursday_active"), F.lit(False))
        & ~F.coalesce(F.col("friday_active"), F.lit(False))
        & ~F.coalesce(F.col("saturday_active"), F.lit(False))
        & ~F.coalesce(F.col("sunday_active"), F.lit(False))
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="AT_LEAST_ONE_ACTIVE_DAY_IN_WEEKLY_PROFILE",
        severity="WARNING",
        validation_passed=no_active_day_count == 0,
        failed_row_count=no_active_day_count,
        validation_message=(
            "All rows have at least one active day in the weekly profile."
            if no_active_day_count == 0
            else "Some rows have no active day in the weekly profile."
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
