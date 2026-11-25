"""
Validation pipeline for silver.silver_gtfs_agency.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_agency is acceptable for downstream planned,
    specialized, and planned-vs-observed use cases.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Profiling validation

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

SOURCE_TABLE = "silver.silver_gtfs_agency"
TABLE_NAME = "silver.silver_gtfs_agency_validation"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_agency. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and profiling validations."
    ),
)
def silver_gtfs_agency_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_agency.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "agency_name",
        "agency_url",
        "agency_timezone",
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

    for column_name in ["agency_name", "agency_url", "agency_timezone", "record_hash"]:
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

    agency_id_null_count = source_df.filter(F.col("agency_id").isNull()).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="AGENCY_ID_PRESENT_COVERAGE",
        severity="WARNING",
        validation_passed=agency_id_null_count == 0,
        failed_row_count=agency_id_null_count,
        validation_message=(
            "All rows have agency_id populated."
            if agency_id_null_count == 0
            else "Some rows have agency_id missing. This may be acceptable for single-agency feeds."
        ),
    )

    duplicate_agency_id_count = (
        source_df.filter(F.col("agency_id").isNotNull())
        .groupBy("agency_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_AGENCY_ID_GRAIN_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=duplicate_agency_id_count == 0,
        failed_row_count=duplicate_agency_id_count,
        validation_message=(
            "No duplicate rows exist at the declared agency_id grain when agency_id is present."
            if duplicate_agency_id_count == 0
            else "Duplicate rows exist at the declared agency_id grain when agency_id is present."
        ),
    )

    invalid_agency_url_count = source_df.filter(
        F.col("agency_url").isNotNull()
        & ~F.col("agency_url").rlike(r"^https?://.+")
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="AGENCY_URL_VALID_HTTP_OR_HTTPS",
        severity="ERROR",
        validation_passed=invalid_agency_url_count == 0,
        failed_row_count=invalid_agency_url_count,
        validation_message=(
            "All non-null agency_url values are valid HTTP or HTTPS URLs."
            if invalid_agency_url_count == 0
            else "Some non-null agency_url values are not valid HTTP or HTTPS URLs."
        ),
    )

    blank_agency_lang_count = source_df.filter(
        F.col("agency_lang").isNotNull() & (F.trim(F.col("agency_lang")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="AGENCY_LANG_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_agency_lang_count == 0,
        failed_row_count=blank_agency_lang_count,
        validation_message=(
            "All non-null agency_lang values are non-blank."
            if blank_agency_lang_count == 0
            else "Some non-null agency_lang values are blank."
        ),
    )

    invalid_agency_lang_format_count = source_df.filter(
        F.col("agency_lang").isNotNull()
        & ~F.col("agency_lang").rlike(r"^[A-Za-z]{2}([_-][A-Za-z]{2})?$")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="AGENCY_LANG_FORMAT_VALID_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=invalid_agency_lang_format_count == 0,
        failed_row_count=invalid_agency_lang_format_count,
        validation_message=(
            "All non-null agency_lang values follow a simple ISO-like language pattern."
            if invalid_agency_lang_format_count == 0
            else "Some non-null agency_lang values do not follow a simple ISO-like language pattern."
        ),
    )

    distinct_timezone_count = source_df.filter(
        F.col("agency_timezone").isNotNull()
    ).select("agency_timezone").distinct().count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="SINGLE_AGENCY_TIMEZONE_PROFILE",
        severity="ERROR",
        validation_passed=distinct_timezone_count <= 1,
        failed_row_count=max(distinct_timezone_count - 1, 0),
        validation_message=(
            "All agencies share the same agency_timezone."
            if distinct_timezone_count <= 1
            else "Multiple distinct agency_timezone values were found."
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
