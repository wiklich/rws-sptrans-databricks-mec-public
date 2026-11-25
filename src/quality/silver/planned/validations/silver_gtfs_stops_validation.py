"""
Validation pipeline for silver.silver_gtfs_stops.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_stops is acceptable for downstream planned,
    specialized, and planned-vs-observed use cases.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Profiling validation

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

SOURCE_TABLE = "silver.silver_gtfs_stops"
TABLE_NAME = "silver.silver_gtfs_stops_validation"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_stops. "
        "Provides pass/fail results across structural, grain, metric, "
        "and profiling validations."
    ),
)
def silver_gtfs_stops_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_stops.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "stop_id",
        "stop_name",
        "stop_desc",
        "stop_lat",
        "stop_lon",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
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

    if "stop_name" in columns:
        blank_stop_name_count = source_df.filter(
            F.col("stop_name").isNotNull() & (F.trim(F.col("stop_name")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="STOP_NAME_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_stop_name_count == 0,
            failed_row_count=blank_stop_name_count,
            validation_message=(
                "All non-null stop_name values are non-blank."
                if blank_stop_name_count == 0
                else "Some non-null stop_name values are blank."
            ),
        )

    if "record_hash" in columns:
        blank_record_hash_count = source_df.filter(
            F.col("record_hash").isNotNull() & (F.trim(F.col("record_hash")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="RECORD_HASH_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_record_hash_count == 0,
            failed_row_count=blank_record_hash_count,
            validation_message=(
                "All non-null record_hash values are non-blank."
                if blank_record_hash_count == 0
                else "Some non-null record_hash values are blank."
            ),
        )

    duplicate_grain_count = (
        source_df.groupBy("stop_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_STOP_ID_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared stop_id grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared stop_id grain."
        ),
    )

    invalid_latitude_count = source_df.filter(
        (F.col("stop_lat") < F.lit(-90.0)) | (F.col("stop_lat") > F.lit(90.0))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="STOP_LAT_WITHIN_GLOBAL_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_latitude_count == 0,
        failed_row_count=invalid_latitude_count,
        validation_message=(
            "All stop_lat values are within the global geographic domain."
            if invalid_latitude_count == 0
            else "Some stop_lat values are outside the global geographic domain."
        ),
    )

    invalid_longitude_count = source_df.filter(
        (F.col("stop_lon") < F.lit(-180.0)) | (F.col("stop_lon") > F.lit(180.0))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="STOP_LON_WITHIN_GLOBAL_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_longitude_count == 0,
        failed_row_count=invalid_longitude_count,
        validation_message=(
            "All stop_lon values are within the global geographic domain."
            if invalid_longitude_count == 0
            else "Some stop_lon values are outside the global geographic domain."
        ),
    )

    redundant_stop_desc_count = source_df.filter(
        F.col("stop_desc").isNotNull()
        & (F.trim(F.col("stop_desc")) != "")
        & (F.lower(F.trim(F.col("stop_desc"))) == F.lower(F.trim(F.col("stop_name"))))
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="STOP_DESC_DOES_NOT_DUPLICATE_STOP_NAME",
        severity="WARNING",
        validation_passed=redundant_stop_desc_count == 0,
        failed_row_count=redundant_stop_desc_count,
        validation_message=(
            "stop_desc does not duplicate stop_name."
            if redundant_stop_desc_count == 0
            else "Some rows have stop_desc duplicating stop_name."
        ),
    )

    blank_stop_desc_count = source_df.filter(
        F.col("stop_desc").isNotNull() & (F.trim(F.col("stop_desc")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="STOP_DESC_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_stop_desc_count == 0,
        failed_row_count=blank_stop_desc_count,
        validation_message=(
            "All non-null stop_desc values are non-blank."
            if blank_stop_desc_count == 0
            else "Some non-null stop_desc values are blank."
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
