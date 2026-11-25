"""
Validation pipeline for silver.silver_gtfs_trips.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_trips is acceptable for downstream planned,
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

SOURCE_TABLE = "silver.silver_gtfs_trips"
TABLE_NAME = "silver.silver_gtfs_trips_validation"

ALLOWED_DIRECTION_IDS = [0, 1]
DIRECTION_DESCRIPTION_MAPPING = {
    0: "outbound",
    1: "inbound",
}


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_trips. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "profiling, and reconciliation validations."
    ),
)
def silver_gtfs_trips_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_trips.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "trip_short_name",
        "direction_id",
        "direction_description",
        "block_id",
        "shape_id",
        "wheelchair_accessible",
        "bikes_allowed",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "route_id",
        "service_id",
        "trip_id",
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

    for column_name in ["route_id", "service_id", "trip_id", "record_hash"]:
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
        source_df.groupBy("trip_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_TRIP_ID_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared trip_id grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared trip_id grain."
        ),
    )

    invalid_direction_id_count = source_df.filter(
        F.col("direction_id").isNotNull()
        & ~F.col("direction_id").isin(ALLOWED_DIRECTION_IDS)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="DIRECTION_ID_BELONGS_TO_APPROVED_DOMAIN_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=invalid_direction_id_count == 0,
        failed_row_count=invalid_direction_id_count,
        validation_message=(
            "All non-null direction_id values belong to the approved domain."
            if invalid_direction_id_count == 0
            else "Some non-null direction_id values are outside the approved domain."
        ),
    )

    mapping_expr = F.create_map(
        *[
            x
            for kv in DIRECTION_DESCRIPTION_MAPPING.items()
            for x in (F.lit(kv[0]), F.lit(kv[1]))
        ]
    )

    direction_description_reconciliation_count = source_df.filter(
        F.col("direction_id").isin(ALLOWED_DIRECTION_IDS)
        & (F.col("direction_description") != mapping_expr.getItem(F.col("direction_id")))
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="DIRECTION_DESCRIPTION_RECONCILES_WITH_DIRECTION_ID",
        severity="ERROR",
        validation_passed=direction_description_reconciliation_count == 0,
        failed_row_count=direction_description_reconciliation_count,
        validation_message=(
            "direction_description reconciles with direction_id."
            if direction_description_reconciliation_count == 0
            else "Some direction_description values do not reconcile with direction_id."
        ),
    )

    direction_description_missing_count = source_df.filter(
        F.col("direction_id").isin(ALLOWED_DIRECTION_IDS)
        & F.col("direction_description").isNull()
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="DIRECTION_DESCRIPTION_PRESENT_WHEN_DIRECTION_ID_PRESENT",
        severity="WARNING",
        validation_passed=direction_description_missing_count == 0,
        failed_row_count=direction_description_missing_count,
        validation_message=(
            "direction_description is populated whenever direction_id is populated."
            if direction_description_missing_count == 0
            else "Some rows have direction_id populated but direction_description missing."
        ),
    )

    blank_trip_headsign_count = source_df.filter(
        F.col("trip_headsign").isNotNull() & (F.trim(F.col("trip_headsign")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="TRIP_HEADSIGN_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_trip_headsign_count == 0,
        failed_row_count=blank_trip_headsign_count,
        validation_message=(
            "All non-null trip_headsign values are non-blank."
            if blank_trip_headsign_count == 0
            else "Some non-null trip_headsign values are blank."
        ),
    )

    blank_trip_short_name_count = source_df.filter(
        F.col("trip_short_name").isNotNull() & (F.trim(F.col("trip_short_name")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="TRIP_SHORT_NAME_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_trip_short_name_count == 0,
        failed_row_count=blank_trip_short_name_count,
        validation_message=(
            "All non-null trip_short_name values are non-blank."
            if blank_trip_short_name_count == 0
            else "Some non-null trip_short_name values are blank."
        ),
    )

    blank_shape_id_count = source_df.filter(
        F.col("shape_id").isNotNull() & (F.trim(F.col("shape_id")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="SHAPE_ID_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_shape_id_count == 0,
        failed_row_count=blank_shape_id_count,
        validation_message=(
            "All non-null shape_id values are non-blank."
            if blank_shape_id_count == 0
            else "Some non-null shape_id values are blank."
        ),
    )

    blank_block_id_count = source_df.filter(
        F.col("block_id").isNotNull() & (F.trim(F.col("block_id")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="BLOCK_ID_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_block_id_count == 0,
        failed_row_count=blank_block_id_count,
        validation_message=(
            "All non-null block_id values are non-blank."
            if blank_block_id_count == 0
            else "Some non-null block_id values are blank."
        ),
    )

    shape_id_missing_count = source_df.filter(F.col("shape_id").isNull()).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="SHAPE_ID_PRESENT_COVERAGE",
        severity="WARNING",
        validation_passed=shape_id_missing_count == 0,
        failed_row_count=shape_id_missing_count,
        validation_message=(
            "All trips have shape_id populated."
            if shape_id_missing_count == 0
            else "Some trips have shape_id missing."
        ),
    )

    blank_wheelchair_accessible_count = source_df.filter(
        F.col("wheelchair_accessible").isNotNull()
        & (F.trim(F.col("wheelchair_accessible")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="WHEELCHAIR_ACCESSIBLE_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_wheelchair_accessible_count == 0,
        failed_row_count=blank_wheelchair_accessible_count,
        validation_message=(
            "All non-null wheelchair_accessible values are non-blank."
            if blank_wheelchair_accessible_count == 0
            else "Some non-null wheelchair_accessible values are blank."
        ),
    )

    blank_bikes_allowed_count = source_df.filter(
        F.col("bikes_allowed").isNotNull()
        & (F.trim(F.col("bikes_allowed")) == "")
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="BIKES_ALLOWED_NOT_BLANK_WHEN_PRESENT",
        severity="WARNING",
        validation_passed=blank_bikes_allowed_count == 0,
        failed_row_count=blank_bikes_allowed_count,
        validation_message=(
            "All non-null bikes_allowed values are non-blank."
            if blank_bikes_allowed_count == 0
            else "Some non-null bikes_allowed values are blank."
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
