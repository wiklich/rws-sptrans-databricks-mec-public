"""
Validation pipeline for silver.silver_gtfs_routes.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_routes is acceptable for downstream planned,
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

SOURCE_TABLE = "silver.silver_gtfs_routes"
TABLE_NAME = "silver.silver_gtfs_routes_validation"

ALLOWED_ROUTE_TYPES = [0, 1, 2, 3, 4, 5, 6, 7, 11, 12]

ROUTE_TYPE_DESCRIPTION_MAPPING = {
    0: "tram_streetcar_light_rail",
    1: "subway_metro",
    2: "rail",
    3: "bus",
    4: "ferry",
    5: "cable_tram",
    6: "aerial_lift",
    7: "funicular",
    11: "trolleybus",
    12: "monorail",
}


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_routes. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, profiling, and reconciliation validations."
    ),
)
def silver_gtfs_routes_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_routes.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_name_display",
        "route_type",
        "route_type_description",
        "route_color",
        "route_color_name",
        "route_text_color",
        "route_text_color_name",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "route_id",
        "route_type",
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

    if "route_id" in columns:
        blank_route_id_count = source_df.filter(
            F.col("route_id").isNotNull() & (F.trim(F.col("route_id")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="ROUTE_ID_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_route_id_count == 0,
            failed_row_count=blank_route_id_count,
            validation_message=(
                "All non-null route_id values are non-blank."
                if blank_route_id_count == 0
                else "Some non-null route_id values are blank."
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
        source_df.groupBy("route_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_ROUTE_ID_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared route_id grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared route_id grain."
        ),
    )

    invalid_route_type_count = source_df.filter(
        ~F.col("route_type").isin(ALLOWED_ROUTE_TYPES)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="ROUTE_TYPE_BELONGS_TO_APPROVED_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_route_type_count == 0,
        failed_row_count=invalid_route_type_count,
        validation_message=(
            "All route_type values belong to the approved domain."
            if invalid_route_type_count == 0
            else "Some route_type values are outside the approved domain."
        ),
    )

    mapping_expr = F.create_map(
        *[
            x
            for kv in ROUTE_TYPE_DESCRIPTION_MAPPING.items()
            for x in (F.lit(kv[0]), F.lit(kv[1]))
        ]
    )

    route_type_description_reconciliation_count = source_df.filter(
        F.col("route_type").isin(ALLOWED_ROUTE_TYPES)
        & (
            F.col("route_type_description")
            != mapping_expr.getItem(F.col("route_type"))
        )
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="ROUTE_TYPE_DESCRIPTION_RECONCILES_WITH_ROUTE_TYPE",
        severity="ERROR",
        validation_passed=route_type_description_reconciliation_count == 0,
        failed_row_count=route_type_description_reconciliation_count,
        validation_message=(
            "route_type_description reconciles with route_type."
            if route_type_description_reconciliation_count == 0
            else "Some route_type_description values do not reconcile with route_type."
        ),
    )

    invalid_route_color_count = source_df.filter(
        F.col("route_color").isNotNull()
        & ~F.col("route_color").rlike("^[0-9A-Fa-f]{6}$")
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="ROUTE_COLOR_VALID_HEX_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=invalid_route_color_count == 0,
        failed_row_count=invalid_route_color_count,
        validation_message=(
            "All non-null route_color values are valid hexadecimal colors."
            if invalid_route_color_count == 0
            else "Some non-null route_color values are not valid hexadecimal colors."
        ),
    )

    invalid_route_text_color_count = source_df.filter(
        F.col("route_text_color").isNotNull()
        & ~F.col("route_text_color").rlike("^[0-9A-Fa-f]{6}$")
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="ROUTE_TEXT_COLOR_VALID_HEX_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=invalid_route_text_color_count == 0,
        failed_row_count=invalid_route_text_color_count,
        validation_message=(
            "All non-null route_text_color values are valid hexadecimal colors."
            if invalid_route_text_color_count == 0
            else "Some non-null route_text_color values are not valid hexadecimal colors."
        ),
    )

    route_color_name_missing_count = source_df.filter(
        F.col("route_color").isNotNull()
        & F.col("route_color").rlike("^[0-9A-Fa-f]{6}$")
        & F.col("route_color_name").isNull()
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="ROUTE_COLOR_NAME_PRESENT_WHEN_ROUTE_COLOR_PRESENT",
        severity="WARNING",
        validation_passed=route_color_name_missing_count == 0,
        failed_row_count=route_color_name_missing_count,
        validation_message=(
            "route_color_name is populated whenever route_color is populated with a valid hex value."
            if route_color_name_missing_count == 0
            else "Some rows have valid route_color values but missing route_color_name."
        ),
    )

    route_text_color_name_missing_count = source_df.filter(
        F.col("route_text_color").isNotNull()
        & F.col("route_text_color").rlike("^[0-9A-Fa-f]{6}$")
        & F.col("route_text_color_name").isNull()
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="ROUTE_TEXT_COLOR_NAME_PRESENT_WHEN_ROUTE_TEXT_COLOR_PRESENT",
        severity="WARNING",
        validation_passed=route_text_color_name_missing_count == 0,
        failed_row_count=route_text_color_name_missing_count,
        validation_message=(
            "route_text_color_name is populated whenever route_text_color is populated with a valid hex value."
            if route_text_color_name_missing_count == 0
            else "Some rows have valid route_text_color values but missing route_text_color_name."
        ),
    )

    missing_display_name_count = source_df.filter(
        F.col("route_name_display").isNull()
        & (
            F.col("route_short_name").isNotNull()
            | F.col("route_long_name").isNotNull()
        )
    ).count()

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="ROUTE_NAME_DISPLAY_PRESENT_WHEN_ROUTE_NAME_ATTRIBUTES_EXIST",
        severity="WARNING",
        validation_passed=missing_display_name_count == 0,
        failed_row_count=missing_display_name_count,
        validation_message=(
            "route_name_display is populated whenever route_short_name or route_long_name exists."
            if missing_display_name_count == 0
            else "Some rows have route name attributes populated but missing route_name_display."
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
