"""
Validation pipeline for silver.silver_gtfs_shapes.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_gtfs_shapes is acceptable for downstream planned,
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
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_validation"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_gtfs_shapes. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and profiling validations."
    ),
)
def silver_gtfs_shapes_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_gtfs_shapes.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
        "shape_dist_traveled",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
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

    if "shape_id" in columns:
        blank_shape_id_count = source_df.filter(
            F.col("shape_id").isNotNull() & (F.trim(F.col("shape_id")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="SHAPE_ID_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_shape_id_count == 0,
            failed_row_count=blank_shape_id_count,
            validation_message=(
                "All non-null shape_id values are non-blank."
                if blank_shape_id_count == 0
                else "Some non-null shape_id values are blank."
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
        source_df.groupBy("shape_id", "shape_pt_sequence")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_SHAPE_ID_AND_SEQUENCE_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        failed_row_count=duplicate_grain_count,
        validation_message=(
            "No duplicate rows exist at the declared (shape_id, shape_pt_sequence) grain."
            if duplicate_grain_count == 0
            else "Duplicate rows exist at the declared (shape_id, shape_pt_sequence) grain."
        ),
    )

    invalid_latitude_count = source_df.filter(
        (F.col("shape_pt_lat") < F.lit(-90.0)) | (F.col("shape_pt_lat") > F.lit(90.0))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="SHAPE_PT_LAT_WITHIN_GLOBAL_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_latitude_count == 0,
        failed_row_count=invalid_latitude_count,
        validation_message=(
            "All shape_pt_lat values are within the global geographic domain."
            if invalid_latitude_count == 0
            else "Some shape_pt_lat values are outside the global geographic domain."
        ),
    )

    invalid_longitude_count = source_df.filter(
        (F.col("shape_pt_lon") < F.lit(-180.0)) | (F.col("shape_pt_lon") > F.lit(180.0))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="SHAPE_PT_LON_WITHIN_GLOBAL_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_longitude_count == 0,
        failed_row_count=invalid_longitude_count,
        validation_message=(
            "All shape_pt_lon values are within the global geographic domain."
            if invalid_longitude_count == 0
            else "Some shape_pt_lon values are outside the global geographic domain."
        ),
    )

    invalid_sequence_count = source_df.filter(F.col("shape_pt_sequence") < 0).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="SHAPE_PT_SEQUENCE_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=invalid_sequence_count == 0,
        failed_row_count=invalid_sequence_count,
        validation_message=(
            "All shape_pt_sequence values are non-negative."
            if invalid_sequence_count == 0
            else "Some shape_pt_sequence values are negative."
        ),
    )

    negative_distance_count = source_df.filter(
        F.col("shape_dist_traveled").isNotNull() & (F.col("shape_dist_traveled") < 0)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="SHAPE_DIST_TRAVELED_NON_NEGATIVE_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=negative_distance_count == 0,
        failed_row_count=negative_distance_count,
        validation_message=(
            "All non-null shape_dist_traveled values are non-negative."
            if negative_distance_count == 0
            else "Some non-null shape_dist_traveled values are negative."
        ),
    )

    distance_window = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")

    distance_order_violation_count = (
        source_df.withColumn(
            "next_shape_dist_traveled",
            F.lead("shape_dist_traveled").over(distance_window),
        )
        .filter(
            F.col("shape_dist_traveled").isNotNull()
            & F.col("next_shape_dist_traveled").isNotNull()
            & (F.col("shape_dist_traveled") > F.col("next_shape_dist_traveled"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="SHAPE_DIST_TRAVELED_MONOTONIC_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=distance_order_violation_count == 0,
        failed_row_count=distance_order_violation_count,
        validation_message=(
            "shape_dist_traveled is monotonic within each shape_id when present."
            if distance_order_violation_count == 0
            else "Some shape_dist_traveled values decrease within the same shape_id."
        ),
    )

    single_point_shape_count = (
        source_df.groupBy("shape_id")
        .agg(F.count(F.lit(1)).alias("point_count"))
        .filter(F.col("point_count") < 2)
        .count()
    )

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="SHAPE_HAS_AT_LEAST_TWO_POINTS",
        severity="WARNING",
        validation_passed=single_point_shape_count == 0,
        failed_row_count=single_point_shape_count,
        validation_message=(
            "All shapes contain at least two points."
            if single_point_shape_count == 0
            else "Some shapes contain fewer than two points."
        ),
    )

    partial_distance_profile_count = (
        source_df.groupBy("shape_id")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(
                F.when(F.col("shape_dist_traveled").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("non_null_distance_count"),
        )
        .filter(
            (F.col("non_null_distance_count") > 0)
            & (F.col("non_null_distance_count") < F.col("row_count"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="PROFILE",
        validation_rule="SHAPE_DIST_TRAVELED_PROFILE_CONSISTENT_WITHIN_SHAPE",
        severity="WARNING",
        validation_passed=partial_distance_profile_count == 0,
        failed_row_count=partial_distance_profile_count,
        validation_message=(
            "Each shape_id has a consistent shape_dist_traveled profile."
            if partial_distance_profile_count == 0
            else "Some shape_id values have partially populated shape_dist_traveled profiles."
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
