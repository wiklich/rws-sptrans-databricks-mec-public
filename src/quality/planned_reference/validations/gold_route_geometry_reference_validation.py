"""
Validation pipeline for gold.gold_route_geometry_reference.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    gold.gold_route_geometry_reference is acceptable for downstream route-map use.

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
    - ERROR rows should be treated as blockers for downstream route-map usage.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_geometry_reference"
HELPER_SOURCE = "main.silver.silver_helper_route_shape_points_reference"
TABLE_NAME = "gold.gold_route_geometry_reference_validation"

ALLOWED_GTFS_DIRECTIONS = [0, 1]


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for gold.gold_route_geometry_reference. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations for downstream route-map use."
    ),
)
def gold_route_geometry_reference_validation() -> DataFrame:
    """
    Create a formal validation table for gold.gold_route_geometry_reference.
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
        "representative_shape_id",
        "trip_count_using_shape",
        "shape_usage_rank",
        "shape_point_count",
        "geometry_point_count",
        "geometry_wkt",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_created_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "line_sign",
        "route_id",
        "gtfs_direction_id",
        "representative_shape_id",
        "trip_count_using_shape",
        "shape_usage_rank",
        "shape_point_count",
        "geometry_point_count",
        "geometry_wkt",
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
        validation_description="All required columns are present in the Gold route geometry table.",
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

    if "representative_shape_id" in columns:
        blank_shape_id_count = source_df.filter(
            F.trim(F.col("representative_shape_id")) == ""
        ).count()

        _append_result(
            results=results,
            validation_rule="REPRESENTATIVE_SHAPE_ID_NOT_BLANK",
            validation_description="representative_shape_id must not be blank.",
            severity="ERROR",
            validation_passed=blank_shape_id_count == 0,
            violation_count=blank_shape_id_count,
        )

    if "geometry_wkt" in columns:
        blank_geometry_wkt_count = source_df.filter(
            F.trim(F.col("geometry_wkt")) == ""
        ).count()

        _append_result(
            results=results,
            validation_rule="GEOMETRY_WKT_NOT_BLANK",
            validation_description="geometry_wkt must be populated and non-blank.",
            severity="ERROR",
            validation_passed=blank_geometry_wkt_count == 0,
            violation_count=blank_geometry_wkt_count,
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

    # ============================================================
    # B. GRAIN VALIDATION
    # ============================================================

    duplicate_grain_count = (
        source_df.groupBy("line_sign", "gtfs_direction_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DUPLICATE_ROWS_AT_GOLD_GRAIN",
        validation_description="No duplicate rows exist at line_sign + gtfs_direction_id.",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        violation_count=duplicate_grain_count,
    )

    # ============================================================
    # C. BUSINESS-RULE VALIDATION
    # ============================================================

    # Selected shape consistent with helper ranking
    inconsistent_selected_shape_rank_count = source_df.filter(
        F.col("shape_usage_rank") != 1
    ).count()

    _append_result(
        results=results,
        validation_rule="SELECTED_SHAPE_IS_RANK_ONE",
        validation_description="Selected representative shape must have shape_usage_rank = 1.",
        severity="ERROR",
        validation_passed=inconsistent_selected_shape_rank_count == 0,
        violation_count=inconsistent_selected_shape_rank_count,
    )

    # Representative shape unique per route-direction
    # This overlaps with grain, but is kept explicit as business validation
    representative_shape_uniqueness_violation_count = (
        source_df.groupBy("line_sign", "gtfs_direction_id")
        .agg(F.countDistinct("representative_shape_id").alias("distinct_shape_count"))
        .filter(F.col("distinct_shape_count") != 1)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="REPRESENTATIVE_SHAPE_UNIQUE_PER_ROUTE_DIRECTION",
        validation_description="Exactly one representative_shape_id must exist per line_sign + gtfs_direction_id.",
        severity="ERROR",
        validation_passed=representative_shape_uniqueness_violation_count == 0,
        violation_count=representative_shape_uniqueness_violation_count,
    )

    # No downstream consumer would need to re-rank candidate shapes
    # Implemented as strict reconciliation to expected representative shapes
    expected_representative_shapes = _build_expected_representative_shapes()

    re_rank_needed_count = (
        source_df.alias("g")
        .join(
            expected_representative_shapes.alias("e"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="inner",
        )
        .filter(
            F.col("g.representative_shape_id") != F.col("e.representative_shape_id")
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DOWNSTREAM_RERANK_NEEDED",
        validation_description="Selected representative shape must match the deterministic helper-based ranking outcome.",
        severity="ERROR",
        validation_passed=re_rank_needed_count == 0,
        violation_count=re_rank_needed_count,
    )

    # ============================================================
    # D. METRIC VALIDATION
    # ============================================================

    invalid_geometry_point_count = source_df.filter(
        F.col("geometry_point_count") < 2
    ).count()

    _append_result(
        results=results,
        validation_rule="GEOMETRY_POINT_COUNT_AT_LEAST_TWO",
        validation_description="geometry_point_count must be greater than or equal to 2.",
        severity="ERROR",
        validation_passed=invalid_geometry_point_count == 0,
        violation_count=invalid_geometry_point_count,
    )

    invalid_trip_count_using_shape = source_df.filter(
        F.col("trip_count_using_shape") <= 0
    ).count()

    _append_result(
        results=results,
        validation_rule="TRIP_COUNT_USING_SHAPE_POSITIVE",
        validation_description="trip_count_using_shape must be greater than 0.",
        severity="ERROR",
        validation_passed=invalid_trip_count_using_shape == 0,
        violation_count=invalid_trip_count_using_shape,
    )

    invalid_shape_usage_rank = source_df.filter(
        F.col("shape_usage_rank") < 1
    ).count()

    _append_result(
        results=results,
        validation_rule="SHAPE_USAGE_RANK_AT_LEAST_ONE",
        validation_description="shape_usage_rank must be greater than or equal to 1.",
        severity="ERROR",
        validation_passed=invalid_shape_usage_rank == 0,
        violation_count=invalid_shape_usage_rank,
    )

    invalid_shape_point_count = source_df.filter(
        F.col("shape_point_count") < 2
    ).count()

    _append_result(
        results=results,
        validation_rule="SHAPE_POINT_COUNT_AT_LEAST_TWO",
        validation_description="shape_point_count must be greater than or equal to 2.",
        severity="WARNING",
        validation_passed=invalid_shape_point_count == 0,
        violation_count=invalid_shape_point_count,
    )

    # ============================================================
    # E. RECONCILIATION VALIDATION
    # ============================================================

    helper_latest = _build_helper_latest()

    # Final representative shape reconciles with helper candidate rows
    representative_shape_reconciliation_failures = (
        source_df.alias("g")
        .join(
            expected_representative_shapes.alias("e"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="full_outer",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("e.line_sign").isNull()
            | (F.col("g.representative_shape_id") != F.col("e.representative_shape_id"))
            | (F.col("g.trip_count_using_shape") != F.col("e.trip_count_using_shape"))
            | (F.col("g.shape_usage_rank") != F.col("e.shape_usage_rank"))
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="REPRESENTATIVE_SHAPE_RECONCILES_WITH_HELPER",
        validation_description="Final representative shape and its ranking metrics must reconcile with helper candidate rows.",
        severity="ERROR",
        validation_passed=representative_shape_reconciliation_failures == 0,
        violation_count=representative_shape_reconciliation_failures,
    )

    # Geometry point count reconciles with selected helper points
    helper_selected_point_counts = (
        helper_latest.alias("h")
        .join(
            expected_representative_shapes.alias("e"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="inner",
        )
        .filter(F.col("h.shape_id") == F.col("e.representative_shape_id"))
        .groupBy(
            F.col("h.line_sign").alias("line_sign"),
            F.col("h.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("h.gtfs_load_date").alias("gtfs_load_date"),
        )
        .agg(F.count(F.lit(1)).cast("long").alias("expected_geometry_point_count"))
    )

    geometry_point_count_mismatch = (
        source_df.alias("g")
        .join(
            helper_selected_point_counts.alias("e"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="full_outer",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("e.line_sign").isNull()
            | (
                F.col("g.geometry_point_count")
                != F.col("e.expected_geometry_point_count")
            )
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="GEOMETRY_POINT_COUNT_RECONCILES_WITH_HELPER_POINTS",
        validation_description="geometry_point_count must reconcile with the count of helper points for the selected representative shape.",
        severity="ERROR",
        validation_passed=geometry_point_count_mismatch == 0,
        violation_count=geometry_point_count_mismatch,
    )

    # Route-direction coverage reconciles with available helper coverage
    expected_coverage_count = (
        expected_representative_shapes.select(
            "line_sign",
            "gtfs_direction_id",
            "gtfs_load_date",
        )
        .dropDuplicates()
        .count()
    )

    actual_coverage_count = (
        source_df.select(
            "line_sign",
            "gtfs_direction_id",
            "gtfs_load_date",
        )
        .dropDuplicates()
        .count()
    )

    coverage_violation_count = abs(expected_coverage_count - actual_coverage_count)

    _append_result(
        results=results,
        validation_rule="ROUTE_DIRECTION_COVERAGE_RECONCILES_WITH_HELPER",
        validation_description="Gold route-direction coverage must reconcile with the available helper coverage.",
        severity="ERROR",
        validation_passed=coverage_violation_count == 0,
        violation_count=coverage_violation_count,
    )

    missing_gold_coverage_count = (
        expected_representative_shapes.select(
            "line_sign",
            "gtfs_direction_id",
            "gtfs_load_date",
        )
        .dropDuplicates()
        .alias("e")
        .join(
            source_df.select(
                "line_sign",
                "gtfs_direction_id",
                "gtfs_load_date",
            )
            .dropDuplicates()
            .alias("g"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="left_anti",
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_MISSING_GOLD_ROUTE_DIRECTION_FROM_HELPER",
        validation_description="Every expected route-direction from helper coverage must exist in Gold.",
        severity="ERROR",
        validation_passed=missing_gold_coverage_count == 0,
        violation_count=missing_gold_coverage_count,
    )

    # INFO rule for monitoring geometry string population pattern
    invalid_linestring_prefix_count = source_df.filter(
        ~F.col("geometry_wkt").startswith("LINESTRING(")
    ).count()

    _append_result(
        results=results,
        validation_rule="GEOMETRY_WKT_LINESTRING_PREFIX",
        validation_description="geometry_wkt should start with LINESTRING( for map-ready route geometry representation.",
        severity="INFO",
        validation_passed=invalid_linestring_prefix_count == 0,
        violation_count=invalid_linestring_prefix_count,
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


def _build_helper_latest() -> DataFrame:
    """
    Read helper rows for the latest GTFS snapshot available in the helper table.
    """
    latest_snapshot = (
        spark.read.table(HELPER_SOURCE)
        .select(F.max("gtfs_load_date").alias("gtfs_load_date"))
        .filter(F.col("gtfs_load_date").isNotNull())
    )

    return spark.read.table(HELPER_SOURCE).join(
        latest_snapshot,
        on="gtfs_load_date",
        how="inner",
    )


def _build_helper_shape_level_candidates() -> DataFrame:
    """
    Collapse the helper table to one row per candidate shape for each
    line_sign + gtfs_direction_id.
    """
    helper_latest = _build_helper_latest()

    return helper_latest.select(
        F.col("line_sign"),
        F.col("route_id"),
        F.col("route_long_name"),
        F.col("route_name_display"),
        F.col("gtfs_direction_id"),
        F.col("direction_description"),
        F.col("shape_id"),
        F.col("trip_headsign_reference"),
        F.col("trip_count_using_shape"),
        F.col("shape_usage_rank"),
        F.col("is_representative_shape_candidate"),
        F.col("shape_point_count"),
        F.col("gtfs_load_date"),
        F.col("gtfs_loaded_at"),
    ).dropDuplicates(
        ["line_sign", "gtfs_direction_id", "shape_id"]
    )


def _build_expected_representative_shapes() -> DataFrame:
    """
    Select one expected representative shape per line_sign + gtfs_direction_id
    using the same deterministic hierarchy used by the Gold pipeline.
    """
    shape_level = _build_helper_shape_level_candidates()

    representative_window = Window.partitionBy(
        "line_sign",
        "gtfs_direction_id",
    ).orderBy(
        F.when(F.col("is_representative_shape_candidate"), F.lit(1))
        .otherwise(F.lit(0))
        .desc(),
        F.col("shape_usage_rank").asc(),
        F.col("trip_count_using_shape").desc(),
        F.col("shape_id").asc(),
    )

    return (
        shape_level.withColumn(
            "representative_shape_row_number",
            F.row_number().over(representative_window),
        )
        .filter(F.col("representative_shape_row_number") == 1)
        .drop("representative_shape_row_number")
        .withColumnRenamed("shape_id", "representative_shape_id")
    )


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
