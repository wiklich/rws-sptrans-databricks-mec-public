"""
Validation pipeline for silver.silver_helper_route_shape_points_reference.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_helper_route_shape_points_reference is acceptable
    for downstream spatial use.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation

Important:
    - Validation is different from auditing.
    - Audit tables expose ambiguity, conflicts, edge cases, and enrichment gaps.
    - This validation table provides an explicit readiness view.
    - ERROR rows should be treated as blockers for downstream spatial usage.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_helper_route_shape_points_reference"
TABLE_NAME = "silver.silver_helper_route_shape_points_reference_validation"

GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_SHAPES_SOURCE = "main.silver.silver_gtfs_shapes"

ALLOWED_GTFS_DIRECTIONS = [0, 1]

@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_helper_route_shape_points_reference. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations."
    ),
)
def silver_helper_route_shape_points_reference_validation() -> DataFrame:
    """
    Create a formal validation table for
    silver.silver_helper_route_shape_points_reference.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    shape_level_df = _shape_level(source_df)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "line_sign",
        "route_id",
        "route_long_name",
        "route_name_display",
        "gtfs_direction_id",
        "direction_description",
        "shape_id",
        "trip_headsign_reference",
        "trip_count_using_shape",
        "shape_usage_rank",
        "is_representative_shape_candidate",
        "shape_point_count",
        "shape_pt_sequence",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_dist_traveled",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "line_sign",
        "route_id",
        "gtfs_direction_id",
        "shape_id",
        "trip_count_using_shape",
        "shape_usage_rank",
        "is_representative_shape_candidate",
        "shape_point_count",
        "shape_pt_sequence",
        "shape_pt_lat",
        "shape_pt_lon",
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

    if "line_sign" in columns:
        blank_line_sign_count = source_df.filter(
            F.trim(F.col("line_sign")) == ""
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="LINE_SIGN_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_line_sign_count == 0,
            failed_row_count=blank_line_sign_count,
            validation_message=(
                "All line_sign values are populated and non-blank."
                if blank_line_sign_count == 0
                else "Some line_sign values are blank."
            ),
        )

    if "shape_id" in columns:
        blank_shape_id_count = source_df.filter(
            F.trim(F.col("shape_id")) == ""
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="SHAPE_ID_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_shape_id_count == 0,
            failed_row_count=blank_shape_id_count,
            validation_message=(
                "All shape_id values are populated and non-blank."
                if blank_shape_id_count == 0
                else "Some shape_id values are blank."
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

    duplicate_key_count = (
        source_df.groupBy(
            "line_sign",
            "gtfs_direction_id",
            "shape_id",
            "shape_pt_sequence",
        )
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
            "No duplicate rows exist at the declared helper grain."
            if duplicate_key_count == 0
            else "Duplicate rows exist at the declared helper grain."
        ),
    )

    rank_one_not_highest_trip_count = (
        shape_level_df.alias("src")
        .join(
            shape_level_df.groupBy("line_sign", "gtfs_direction_id")
            .agg(
                F.max("trip_count_using_shape").alias("max_trip_count_using_shape"),
            )
            .alias("maxv"),
            on=["line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .filter(
            (F.col("src.shape_usage_rank") == 1)
            & (
                F.col("src.trip_count_using_shape")
                != F.col("maxv.max_trip_count_using_shape")
            )
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="RANK_ONE_ASSIGNED_ONLY_TO_HIGHEST_TRIP_COUNT_SHAPE",
        severity="ERROR",
        validation_passed=rank_one_not_highest_trip_count == 0,
        failed_row_count=rank_one_not_highest_trip_count,
        validation_message=(
            "shape_usage_rank = 1 is only assigned to shapes with the highest trip_count_using_shape."
            if rank_one_not_highest_trip_count == 0
            else "Some rank-1 shapes do not have the highest trip_count_using_shape."
        ),
    )

    representative_flag_logic_violation_count = shape_level_df.filter(
        (F.col("is_representative_shape_candidate") == F.lit(True))
        & (F.col("shape_usage_rank") != F.lit(1))
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="REPRESENTATIVE_FLAG_IS_CONSISTENT_WITH_RANK_ONE",
        severity="ERROR",
        validation_passed=representative_flag_logic_violation_count == 0,
        failed_row_count=representative_flag_logic_violation_count,
        validation_message=(
            "is_representative_shape_candidate is consistent with shape_usage_rank = 1."
            if representative_flag_logic_violation_count == 0
            else "Some rows mark representative shape candidates without shape_usage_rank = 1."
        ),
    )

    representative_candidate_count_violation = (
        shape_level_df.groupBy("line_sign", "gtfs_direction_id")
        .agg(
            F.sum(
                F.when(
                    F.col("is_representative_shape_candidate") == F.lit(True),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("representative_candidate_count")
        )
        .filter(F.col("representative_candidate_count") != 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="EXACTLY_ONE_REPRESENTATIVE_CANDIDATE_PER_LINE_DIRECTION",
        severity="ERROR",
        validation_passed=representative_candidate_count_violation == 0,
        failed_row_count=representative_candidate_count_violation,
        validation_message=(
            "Exactly one representative shape candidate exists per line_sign and gtfs_direction_id."
            if representative_candidate_count_violation == 0
            else "Some line_sign and gtfs_direction_id groups do not have exactly one representative candidate."
        ),
    )

    invalid_trip_count_count = shape_level_df.filter(
        F.col("trip_count_using_shape") <= 0
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="TRIP_COUNT_USING_SHAPE_POSITIVE",
        severity="ERROR",
        validation_passed=invalid_trip_count_count == 0,
        failed_row_count=invalid_trip_count_count,
        validation_message=(
            "All trip_count_using_shape values are positive."
            if invalid_trip_count_count == 0
            else "Some trip_count_using_shape values are non-positive."
        ),
    )

    invalid_shape_point_count = shape_level_df.filter(
        F.col("shape_point_count") <= 1
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="SHAPE_POINT_COUNT_GREATER_THAN_ONE",
        severity="ERROR",
        validation_passed=invalid_shape_point_count == 0,
        failed_row_count=invalid_shape_point_count,
        validation_message=(
            "All shape_point_count values are greater than one."
            if invalid_shape_point_count == 0
            else "Some shape_point_count values are less than or equal to one."
        ),
    )

    invalid_shape_usage_rank_count = shape_level_df.filter(
        F.col("shape_usage_rank") < 1
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="SHAPE_USAGE_RANK_GREATER_THAN_OR_EQUAL_TO_ONE",
        severity="ERROR",
        validation_passed=invalid_shape_usage_rank_count == 0,
        failed_row_count=invalid_shape_usage_rank_count,
        validation_message=(
            "All shape_usage_rank values are greater than or equal to one."
            if invalid_shape_usage_rank_count == 0
            else "Some shape_usage_rank values are less than one."
        ),
    )

    invalid_shape_pt_sequence_count = source_df.filter(
        F.col("shape_pt_sequence") <= 0
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="SHAPE_PT_SEQUENCE_POSITIVE",
        severity="WARNING",
        validation_passed=invalid_shape_pt_sequence_count == 0,
        failed_row_count=invalid_shape_pt_sequence_count,
        validation_message=(
            "All shape_pt_sequence values are positive."
            if invalid_shape_pt_sequence_count == 0
            else "Some shape_pt_sequence values are non-positive."
        ),
    )

    actual_shape_point_counts = (
        source_df.groupBy("line_sign", "gtfs_direction_id", "shape_id")
        .agg(F.count(F.lit(1)).cast("long").alias("actual_shape_point_count"))
    )

    shape_point_count_mismatch = (
        shape_level_df.alias("src")
        .join(
            actual_shape_point_counts.alias("act"),
            on=["line_sign", "gtfs_direction_id", "shape_id"],
            how="inner",
        )
        .filter(F.col("src.shape_point_count") != F.col("act.actual_shape_point_count"))
        .count()
    )

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="SHAPE_POINT_COUNT_RECONCILES_WITH_DETAILED_ROWS",
        severity="ERROR",
        validation_passed=shape_point_count_mismatch == 0,
        failed_row_count=shape_point_count_mismatch,
        validation_message=(
            "shape_point_count reconciles with the actual number of detailed rows."
            if shape_point_count_mismatch == 0
            else "Some shape_point_count values do not reconcile with the actual number of detailed rows."
        ),
    )

    latest_common_snapshot_df = _build_latest_common_snapshot()
    latest_common_snapshot_row = latest_common_snapshot_df.collect()

    latest_common_snapshot = (
        latest_common_snapshot_row[0]["gtfs_load_date"]
        if len(latest_common_snapshot_row) > 0
        else None
    )

    if latest_common_snapshot is None:
        snapshot_mismatch_count = 1
    else:
        snapshot_mismatch_count = source_df.filter(
            F.col("gtfs_load_date") != F.lit(latest_common_snapshot)
        ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="HELPER_USES_LATEST_COMMON_GTFS_SNAPSHOT",
        severity="ERROR",
        validation_passed=snapshot_mismatch_count == 0,
        failed_row_count=snapshot_mismatch_count,
        validation_message=(
            "All helper rows reconcile with the latest common GTFS snapshot."
            if snapshot_mismatch_count == 0
            else "Some helper rows do not reconcile with the latest common GTFS snapshot."
        ),
    )

    expected_shape_usage = _build_expected_shape_usage()

    shape_usage_reconciliation_failures = (
        shape_level_df.alias("src")
        .join(
            expected_shape_usage.alias("exp"),
            on=["gtfs_load_date", "line_sign", "gtfs_direction_id", "shape_id"],
            how="full_outer",
        )
        .filter(
            F.col("src.shape_id").isNull()
            | F.col("exp.shape_id").isNull()
            | (
                F.col("src.trip_count_using_shape")
                != F.col("exp.trip_count_using_shape")
            )
            | (F.col("src.shape_usage_rank") != F.col("exp.shape_usage_rank"))
            | (
                F.col("src.is_representative_shape_candidate")
                != F.col("exp.is_representative_shape_candidate")
            )
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="SHAPE_USAGE_AND_RANK_RECONCILE_WITH_UPSTREAM_TRIP_RELATIONSHIPS",
        severity="ERROR",
        validation_passed=shape_usage_reconciliation_failures == 0,
        failed_row_count=shape_usage_reconciliation_failures,
        validation_message=(
            "trip_count_using_shape, shape_usage_rank, and representative flag reconcile with upstream trip-to-shape relationships."
            if shape_usage_reconciliation_failures == 0
            else "Some shape usage metrics do not reconcile with upstream trip-to-shape relationships."
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

def _build_latest_common_snapshot() -> DataFrame:
    """
    Build a one-row DataFrame containing the latest common GTFS snapshot date
    shared across routes, trips, and shapes.
    """
    routes_dates = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    trips_dates = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    shapes_dates = (
        spark.read.table(GTFS_SHAPES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    return (
        routes_dates.join(trips_dates, on="gtfs_load_date", how="inner")
        .join(shapes_dates, on="gtfs_load_date", how="inner")
        .agg(F.max("gtfs_load_date").alias("gtfs_load_date"))
        .filter(F.col("gtfs_load_date").isNotNull())
    )


def _build_routes_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            "gtfs_load_date",
            "route_id",
            F.col("route_short_name").alias("line_sign"),
            "route_long_name",
            "route_name_display",
            F.col("gtfs_loaded_at").alias("route_gtfs_loaded_at"),
        )
        .filter(
            F.col("route_id").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
        )
    )


def _build_trips_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            "gtfs_load_date",
            "trip_id",
            "route_id",
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
            "direction_description",
            "trip_headsign",
            "shape_id",
            F.col("gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("shape_id").isNotNull()
        )
    )


def _build_shapes_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_SHAPES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            "gtfs_load_date",
            "shape_id",
            "shape_pt_lat",
            "shape_pt_lon",
            F.col("shape_pt_sequence").cast("int").alias("shape_pt_sequence"),
            "shape_dist_traveled",
            F.col("gtfs_loaded_at").alias("shape_gtfs_loaded_at"),
        )
        .filter(
            F.col("shape_id").isNotNull()
            & F.col("shape_pt_lat").isNotNull()
            & F.col("shape_pt_lon").isNotNull()
            & F.col("shape_pt_sequence").isNotNull()
        )
    )


def _build_route_trip_base() -> DataFrame:
    routes_latest = _build_routes_latest()
    trips_latest = _build_trips_latest()

    return routes_latest.join(
        trips_latest,
        on=["gtfs_load_date", "route_id"],
        how="inner",
    ).select(
        "gtfs_load_date",
        "line_sign",
        "route_id",
        "route_long_name",
        "route_name_display",
        "trip_id",
        "gtfs_direction_id",
        "direction_description",
        "trip_headsign",
        "shape_id",
    )


def _build_expected_shape_usage() -> DataFrame:
    route_trip_base = _build_route_trip_base()

    shape_usage = (
        route_trip_base.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "shape_id",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias("trip_count_using_shape"),
        )
    )

    shape_rank_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
    ).orderBy(
        F.col("trip_count_using_shape").desc(),
        F.col("shape_id").asc(),
    )

    return (
        shape_usage.withColumn(
            "shape_usage_rank",
            F.row_number().over(shape_rank_window).cast("int"),
        )
        .withColumn(
            "is_representative_shape_candidate",
            (F.col("shape_usage_rank") == F.lit(1)).cast("boolean"),
        )
    )


def _shape_level(df: DataFrame) -> DataFrame:
    """
    Collapse the point-level helper into one row per
    line_sign + gtfs_direction_id + shape_id.
    """
    return df.dropDuplicates(["line_sign", "gtfs_direction_id", "shape_id"])


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
