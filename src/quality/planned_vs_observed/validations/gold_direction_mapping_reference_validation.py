"""
Validation pipeline for gold.gold_direction_mapping_reference.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    gold.gold_direction_mapping_reference is acceptable for downstream
    analytical use.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation

Important:
    - Validation is different from auditing.
    - Audit tables expose ambiguity, conflicts, and unusual patterns.
    - This validation table provides an explicit readiness view.
    - This is a non-blocking analytical validation output, but ERROR rows
      should be treated as blockers for formal acceptance.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_direction_mapping_reference"
TABLE_NAME = "gold.gold_direction_mapping_reference_validation"

OBSERVED_SOURCE = "main.silver.silver_sptrans_positions"
GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_STOP_TIMES_SOURCE = "main.silver.silver_gtfs_stop_times"

ALLOWED_OBSERVED_DIRECTIONS = [1, 2]
ALLOWED_GTFS_DIRECTIONS = [-1, 0, 1]
ALLOWED_MAPPING_CONFIDENCE = ["high", "moderate", "low", "none"]
MAX_EVIDENCE_SCORE = 100.0
MIN_VALIDATED_VEHICLE_COUNT = 5


def _build_gtfs_direction_reference() -> DataFrame:
    """
    Build a lightweight GTFS direction reference from routes, trips, and stop_times.

    This is used for reconciliation validation to confirm that mapped
    line_sign + gtfs_direction_id combinations exist in the GTFS stop-based layer.
    """
    routes = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select(
            F.col("route_id"),
            F.col("route_short_name").alias("line_sign"),
        )
        .filter(F.col("route_id").isNotNull() & F.col("line_sign").isNotNull())
    )

    trips = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select(
            F.col("trip_id"),
            F.col("route_id"),
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
    )

    stop_times = (
        spark.read.table(GTFS_STOP_TIMES_SOURCE)
        .select("trip_id")
        .filter(F.col("trip_id").isNotNull())
        .distinct()
    )

    return (
        trips.join(routes, on="route_id", how="inner")
        .join(stop_times, on="trip_id", how="inner")
        .select("line_sign", "gtfs_direction_id")
        .distinct()
    )


def _build_observed_direction_reference() -> DataFrame:
    """
    Build a lightweight observed reference from SPTrans positions.

    This is used for reconciliation validation to confirm that validated
    mappings exist in the observed source.
    """
    return (
        spark.read.table(OBSERVED_SOURCE)
        .select(
            F.col("line_sign"),
            F.col("direction").cast("int").alias("observed_direction"),
        )
        .filter(
            F.col("line_sign").isNotNull()
            & F.col("observed_direction").isNotNull()
        )
        .distinct()
    )


def _append_result(
    results: list[dict],
    validation_category: str,
    validation_rule: str,
    severity: str,
    validation_passed: bool,
    failed_row_count: int | None,
    validation_message: str,
) -> None:
    """
    Append one validation result row to the output list.
    """
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


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for gold.gold_direction_mapping_reference. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations."
    ),
)
def gold_direction_mapping_reference_validation() -> DataFrame:
    """
    Create a formal validation table for gold.gold_direction_mapping_reference.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results: list[dict] = []

    columns = set(source_df.columns)

    required_columns = [
        "line_sign",
        "observed_direction",
        "gtfs_direction_id",
        "evidence_type",
        "evidence_score",
        "mapping_confidence",
        "line_category",
        "dist_first_km",
        "dist_last_km",
        "vehicle_count",
        "low_sample_flag",
        "direction_collision_flag",
        "review_required_flag",
        "validated_flag",
        "record_created_at",
    ]

    required_not_null_columns = [
        "line_sign",
        "observed_direction",
        "gtfs_direction_id",
        "mapping_confidence",
        "validated_flag",
        "review_required_flag",
        "record_created_at",
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

    if "observed_direction" in columns:
        invalid_observed_direction_count = source_df.filter(
            ~F.col("observed_direction").isin(ALLOWED_OBSERVED_DIRECTIONS)
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="OBSERVED_DIRECTION_ALLOWED_DOMAIN",
            severity="ERROR",
            validation_passed=invalid_observed_direction_count == 0,
            failed_row_count=invalid_observed_direction_count,
            validation_message=(
                "All observed_direction values belong to the approved domain."
                if invalid_observed_direction_count == 0
                else "Some observed_direction values are outside the approved domain."
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
        source_df.groupBy("line_sign", "observed_direction")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_LINE_SIGN_OBSERVED_DIRECTION_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_key_count == 0,
        failed_row_count=duplicate_key_count,
        validation_message=(
            "No duplicate rows exist at the declared Gold grain."
            if duplicate_key_count == 0
            else "Duplicate rows exist at the declared Gold grain."
        ),
    )

    validated_and_review_required_count = source_df.filter(
        (F.col("validated_flag") == F.lit(True))
        & (F.col("review_required_flag") == F.lit(True))
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="VALIDATED_FLAG_NEVER_TRUE_WHEN_REVIEW_REQUIRED_TRUE",
        severity="ERROR",
        validation_passed=validated_and_review_required_count == 0,
        failed_row_count=validated_and_review_required_count,
        validation_message=(
            "No rows are simultaneously validated and review-required."
            if validated_and_review_required_count == 0
            else "Some rows are simultaneously validated and review-required."
        ),
    )

    low_sample_and_validated_count = source_df.filter(
        (F.col("low_sample_flag") == F.lit(True))
        & (F.col("validated_flag") == F.lit(True))
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="LOW_SAMPLE_ROWS_NOT_PRODUCTION_SAFE",
        severity="ERROR",
        validation_passed=low_sample_and_validated_count == 0,
        failed_row_count=low_sample_and_validated_count,
        validation_message=(
            "No low-sample rows are marked as validated."
            if low_sample_and_validated_count == 0
            else "Some low-sample rows are incorrectly marked as validated."
        ),
    )

    vehicle_count_negative_count = source_df.filter(F.col("vehicle_count") < 0).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="VEHICLE_COUNT_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=vehicle_count_negative_count == 0,
        failed_row_count=vehicle_count_negative_count,
        validation_message=(
            "All vehicle_count values are non-negative."
            if vehicle_count_negative_count == 0
            else "Some vehicle_count values are negative."
        ),
    )

    dist_first_negative_count = source_df.filter(F.col("dist_first_km") < 0).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="DIST_FIRST_KM_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=dist_first_negative_count == 0,
        failed_row_count=dist_first_negative_count,
        validation_message=(
            "All dist_first_km values are non-negative."
            if dist_first_negative_count == 0
            else "Some dist_first_km values are negative."
        ),
    )

    dist_last_negative_count = source_df.filter(F.col("dist_last_km") < 0).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="DIST_LAST_KM_NON_NEGATIVE",
        severity="ERROR",
        validation_passed=dist_last_negative_count == 0,
        failed_row_count=dist_last_negative_count,
        validation_message=(
            "All dist_last_km values are non-negative."
            if dist_last_negative_count == 0
            else "Some dist_last_km values are negative."
        ),
    )

    invalid_mapping_confidence_count = source_df.filter(
        ~F.col("mapping_confidence").isin(ALLOWED_MAPPING_CONFIDENCE)
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="MAPPING_CONFIDENCE_ALLOWED_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_mapping_confidence_count == 0,
        failed_row_count=invalid_mapping_confidence_count,
        validation_message=(
            "All mapping_confidence values belong to the approved domain."
            if invalid_mapping_confidence_count == 0
            else "Some mapping_confidence values are outside the approved domain."
        ),
    )

    invalid_evidence_score_count = source_df.filter(
        F.col("evidence_score").isNotNull()
        & (
            (F.col("evidence_score") < 0)
            | (F.col("evidence_score") > F.lit(MAX_EVIDENCE_SCORE))
        )
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="EVIDENCE_SCORE_EXPECTED_BOUNDS",
        severity="ERROR",
        validation_passed=invalid_evidence_score_count == 0,
        failed_row_count=invalid_evidence_score_count,
        validation_message=(
            "All evidence_score values are within expected bounds."
            if invalid_evidence_score_count == 0
            else "Some evidence_score values are outside expected bounds."
        ),
    )

    observed_reference_df = _build_observed_direction_reference()

    missing_observed_reconciliation_count = (
        source_df.filter(F.col("validated_flag") == F.lit(True))
        .join(
            observed_reference_df,
            on=["line_sign", "observed_direction"],
            how="left_anti",
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="VALIDATED_MAPPINGS_RECONCILE_WITH_OBSERVED_SOURCE",
        severity="ERROR",
        validation_passed=missing_observed_reconciliation_count == 0,
        failed_row_count=missing_observed_reconciliation_count,
        validation_message=(
            "All validated mappings reconcile with the observed source."
            if missing_observed_reconciliation_count == 0
            else "Some validated mappings do not reconcile with the observed source."
        ),
    )

    gtfs_direction_reference_df = _build_gtfs_direction_reference()

    missing_gtfs_reconciliation_count = (
        source_df.filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("gtfs_direction_id") >= 0)
        )
        .join(
            gtfs_direction_reference_df,
            on=["line_sign", "gtfs_direction_id"],
            how="left_anti",
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="VALIDATED_MAPPINGS_RECONCILE_WITH_GTFS_DIRECTION_REFERENCE",
        severity="ERROR",
        validation_passed=missing_gtfs_reconciliation_count == 0,
        failed_row_count=missing_gtfs_reconciliation_count,
        validation_message=(
            "All validated mappings reconcile with the GTFS direction reference."
            if missing_gtfs_reconciliation_count == 0
            else "Some validated mappings do not reconcile with the GTFS direction reference."
        ),
    )

    expected_collision_flags_df = (
        source_df.groupBy("line_sign")
        .agg(
            F.countDistinct("observed_direction").alias("observed_direction_count"),
            F.countDistinct(
                F.when(F.col("gtfs_direction_id") >= 0, F.col("gtfs_direction_id"))
            ).alias("distinct_gtfs_direction_count"),
        )
        .withColumn(
            "expected_direction_collision_flag",
            (F.col("observed_direction_count") > 1)
            & (F.col("distinct_gtfs_direction_count") < F.col("observed_direction_count")),
        )
        .select("line_sign", "expected_direction_collision_flag")
    )

    collision_flag_mismatch_count = (
        source_df.join(expected_collision_flags_df, on="line_sign", how="inner")
        .filter(
            F.col("direction_collision_flag")
            != F.col("expected_direction_collision_flag")
        )
        .count()
    )

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="DIRECTION_COLLISION_FLAG_RECONCILES_WITH_LINE_LEVEL_LOGIC",
        severity="ERROR",
        validation_passed=collision_flag_mismatch_count == 0,
        failed_row_count=collision_flag_mismatch_count,
        validation_message=(
            "All direction_collision_flag values reconcile with line-level consistency logic."
            if collision_flag_mismatch_count == 0
            else "Some direction_collision_flag values do not reconcile with line-level consistency logic."
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
