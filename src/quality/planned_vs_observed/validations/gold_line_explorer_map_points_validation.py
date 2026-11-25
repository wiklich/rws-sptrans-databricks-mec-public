"""
Validation pipeline for gold.gold_line_explorer_map_points.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    gold.gold_line_explorer_map_points is acceptable for downstream map use.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_line_explorer_map_points"
TABLE_NAME = "gold.gold_line_explorer_map_points_validation"

SPTRANS_POSITIONS_SOURCE = "main.silver.silver_sptrans_positions"
DIRECTION_MAPPING_SOURCE = "main.gold.gold_direction_mapping_reference"
SERVICE_ADHERENCE_SOURCE = "main.gold.gold_planned_vs_observed_service_adherence_hourly"

ALLOWED_GTFS_DIRECTIONS = [0, 1]


def _build_positions_base() -> DataFrame:
    """
    Read observed SPTrans positions and keep only structurally valid map points.
    """
    return (
        spark.read.table(SPTRANS_POSITIONS_SOURCE)
        .select(
            F.col("event_id"),
            F.col("captured_date_local").cast("date").alias("service_date"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("captured_hour_local").cast("int").alias("hour"),
            F.col("line_sign"),
            F.col("direction").cast("int").alias("observed_direction"),
            F.col("direction_description"),
            F.col("vehicle_prefix").cast("long").alias("vehicle_prefix"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
        )
        .filter(
            F.col("event_id").isNotNull()
            & F.col("service_date").isNotNull()
            & F.col("captured_at_local").isNotNull()
            & F.col("hour").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("observed_direction").isNotNull()
            & F.col("vehicle_prefix").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .dropDuplicates(["event_id"])
    )


def _build_validated_direction_mapping() -> DataFrame:
    """
    Read only production-safe direction mappings.
    """
    return (
        spark.read.table(DIRECTION_MAPPING_SOURCE)
        .select(
            F.col("line_sign"),
            F.col("observed_direction").cast("int").alias("observed_direction"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("validated_flag"),
            F.col("review_required_flag"),
        )
        .filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("review_required_flag") == F.lit(False))
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("observed_direction").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
        .dropDuplicates(["line_sign", "observed_direction"])
    )


def _build_hourly_service_adherence() -> DataFrame:
    """
    Read hourly planned vs observed service adherence metrics at the dashboard join grain.
    """
    return (
        spark.read.table(SERVICE_ADHERENCE_SOURCE)
        .select(
            F.col("service_date").cast("date").alias("service_date"),
            F.col("line_sign"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("hour").cast("int").alias("hour"),
            F.col("planned_headway_minutes").cast("double").alias(
                "planned_headway_minutes"
            ),
            F.col("observed_headway_avg_minutes").cast("double").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("headway_deviation_minutes").cast("double").alias(
                "headway_deviation_minutes"
            ),
            F.col("headway_deviation_pct").cast("double").alias(
                "headway_deviation_pct"
            ),
            F.col("missed_service_flag").cast("boolean").alias("missed_service_flag"),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("hour").isNotNull()
        )
        .dropDuplicates(["service_date", "line_sign", "gtfs_direction_id", "hour"])
    )


def _build_mapped_points() -> DataFrame:
    """
    Join observed points with validated direction mapping.
    """
    positions_base = _build_positions_base()
    validated_mapping = _build_validated_direction_mapping()

    return positions_base.join(
        validated_mapping,
        on=["line_sign", "observed_direction"],
        how="inner",
    )


def _build_expected_gold_rows() -> DataFrame:
    """
    Rebuild the expected Gold rows from upstream logic for reconciliation.
    """
    mapped_points = _build_mapped_points()
    hourly_adherence = _build_hourly_service_adherence()

    return (
        mapped_points.alias("p")
        .join(
            hourly_adherence.alias("a"),
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="left",
        )
        .withColumn(
            "adherence_band",
            F.when(
                F.col("a.headway_deviation_minutes").isNull(),
                F.lit("No Gold Match"),
            )
            .when(
                F.col("a.headway_deviation_minutes") >= F.lit(10.0),
                F.lit("High Deviation"),
            )
            .when(
                F.col("a.headway_deviation_minutes") >= F.lit(3.0),
                F.lit("Moderate Deviation"),
            )
            .when(
                F.col("a.headway_deviation_minutes") > F.lit(0.0),
                F.lit("Low Deviation"),
            )
            .otherwise(F.lit("Aligned or Better")),
        )
        .select(
            F.col("event_id"),
            F.col("service_date"),
            F.col("captured_at_local"),
            F.col("hour"),
            F.col("line_sign"),
            F.col("observed_direction"),
            F.col("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("vehicle_prefix"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("planned_headway_minutes"),
            F.col("observed_headway_avg_minutes"),
            F.col("headway_deviation_minutes"),
            F.col("headway_deviation_pct"),
            F.col("missed_service_flag"),
            F.col("adherence_band"),
        )
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


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for gold.gold_line_explorer_map_points. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations for downstream map use."
    ),
)
def gold_line_explorer_map_points_validation() -> DataFrame:
    """
    Create a formal validation table for gold.gold_line_explorer_map_points.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    expected_df = _build_expected_gold_rows()
    validated_mapping = _build_validated_direction_mapping()
    mapped_points = _build_mapped_points()

    results = []
    columns = set(source_df.columns)

    required_columns = [
        "event_id",
        "service_date",
        "captured_at_local",
        "hour",
        "line_sign",
        "observed_direction",
        "gtfs_direction_id",
        "direction_description",
        "vehicle_prefix",
        "latitude",
        "longitude",
        "planned_headway_minutes",
        "observed_headway_avg_minutes",
        "headway_deviation_minutes",
        "headway_deviation_pct",
        "missed_service_flag",
        "adherence_band",
        "record_created_at",
        "record_hash",
    ]

    required_not_null_columns = [
        "event_id",
        "service_date",
        "captured_at_local",
        "hour",
        "line_sign",
        "observed_direction",
        "gtfs_direction_id",
        "vehicle_prefix",
        "latitude",
        "longitude",
        "adherence_band",
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
        validation_description="All required columns are present in the Gold line explorer map points table.",
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

    if "vehicle_prefix" in columns:
        null_vehicle_prefix_count = source_df.filter(
            F.col("vehicle_prefix").isNull()
        ).count()

        _append_result(
            results=results,
            validation_rule="VEHICLE_PREFIX_POPULATED",
            validation_description="vehicle_prefix must be populated.",
            severity="ERROR",
            validation_passed=null_vehicle_prefix_count == 0,
            violation_count=null_vehicle_prefix_count,
        )

    if "latitude" in columns:
        null_latitude_count = source_df.filter(F.col("latitude").isNull()).count()

        _append_result(
            results=results,
            validation_rule="LATITUDE_POPULATED",
            validation_description="latitude must be populated.",
            severity="ERROR",
            validation_passed=null_latitude_count == 0,
            violation_count=null_latitude_count,
        )

    if "longitude" in columns:
        null_longitude_count = source_df.filter(F.col("longitude").isNull()).count()

        _append_result(
            results=results,
            validation_rule="LONGITUDE_POPULATED",
            validation_description="longitude must be populated.",
            severity="ERROR",
            validation_passed=null_longitude_count == 0,
            violation_count=null_longitude_count,
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
        source_df.groupBy(
            "service_date",
            "captured_at_local",
            "line_sign",
            "gtfs_direction_id",
            "vehicle_prefix",
            "latitude",
            "longitude",
        )
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_rule="NO_DUPLICATE_ROWS_AT_EVENT_LEVEL_GRAIN",
        validation_description="No duplicate rows exist at the declared event-level map point grain.",
        severity="ERROR",
        validation_passed=duplicate_grain_count == 0,
        violation_count=duplicate_grain_count,
    )

    # ============================================================
    # C. BUSINESS-RULE VALIDATION
    # ============================================================

    approved_mapping_only_count = (
        source_df.alias("g")
        .join(
            validated_mapping.alias("m"),
            on=[
                F.col("g.line_sign") == F.col("m.line_sign"),
                F.col("g.observed_direction") == F.col("m.observed_direction"),
                F.col("g.gtfs_direction_id") == F.col("m.gtfs_direction_id"),
            ],
            how="left_anti",
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="ROWS_USE_APPROVED_DIRECTION_MAPPING_ONLY",
        validation_description="All rows must use approved validated direction mapping only.",
        severity="ERROR",
        validation_passed=approved_mapping_only_count == 0,
        violation_count=approved_mapping_only_count,
    )

    adherence_band_mismatch_count = source_df.filter(
        ~F.col("adherence_band").eqNullSafe(
            F.when(
                F.col("headway_deviation_minutes").isNull(),
                F.lit("No Gold Match"),
            )
            .when(
                F.col("headway_deviation_minutes") >= F.lit(10.0),
                F.lit("High Deviation"),
            )
            .when(
                F.col("headway_deviation_minutes") >= F.lit(3.0),
                F.lit("Moderate Deviation"),
            )
            .when(
                F.col("headway_deviation_minutes") > F.lit(0.0),
                F.lit("Low Deviation"),
            )
            .otherwise(F.lit("Aligned or Better"))
        )
    ).count()

    _append_result(
        results=results,
        validation_rule="ADHERENCE_BAND_CONSISTENT_WITH_HEADWAY_DEVIATION",
        validation_description="adherence_band must be consistent with headway_deviation_minutes according to the approved classification logic.",
        severity="ERROR",
        validation_passed=adherence_band_mismatch_count == 0,
        violation_count=adherence_band_mismatch_count,
    )

    # ============================================================
    # D. METRIC VALIDATION
    # ============================================================

    invalid_latitude_range_count = source_df.filter(
        (F.col("latitude") < F.lit(-90.0)) | (F.col("latitude") > F.lit(90.0))
    ).count()

    _append_result(
        results=results,
        validation_rule="LATITUDE_RANGE_VALID",
        validation_description="latitude must be within the valid numeric range [-90, 90].",
        severity="ERROR",
        validation_passed=invalid_latitude_range_count == 0,
        violation_count=invalid_latitude_range_count,
    )

    invalid_longitude_range_count = source_df.filter(
        (F.col("longitude") < F.lit(-180.0)) | (F.col("longitude") > F.lit(180.0))
    ).count()

    _append_result(
        results=results,
        validation_rule="LONGITUDE_RANGE_VALID",
        validation_description="longitude must be within the valid numeric range [-180, 180].",
        severity="ERROR",
        validation_passed=invalid_longitude_range_count == 0,
        violation_count=invalid_longitude_range_count,
    )

    invalid_hour_count = source_df.filter(
        (F.col("hour") < F.lit(0)) | (F.col("hour") > F.lit(23))
    ).count()

    _append_result(
        results=results,
        validation_rule="HOUR_WITHIN_EXPECTED_BOUNDS",
        validation_description="hour must be within expected bounds [0, 23].",
        severity="ERROR",
        validation_passed=invalid_hour_count == 0,
        violation_count=invalid_hour_count,
    )

    incoherent_headway_deviation_pct_count = source_df.filter(
        (
            F.col("headway_deviation_pct").isNotNull()
            & F.col("headway_deviation_minutes").isNull()
        )
        | (
            F.col("headway_deviation_pct").isNotNull()
            & F.col("planned_headway_minutes").isNull()
        )
        | (
            F.col("headway_deviation_pct").isNotNull()
            & (F.col("planned_headway_minutes") <= F.lit(0.0))
        )
    ).count()

    _append_result(
        results=results,
        validation_rule="HEADWAY_DEVIATION_PCT_COHERENT_WHEN_POPULATED",
        validation_description="headway_deviation_pct must only be populated when required supporting fields are present and valid.",
        severity="WARNING",
        validation_passed=incoherent_headway_deviation_pct_count == 0,
        violation_count=incoherent_headway_deviation_pct_count,
    )

    # ============================================================
    # E. RECONCILIATION VALIDATION
    # ============================================================

    point_level_rows_missing_from_gold = (
        expected_df.select("event_id")
        .dropDuplicates()
        .alias("e")
        .join(
            source_df.select("event_id").dropDuplicates().alias("g"),
            on="event_id",
            how="left_anti",
        )
        .count()
    )

    unexpected_gold_rows = (
        source_df.select("event_id")
        .dropDuplicates()
        .alias("g")
        .join(
            expected_df.select("event_id").dropDuplicates().alias("e"),
            on="event_id",
            how="left_anti",
        )
        .count()
    )

    point_level_rowset_reconciliation_count = (
        point_level_rows_missing_from_gold + unexpected_gold_rows
    )

    _append_result(
        results=results,
        validation_rule="POINT_LEVEL_ROWS_RECONCILE_WITH_FILTERED_OBSERVED_TELEMETRY",
        validation_description="Point-level Gold rows must reconcile with the filtered observed telemetry source after approved direction mapping.",
        severity="ERROR",
        validation_passed=point_level_rowset_reconciliation_count == 0,
        violation_count=point_level_rowset_reconciliation_count,
    )

    mapped_combo_reconciliation_count = (
        mapped_points.select(
            "line_sign", "observed_direction", "gtfs_direction_id"
        )
        .dropDuplicates()
        .alias("e")
        .join(
            source_df.select(
                "line_sign", "observed_direction", "gtfs_direction_id"
            )
            .dropDuplicates()
            .alias("g"),
            on=["line_sign", "observed_direction", "gtfs_direction_id"],
            how="full_outer",
        )
        .filter(
            F.col("e.line_sign").isNull() | F.col("g.line_sign").isNull()
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="MAPPED_LINE_DIRECTION_COMBINATIONS_RECONCILE_WITH_VALIDATED_DIRECTION_MAPPING",
        validation_description="Mapped line-direction combinations must reconcile with the approved validated direction mapping applied to observed telemetry.",
        severity="ERROR",
        validation_passed=mapped_combo_reconciliation_count == 0,
        violation_count=mapped_combo_reconciliation_count,
    )

    adherence_field_reconciliation_count = (
        source_df.alias("g")
        .join(
            expected_df.alias("e"),
            on="event_id",
            how="full_outer",
        )
        .filter(
            F.col("g.event_id").isNull()
            | F.col("e.event_id").isNull()
            | ~F.col("g.planned_headway_minutes").eqNullSafe(
                F.col("e.planned_headway_minutes")
            )
            | ~F.col("g.observed_headway_avg_minutes").eqNullSafe(
                F.col("e.observed_headway_avg_minutes")
            )
            | ~F.col("g.headway_deviation_minutes").eqNullSafe(
                F.col("e.headway_deviation_minutes")
            )
            | ~F.col("g.headway_deviation_pct").eqNullSafe(
                F.col("e.headway_deviation_pct")
            )
            | ~F.col("g.missed_service_flag").eqNullSafe(
                F.col("e.missed_service_flag")
            )
        )
        .count()
    )

    _append_result(
        results=results,
        validation_rule="HOURLY_ADHERENCE_FIELDS_RECONCILE_WITH_SERVICE_ADHERENCE_GOLD",
        validation_description="Hourly adherence fields must reconcile with gold.gold_planned_vs_observed_service_adherence_hourly after the approved join logic.",
        severity="ERROR",
        validation_passed=adherence_field_reconciliation_count == 0,
        violation_count=adherence_field_reconciliation_count,
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
