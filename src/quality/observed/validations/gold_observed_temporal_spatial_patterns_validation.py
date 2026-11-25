"""
Gold observed temporal-spatial patterns validation pipeline.

Purpose:
    Validate whether gold_observed_temporal_spatial_patterns is structurally and
    logically acceptable for downstream use as the canonical observed
    temporal-spatial patterns layer in the Observed Spatial Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation

Output:
    - gold.gold_observed_temporal_spatial_patterns_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_observed_temporal_spatial_patterns"
SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_observed_temporal_spatial_patterns_validation"

FIXED_GRID_CELL_SIZE_DEGREES = 0.01
SPATIAL_RESOLUTION = "FIXED_GRID_0_01_DEG"
FLOAT_TOLERANCE = 1e-9


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_observed_temporal_spatial_patterns with one "
        "row per validation rule, including category, severity, pass/fail "
        "status, violation count, and execution timestamp."
    ),
)
def gold_observed_temporal_spatial_patterns_validation() -> DataFrame:
    """
    Materialize the validation results for gold_observed_temporal_spatial_patterns.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    silver_source_df = spark.read.table(SILVER_SOURCE_TABLE)

    return _build_gold_observed_temporal_spatial_patterns_validation(
        validated_df,
        silver_source_df,
    )


def _build_gold_observed_temporal_spatial_patterns_validation(
    validated_df: DataFrame,
    silver_source_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_observed_temporal_spatial_patterns.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_temporal_spatial_patterns.
        silver_source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    expected_gold_df = _build_expected_temporal_spatial_patterns_from_silver(
        silver_source_df
    )
    cell_consistency_df = _build_cell_consistency_df(validated_df, expected_gold_df)
    hourly_reconciliation_df = _build_hourly_reconciliation_df(
        validated_df,
        expected_gold_df,
    )

    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_analysis_hour_local_not_null(validated_df),
        _validate_spatial_cell_id_not_null(validated_df),
        _validate_spatial_cell_id_not_blank(validated_df),
        _validate_spatial_resolution_not_null(validated_df),
        _validate_spatial_resolution_not_blank(validated_df),
        _validate_cell_center_latitude_not_null(validated_df),
        _validate_cell_center_longitude_not_null(validated_df),
        _validate_observed_position_records_count_not_null(validated_df),
        _validate_distinct_observed_vehicles_count_not_null(validated_df),
        _validate_distinct_observed_lines_count_not_null(validated_df),
        _validate_unique_analysis_date_hour_spatial_cell_id(validated_df),
        _validate_all_expected_hourly_spatial_cells_present(cell_consistency_df),
        _validate_no_unexpected_hourly_spatial_cells_present(cell_consistency_df),
        _validate_spatial_resolution_matches_approved_constant(validated_df),
        _validate_analysis_hour_local_valid_range(validated_df),
        _validate_spatial_resolution_reconciles_with_expected_mapping(
            cell_consistency_df
        ),
        _validate_analysis_hour_local_reconciles_with_expected_mapping(
            cell_consistency_df
        ),
        _validate_cell_center_latitude_reconciles_with_expected_mapping(
            cell_consistency_df
        ),
        _validate_cell_center_longitude_reconciles_with_expected_mapping(
            cell_consistency_df
        ),
        _validate_distinct_observed_vehicles_count_uses_vehicle_prefix(
            cell_consistency_df
        ),
        _validate_distinct_observed_lines_count_uses_line_sign(cell_consistency_df),
        _validate_observed_position_records_count_minimum(validated_df),
        _validate_distinct_observed_vehicles_count_non_negative(validated_df),
        _validate_distinct_observed_lines_count_non_negative(validated_df),
        _validate_captured_at_interval_order(validated_df),
        _validate_cell_center_latitude_valid_range(validated_df),
        _validate_cell_center_longitude_valid_range(validated_df),
        _validate_cell_level_observed_position_records_count_reconciles(
            cell_consistency_df
        ),
        _validate_cell_level_distinct_observed_vehicles_count_reconciles(
            cell_consistency_df
        ),
        _validate_cell_level_distinct_observed_lines_count_reconciles(
            cell_consistency_df
        ),
        _validate_cell_level_min_captured_at_local_reconciles(cell_consistency_df),
        _validate_cell_level_max_captured_at_local_reconciles(cell_consistency_df),
        _validate_hourly_total_observed_position_records_count_reconciles(
            hourly_reconciliation_df
        ),
        _validate_hourly_active_spatial_cells_count_reconciles(
            hourly_reconciliation_df
        ),
    ]

    return reduce(lambda left, right: left.unionByName(right), validation_dfs)


def _build_validation_result(
    validated_df: DataFrame,
    validation_category: str,
    validation_rule: str,
    validation_description: str,
    severity: str,
    violation_condition,
) -> DataFrame:
    """
    Build a single validation result row.

    Args:
        validated_df:
            DataFrame being validated.
        validation_category:
            Logical category of the validation.
        validation_rule:
            Short rule identifier.
        validation_description:
            Human-readable description of the validation.
        severity:
            Validation severity (ERROR, WARNING, INFO).
        violation_condition:
            Spark boolean column expression identifying violating rows.

    Returns:
        DataFrame:
            A single-row DataFrame with validation result information.
    """
    return (
        validated_df.agg(
            F.sum(F.when(violation_condition, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit(validation_category))
        .withColumn("validation_rule", F.lit(validation_rule))
        .withColumn("validation_description", F.lit(validation_description))
        .withColumn("severity", F.lit(severity))
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
        )
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )


def _prepare_temporal_spatial_source(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for expected temporal-spatial aggregation.

    Processing steps:
        1. Preserve or derive the local analysis date.
        2. Preserve or derive the local analysis hour.
        3. Keep only fields needed for hourly spatial aggregation.
        4. Filter rows with invalid coordinates or invalid timestamps.
        5. Filter rows with invalid coordinate ranges.
        6. Filter rows with invalid local hour values.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Prepared source dataset ready for temporal-spatial bucketing.
    """
    return (
        source_df.select(
            F.coalesce(
                F.col("captured_date_local").cast("date"),
                F.to_date(F.col("captured_at_local")),
            ).alias("analysis_date"),
            F.coalesce(
                F.col("captured_hour_local").cast("int"),
                F.hour(F.col("captured_at_local")),
            ).alias("analysis_hour_local"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("line_sign").cast("string").alias("line_sign"),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("analysis_hour_local").isNotNull())
        .where(F.col("captured_at_local").isNotNull())
        .where(F.col("latitude").isNotNull())
        .where(F.col("longitude").isNotNull())
        .where(F.col("latitude").between(-90.0, 90.0))
        .where(F.col("longitude").between(-180.0, 180.0))
        .where(F.col("analysis_hour_local").between(0, 23))
    )


def _add_spatial_cell_columns(prepared_df: DataFrame) -> DataFrame:
    """
    Add fixed-grid spatial cell columns using the approved spatial encoding rule.

    Args:
        prepared_df:
            Prepared source dataset.

    Returns:
        DataFrame:
            Spatialized dataset with spatial_cell_id, spatial_resolution, and
            representative cell center coordinates.
    """
    lat_index_col = F.floor(
        F.col("latitude") / F.lit(FIXED_GRID_CELL_SIZE_DEGREES)
    ).cast("long")
    lon_index_col = F.floor(
        F.col("longitude") / F.lit(FIXED_GRID_CELL_SIZE_DEGREES)
    ).cast("long")

    return (
        prepared_df.withColumn("cell_lat_index", lat_index_col)
        .withColumn("cell_lon_index", lon_index_col)
        .withColumn("spatial_resolution", F.lit(SPATIAL_RESOLUTION))
        .withColumn(
            "cell_center_latitude",
            F.round(
                (
                    (F.col("cell_lat_index").cast("double") + F.lit(0.5))
                    * F.lit(FIXED_GRID_CELL_SIZE_DEGREES)
                ),
                6,
            ),
        )
        .withColumn(
            "cell_center_longitude",
            F.round(
                (
                    (F.col("cell_lon_index").cast("double") + F.lit(0.5))
                    * F.lit(FIXED_GRID_CELL_SIZE_DEGREES)
                ),
                6,
            ),
        )
        .withColumn(
            "spatial_cell_id",
            F.concat_ws(
                "|",
                F.col("spatial_resolution"),
                F.col("cell_lat_index").cast("string"),
                F.col("cell_lon_index").cast("string"),
            ),
        )
    )


def _build_expected_temporal_spatial_patterns_from_silver(
    silver_source_df: DataFrame,
) -> DataFrame:
    """
    Recompute the expected temporal-spatial output from Silver using the same
    approved spatial encoding logic as the Gold table.

    Args:
        silver_source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Expected hourly temporal-spatial distribution grouped at
            analysis_date + analysis_hour_local + spatial_cell_id.
    """
    prepared_df = _prepare_temporal_spatial_source(silver_source_df)
    spatialized_df = _add_spatial_cell_columns(prepared_df)

    return (
        spatialized_df.groupBy(
            "analysis_date",
            "analysis_hour_local",
            "spatial_cell_id",
            "spatial_resolution",
            "cell_center_latitude",
            "cell_center_longitude",
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias(
                "expected_observed_position_records_count"
            ),
            F.countDistinct(
                F.when(
                    F.col("vehicle_prefix").isNotNull()
                    & (F.trim(F.col("vehicle_prefix")) != ""),
                    F.col("vehicle_prefix"),
                )
            ).cast("long").alias("expected_distinct_observed_vehicles_count"),
            F.countDistinct(
                F.when(
                    F.col("line_sign").isNotNull()
                    & (F.trim(F.col("line_sign")) != ""),
                    F.col("line_sign"),
                )
            ).cast("long").alias("expected_distinct_observed_lines_count"),
            F.min("captured_at_local").alias("expected_min_captured_at_local"),
            F.max("captured_at_local").alias("expected_max_captured_at_local"),
        )
    )


def _build_cell_consistency_df(
    validated_df: DataFrame,
    expected_gold_df: DataFrame,
) -> DataFrame:
    """
    Build a full cell-level consistency DataFrame between actual Gold output
    and expected output recomputed from Silver.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_temporal_spatial_patterns.
        expected_gold_df:
            Expected output recomputed from Silver.

    Returns:
        DataFrame:
            Cell-level consistency DataFrame with actual and expected fields.
    """
    actual_df = validated_df.select(
        "analysis_date",
        "analysis_hour_local",
        "spatial_cell_id",
        F.col("spatial_resolution").alias("actual_spatial_resolution"),
        F.col("cell_center_latitude").alias("actual_cell_center_latitude"),
        F.col("cell_center_longitude").alias("actual_cell_center_longitude"),
        F.col("observed_position_records_count").alias(
            "actual_observed_position_records_count"
        ),
        F.col("distinct_observed_vehicles_count").alias(
            "actual_distinct_observed_vehicles_count"
        ),
        F.col("distinct_observed_lines_count").alias(
            "actual_distinct_observed_lines_count"
        ),
        F.col("min_captured_at_local").alias("actual_min_captured_at_local"),
        F.col("max_captured_at_local").alias("actual_max_captured_at_local"),
    )

    expected_df = expected_gold_df.select(
        "analysis_date",
        "analysis_hour_local",
        "spatial_cell_id",
        F.col("spatial_resolution").alias("expected_spatial_resolution"),
        F.col("cell_center_latitude").alias("expected_cell_center_latitude"),
        F.col("cell_center_longitude").alias("expected_cell_center_longitude"),
        "expected_observed_position_records_count",
        "expected_distinct_observed_vehicles_count",
        "expected_distinct_observed_lines_count",
        "expected_min_captured_at_local",
        "expected_max_captured_at_local",
    )

    return actual_df.join(
        expected_df,
        on=["analysis_date", "analysis_hour_local", "spatial_cell_id"],
        how="full",
    )


def _build_hourly_reconciliation_df(
    validated_df: DataFrame,
    expected_gold_df: DataFrame,
) -> DataFrame:
    """
    Build an hourly reconciliation DataFrame between actual Gold totals and
    expected totals recomputed from Silver.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_temporal_spatial_patterns.
        expected_gold_df:
            Expected output recomputed from Silver.

    Returns:
        DataFrame:
            Hourly reconciliation DataFrame with actual and expected totals.
    """
    actual_hourly_df = (
        validated_df.groupBy("analysis_date", "analysis_hour_local")
        .agg(
            F.countDistinct("spatial_cell_id").cast("long").alias(
                "actual_active_spatial_cells_count"
            ),
            F.coalesce(F.sum("observed_position_records_count"), F.lit(0))
            .cast("long")
            .alias("actual_total_observed_position_records_count"),
        )
    )

    expected_hourly_df = (
        expected_gold_df.groupBy("analysis_date", "analysis_hour_local")
        .agg(
            F.countDistinct("spatial_cell_id").cast("long").alias(
                "expected_active_spatial_cells_count"
            ),
            F.coalesce(F.sum("expected_observed_position_records_count"), F.lit(0))
            .cast("long")
            .alias("expected_total_observed_position_records_count"),
        )
    )

    return actual_hourly_df.join(
        expected_hourly_df,
        on=["analysis_date", "analysis_hour_local"],
        how="full",
    )


def _validate_analysis_date_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that analysis_date is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="analysis_date_not_null",
        validation_description="analysis_date must not be null.",
        severity="ERROR",
        violation_condition=F.col("analysis_date").isNull(),
    )


def _validate_analysis_hour_local_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that analysis_hour_local is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="analysis_hour_local_not_null",
        validation_description="analysis_hour_local must not be null.",
        severity="ERROR",
        violation_condition=F.col("analysis_hour_local").isNull(),
    )


def _validate_spatial_cell_id_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that spatial_cell_id is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="spatial_cell_id_not_null",
        validation_description="spatial_cell_id must not be null.",
        severity="ERROR",
        violation_condition=F.col("spatial_cell_id").isNull(),
    )


def _validate_spatial_cell_id_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that spatial_cell_id is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="spatial_cell_id_not_blank",
        validation_description="spatial_cell_id must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("spatial_cell_id")) == "",
    )


def _validate_spatial_resolution_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that spatial_resolution is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="spatial_resolution_not_null",
        validation_description="spatial_resolution must not be null.",
        severity="ERROR",
        violation_condition=F.col("spatial_resolution").isNull(),
    )


def _validate_spatial_resolution_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that spatial_resolution is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="spatial_resolution_not_blank",
        validation_description="spatial_resolution must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("spatial_resolution")) == "",
    )


def _validate_cell_center_latitude_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that cell_center_latitude is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="cell_center_latitude_not_null",
        validation_description="cell_center_latitude must not be null.",
        severity="ERROR",
        violation_condition=F.col("cell_center_latitude").isNull(),
    )


def _validate_cell_center_longitude_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that cell_center_longitude is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="cell_center_longitude_not_null",
        validation_description="cell_center_longitude must not be null.",
        severity="ERROR",
        violation_condition=F.col("cell_center_longitude").isNull(),
    )


def _validate_observed_position_records_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that observed_position_records_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_position_records_count_not_null",
        validation_description="observed_position_records_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_position_records_count").isNull(),
    )


def _validate_distinct_observed_vehicles_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_vehicles_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_observed_vehicles_count_not_null",
        validation_description="distinct_observed_vehicles_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_observed_vehicles_count").isNull(),
    )


def _validate_distinct_observed_lines_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_lines_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_observed_lines_count_not_null",
        validation_description="distinct_observed_lines_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_observed_lines_count").isNull(),
    )


def _validate_unique_analysis_date_hour_spatial_cell_id(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that analysis_date + analysis_hour_local + spatial_cell_id is unique
    at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy(
            "analysis_date",
            "analysis_hour_local",
            "spatial_cell_id",
        )
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn(
            "validation_rule",
            F.lit("unique_analysis_date_hour_spatial_cell_id"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "There must be one and only one row per analysis_date, "
                "analysis_hour_local, and spatial_cell_id."
            ),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
        )
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )


def _validate_all_expected_hourly_spatial_cells_present(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that every expected hourly spatial cell is present in the Gold output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="GRAIN",
        validation_rule="all_expected_hourly_spatial_cells_present",
        validation_description=(
            "Every expected analysis_date + analysis_hour_local + spatial_cell_id "
            "derived from valid Silver coordinates must be present in the Gold output."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_observed_position_records_count").isNotNull()
            & F.col("actual_observed_position_records_count").isNull()
        ),
    )


def _validate_no_unexpected_hourly_spatial_cells_present(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that no unexpected hourly spatial cell exists in the Gold output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="GRAIN",
        validation_rule="no_unexpected_hourly_spatial_cells_present",
        validation_description=(
            "No analysis_date + analysis_hour_local + spatial_cell_id may exist "
            "in the Gold output unless it is derivable from valid Silver "
            "coordinates using the approved spatial aggregation logic."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_observed_position_records_count").isNotNull()
            & F.col("expected_observed_position_records_count").isNull()
        ),
    )


def _validate_spatial_resolution_matches_approved_constant(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that spatial_resolution matches the approved constant.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="spatial_resolution_matches_approved_constant",
        validation_description=(
            "spatial_resolution must match the approved spatial aggregation logic."
        ),
        severity="ERROR",
        violation_condition=F.col("spatial_resolution") != F.lit(SPATIAL_RESOLUTION),
    )


def _validate_analysis_hour_local_valid_range(validated_df: DataFrame) -> DataFrame:
    """
    Validate that analysis_hour_local is inside the valid local-hour range.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="analysis_hour_local_valid_range",
        validation_description=(
            "analysis_hour_local must be between 0 and 23 inclusive."
        ),
        severity="ERROR",
        violation_condition=~F.col("analysis_hour_local").between(0, 23),
    )


def _validate_spatial_resolution_reconciles_with_expected_mapping(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that actual spatial_resolution reconciles with the expected mapping.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="spatial_resolution_reconciles_with_expected_mapping",
        validation_description=(
            "spatial_resolution must reconcile with the approved coordinate-based "
            "spatial aggregation mapping."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_spatial_resolution").isNotNull()
            & (
                F.col("actual_spatial_resolution").isNull()
                | (
                    F.col("actual_spatial_resolution")
                    != F.col("expected_spatial_resolution")
                )
            )
        ),
    )


def _validate_analysis_hour_local_reconciles_with_expected_mapping(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that analysis_hour_local reconciles with the local-hour mapping.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="analysis_hour_local_reconciles_with_expected_mapping",
        validation_description=(
            "analysis_hour_local must reconcile with the approved local-hour "
            "assignment logic."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_observed_position_records_count").isNotNull()
            & F.col("expected_observed_position_records_count").isNotNull()
            & (
                F.col("analysis_hour_local").isNull()
                | ~F.col("analysis_hour_local").between(0, 23)
            )
        ),
    )


def _validate_cell_center_latitude_reconciles_with_expected_mapping(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that actual cell_center_latitude reconciles with the expected mapping.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="cell_center_latitude_reconciles_with_expected_mapping",
        validation_description=(
            "cell_center_latitude must reconcile with the approved coordinate-based "
            "spatial aggregation logic."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_cell_center_latitude").isNotNull()
            & (
                F.col("actual_cell_center_latitude").isNull()
                | (
                    F.abs(
                        F.col("actual_cell_center_latitude")
                        - F.col("expected_cell_center_latitude")
                    )
                    > F.lit(FLOAT_TOLERANCE)
                )
            )
        ),
    )


def _validate_cell_center_longitude_reconciles_with_expected_mapping(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that actual cell_center_longitude reconciles with the expected mapping.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="cell_center_longitude_reconciles_with_expected_mapping",
        validation_description=(
            "cell_center_longitude must reconcile with the approved coordinate-based "
            "spatial aggregation logic."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_cell_center_longitude").isNotNull()
            & (
                F.col("actual_cell_center_longitude").isNull()
                | (
                    F.abs(
                        F.col("actual_cell_center_longitude")
                        - F.col("expected_cell_center_longitude")
                    )
                    > F.lit(FLOAT_TOLERANCE)
                )
            )
        ),
    )


def _validate_distinct_observed_vehicles_count_uses_vehicle_prefix(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_vehicles_count reconciles with counts based on vehicle_prefix.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="distinct_observed_vehicles_count_uses_vehicle_prefix",
        validation_description=(
            "distinct_observed_vehicles_count must reconcile with the expected "
            "distinct count derived from vehicle_prefix."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_distinct_observed_vehicles_count").isNotNull()
            & (
                F.col("actual_distinct_observed_vehicles_count").isNull()
                | (
                    F.col("actual_distinct_observed_vehicles_count")
                    != F.col("expected_distinct_observed_vehicles_count")
                )
            )
        ),
    )


def _validate_distinct_observed_lines_count_uses_line_sign(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_lines_count reconciles with counts based on line_sign.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="distinct_observed_lines_count_uses_line_sign",
        validation_description=(
            "distinct_observed_lines_count must reconcile with the expected "
            "distinct count derived from line_sign."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_distinct_observed_lines_count").isNotNull()
            & (
                F.col("actual_distinct_observed_lines_count").isNull()
                | (
                    F.col("actual_distinct_observed_lines_count")
                    != F.col("expected_distinct_observed_lines_count")
                )
            )
        ),
    )


def _validate_observed_position_records_count_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that observed_position_records_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_position_records_count_minimum",
        validation_description=(
            "observed_position_records_count must be greater than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("observed_position_records_count").isNull()
            | (F.col("observed_position_records_count") < 1)
        ),
    )


def _validate_distinct_observed_vehicles_count_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_vehicles_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_observed_vehicles_count_non_negative",
        validation_description=(
            "distinct_observed_vehicles_count must be greater than or equal to 0."
        ),
        severity="ERROR",
        violation_condition=F.col("distinct_observed_vehicles_count") < 0,
    )


def _validate_distinct_observed_lines_count_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_lines_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_observed_lines_count_non_negative",
        validation_description=(
            "distinct_observed_lines_count must be greater than or equal to 0."
        ),
        severity="ERROR",
        violation_condition=F.col("distinct_observed_lines_count") < 0,
    )


def _validate_captured_at_interval_order(validated_df: DataFrame) -> DataFrame:
    """
    Validate that min_captured_at_local is less than or equal to max_captured_at_local
    when both timestamps are populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="captured_at_interval_order",
        validation_description=(
            "min_captured_at_local must be less than or equal to "
            "max_captured_at_local when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("min_captured_at_local").isNotNull()
            & F.col("max_captured_at_local").isNotNull()
            & (F.col("min_captured_at_local") > F.col("max_captured_at_local"))
        ),
    )


def _validate_cell_center_latitude_valid_range(validated_df: DataFrame) -> DataFrame:
    """
    Validate that cell_center_latitude is in a valid geographic range.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="cell_center_latitude_valid_range",
        validation_description=(
            "cell_center_latitude must be between -90 and 90 inclusive."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("cell_center_latitude").isNull()
            | ~F.col("cell_center_latitude").between(-90.0, 90.0)
        ),
    )


def _validate_cell_center_longitude_valid_range(validated_df: DataFrame) -> DataFrame:
    """
    Validate that cell_center_longitude is in a valid geographic range.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="cell_center_longitude_valid_range",
        validation_description=(
            "cell_center_longitude must be between -180 and 180 inclusive."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("cell_center_longitude").isNull()
            | ~F.col("cell_center_longitude").between(-180.0, 180.0)
        ),
    )


def _validate_cell_level_observed_position_records_count_reconciles(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that cell-level observed_position_records_count reconciles with the expected source-derived output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="RECONCILIATION",
        validation_rule="cell_level_observed_position_records_count_reconciles",
        validation_description=(
            "Cell-level observed_position_records_count must reconcile with the "
            "expected count of valid coordinate-level source rows."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_observed_position_records_count").isNotNull()
            & (
                F.col("actual_observed_position_records_count").isNull()
                | (
                    F.col("actual_observed_position_records_count")
                    != F.col("expected_observed_position_records_count")
                )
            )
        ),
    )


def _validate_cell_level_distinct_observed_vehicles_count_reconciles(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that cell-level distinct_observed_vehicles_count reconciles with the expected source-derived output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="RECONCILIATION",
        validation_rule="cell_level_distinct_observed_vehicles_count_reconciles",
        validation_description=(
            "Cell-level distinct_observed_vehicles_count must reconcile with the "
            "expected distinct count derived from valid source rows."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_distinct_observed_vehicles_count").isNotNull()
            & (
                F.col("actual_distinct_observed_vehicles_count").isNull()
                | (
                    F.col("actual_distinct_observed_vehicles_count")
                    != F.col("expected_distinct_observed_vehicles_count")
                )
            )
        ),
    )


def _validate_cell_level_distinct_observed_lines_count_reconciles(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that cell-level distinct_observed_lines_count reconciles with the expected source-derived output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="RECONCILIATION",
        validation_rule="cell_level_distinct_observed_lines_count_reconciles",
        validation_description=(
            "Cell-level distinct_observed_lines_count must reconcile with the "
            "expected distinct count derived from valid source rows."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_distinct_observed_lines_count").isNotNull()
            & (
                F.col("actual_distinct_observed_lines_count").isNull()
                | (
                    F.col("actual_distinct_observed_lines_count")
                    != F.col("expected_distinct_observed_lines_count")
                )
            )
        ),
    )


def _validate_cell_level_min_captured_at_local_reconciles(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that cell-level min_captured_at_local reconciles with the expected source-derived output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="RECONCILIATION",
        validation_rule="cell_level_min_captured_at_local_reconciles",
        validation_description=(
            "Cell-level min_captured_at_local must reconcile with the expected "
            "minimum timestamp derived from valid source rows."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_min_captured_at_local").isNotNull()
            & (
                F.col("actual_min_captured_at_local").isNull()
                | (
                    F.col("actual_min_captured_at_local")
                    != F.col("expected_min_captured_at_local")
                )
            )
        ),
    )


def _validate_cell_level_max_captured_at_local_reconciles(
    cell_consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that cell-level max_captured_at_local reconciles with the expected source-derived output.
    """
    return _build_validation_result(
        validated_df=cell_consistency_df,
        validation_category="RECONCILIATION",
        validation_rule="cell_level_max_captured_at_local_reconciles",
        validation_description=(
            "Cell-level max_captured_at_local must reconcile with the expected "
            "maximum timestamp derived from valid source rows."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_max_captured_at_local").isNotNull()
            & (
                F.col("actual_max_captured_at_local").isNull()
                | (
                    F.col("actual_max_captured_at_local")
                    != F.col("expected_max_captured_at_local")
                )
            )
        ),
    )


def _validate_hourly_total_observed_position_records_count_reconciles(
    hourly_reconciliation_df: DataFrame,
) -> DataFrame:
    """
    Validate that hourly total observed position-record volume reconciles with
    the expected source-derived total.
    """
    return _build_validation_result(
        validated_df=hourly_reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="hourly_total_observed_position_records_count_reconciles",
        validation_description=(
            "For each analysis_date + analysis_hour_local, the sum of spatial "
            "cell position counts must equal the count of valid coordinate-level "
            "source rows for that same date-hour."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_total_observed_position_records_count").isNull()
            | F.col("expected_total_observed_position_records_count").isNull()
            | (
                F.col("actual_total_observed_position_records_count")
                != F.col("expected_total_observed_position_records_count")
            )
        ),
    )


def _validate_hourly_active_spatial_cells_count_reconciles(
    hourly_reconciliation_df: DataFrame,
) -> DataFrame:
    """
    Validate that hourly active spatial cell count reconciles with the expected
    source-derived total.
    """
    return _build_validation_result(
        validated_df=hourly_reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="hourly_active_spatial_cells_count_reconciles",
        validation_description=(
            "For each analysis_date + analysis_hour_local, the number of active "
            "spatial cells must equal the number of expected hourly spatial cells "
            "derived from valid source coordinates."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_active_spatial_cells_count").isNull()
            | F.col("expected_active_spatial_cells_count").isNull()
            | (
                F.col("actual_active_spatial_cells_count")
                != F.col("expected_active_spatial_cells_count")
            )
        ),
    )
