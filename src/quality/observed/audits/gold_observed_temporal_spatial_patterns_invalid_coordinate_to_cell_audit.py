"""
Gold observed temporal-spatial patterns invalid coordinate-to-cell audit pipeline.

Purpose:
    Identify potential issues in the spatial encoding step used by
    gold_observed_temporal_spatial_patterns.

Audit focus:
    - inconsistent cell center values
    - null representative coordinates when required
    - coordinates outside expected valid range before cell derivation

Why this audit matters:
    This audit helps validate that the spatial encoding logic is stable and
    that the Gold temporal-spatial cell representation is compatible with the
    source coordinate data and the approved spatial aggregation rule.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
GOLD_SOURCE_TABLE = "main.gold.gold_observed_temporal_spatial_patterns"
OUTPUT_TABLE_NAME = (
    "gold.gold_observed_temporal_spatial_patterns_invalid_coordinate_to_cell_audit"
)

FIXED_GRID_CELL_SIZE_DEGREES = 0.01
SPATIAL_RESOLUTION = "FIXED_GRID_0_01_DEG"
COORDINATE_COMPARISON_TOLERANCE = 1e-6


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying potential issues in the spatial encoding step "
        "for gold_observed_temporal_spatial_patterns, including invalid source "
        "coordinates, null representative cell coordinates, out-of-range cell "
        "center values, inconsistent cell center values, and unexpected "
        "spatial resolution values."
    ),
)
def gold_observed_temporal_spatial_patterns_invalid_coordinate_to_cell_audit() -> DataFrame:
    """
    Materialize the invalid coordinate-to-cell audit for
    gold_observed_temporal_spatial_patterns.

    Returns:
        DataFrame:
            Audit rows for source-coordinate and Gold cell-encoding issues.
    """
    silver_source_df = spark.read.table(SILVER_SOURCE_TABLE)
    gold_source_df = spark.read.table(GOLD_SOURCE_TABLE)

    return _build_gold_observed_temporal_spatial_patterns_invalid_coordinate_to_cell_audit(
        silver_source_df,
        gold_source_df,
    )


def _prepare_temporal_spatial_source(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions source for temporal-spatial encoding audit.

    Processing steps:
        1. Preserve or derive the local analysis date.
        2. Preserve or derive the local analysis hour.
        3. Keep only fields needed for coordinate validation and mapping checks.
        4. Cast fields to expected analytical types.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Prepared source dataset for coordinate validation and mapping checks.
    """
    return source_df.select(
        F.coalesce(
            F.col("captured_date_local").cast("date"),
            F.to_date(F.col("captured_at_local")),
        ).alias("analysis_date"),
        F.coalesce(
            F.col("captured_hour_local").cast("int"),
            F.hour(F.col("captured_at_local")),
        ).alias("analysis_hour_local"),
        F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
        F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
        F.col("line_sign").cast("string").alias("line_sign"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
    )


def _add_spatial_cell_columns(prepared_df: DataFrame) -> DataFrame:
    """
    Add fixed-grid spatial cell columns using the same encoding rule used by
    the Gold temporal-spatial patterns table.

    Args:
        prepared_df:
            Prepared source dataset with valid coordinates.

    Returns:
        DataFrame:
            Spatialized source dataset with derived expected cell identifiers and
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
        .withColumn("expected_spatial_resolution", F.lit(SPATIAL_RESOLUTION))
        .withColumn(
            "expected_cell_center_latitude",
            F.round(
                (
                    (F.col("cell_lat_index").cast("double") + F.lit(0.5))
                    * F.lit(FIXED_GRID_CELL_SIZE_DEGREES)
                ),
                6,
            ),
        )
        .withColumn(
            "expected_cell_center_longitude",
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
                F.col("expected_spatial_resolution"),
                F.col("cell_lat_index").cast("string"),
                F.col("cell_lon_index").cast("string"),
            ),
        )
    )


def _build_invalid_source_coordinate_rows(prepared_silver_df: DataFrame) -> DataFrame:
    """
    Build audit rows for invalid source coordinates before spatial cell derivation.

    Args:
        prepared_silver_df:
            Prepared Silver source dataset.

    Returns:
        DataFrame:
            Audit rows for invalid source coordinate records.
    """
    invalid_source_df = (
        prepared_silver_df.withColumn(
            "has_null_latitude",
            F.col("latitude").isNull(),
        )
        .withColumn(
            "has_null_longitude",
            F.col("longitude").isNull(),
        )
        .withColumn(
            "has_latitude_out_of_range",
            F.col("latitude").isNotNull() & ~F.col("latitude").between(-90.0, 90.0),
        )
        .withColumn(
            "has_longitude_out_of_range",
            F.col("longitude").isNotNull() & ~F.col("longitude").between(-180.0, 180.0),
        )
        .withColumn(
            "has_invalid_analysis_hour_local",
            F.col("analysis_hour_local").isNull()
            | ~F.col("analysis_hour_local").between(0, 23),
        )
        .withColumn(
            "has_invalid_source_coordinates",
            F.col("has_null_latitude")
            | F.col("has_null_longitude")
            | F.col("has_latitude_out_of_range")
            | F.col("has_longitude_out_of_range"),
        )
        .where(
            (F.col("has_invalid_source_coordinates") == True)
            | (F.col("has_invalid_analysis_hour_local") == True)
        )
        .withColumn("audit_source", F.lit("SILVER_SOURCE"))
        .withColumn(
            "audit_reason",
            F.concat_ws(
                ", ",
                F.when(F.col("has_null_latitude"), F.lit("NULL_LATITUDE")),
                F.when(F.col("has_null_longitude"), F.lit("NULL_LONGITUDE")),
                F.when(
                    F.col("has_latitude_out_of_range"),
                    F.lit("LATITUDE_OUT_OF_RANGE"),
                ),
                F.when(
                    F.col("has_longitude_out_of_range"),
                    F.lit("LONGITUDE_OUT_OF_RANGE"),
                ),
                F.when(
                    F.col("has_invalid_analysis_hour_local"),
                    F.lit("INVALID_ANALYSIS_HOUR_LOCAL"),
                ),
            ),
        )
        .withColumn("spatial_cell_id", F.lit(None).cast("string"))
        .withColumn("actual_spatial_resolution", F.lit(None).cast("string"))
        .withColumn("expected_spatial_resolution", F.lit(None).cast("string"))
        .withColumn("actual_cell_center_latitude", F.lit(None).cast("double"))
        .withColumn("actual_cell_center_longitude", F.lit(None).cast("double"))
        .withColumn("expected_cell_center_latitude", F.lit(None).cast("double"))
        .withColumn("expected_cell_center_longitude", F.lit(None).cast("double"))
        .withColumn("has_null_representative_coordinates", F.lit(False))
        .withColumn("has_cell_center_out_of_range", F.lit(False))
        .withColumn("has_inconsistent_cell_center_values", F.lit(False))
        .withColumn("has_unexpected_spatial_resolution", F.lit(False))
        .withColumn("has_missing_expected_mapping_from_source", F.lit(False))
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "audit_source",
            "audit_reason",
            "analysis_date",
            "analysis_hour_local",
            "spatial_cell_id",
            "captured_at_local",
            "vehicle_prefix",
            "line_sign",
            F.col("latitude").alias("raw_latitude"),
            F.col("longitude").alias("raw_longitude"),
            "actual_spatial_resolution",
            "expected_spatial_resolution",
            "actual_cell_center_latitude",
            "actual_cell_center_longitude",
            "expected_cell_center_latitude",
            "expected_cell_center_longitude",
            "has_invalid_source_coordinates",
            "has_invalid_analysis_hour_local",
            "has_null_representative_coordinates",
            "has_cell_center_out_of_range",
            "has_inconsistent_cell_center_values",
            "has_unexpected_spatial_resolution",
            "has_missing_expected_mapping_from_source",
            "record_created_at",
        )
    )

    return invalid_source_df


def _build_gold_cell_consistency_rows(
    prepared_silver_df: DataFrame,
    gold_source_df: DataFrame,
) -> DataFrame:
    """
    Build audit rows for Gold cell-representation inconsistencies.

    Args:
        prepared_silver_df:
            Prepared Silver source dataset.
        gold_source_df:
            Input DataFrame from gold.gold_observed_temporal_spatial_patterns.

    Returns:
        DataFrame:
            Audit rows for Gold cell-representation inconsistencies.
    """
    valid_source_df = (
        prepared_silver_df.where(F.col("analysis_date").isNotNull())
        .where(F.col("analysis_hour_local").isNotNull())
        .where(F.col("analysis_hour_local").between(0, 23))
        .where(F.col("captured_at_local").isNotNull())
        .where(F.col("latitude").isNotNull())
        .where(F.col("longitude").isNotNull())
        .where(F.col("latitude").between(-90.0, 90.0))
        .where(F.col("longitude").between(-180.0, 180.0))
    )

    expected_mapping_df = (
        _add_spatial_cell_columns(valid_source_df)
        .select(
            "analysis_date",
            "analysis_hour_local",
            "spatial_cell_id",
            "expected_spatial_resolution",
            "expected_cell_center_latitude",
            "expected_cell_center_longitude",
        )
        .distinct()
    )

    gold_consistency_df = (
        gold_source_df.select(
            F.col("analysis_date").cast("date").alias("analysis_date"),
            F.col("analysis_hour_local").cast("int").alias("analysis_hour_local"),
            F.col("spatial_cell_id").cast("string").alias("spatial_cell_id"),
            F.col("spatial_resolution").cast("string").alias("actual_spatial_resolution"),
            F.col("cell_center_latitude").cast("double").alias(
                "actual_cell_center_latitude"
            ),
            F.col("cell_center_longitude").cast("double").alias(
                "actual_cell_center_longitude"
            ),
            F.col("min_captured_at_local").cast("timestamp").alias("captured_at_local"),
        )
        .join(
            expected_mapping_df,
            on=["analysis_date", "analysis_hour_local", "spatial_cell_id"],
            how="left",
        )
        .withColumn("has_invalid_source_coordinates", F.lit(False))
        .withColumn("has_invalid_analysis_hour_local", F.lit(False))
        .withColumn(
            "has_null_representative_coordinates",
            F.col("actual_cell_center_latitude").isNull()
            | F.col("actual_cell_center_longitude").isNull(),
        )
        .withColumn(
            "has_cell_center_out_of_range",
            (
                F.col("actual_cell_center_latitude").isNotNull()
                & ~F.col("actual_cell_center_latitude").between(-90.0, 90.0)
            )
            | (
                F.col("actual_cell_center_longitude").isNotNull()
                & ~F.col("actual_cell_center_longitude").between(-180.0, 180.0)
            ),
        )
        .withColumn(
            "has_unexpected_spatial_resolution",
            F.col("actual_spatial_resolution").isNull()
            | (F.col("actual_spatial_resolution") != F.lit(SPATIAL_RESOLUTION)),
        )
        .withColumn(
            "has_missing_expected_mapping_from_source",
            F.col("expected_spatial_resolution").isNull(),
        )
        .withColumn(
            "has_inconsistent_cell_center_values",
            F.col("expected_spatial_resolution").isNotNull()
            & F.col("actual_cell_center_latitude").isNotNull()
            & F.col("actual_cell_center_longitude").isNotNull()
            & (
                (
                    F.abs(
                        F.col("actual_cell_center_latitude")
                        - F.col("expected_cell_center_latitude")
                    )
                    > F.lit(COORDINATE_COMPARISON_TOLERANCE)
                )
                | (
                    F.abs(
                        F.col("actual_cell_center_longitude")
                        - F.col("expected_cell_center_longitude")
                    )
                    > F.lit(COORDINATE_COMPARISON_TOLERANCE)
                )
            ),
        )
        .withColumn(
            "has_coordinate_to_cell_issue",
            F.col("has_null_representative_coordinates")
            | F.col("has_cell_center_out_of_range")
            | F.col("has_unexpected_spatial_resolution")
            | F.col("has_missing_expected_mapping_from_source")
            | F.col("has_inconsistent_cell_center_values"),
        )
        .where(F.col("has_coordinate_to_cell_issue") == True)
        .withColumn("audit_source", F.lit("GOLD_CELL_ENCODING"))
        .withColumn(
            "audit_reason",
            F.concat_ws(
                ", ",
                F.when(
                    F.col("has_null_representative_coordinates"),
                    F.lit("NULL_REPRESENTATIVE_COORDINATES"),
                ),
                F.when(
                    F.col("has_cell_center_out_of_range"),
                    F.lit("CELL_CENTER_OUT_OF_RANGE"),
                ),
                F.when(
                    F.col("has_unexpected_spatial_resolution"),
                    F.lit("UNEXPECTED_SPATIAL_RESOLUTION"),
                ),
                F.when(
                    F.col("has_missing_expected_mapping_from_source"),
                    F.lit("MISSING_EXPECTED_MAPPING_FROM_SOURCE"),
                ),
                F.when(
                    F.col("has_inconsistent_cell_center_values"),
                    F.lit("INCONSISTENT_CELL_CENTER_VALUES"),
                ),
            ),
        )
        .withColumn("vehicle_prefix", F.lit(None).cast("string"))
        .withColumn("line_sign", F.lit(None).cast("string"))
        .withColumn("raw_latitude", F.lit(None).cast("double"))
        .withColumn("raw_longitude", F.lit(None).cast("double"))
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "audit_source",
            "audit_reason",
            "analysis_date",
            "analysis_hour_local",
            "spatial_cell_id",
            "captured_at_local",
            "vehicle_prefix",
            "line_sign",
            "raw_latitude",
            "raw_longitude",
            "actual_spatial_resolution",
            "expected_spatial_resolution",
            "actual_cell_center_latitude",
            "actual_cell_center_longitude",
            "expected_cell_center_latitude",
            "expected_cell_center_longitude",
            "has_invalid_source_coordinates",
            "has_invalid_analysis_hour_local",
            "has_null_representative_coordinates",
            "has_cell_center_out_of_range",
            "has_inconsistent_cell_center_values",
            "has_unexpected_spatial_resolution",
            "has_missing_expected_mapping_from_source",
            "record_created_at",
        )
    )

    return gold_consistency_df


def _build_gold_observed_temporal_spatial_patterns_invalid_coordinate_to_cell_audit(
    silver_source_df: DataFrame,
    gold_source_df: DataFrame,
) -> DataFrame:
    """
    Build the invalid coordinate-to-cell audit dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions source.
        2. Identify invalid source coordinate rows before spatial derivation.
        3. Recompute expected spatial mapping for valid source coordinates.
        4. Validate the Gold representative spatial cell values against the
           expected mapping.
        5. Union all coordinate-to-cell issues into a single audit output.

    Args:
        silver_source_df:
            Input DataFrame from silver.silver_sptrans_positions.
        gold_source_df:
            Input DataFrame from gold.gold_observed_temporal_spatial_patterns.

    Returns:
        DataFrame:
            Audit DataFrame containing invalid source-coordinate rows and Gold
            cell-encoding inconsistencies.
    """
    prepared_silver_df = _prepare_temporal_spatial_source(silver_source_df)

    invalid_source_rows_df = _build_invalid_source_coordinate_rows(prepared_silver_df)
    gold_cell_consistency_rows_df = _build_gold_cell_consistency_rows(
        prepared_silver_df,
        gold_source_df,
    )

    return invalid_source_rows_df.unionByName(gold_cell_consistency_rows_df)
