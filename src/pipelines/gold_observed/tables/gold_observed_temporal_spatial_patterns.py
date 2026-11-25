"""
Gold observed temporal-spatial patterns pipeline.

Purpose:
    Build the canonical hourly spatial distribution layer used by the
    Observed Spatial Analysis component.

Business role:
    This dataset represents a business-ready hourly view of observed spatial
    presence derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_observed_temporal_spatial_patterns

Declared grain:
    - one row per analysis_date, analysis_hour_local, and spatial_cell_id

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per analysis_date, analysis_hour_local, and spatial_cell_id is allowed
    - spatial aggregation must be based only on valid latitude and longitude
    - temporal aggregation must use local time, not UTC
    - distinct observed lines should use line_sign
    - distinct observed vehicles should use vehicle_prefix
    - the table must describe observed temporal-spatial distribution,
      not official service coverage
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_observed.schemas.schema_gold_observed_temporal_spatial_patterns import (
    get_gold_observed_temporal_spatial_patterns_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_observed_temporal_spatial_patterns"

FIXED_GRID_CELL_SIZE_DEGREES = 0.01
SPATIAL_RESOLUTION = "FIXED_GRID_0_01_DEG"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical observed temporal-spatial patterns table derived from "
        "silver_sptrans_positions. Contains one row per analysis_date, "
        "analysis_hour_local, and spatial_cell_id with observed position "
        "volume, distinct vehicles, distinct lines, representative cell center "
        "coordinates, and local timestamp bounds."
    ),
    schema=get_gold_observed_temporal_spatial_patterns_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_analysis_hour_local",
    "analysis_hour_local IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_spatial_cell_id",
    "spatial_cell_id IS NOT NULL AND TRIM(spatial_cell_id) <> ''",
)
@dp.expect_or_drop(
    "valid_spatial_resolution",
    "spatial_resolution IS NOT NULL AND TRIM(spatial_resolution) <> ''",
)
@dp.expect_or_drop(
    "valid_observed_position_records_count",
    "observed_position_records_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_observed_vehicles_count",
    "distinct_observed_vehicles_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_distinct_observed_lines_count",
    "distinct_observed_lines_count IS NOT NULL",
)
def gold_observed_temporal_spatial_patterns() -> DataFrame:
    """
    Materialize the canonical Gold observed temporal-spatial patterns table.

    Returns:
        DataFrame:
            Hourly observed temporal-spatial patterns output derived from
            Silver SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_observed_temporal_spatial_patterns(source_df)


def _prepare_temporal_spatial_source(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for temporal-spatial aggregation.

    Processing steps:
        1. Preserve or derive the local analysis date.
        2. Preserve or derive the local analysis hour.
        3. Keep only fields needed for hourly spatial aggregation.
        4. Filter rows with invalid coordinates or invalid timestamps.
        5. Filter rows with invalid coordinate ranges.

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
    Add fixed-grid spatial cell columns.

    Spatial rule:
        - latitude and longitude are bucketed using a fixed-degree grid
        - spatial_cell_id is derived from the cell indexes
        - representative cell center coordinates are derived from the indexes

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


def _build_gold_observed_temporal_spatial_patterns(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the canonical Gold observed temporal-spatial patterns dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions source.
        2. Derive the approved spatial aggregation key.
        3. Aggregate records by analysis_date, analysis_hour_local, and spatial cell.
        4. Calculate observed position volume, distinct vehicles, distinct lines,
           and timestamp bounds.
        5. Materialize the final Gold output.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical observed temporal-spatial patterns output with one row per
            analysis_date, analysis_hour_local, and spatial_cell_id.
    """
    prepared_df = _prepare_temporal_spatial_source(source_df)
    spatialized_df = _add_spatial_cell_columns(prepared_df)

    aggregated_df = (
        spatialized_df.groupBy(
            "analysis_date",
            "analysis_hour_local",
            "spatial_cell_id",
            "spatial_resolution",
            "cell_center_latitude",
            "cell_center_longitude",
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("observed_position_records_count"),
            F.countDistinct(
                F.when(
                    F.col("vehicle_prefix").isNotNull()
                    & (F.trim(F.col("vehicle_prefix")) != ""),
                    F.col("vehicle_prefix"),
                )
            ).cast("long").alias("distinct_observed_vehicles_count"),
            F.countDistinct(
                F.when(
                    F.col("line_sign").isNotNull()
                    & (F.trim(F.col("line_sign")) != ""),
                    F.col("line_sign"),
                )
            ).cast("long").alias("distinct_observed_lines_count"),
            F.min("captured_at_local").alias("min_captured_at_local"),
            F.max("captured_at_local").alias("max_captured_at_local"),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "analysis_date",
        "analysis_hour_local",
        "spatial_cell_id",
        "spatial_resolution",
        "cell_center_latitude",
        "cell_center_longitude",
        "observed_position_records_count",
        "distinct_observed_vehicles_count",
        "distinct_observed_lines_count",
        "min_captured_at_local",
        "max_captured_at_local",
        "record_created_at",
    )
