"""
Gold vehicle observed spatial traces pipeline.

Purpose:
    Build the canonical observed spatial trace layer used by the
    Observed Spatial Analysis component.

Business role:
    This dataset represents a business-ready trace-detail view of observed
    vehicle position sequences derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_vehicle_observed_spatial_traces

Declared grain:
    - one row per analysis_date, vehicle_prefix, and trace_sequence

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per analysis_date, vehicle_prefix, and trace_sequence is allowed
    - trace_sequence must be ordered by local timestamp within each vehicle-day
    - only records with valid vehicle identifier, valid local timestamp, and
      valid coordinates may contribute to the trace
    - the table must represent observed trace order, not validated operational
      path, route adherence, or exact movement continuity
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.gold_observed.schemas.schema_gold_vehicle_observed_spatial_traces import (
    get_gold_vehicle_observed_spatial_traces_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observed_spatial_traces"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical observed vehicle spatial traces table derived from "
        "silver_sptrans_positions. Contains one row per analysis_date, "
        "vehicle_prefix, and trace_sequence with ordered observed coordinates, "
        "contextual line and direction fields, previous-point context, and "
        "trace-ready local timestamps."
    ),
    schema=get_gold_vehicle_observed_spatial_traces_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_vehicle_prefix",
    "vehicle_prefix IS NOT NULL AND TRIM(vehicle_prefix) <> ''",
)
@dp.expect_or_drop(
    "valid_trace_sequence",
    "trace_sequence IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_captured_at_local",
    "captured_at_local IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_latitude",
    "latitude IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_longitude",
    "longitude IS NOT NULL",
)
def gold_vehicle_observed_spatial_traces() -> DataFrame:
    """
    Materialize the canonical Gold vehicle observed spatial traces table.

    Returns:
        DataFrame:
            Observed spatial trace output derived from Silver SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_vehicle_observed_spatial_traces(source_df)


def _prepare_trace_source(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for trace sequencing.

    Processing steps:
        1. Preserve or derive the local analysis date.
        2. Keep only fields needed for trace construction.
        3. Filter rows with invalid vehicle identifier, invalid timestamp,
           or invalid coordinates.
        4. Filter rows with invalid coordinate ranges.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Prepared source dataset ready for vehicle trace sequencing.
    """
    return (
        source_df.select(
            F.coalesce(
                F.col("captured_date_local").cast("date"),
                F.to_date(F.col("captured_at_local")),
            ).alias("analysis_date"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("line_sign").cast("string").alias("line_sign"),
            F.col("direction").cast("string").alias("direction"),
            F.col("direction_description").cast("string").alias(
                "direction_description"
            ),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
        .where(F.col("captured_at_local").isNotNull())
        .where(F.col("latitude").isNotNull())
        .where(F.col("longitude").isNotNull())
        .where(F.col("latitude").between(-90.0, 90.0))
        .where(F.col("longitude").between(-180.0, 180.0))
    )


def _build_gold_vehicle_observed_spatial_traces(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold vehicle observed spatial traces dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions source.
        2. Order observed records by local timestamp within each
           analysis_date + vehicle_prefix.
        3. Assign a stable trace_sequence.
        4. Derive previous-point context fields.
        5. Materialize the final Gold output.

    Deterministic ordering rule:
        - captured_at_local ascending
        - latitude ascending
        - longitude ascending
        - line_sign ascending
        - direction ascending
        - direction_description ascending

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical observed vehicle trace output with one row per
            analysis_date, vehicle_prefix, and trace_sequence.
    """
    prepared_df = _prepare_trace_source(source_df)

    trace_window = Window.partitionBy("analysis_date", "vehicle_prefix").orderBy(
        F.col("captured_at_local").asc(),
        F.col("latitude").asc(),
        F.col("longitude").asc(),
        F.col("line_sign").asc_nulls_last(),
        F.col("direction").asc_nulls_last(),
        F.col("direction_description").asc_nulls_last(),
    )

    traced_df = (
        prepared_df.withColumn(
            "trace_sequence",
            F.row_number().over(trace_window).cast("long"),
        )
        .withColumn(
            "previous_captured_at_local",
            F.lag("captured_at_local").over(trace_window),
        )
        .withColumn(
            "previous_latitude",
            F.lag("latitude").over(trace_window),
        )
        .withColumn(
            "previous_longitude",
            F.lag("longitude").over(trace_window),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return traced_df.select(
        "analysis_date",
        "vehicle_prefix",
        "trace_sequence",
        "captured_at_local",
        "latitude",
        "longitude",
        "line_sign",
        "direction",
        "direction_description",
        "previous_captured_at_local",
        "previous_latitude",
        "previous_longitude",
        "record_created_at",
    )
