"""
Gold observed lines daily pipeline.

Purpose:
    Build the canonical observed-side daily line catalog used by the
    Planned vs Observed Service Comparison component.

Business role:
    This dataset represents the SPTrans observed line reference already cleaned
    and validated in the Silver layer. It normalizes and aggregates observed
    line identifiers at daily grain so they can be compared against planned
    GTFS line codes.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_observed_lines_daily

Official business key:
    - observed side: line_sign
    - normalized comparison key: line_code = UPPER(TRIM(line_sign))

Important rules:
    - Gold must consume the cleaned Silver dataset
    - one row per analysis_date and normalized line_code
    - no semantic rewriting of line identifiers is allowed
    - event-level duplication must be collapsed into daily line-level output
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_observed_lines_daily import (
    get_gold_observed_lines_daily_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_observed_lines_daily"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical observed daily lines reference derived from "
        "silver_sptrans_positions. Contains one row per analysis date and "
        "normalized observed line code and is used as the observed-side "
        "business reference for the Planned vs Observed Service Comparison component."
    ),
    schema=get_gold_observed_lines_daily_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_line_code",
    "line_code IS NOT NULL AND TRIM(line_code) <> ''",
)
@dp.expect_or_drop(
    "valid_observed_line_sign",
    "observed_line_sign IS NOT NULL AND TRIM(observed_line_sign) <> ''",
)
def gold_observed_lines_daily() -> DataFrame:
    """
    Materialize the canonical Gold observed daily lines reference table.

    Returns:
        DataFrame:
            Observed-side normalized daily line catalog derived from
            Silver SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_observed_lines_daily(source_df)


def _normalize_line_code(column_name: str):
    """
    Normalize a public transit line identifier into the official comparison key.

    Normalization rule:
        UPPER(TRIM(value))

    Args:
        column_name:
            Name of the input column containing the raw observed line identifier.

    Returns:
        pyspark.sql.Column:
            Normalized line code expression.
    """
    return F.upper(F.trim(F.col(column_name)))


def _build_gold_observed_lines_daily(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold observed daily lines reference from the
    Silver SPTrans positions table.

    Processing steps:
        1. Select observed-side business and supporting fields from Silver.
        2. Derive analysis_date from captured_date_local.
        3. Normalize line_sign into line_code.
        4. Aggregate to one row per analysis_date and normalized line_code.
        5. Preserve representative observed-side fields and daily counts.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical observed daily lines reference with one row per
            analysis_date and normalized line code.
    """
    prepared_df = (
        source_df.select(
            F.col("captured_date_local").cast("date").alias("analysis_date"),
            F.col("line_sign").cast("string").alias("observed_line_sign"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("observed_line_sign").isNotNull())
        .where(F.trim(F.col("observed_line_sign")) != "")
        .withColumn("line_code", _normalize_line_code("observed_line_sign"))
    )

    aggregated_df = (
        prepared_df.groupBy("analysis_date", "line_code")
        .agg(
            F.first("observed_line_sign", ignorenulls=True).alias("observed_line_sign"),
            F.count(F.lit(1)).cast("long").alias("observed_events_count"),
            F.countDistinct("vehicle_prefix").cast("long").alias("observed_vehicles_count"),
            F.min("captured_at_local").alias("min_captured_at_local"),
            F.max("captured_at_local").alias("max_captured_at_local"),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "analysis_date",
        "line_code",
        "observed_line_sign",
        "observed_events_count",
        "observed_vehicles_count",
        "min_captured_at_local",
        "max_captured_at_local",
        "record_created_at",
    )
