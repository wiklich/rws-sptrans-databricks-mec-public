"""
Gold planned lines reference pipeline.

Purpose:
    Build the canonical planned-side line catalog used by the
    Planned vs Observed Service Comparison component.

Business role:
    This dataset represents the GTFS planned line reference already cleaned and
    validated in the Silver layer. It normalizes and deduplicates GTFS route
    identifiers so they can be compared against observed line codes from the
    SPTrans Posicao dataset.

Input:
    - silver.silver_gtfs_routes

Output:
    - gold.gold_planned_lines_reference

Official business key:
    - planned side: route_id
    - normalized comparison key: line_code = UPPER(TRIM(route_id))

Important rules:
    - Gold must consume the cleaned Silver dataset, not Bronze
    - one row per normalized line_code
    - no semantic rewriting of line identifiers is allowed
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_planned_reference.schemas.schema_gold_planned_lines_reference import (
    get_gold_planned_lines_reference_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_gtfs_routes"
OUTPUT_TABLE_NAME = "gold.gold_planned_lines_reference"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical planned lines reference derived from silver_gtfs_routes. "
        "Contains one row per normalized line code and is used as the planned-side "
        "business reference for the Planned vs Observed Service Comparison component."
    ),
    schema=get_gold_planned_lines_reference_schema(),
)
@dp.expect_or_drop("valid_line_code", "line_code IS NOT NULL AND TRIM(line_code) <> ''")
@dp.expect_or_drop(
    "valid_planned_route_id",
    "planned_route_id IS NOT NULL AND TRIM(planned_route_id) <> ''",
)
def gold_planned_lines_reference() -> DataFrame:
    """
    Materialize the canonical Gold planned lines reference table.

    Returns:
        DataFrame:
            Planned-side normalized line catalog derived from Silver GTFS routes.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_planned_lines_reference(source_df)

def _normalize_line_code(column_name: str):
    """
    Normalize a public transit line identifier into the official comparison key.

    Normalization rule:
        UPPER(TRIM(value))

    Args:
        column_name:
            Name of the input column containing the raw planned line identifier.

    Returns:
        pyspark.sql.Column:
            Normalized line code expression.
    """
    return F.upper(F.trim(F.col(column_name)))


def _build_gold_planned_lines_reference(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold planned lines reference from the Silver GTFS routes table.

    Processing steps:
        1. Select planned-side route fields from Silver.
        2. Normalize route_id into line_code.
        3. Deduplicate by normalized line_code.
        4. Preserve business-descriptive and lineage fields needed downstream.

    Args:
        source_df:
            Input DataFrame from silver.silver_gtfs_routes.

    Returns:
        DataFrame:
            Canonical planned lines reference with one row per normalized line code.
    """
    prepared_df = (
        source_df.select(
            F.col("route_id").cast("string").alias("planned_route_id"),
            F.col("route_short_name").cast("string").alias("planned_route_short_name"),
            F.col("route_long_name").cast("string").alias("planned_route_long_name"),
            F.col("route_name_display").cast("string").alias("planned_route_name_display"),
            F.col("route_type").cast("int").alias("route_type"),
            F.col("route_type_description").cast("string").alias("route_type_description"),
            F.col("route_color").cast("string").alias("route_color"),
            F.col("route_color_name").cast("string").alias("route_color_name"),
            F.col("route_text_color").cast("string").alias("route_text_color"),
            F.col("route_text_color_name").cast("string").alias("route_text_color_name"),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )
        .withColumn("line_code", _normalize_line_code("planned_route_id"))
    )

    deduplicated_df = (
        prepared_df.groupBy("line_code")
        .agg(
            F.first("planned_route_id", ignorenulls=True).alias("planned_route_id"),
            F.first("planned_route_short_name", ignorenulls=True).alias("planned_route_short_name"),
            F.first("planned_route_long_name", ignorenulls=True).alias("planned_route_long_name"),
            F.first("planned_route_name_display", ignorenulls=True).alias("planned_route_name_display"),
            F.first("route_type", ignorenulls=True).alias("route_type"),
            F.first("route_type_description", ignorenulls=True).alias("route_type_description"),
            F.first("route_color", ignorenulls=True).alias("route_color"),
            F.first("route_color_name", ignorenulls=True).alias("route_color_name"),
            F.first("route_text_color", ignorenulls=True).alias("route_text_color"),
            F.first("route_text_color_name", ignorenulls=True).alias("route_text_color_name"),
            F.first("gtfs_load_date", ignorenulls=True).alias("gtfs_load_date"),
            F.first("gtfs_loaded_at", ignorenulls=True).alias("gtfs_loaded_at"),
            F.first("record_hash", ignorenulls=True).alias("record_hash"),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return deduplicated_df.select(
        "line_code",
        "planned_route_id",
        "planned_route_short_name",
        "planned_route_long_name",
        "planned_route_name_display",
        "route_type",
        "route_type_description",
        "route_color",
        "route_color_name",
        "route_text_color",
        "route_text_color_name",
        "gtfs_load_date",
        "gtfs_loaded_at",
        "record_hash",
        "record_created_at",
    )
