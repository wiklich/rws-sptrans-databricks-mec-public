"""
Gold planned lines reference duplicate normalized line code audit pipeline.

Purpose:
    Identify cases where multiple upstream Silver GTFS route records collapse
    into the same normalized planned line code.

Audit focus:
    - same normalized line_code generated from multiple planned_route_id values
    - same normalized line_code generated from multiple raw formatting variants
      of route_id

Why this audit matters:
    This is the most important audit for gold_planned_lines_reference because it
    validates the stability of the planned-side business key after normalization.

Input:
    - silver.silver_gtfs_routes

Output:
    - gold.gold_planned_lines_reference_duplicate_line_code_audit
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_SOURCE_TABLE = "main.silver.silver_gtfs_routes"
OUTPUT_TABLE_NAME = "gold.gold_planned_lines_reference_duplicate_line_code_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying normalized planned line codes that collapse "
        "multiple upstream Silver GTFS route records into the same Gold business key."
    ),
)
def gold_planned_lines_reference_duplicate_line_code_audit() -> DataFrame:
    """
    Materialize the duplicate normalized line code audit for the planned lines reference.

    Returns:
        DataFrame:
            Audit rows for normalized line codes generated from more than one
            upstream Silver GTFS route record.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_planned_lines_reference_duplicate_line_code_audit(source_df)


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


def _build_gold_planned_lines_reference_duplicate_line_code_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate normalized line code audit dataset.

    Processing steps:
        1. Read Silver GTFS routes.
        2. Keep only records with a valid route_id.
        3. Normalize route_id into line_code.
        4. Aggregate by normalized line_code.
        5. Keep only line_code values backed by more than one upstream record.

    Args:
        source_df:
            Input DataFrame from silver.silver_gtfs_routes.

    Returns:
        DataFrame:
            Audit DataFrame containing normalized line codes that collapse
            multiple upstream records.
    """
    prepared_df = (
        source_df.select(
            F.col("route_id").cast("string").alias("planned_route_id"),
            F.col("route_short_name").cast("string").alias("planned_route_short_name"),
            F.col("route_long_name").cast("string").alias("planned_route_long_name"),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
        )
        .where(F.col("planned_route_id").isNotNull())
        .where(F.trim(F.col("planned_route_id")) != "")
        .withColumn("line_code", _normalize_line_code("planned_route_id"))
        .withColumn("raw_route_id_trimmed", F.trim(F.col("planned_route_id")))
    )

    audit_df = (
        prepared_df.groupBy("line_code")
        .agg(
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("planned_route_id").alias("distinct_planned_route_id_count"),
            F.countDistinct("raw_route_id_trimmed").alias("distinct_trimmed_route_id_count"),
            F.collect_set("planned_route_id").alias("planned_route_ids"),
            F.collect_set("raw_route_id_trimmed").alias("trimmed_route_ids"),
            F.collect_set("planned_route_short_name").alias("planned_route_short_names"),
            F.collect_set("planned_route_long_name").alias("planned_route_long_names"),
            F.min("gtfs_load_date").alias("min_gtfs_load_date"),
            F.max("gtfs_load_date").alias("max_gtfs_load_date"),
            F.min("gtfs_loaded_at").alias("min_gtfs_loaded_at"),
            F.max("gtfs_loaded_at").alias("max_gtfs_loaded_at"),
        )
        .withColumn(
            "has_multiple_source_records",
            F.col("source_record_count") > F.lit(1),
        )
        .withColumn(
            "has_multiple_planned_route_ids",
            F.col("distinct_planned_route_id_count") > F.lit(1),
        )
        .withColumn(
            "has_multiple_trimmed_route_id_variants",
            F.col("distinct_trimmed_route_id_count") > F.lit(1),
        )
        .where(F.col("source_record_count") > 1)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "line_code",
            "source_record_count",
            "distinct_planned_route_id_count",
            "distinct_trimmed_route_id_count",
            "planned_route_ids",
            "trimmed_route_ids",
            "planned_route_short_names",
            "planned_route_long_names",
            "has_multiple_source_records",
            "has_multiple_planned_route_ids",
            "has_multiple_trimmed_route_id_variants",
            "min_gtfs_load_date",
            "max_gtfs_load_date",
            "min_gtfs_loaded_at",
            "max_gtfs_loaded_at",
            "record_created_at",
        )
    )

    return audit_df