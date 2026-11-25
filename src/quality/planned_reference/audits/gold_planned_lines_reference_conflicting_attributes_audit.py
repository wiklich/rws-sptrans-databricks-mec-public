"""
Gold planned lines reference conflicting descriptive attributes audit pipeline.

Purpose:
    Identify cases where the same normalized planned line code is associated
    with conflicting descriptive attributes in upstream Silver GTFS routes.

Audit focus:
    - same normalized line_code with multiple planned_route_long_name values
    - same normalized line_code with multiple route_type values
    - same normalized line_code with multiple route_color values
    - same normalized line_code with other conflicting descriptive attributes

Why this audit matters:
    This audit is useful because gold_planned_lines_reference keeps only one
    representative row after deduplication. If upstream descriptive attributes
    conflict for the same normalized line code, the Gold table may hide that
    ambiguity by selecting only one value.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_SOURCE_TABLE = "main.silver.silver_gtfs_routes"
OUTPUT_TABLE_NAME = "gold.gold_planned_lines_reference_conflicting_attributes_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying normalized planned line codes that have "
        "conflicting descriptive attributes in upstream Silver GTFS routes."
    ),
)
def gold_planned_lines_reference_conflicting_attributes_audit() -> DataFrame:
    """
    Materialize the conflicting descriptive attributes audit for the planned lines reference.

    Returns:
        DataFrame:
            Audit rows for normalized line codes associated with conflicting
            upstream descriptive attributes.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_planned_lines_reference_conflicting_attributes_audit(source_df)


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


def _build_gold_planned_lines_reference_conflicting_attributes_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the conflicting descriptive attributes audit dataset.

    Processing steps:
        1. Read Silver GTFS routes.
        2. Keep only records with a valid route_id.
        3. Normalize route_id into line_code.
        4. Aggregate descriptive attributes by normalized line_code.
        5. Keep only line_code values with conflicting attribute values.

    Args:
        source_df:
            Input DataFrame from silver.silver_gtfs_routes.

    Returns:
        DataFrame:
            Audit DataFrame containing normalized line codes with conflicting
            descriptive attributes upstream.
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
        )
        .where(F.col("planned_route_id").isNotNull())
        .where(F.trim(F.col("planned_route_id")) != "")
        .withColumn("line_code", _normalize_line_code("planned_route_id"))
    )

    audit_df = (
        prepared_df.groupBy("line_code")
        .agg(
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("planned_route_id").alias("distinct_planned_route_id_count"),
            F.countDistinct("planned_route_short_name").alias("distinct_planned_route_short_name_count"),
            F.countDistinct("planned_route_long_name").alias("distinct_planned_route_long_name_count"),
            F.countDistinct("planned_route_name_display").alias("distinct_planned_route_name_display_count"),
            F.countDistinct("route_type").alias("distinct_route_type_count"),
            F.countDistinct("route_type_description").alias("distinct_route_type_description_count"),
            F.countDistinct("route_color").alias("distinct_route_color_count"),
            F.countDistinct("route_color_name").alias("distinct_route_color_name_count"),
            F.countDistinct("route_text_color").alias("distinct_route_text_color_count"),
            F.countDistinct("route_text_color_name").alias("distinct_route_text_color_name_count"),
            F.collect_set("planned_route_id").alias("planned_route_ids"),
            F.collect_set("planned_route_short_name").alias("planned_route_short_names"),
            F.collect_set("planned_route_long_name").alias("planned_route_long_names"),
            F.collect_set("planned_route_name_display").alias("planned_route_name_displays"),
            F.collect_set("route_type").alias("route_types"),
            F.collect_set("route_type_description").alias("route_type_descriptions"),
            F.collect_set("route_color").alias("route_colors"),
            F.collect_set("route_color_name").alias("route_color_names"),
            F.collect_set("route_text_color").alias("route_text_colors"),
            F.collect_set("route_text_color_name").alias("route_text_color_names"),
            F.min("gtfs_load_date").alias("min_gtfs_load_date"),
            F.max("gtfs_load_date").alias("max_gtfs_load_date"),
            F.min("gtfs_loaded_at").alias("min_gtfs_loaded_at"),
            F.max("gtfs_loaded_at").alias("max_gtfs_loaded_at"),
        )
        .withColumn(
            "has_conflicting_planned_route_short_name",
            F.col("distinct_planned_route_short_name_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_planned_route_long_name",
            F.col("distinct_planned_route_long_name_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_planned_route_name_display",
            F.col("distinct_planned_route_name_display_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_route_type",
            F.col("distinct_route_type_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_route_type_description",
            F.col("distinct_route_type_description_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_route_color",
            F.col("distinct_route_color_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_route_color_name",
            F.col("distinct_route_color_name_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_route_text_color",
            F.col("distinct_route_text_color_count") > F.lit(1),
        )
        .withColumn(
            "has_conflicting_route_text_color_name",
            F.col("distinct_route_text_color_name_count") > F.lit(1),
        )
        .withColumn(
            "has_any_conflicting_attribute",
            (
                (F.col("distinct_planned_route_short_name_count") > 1)
                | (F.col("distinct_planned_route_long_name_count") > 1)
                | (F.col("distinct_planned_route_name_display_count") > 1)
                | (F.col("distinct_route_type_count") > 1)
                | (F.col("distinct_route_type_description_count") > 1)
                | (F.col("distinct_route_color_count") > 1)
                | (F.col("distinct_route_color_name_count") > 1)
                | (F.col("distinct_route_text_color_count") > 1)
                | (F.col("distinct_route_text_color_name_count") > 1)
            ),
        )
        .where(F.col("has_any_conflicting_attribute") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "line_code",
            "source_record_count",
            "distinct_planned_route_id_count",
            "distinct_planned_route_short_name_count",
            "distinct_planned_route_long_name_count",
            "distinct_planned_route_name_display_count",
            "distinct_route_type_count",
            "distinct_route_type_description_count",
            "distinct_route_color_count",
            "distinct_route_color_name_count",
            "distinct_route_text_color_count",
            "distinct_route_text_color_name_count",
            "planned_route_ids",
            "planned_route_short_names",
            "planned_route_long_names",
            "planned_route_name_displays",
            "route_types",
            "route_type_descriptions",
            "route_colors",
            "route_color_names",
            "route_text_colors",
            "route_text_color_names",
            "has_conflicting_planned_route_short_name",
            "has_conflicting_planned_route_long_name",
            "has_conflicting_planned_route_name_display",
            "has_conflicting_route_type",
            "has_conflicting_route_type_description",
            "has_conflicting_route_color",
            "has_conflicting_route_color_name",
            "has_conflicting_route_text_color",
            "has_conflicting_route_text_color_name",
            "has_any_conflicting_attribute",
            "min_gtfs_load_date",
            "max_gtfs_load_date",
            "min_gtfs_loaded_at",
            "max_gtfs_loaded_at",
            "record_created_at",
        )
    )

    return audit_df