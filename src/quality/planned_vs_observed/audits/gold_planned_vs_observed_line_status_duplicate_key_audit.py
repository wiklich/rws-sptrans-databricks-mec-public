"""
Gold planned vs observed line status duplicate detailed comparison key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same detailed
    comparison key in gold_planned_vs_observed_line_status.

Audit focus:
    - same analysis_date + line_code appearing more than once
    - accidental duplication introduced by comparison join logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_planned_vs_observed_line_status.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_line_status"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_line_status_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate detailed comparison keys in "
        "gold_planned_vs_observed_line_status. Detects cases where more than "
        "one row exists for the same analysis_date and line_code."
    ),
)
def gold_planned_vs_observed_line_status_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate detailed comparison key audit for
    gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Audit rows for duplicate analysis_date + line_code keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_line_status_duplicate_key_audit(source_df)


def _build_gold_planned_vs_observed_line_status_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate detailed comparison key audit dataset.

    Processing steps:
        1. Read gold_planned_vs_observed_line_status.
        2. Group by analysis_date and line_code.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate detailed comparison keys.
    """
    audit_df = (
        source_df.groupBy("analysis_date", "line_code")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("match_status").alias("match_statuses"),
            F.collect_set("planned_route_id").alias("planned_route_ids"),
            F.collect_set("observed_line_sign").alias("observed_line_signs"),
            F.collect_set("is_planned").alias("is_planned_values"),
            F.collect_set("is_observed").alias("is_observed_values"),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_detailed_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_detailed_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "line_code",
            "duplicate_row_count",
            "match_statuses",
            "planned_route_ids",
            "observed_line_signs",
            "is_planned_values",
            "is_observed_values",
            "has_duplicate_detailed_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
