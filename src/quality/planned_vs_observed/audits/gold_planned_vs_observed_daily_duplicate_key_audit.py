"""
Gold planned vs observed daily duplicate daily summary key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same daily summary
    key in gold_planned_vs_observed_daily.

Audit focus:
    - same analysis_date appearing more than once
    - accidental duplication introduced by aggregation logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_planned_vs_observed_daily.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_daily"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_daily_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate daily summary keys in "
        "gold_planned_vs_observed_daily. Detects cases where more than one row "
        "exists for the same analysis_date."
    ),
)
def gold_planned_vs_observed_daily_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate daily summary key audit for
    gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Audit rows for duplicate analysis_date keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_daily_duplicate_key_audit(source_df)


def _build_gold_planned_vs_observed_daily_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate daily summary key audit dataset.

    Processing steps:
        1. Read gold_planned_vs_observed_daily.
        2. Group by analysis_date.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate daily summary keys.
    """
    audit_df = (
        source_df.groupBy("analysis_date")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("planned_lines_count").alias("planned_lines_count_values"),
            F.collect_set("observed_lines_count").alias("observed_lines_count_values"),
            F.collect_set("planned_lines_observed_count").alias(
                "planned_lines_observed_count_values"
            ),
            F.collect_set("planned_lines_not_observed_count").alias(
                "planned_lines_not_observed_count_values"
            ),
            F.collect_set("observed_lines_not_in_gtfs_count").alias(
                "observed_lines_not_in_gtfs_count_values"
            ),
            F.collect_set("planned_lines_observed_pct").alias(
                "planned_lines_observed_pct_values"
            ),
            F.collect_set("gtfs_load_date").alias("gtfs_load_dates"),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_daily_summary_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_daily_summary_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "duplicate_row_count",
            "planned_lines_count_values",
            "observed_lines_count_values",
            "planned_lines_observed_count_values",
            "planned_lines_not_observed_count_values",
            "observed_lines_not_in_gtfs_count_values",
            "planned_lines_observed_pct_values",
            "gtfs_load_dates",
            "has_duplicate_daily_summary_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
