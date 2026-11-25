"""
Gold observed lines daily duplicate daily observed line key audit pipeline.

Purpose:
    Identify cases where multiple output rows would exist for the same
    normalized daily observed business key.

Audit focus:
    - same analysis_date + line_code appearing more than once
    - accidental failure to collapse event-level duplication correctly

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_observed_lines_daily.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_lines_daily"
OUTPUT_TABLE_NAME = "gold.gold_observed_lines_daily_duplicate_daily_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate daily observed line keys in "
        "gold_observed_lines_daily. Detects cases where more than one row "
        "exists for the same analysis_date and normalized line_code."
    ),
)
def gold_observed_lines_daily_duplicate_daily_key_audit() -> DataFrame:
    """
    Materialize the duplicate daily observed line key audit for gold_observed_lines_daily.

    Returns:
        DataFrame:
            Audit rows for duplicate analysis_date + line_code keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_lines_daily_duplicate_daily_key_audit(source_df)


def _build_gold_observed_lines_daily_duplicate_daily_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate daily observed line key audit dataset.

    Processing steps:
        1. Read gold_observed_lines_daily.
        2. Group by analysis_date and line_code.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_lines_daily.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate daily observed keys.
    """
    audit_df = (
        source_df.groupBy("analysis_date", "line_code")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("observed_line_sign").alias("observed_line_signs"),
            F.collect_set("observed_events_count").alias("observed_events_count_values"),
            F.collect_set("observed_vehicles_count").alias("observed_vehicles_count_values"),
            F.min("min_captured_at_local").alias("min_min_captured_at_local"),
            F.max("max_captured_at_local").alias("max_max_captured_at_local"),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_daily_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_daily_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "line_code",
            "duplicate_row_count",
            "observed_line_signs",
            "observed_events_count_values",
            "observed_vehicles_count_values",
            "min_min_captured_at_local",
            "max_max_captured_at_local",
            "has_duplicate_daily_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
