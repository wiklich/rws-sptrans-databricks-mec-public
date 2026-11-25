"""
Gold observed intraday hourly activity duplicate hourly key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same hourly key in
    gold_observed_intraday_hourly_activity.

Audit focus:
    - same analysis_date + analysis_hour_local appearing more than once
    - accidental duplication introduced by aggregation logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_observed_intraday_hourly_activity.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_intraday_hourly_activity"
OUTPUT_TABLE_NAME = "gold.gold_observed_intraday_hourly_activity_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate hourly keys in "
        "gold_observed_intraday_hourly_activity. Detects cases where more than "
        "one row exists for the same analysis_date and analysis_hour_local."
    ),
)
def gold_observed_intraday_hourly_activity_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate hourly key audit for
    gold_observed_intraday_hourly_activity.

    Returns:
        DataFrame:
            Audit rows for duplicate analysis_date + analysis_hour_local keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_intraday_hourly_activity_duplicate_key_audit(source_df)


def _build_gold_observed_intraday_hourly_activity_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate hourly key audit dataset.

    Processing steps:
        1. Read gold_observed_intraday_hourly_activity.
        2. Group by analysis_date and analysis_hour_local.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_intraday_hourly_activity.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate hourly activity keys.
    """
    audit_df = (
        source_df.groupBy("analysis_date", "analysis_hour_local")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("observed_position_records_count").alias(
                "observed_position_records_count_values"
            ),
            F.collect_set("distinct_observed_vehicles_count").alias(
                "distinct_observed_vehicles_count_values"
            ),
            F.collect_set("distinct_observed_lines_count").alias(
                "distinct_observed_lines_count_values"
            ),
            F.collect_set("distinct_minutes_with_data").alias(
                "distinct_minutes_with_data_values"
            ),
            F.min("min_captured_at_local").alias("min_min_captured_at_local"),
            F.max("max_captured_at_local").alias("max_max_captured_at_local"),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_hourly_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_hourly_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "analysis_hour_local",
            "duplicate_row_count",
            "observed_position_records_count_values",
            "distinct_observed_vehicles_count_values",
            "distinct_observed_lines_count_values",
            "distinct_minutes_with_data_values",
            "min_min_captured_at_local",
            "max_max_captured_at_local",
            "has_duplicate_hourly_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
