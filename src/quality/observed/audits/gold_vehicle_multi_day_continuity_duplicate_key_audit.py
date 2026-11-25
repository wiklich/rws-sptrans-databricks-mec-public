"""
Gold vehicle multi-day continuity duplicate vehicle key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same vehicle key in
    gold_vehicle_multi_day_continuity.

Audit focus:
    - same vehicle_prefix appearing more than once
    - accidental duplication introduced by aggregation logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_vehicle_multi_day_continuity.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_multi_day_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_multi_day_continuity_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate vehicle keys in "
        "gold_vehicle_multi_day_continuity. Detects cases where more than one "
        "row exists for the same vehicle_prefix."
    ),
)
def gold_vehicle_multi_day_continuity_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate vehicle key audit for
    gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Audit rows for duplicate vehicle_prefix keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_multi_day_continuity_duplicate_key_audit(source_df)


def _build_gold_vehicle_multi_day_continuity_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate vehicle key audit dataset.

    Processing steps:
        1. Read gold_vehicle_multi_day_continuity.
        2. Group by vehicle_prefix.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate vehicle multi-day continuity keys.
    """
    audit_df = (
        source_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("observed_dates_count").alias("observed_dates_count_values"),
            F.collect_set("first_observed_date").alias("first_observed_date_values"),
            F.collect_set("last_observed_date").alias("last_observed_date_values"),
            F.collect_set("observed_date_span_days").alias(
                "observed_date_span_days_values"
            ),
            F.collect_set("missing_dates_within_span_count").alias(
                "missing_dates_within_span_count_values"
            ),
            F.collect_set("date_presence_ratio").alias("date_presence_ratio_values"),
            F.min("first_captured_at_local").alias("min_first_captured_at_local"),
            F.max("last_captured_at_local").alias("max_last_captured_at_local"),
            F.collect_set("total_observations_count").alias(
                "total_observations_count_values"
            ),
            F.collect_set("avg_observations_per_observed_date").alias(
                "avg_observations_per_observed_date_values"
            ),
            F.collect_set("max_consecutive_observed_dates").alias(
                "max_consecutive_observed_dates_values"
            ),
            F.collect_set("max_consecutive_missing_dates_within_span").alias(
                "max_consecutive_missing_dates_within_span_values"
            ),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_vehicle_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_vehicle_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "vehicle_prefix",
            "duplicate_row_count",
            "observed_dates_count_values",
            "first_observed_date_values",
            "last_observed_date_values",
            "observed_date_span_days_values",
            "missing_dates_within_span_count_values",
            "date_presence_ratio_values",
            "min_first_captured_at_local",
            "max_last_captured_at_local",
            "total_observations_count_values",
            "avg_observations_per_observed_date_values",
            "max_consecutive_observed_dates_values",
            "max_consecutive_missing_dates_within_span_values",
            "has_duplicate_vehicle_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
