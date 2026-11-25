"""
Gold recurring update patterns duplicate recurring key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same recurring
    temporal bucket in gold_recurring_update_patterns.

Audit focus:
    - same day_of_week + hour_of_day appearing more than once
    - accidental duplication introduced by aggregation logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_recurring_update_patterns.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_recurring_update_patterns"
OUTPUT_TABLE_NAME = "gold.gold_recurring_update_patterns_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate recurring temporal keys in "
        "gold_recurring_update_patterns. Detects cases where more than one row "
        "exists for the same day_of_week and hour_of_day."
    ),
)
def gold_recurring_update_patterns_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate recurring key audit for
    gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Audit rows for duplicate day_of_week + hour_of_day keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_recurring_update_patterns_duplicate_key_audit(source_df)


def _build_gold_recurring_update_patterns_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate recurring key audit dataset.

    Processing steps:
        1. Read gold_recurring_update_patterns.
        2. Group by day_of_week and hour_of_day.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate recurring update pattern keys.
    """
    audit_df = (
        source_df.groupBy("day_of_week", "hour_of_day")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("observations_count").alias("observations_count_values"),
            F.collect_set("measured_intervals_count").alias(
                "measured_intervals_count_values"
            ),
            F.collect_set("avg_update_interval_seconds").alias(
                "avg_update_interval_seconds_values"
            ),
            F.collect_set("min_update_interval_seconds").alias(
                "min_update_interval_seconds_values"
            ),
            F.collect_set("max_update_interval_seconds").alias(
                "max_update_interval_seconds_values"
            ),
            F.collect_set("stddev_update_interval_seconds").alias(
                "stddev_update_interval_seconds_values"
            ),
            F.collect_set("distinct_vehicles_count").alias(
                "distinct_vehicles_count_values"
            ),
            F.collect_set("distinct_dates_count").alias(
                "distinct_dates_count_values"
            ),
            F.collect_set("avg_intervals_per_vehicle").alias(
                "avg_intervals_per_vehicle_values"
            ),
            F.collect_set("avg_intervals_per_date").alias(
                "avg_intervals_per_date_values"
            ),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_recurring_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_recurring_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "day_of_week",
            "hour_of_day",
            "duplicate_row_count",
            "observations_count_values",
            "measured_intervals_count_values",
            "avg_update_interval_seconds_values",
            "min_update_interval_seconds_values",
            "max_update_interval_seconds_values",
            "stddev_update_interval_seconds_values",
            "distinct_vehicles_count_values",
            "distinct_dates_count_values",
            "avg_intervals_per_vehicle_values",
            "avg_intervals_per_date_values",
            "has_duplicate_recurring_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
