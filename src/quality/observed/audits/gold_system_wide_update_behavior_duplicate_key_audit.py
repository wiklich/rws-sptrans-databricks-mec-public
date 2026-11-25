"""
Gold system-wide update behavior duplicate aggregation key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same declared
    aggregation scope in gold_system_wide_update_behavior.

Audit focus:
    - same aggregation_scope appearing more than once
    - accidental duplication introduced by summarization logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_system_wide_update_behavior.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_system_wide_update_behavior"
OUTPUT_TABLE_NAME = "gold.gold_system_wide_update_behavior_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate aggregation scope keys in "
        "gold_system_wide_update_behavior. Detects cases where more than one "
        "row exists for the same aggregation_scope."
    ),
)
def gold_system_wide_update_behavior_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate aggregation key audit for
    gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Audit rows for duplicate aggregation_scope keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_system_wide_update_behavior_duplicate_key_audit(source_df)


def _build_gold_system_wide_update_behavior_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate aggregation key audit dataset.

    Processing steps:
        1. Read gold_system_wide_update_behavior.
        2. Group by aggregation_scope.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate system-wide aggregation keys.
    """
    audit_df = (
        source_df.groupBy("aggregation_scope")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("vehicles_count").alias("vehicles_count_values"),
            F.collect_set("total_observations_count").alias(
                "total_observations_count_values"
            ),
            F.collect_set("total_measured_intervals_count").alias(
                "total_measured_intervals_count_values"
            ),
            F.collect_set("system_avg_update_interval_seconds").alias(
                "system_avg_update_interval_seconds_values"
            ),
            F.collect_set("system_min_update_interval_seconds").alias(
                "system_min_update_interval_seconds_values"
            ),
            F.collect_set("system_max_update_interval_seconds").alias(
                "system_max_update_interval_seconds_values"
            ),
            F.collect_set("system_avg_vehicle_stddev_interval_seconds").alias(
                "system_avg_vehicle_stddev_interval_seconds_values"
            ),
            F.collect_set("total_distinct_minutes_with_data").alias(
                "total_distinct_minutes_with_data_values"
            ),
            F.collect_set("avg_continuity_support_ratio").alias(
                "avg_continuity_support_ratio_values"
            ),
            F.collect_set("total_large_gaps_count").alias(
                "total_large_gaps_count_values"
            ),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_aggregation_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_aggregation_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "aggregation_scope",
            "duplicate_row_count",
            "vehicles_count_values",
            "total_observations_count_values",
            "total_measured_intervals_count_values",
            "system_avg_update_interval_seconds_values",
            "system_min_update_interval_seconds_values",
            "system_max_update_interval_seconds_values",
            "system_avg_vehicle_stddev_interval_seconds_values",
            "total_distinct_minutes_with_data_values",
            "avg_continuity_support_ratio_values",
            "total_large_gaps_count_values",
            "has_duplicate_aggregation_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
