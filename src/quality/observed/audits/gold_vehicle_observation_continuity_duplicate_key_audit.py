"""
Gold vehicle observation continuity duplicate vehicle key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same vehicle key in
    gold_vehicle_observation_continuity.

Audit focus:
    - same vehicle_prefix appearing more than once
    - accidental duplication introduced by aggregation logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_vehicle_observation_continuity.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observation_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observation_continuity_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate vehicle keys in "
        "gold_vehicle_observation_continuity. Detects cases where more than "
        "one row exists for the same vehicle_prefix."
    ),
)
def gold_vehicle_observation_continuity_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate vehicle key audit for
    gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Audit rows for duplicate vehicle_prefix keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observation_continuity_duplicate_key_audit(source_df)


def _build_gold_vehicle_observation_continuity_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate vehicle key audit dataset.

    Processing steps:
        1. Read gold_vehicle_observation_continuity.
        2. Group by vehicle_prefix.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate vehicle observation continuity keys.
    """
    audit_df = (
        source_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("observations_count").alias("observations_count_values"),
            F.collect_set("measured_gaps_count").alias("measured_gaps_count_values"),
            F.min("first_captured_at_local").alias("min_first_captured_at_local"),
            F.max("last_captured_at_local").alias("max_last_captured_at_local"),
            F.collect_set("observed_span_seconds").alias("observed_span_seconds_values"),
            F.collect_set("distinct_minutes_with_data").alias(
                "distinct_minutes_with_data_values"
            ),
            F.collect_set("distinct_hours_with_data").alias(
                "distinct_hours_with_data_values"
            ),
            F.collect_set("avg_gap_seconds").alias("avg_gap_seconds_values"),
            F.collect_set("max_gap_seconds").alias("max_gap_seconds_values"),
            F.collect_set("large_gaps_count").alias("large_gaps_count_values"),
            F.collect_set("continuity_support_ratio").alias(
                "continuity_support_ratio_values"
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
            "observations_count_values",
            "measured_gaps_count_values",
            "min_first_captured_at_local",
            "max_last_captured_at_local",
            "observed_span_seconds_values",
            "distinct_minutes_with_data_values",
            "distinct_hours_with_data_values",
            "avg_gap_seconds_values",
            "max_gap_seconds_values",
            "large_gaps_count_values",
            "continuity_support_ratio_values",
            "has_duplicate_vehicle_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
