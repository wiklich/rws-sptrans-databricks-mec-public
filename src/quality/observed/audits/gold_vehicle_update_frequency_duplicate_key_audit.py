"""
Gold vehicle update frequency duplicate vehicle key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same vehicle key in
    gold_vehicle_update_frequency.

Audit focus:
    - same vehicle_prefix appearing more than once
    - accidental duplication introduced by aggregation logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_vehicle_update_frequency.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_update_frequency"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_update_frequency_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate vehicle keys in "
        "gold_vehicle_update_frequency. Detects cases where more than one row "
        "exists for the same vehicle_prefix."
    ),
)
def gold_vehicle_update_frequency_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate vehicle key audit for
    gold_vehicle_update_frequency.

    Returns:
        DataFrame:
            Audit rows for duplicate vehicle_prefix keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_update_frequency_duplicate_key_audit(source_df)


def _build_gold_vehicle_update_frequency_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate vehicle key audit dataset.

    Processing steps:
        1. Read gold_vehicle_update_frequency.
        2. Group by vehicle_prefix.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate vehicle update frequency keys.
    """
    audit_df = (
        source_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("updates_count").alias("updates_count_values"),
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
            F.min("first_captured_at_local").alias("min_first_captured_at_local"),
            F.max("last_captured_at_local").alias("max_last_captured_at_local"),
            F.collect_set("observed_span_seconds").alias("observed_span_seconds_values"),
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
            "updates_count_values",
            "measured_intervals_count_values",
            "avg_update_interval_seconds_values",
            "min_update_interval_seconds_values",
            "max_update_interval_seconds_values",
            "stddev_update_interval_seconds_values",
            "min_first_captured_at_local",
            "max_last_captured_at_local",
            "observed_span_seconds_values",
            "has_duplicate_vehicle_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
