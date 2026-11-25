"""
Gold vehicle observed spatial traces duplicate trace key audit pipeline.

Purpose:
    Identify unexpected duplication at the declared Gold grain in
    gold_vehicle_observed_spatial_traces.

Audit focus:
    - same analysis_date + vehicle_prefix + trace_sequence appearing more than once
    - accidental duplication introduced by sequencing logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_vehicle_observed_spatial_traces.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observed_spatial_traces"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observed_spatial_traces_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate trace keys in "
        "gold_vehicle_observed_spatial_traces. Detects cases where more than "
        "one row exists for the same analysis_date, vehicle_prefix, and "
        "trace_sequence."
    ),
)
def gold_vehicle_observed_spatial_traces_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate trace key audit for
    gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit rows for duplicate analysis_date + vehicle_prefix +
            trace_sequence keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observed_spatial_traces_duplicate_key_audit(source_df)


def _build_gold_vehicle_observed_spatial_traces_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate trace key audit dataset.

    Processing steps:
        1. Read gold_vehicle_observed_spatial_traces.
        2. Group by analysis_date, vehicle_prefix, and trace_sequence.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate trace keys.
    """
    audit_df = (
        source_df.groupBy(
            "analysis_date",
            "vehicle_prefix",
            "trace_sequence",
        )
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("captured_at_local").alias("captured_at_local_values"),
            F.collect_set("latitude").alias("latitude_values"),
            F.collect_set("longitude").alias("longitude_values"),
            F.collect_set("line_sign").alias("line_sign_values"),
            F.collect_set("direction").alias("direction_values"),
            F.collect_set("direction_description").alias(
                "direction_description_values"
            ),
            F.collect_set("previous_captured_at_local").alias(
                "previous_captured_at_local_values"
            ),
            F.collect_set("previous_latitude").alias("previous_latitude_values"),
            F.collect_set("previous_longitude").alias("previous_longitude_values"),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_trace_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_trace_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "vehicle_prefix",
            "trace_sequence",
            "duplicate_row_count",
            "captured_at_local_values",
            "latitude_values",
            "longitude_values",
            "line_sign_values",
            "direction_values",
            "direction_description_values",
            "previous_captured_at_local_values",
            "previous_latitude_values",
            "previous_longitude_values",
            "has_duplicate_trace_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
