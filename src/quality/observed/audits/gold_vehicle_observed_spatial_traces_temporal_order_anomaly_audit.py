"""
Gold vehicle observed spatial traces temporal order anomaly audit pipeline.

Purpose:
    Identify trace rows whose sequence is not compatible with timestamp order
    in gold_vehicle_observed_spatial_traces.

Audit focus:
    - later trace_sequence with earlier timestamp
    - duplicated timestamp ordering ambiguity within the same vehicle-day trace

Why this audit matters:
    This audit validates that the observed trace sequencing remains compatible
    with local timestamp order and highlights ambiguous trace ordering cases
    that may weaken interpretation.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observed_spatial_traces"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observed_spatial_traces_temporal_order_anomaly_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying temporal order anomalies in "
        "gold_vehicle_observed_spatial_traces, including decreasing timestamps "
        "across trace_sequence and duplicated timestamp ordering ambiguity "
        "within the same analysis_date and vehicle_prefix trace."
    ),
)
def gold_vehicle_observed_spatial_traces_temporal_order_anomaly_audit() -> DataFrame:
    """
    Materialize the temporal order anomaly audit for
    gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit rows for temporal-order and timestamp-ambiguity anomalies.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observed_spatial_traces_temporal_order_anomaly_audit(
        source_df
    )


def _build_gold_vehicle_observed_spatial_traces_temporal_order_anomaly_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the temporal order anomaly audit dataset.

    Processing steps:
        1. Read gold_vehicle_observed_spatial_traces.
        2. Recompute expected previous trace context using trace_sequence order.
        3. Detect decreasing timestamps across sequence order.
        4. Detect duplicated timestamps within the same vehicle-day trace.
        5. Detect mismatches between stored previous timestamp and expected
           previous timestamp.
        6. Keep only rows that match at least one anomaly condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit DataFrame containing temporal order anomalies.
    """
    trace_window = Window.partitionBy("analysis_date", "vehicle_prefix").orderBy(
        F.col("trace_sequence").asc()
    )

    duplicate_timestamp_window = Window.partitionBy(
        "analysis_date",
        "vehicle_prefix",
        "captured_at_local",
    )

    audit_df = (
        source_df.withColumn(
            "expected_previous_trace_sequence",
            F.lag("trace_sequence").over(trace_window),
        )
        .withColumn(
            "expected_previous_captured_at_local",
            F.lag("captured_at_local").over(trace_window),
        )
        .withColumn(
            "expected_previous_latitude",
            F.lag("latitude").over(trace_window),
        )
        .withColumn(
            "expected_previous_longitude",
            F.lag("longitude").over(trace_window),
        )
        .withColumn(
            "same_timestamp_row_count",
            F.count(F.lit(1)).over(duplicate_timestamp_window).cast("long"),
        )
        .withColumn(
            "has_decreasing_timestamp_vs_previous_sequence",
            F.col("expected_previous_captured_at_local").isNotNull()
            & (F.col("captured_at_local") < F.col("expected_previous_captured_at_local")),
        )
        .withColumn(
            "has_previous_timestamp_mismatch",
            (
                F.col("expected_previous_captured_at_local").isNull()
                & F.col("previous_captured_at_local").isNotNull()
            )
            | (
                F.col("expected_previous_captured_at_local").isNotNull()
                & F.col("previous_captured_at_local").isNull()
            )
            | (
                F.col("expected_previous_captured_at_local").isNotNull()
                & F.col("previous_captured_at_local").isNotNull()
                & (
                    F.col("previous_captured_at_local")
                    != F.col("expected_previous_captured_at_local")
                )
            ),
        )
        .withColumn(
            "has_duplicated_timestamp_ordering_ambiguity",
            F.col("same_timestamp_row_count") > F.lit(1),
        )
        .withColumn(
            "has_temporal_order_anomaly",
            F.col("has_decreasing_timestamp_vs_previous_sequence")
            | F.col("has_previous_timestamp_mismatch")
            | F.col("has_duplicated_timestamp_ordering_ambiguity"),
        )
        .where(F.col("has_temporal_order_anomaly") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "vehicle_prefix",
            "trace_sequence",
            "captured_at_local",
            "latitude",
            "longitude",
            "line_sign",
            "direction",
            "direction_description",
            "previous_captured_at_local",
            "previous_latitude",
            "previous_longitude",
            "expected_previous_trace_sequence",
            "expected_previous_captured_at_local",
            "expected_previous_latitude",
            "expected_previous_longitude",
            "same_timestamp_row_count",
            "has_decreasing_timestamp_vs_previous_sequence",
            "has_previous_timestamp_mismatch",
            "has_duplicated_timestamp_ordering_ambiguity",
            "has_temporal_order_anomaly",
            "record_created_at",
        )
    )

    return audit_df
