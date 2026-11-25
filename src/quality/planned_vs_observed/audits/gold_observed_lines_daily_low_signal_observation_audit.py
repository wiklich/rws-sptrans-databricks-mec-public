"""
Gold observed lines daily suspicious low-signal observation audit pipeline.

Purpose:
    Identify cases where an observed daily line appears with very low activity.

Audit focus:
    - observed_events_count = 1
    - observed_vehicles_count = 1

Why this audit matters:
    This audit is useful for interpretation and downstream confidence analysis.
    Low-signal observations are not necessarily invalid, but they may indicate
    weak daily support for an observed line in the operational dataset.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_lines_daily"
OUTPUT_TABLE_NAME = "gold.gold_observed_lines_daily_low_signal_observation_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying low-signal observed daily lines in "
        "gold_observed_lines_daily, such as lines backed by only one event "
        "or one distinct vehicle."
    ),
)
def gold_observed_lines_daily_low_signal_observation_audit() -> DataFrame:
    """
    Materialize the low-signal observation audit for gold_observed_lines_daily.

    Returns:
        DataFrame:
            Audit rows for observed daily lines with very low activity.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_lines_daily_low_signal_observation_audit(source_df)


def _build_gold_observed_lines_daily_low_signal_observation_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious low-signal observation audit dataset.

    Processing steps:
        1. Read gold_observed_lines_daily.
        2. Identify rows with very low daily support.
        3. Flag event-level and vehicle-level low-signal cases.
        4. Keep only rows that match at least one low-signal condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_lines_daily.

    Returns:
        DataFrame:
            Audit DataFrame containing low-signal observed daily lines.
    """
    audit_df = (
        source_df.withColumn(
            "has_single_observed_event",
            F.col("observed_events_count") == F.lit(1),
        )
        .withColumn(
            "has_single_observed_vehicle",
            F.col("observed_vehicles_count") == F.lit(1),
        )
        .withColumn(
            "has_low_signal_observation",
            (F.col("observed_events_count") == 1)
            | (F.col("observed_vehicles_count") == 1),
        )
        .where(F.col("has_low_signal_observation") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "line_code",
            "observed_line_sign",
            "observed_events_count",
            "observed_vehicles_count",
            "min_captured_at_local",
            "max_captured_at_local",
            "has_single_observed_event",
            "has_single_observed_vehicle",
            "has_low_signal_observation",
            "record_created_at",
        )
    )

    return audit_df
