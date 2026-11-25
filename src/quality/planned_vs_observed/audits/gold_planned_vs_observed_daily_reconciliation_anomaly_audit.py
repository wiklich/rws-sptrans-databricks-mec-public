"""
Gold planned vs observed daily summary reconciliation anomaly audit pipeline.

Purpose:
    Identify cases where daily summary metrics in gold_planned_vs_observed_daily
    do not reconcile with the expected business identities.

Audit focus:
    - planned_lines_observed_count + planned_lines_not_observed_count != planned_lines_count
    - planned_lines_observed_count + observed_lines_not_in_gtfs_count != observed_lines_count

Why this audit matters:
    This audit is useful because reconciliation is central to trust in the KPI output.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_daily"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_daily_reconciliation_anomaly_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying daily summary rows in "
        "gold_planned_vs_observed_daily where KPI reconciliation identities "
        "do not hold."
    ),
)
def gold_planned_vs_observed_daily_reconciliation_anomaly_audit() -> DataFrame:
    """
    Materialize the daily summary reconciliation anomaly audit for
    gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Audit rows for analysis dates with KPI reconciliation anomalies.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_daily_reconciliation_anomaly_audit(source_df)


def _build_gold_planned_vs_observed_daily_reconciliation_anomaly_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the daily summary reconciliation anomaly audit dataset.

    Processing steps:
        1. Read gold_planned_vs_observed_daily.
        2. Compute reconciliation-side derived values.
        3. Compare actual KPI values with expected business identities.
        4. Keep only rows where at least one reconciliation rule is violated.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Audit DataFrame containing analysis dates with reconciliation anomalies.
    """
    audit_df = (
        source_df.withColumn(
            "expected_planned_lines_count",
            F.col("planned_lines_observed_count") + F.col("planned_lines_not_observed_count"),
        )
        .withColumn(
            "expected_observed_lines_count",
            F.col("planned_lines_observed_count") + F.col("observed_lines_not_in_gtfs_count"),
        )
        .withColumn(
            "has_planned_count_reconciliation_anomaly",
            F.col("planned_lines_count") != F.col("expected_planned_lines_count"),
        )
        .withColumn(
            "has_observed_count_reconciliation_anomaly",
            F.col("observed_lines_count") != F.col("expected_observed_lines_count"),
        )
        .withColumn(
            "has_any_reconciliation_anomaly",
            F.col("has_planned_count_reconciliation_anomaly")
            | F.col("has_observed_count_reconciliation_anomaly"),
        )
        .where(F.col("has_any_reconciliation_anomaly") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "planned_lines_count",
            "planned_lines_observed_count",
            "planned_lines_not_observed_count",
            "expected_planned_lines_count",
            "has_planned_count_reconciliation_anomaly",
            "observed_lines_count",
            "observed_lines_not_in_gtfs_count",
            "expected_observed_lines_count",
            "has_observed_count_reconciliation_anomaly",
            "has_any_reconciliation_anomaly",
            "gtfs_load_date",
            "record_created_at",
        )
    )

    return audit_df
