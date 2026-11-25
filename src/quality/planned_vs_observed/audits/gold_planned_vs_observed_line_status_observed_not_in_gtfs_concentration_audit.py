"""
Gold planned vs observed line status observed-not-in-GTFS concentration audit pipeline.

Purpose:
    Identify analysis dates with unusually high presence of
    OBSERVED_NOT_IN_GTFS cases in gold_planned_vs_observed_line_status.

Audit focus:
    - unusually large number of unmatched observed lines on one analysis date
    - unusually high proportion of observed-only cases on one analysis date
    - sudden spikes in observed-only cases relative to the general distribution

Why this audit matters:
    This audit is useful for interpretation and downstream confidence analysis.
    A high concentration of OBSERVED_NOT_IN_GTFS rows may indicate special
    services, scope differences, temporary codes, or upstream catalog alignment issues.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_line_status"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_line_status_observed_not_in_gtfs_concentration_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying analysis dates with unusually high "
        "concentration of OBSERVED_NOT_IN_GTFS cases in "
        "gold_planned_vs_observed_line_status."
    ),
)
def gold_planned_vs_observed_line_status_observed_not_in_gtfs_concentration_audit() -> DataFrame:
    """
    Materialize the observed-not-in-GTFS concentration audit for
    gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Audit rows for analysis dates with unusually high concentration
            of OBSERVED_NOT_IN_GTFS cases.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_line_status_observed_not_in_gtfs_concentration_audit(source_df)


def _build_gold_planned_vs_observed_line_status_observed_not_in_gtfs_concentration_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the observed-not-in-GTFS concentration audit dataset.

    Processing steps:
        1. Read gold_planned_vs_observed_line_status.
        2. Aggregate observed-side comparison metrics by analysis_date.
        3. Compute daily unmatched observed counts and percentages.
        4. Compute global reference statistics across all dates.
        5. Flag dates with unusually high concentration of OBSERVED_NOT_IN_GTFS cases.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Audit DataFrame containing dates with unusually high concentration
            of OBSERVED_NOT_IN_GTFS cases.
    """
    daily_df = (
        source_df.groupBy("analysis_date")
        .agg(
            F.count(F.when(F.col("is_observed") == True, 1)).cast("long").alias("observed_lines_count"),
            F.count(F.when(F.col("match_status") == "OBSERVED_NOT_IN_GTFS", 1)).cast("long").alias(
                "observed_not_in_gtfs_count"
            ),
            F.collect_set(
                F.when(F.col("match_status") == "OBSERVED_NOT_IN_GTFS", F.col("line_code"))
            ).alias("observed_not_in_gtfs_line_codes"),
            F.collect_set(
                F.when(F.col("match_status") == "OBSERVED_NOT_IN_GTFS", F.col("observed_line_sign"))
            ).alias("observed_not_in_gtfs_line_signs"),
        )
        .withColumn(
            "observed_not_in_gtfs_pct",
            F.when(
                F.col("observed_lines_count") > 0,
                F.round(
                    (F.col("observed_not_in_gtfs_count") / F.col("observed_lines_count")) * F.lit(100.0),
                    2,
                ),
            ).otherwise(F.lit(None)),
        )
    )

    stats_df = daily_df.agg(
        F.avg("observed_not_in_gtfs_count").alias("avg_observed_not_in_gtfs_count"),
        F.stddev_pop("observed_not_in_gtfs_count").alias("stddev_observed_not_in_gtfs_count"),
        F.avg("observed_not_in_gtfs_pct").alias("avg_observed_not_in_gtfs_pct"),
        F.stddev_pop("observed_not_in_gtfs_pct").alias("stddev_observed_not_in_gtfs_pct"),
        F.max("observed_not_in_gtfs_count").alias("max_observed_not_in_gtfs_count"),
        F.max("observed_not_in_gtfs_pct").alias("max_observed_not_in_gtfs_pct"),
    )

    audit_df = (
        daily_df.crossJoin(stats_df)
        .withColumn(
            "count_alert_threshold",
            F.col("avg_observed_not_in_gtfs_count")
            + (F.coalesce(F.col("stddev_observed_not_in_gtfs_count"), F.lit(0.0)) * F.lit(2.0)),
        )
        .withColumn(
            "pct_alert_threshold",
            F.col("avg_observed_not_in_gtfs_pct")
            + (F.coalesce(F.col("stddev_observed_not_in_gtfs_pct"), F.lit(0.0)) * F.lit(2.0)),
        )
        .withColumn(
            "has_high_unmatched_observed_count",
            F.col("observed_not_in_gtfs_count") > F.col("count_alert_threshold"),
        )
        .withColumn(
            "has_high_unmatched_observed_pct",
            F.col("observed_not_in_gtfs_pct") > F.col("pct_alert_threshold"),
        )
        .withColumn(
            "has_any_observed_not_in_gtfs_case",
            F.col("observed_not_in_gtfs_count") > F.lit(0),
        )
        .withColumn(
            "has_unusual_observed_not_in_gtfs_concentration",
            F.col("has_high_unmatched_observed_count") | F.col("has_high_unmatched_observed_pct"),
        )
        .where(F.col("has_unusual_observed_not_in_gtfs_concentration") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "observed_lines_count",
            "observed_not_in_gtfs_count",
            "observed_not_in_gtfs_pct",
            "observed_not_in_gtfs_line_codes",
            "observed_not_in_gtfs_line_signs",
            "avg_observed_not_in_gtfs_count",
            "stddev_observed_not_in_gtfs_count",
            "avg_observed_not_in_gtfs_pct",
            "stddev_observed_not_in_gtfs_pct",
            "count_alert_threshold",
            "pct_alert_threshold",
            "has_any_observed_not_in_gtfs_case",
            "has_high_unmatched_observed_count",
            "has_high_unmatched_observed_pct",
            "has_unusual_observed_not_in_gtfs_concentration",
            "record_created_at",
        )
    )

    return audit_df
