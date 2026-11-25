"""
Gold planned vs observed daily suspicious coverage percentage audit pipeline.

Purpose:
    Identify cases where planned_lines_observed_pct shows unusually low or
    unusually high values in gold_planned_vs_observed_daily.

Audit focus:
    - unusually low planned coverage percentage
    - unusually high planned coverage percentage
    - sudden drops relative to surrounding analysis dates
    - unexpected spikes relative to surrounding analysis dates

Why this audit matters:
    This audit is useful for interpretation and downstream confidence analysis.
    Large drops or spikes in planned coverage may indicate partial collection
    windows, upstream alignment changes, or unusual operational conditions.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_daily"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_daily_suspicious_coverage_percentage_audit"

STDDEV_MULTIPLIER = 2.0
ADJACENT_DELTA_THRESHOLD_PCT_POINTS = 10.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying unusually low or high planned coverage "
        "percentages, as well as sudden drops or spikes relative to surrounding "
        "analysis dates in gold_planned_vs_observed_daily."
    ),
)
def gold_planned_vs_observed_daily_suspicious_coverage_percentage_audit() -> DataFrame:
    """
    Materialize the suspicious coverage percentage audit for
    gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Audit rows for analysis dates with suspicious planned coverage
            percentage behavior.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_daily_suspicious_coverage_percentage_audit(
        source_df
    )


def _build_gold_planned_vs_observed_daily_suspicious_coverage_percentage_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious coverage percentage audit dataset.

    Processing steps:
        1. Read gold_planned_vs_observed_daily.
        2. Compute global average and standard deviation for planned coverage.
        3. Compute previous and next day coverage percentages.
        4. Detect unusually low/high values using global thresholds.
        5. Detect sudden drops/spikes relative to surrounding dates.
        6. Keep only rows flagged as suspicious.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Audit DataFrame containing analysis dates with suspicious planned
            coverage percentage behavior.
    """
    ordered_window = Window.orderBy("analysis_date")

    stats_df = source_df.agg(
        F.avg("planned_lines_observed_pct").alias("avg_planned_lines_observed_pct"),
        F.stddev_pop("planned_lines_observed_pct").alias(
            "stddev_planned_lines_observed_pct"
        ),
        F.min("planned_lines_observed_pct").alias("min_planned_lines_observed_pct"),
        F.max("planned_lines_observed_pct").alias("max_planned_lines_observed_pct"),
    )

    prepared_df = (
        source_df.withColumn(
            "previous_planned_lines_observed_pct",
            F.lag("planned_lines_observed_pct").over(ordered_window),
        )
        .withColumn(
            "next_planned_lines_observed_pct",
            F.lead("planned_lines_observed_pct").over(ordered_window),
        )
        .withColumn(
            "delta_vs_previous_pct_points",
            F.when(
                F.col("previous_planned_lines_observed_pct").isNotNull(),
                F.round(
                    F.col("planned_lines_observed_pct")
                    - F.col("previous_planned_lines_observed_pct"),
                    2,
                ),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "delta_vs_next_pct_points",
            F.when(
                F.col("next_planned_lines_observed_pct").isNotNull(),
                F.round(
                    F.col("planned_lines_observed_pct")
                    - F.col("next_planned_lines_observed_pct"),
                    2,
                ),
            ).otherwise(F.lit(None)),
        )
    )

    audit_df = (
        prepared_df.crossJoin(stats_df)
        .withColumn(
            "low_coverage_threshold",
            F.round(
                F.col("avg_planned_lines_observed_pct")
                - (
                    F.coalesce(
                        F.col("stddev_planned_lines_observed_pct"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
                2,
            ),
        )
        .withColumn(
            "high_coverage_threshold",
            F.round(
                F.col("avg_planned_lines_observed_pct")
                + (
                    F.coalesce(
                        F.col("stddev_planned_lines_observed_pct"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
                2,
            ),
        )
        .withColumn(
            "has_unusually_low_coverage_pct",
            F.col("planned_lines_observed_pct").isNotNull()
            & (F.col("planned_lines_observed_pct") < F.col("low_coverage_threshold")),
        )
        .withColumn(
            "has_unusually_high_coverage_pct",
            F.col("planned_lines_observed_pct").isNotNull()
            & (F.col("planned_lines_observed_pct") > F.col("high_coverage_threshold")),
        )
        .withColumn(
            "has_sudden_drop_vs_previous_date",
            F.col("delta_vs_previous_pct_points").isNotNull()
            & (
                F.col("delta_vs_previous_pct_points")
                <= F.lit(-ADJACENT_DELTA_THRESHOLD_PCT_POINTS)
            ),
        )
        .withColumn(
            "has_sudden_spike_vs_previous_date",
            F.col("delta_vs_previous_pct_points").isNotNull()
            & (
                F.col("delta_vs_previous_pct_points")
                >= F.lit(ADJACENT_DELTA_THRESHOLD_PCT_POINTS)
            ),
        )
        .withColumn(
            "has_sudden_drop_vs_next_date",
            F.col("delta_vs_next_pct_points").isNotNull()
            & (
                F.col("delta_vs_next_pct_points")
                <= F.lit(-ADJACENT_DELTA_THRESHOLD_PCT_POINTS)
            ),
        )
        .withColumn(
            "has_sudden_spike_vs_next_date",
            F.col("delta_vs_next_pct_points").isNotNull()
            & (
                F.col("delta_vs_next_pct_points")
                >= F.lit(ADJACENT_DELTA_THRESHOLD_PCT_POINTS)
            ),
        )
        .withColumn(
            "has_suspicious_coverage_percentage",
            F.col("has_unusually_low_coverage_pct")
            | F.col("has_unusually_high_coverage_pct")
            | F.col("has_sudden_drop_vs_previous_date")
            | F.col("has_sudden_spike_vs_previous_date")
            | F.col("has_sudden_drop_vs_next_date")
            | F.col("has_sudden_spike_vs_next_date"),
        )
        .where(F.col("has_suspicious_coverage_percentage") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "planned_lines_count",
            "observed_lines_count",
            "planned_lines_observed_count",
            "planned_lines_not_observed_count",
            "observed_lines_not_in_gtfs_count",
            "planned_lines_observed_pct",
            "previous_planned_lines_observed_pct",
            "next_planned_lines_observed_pct",
            "delta_vs_previous_pct_points",
            "delta_vs_next_pct_points",
            "avg_planned_lines_observed_pct",
            "stddev_planned_lines_observed_pct",
            "min_planned_lines_observed_pct",
            "max_planned_lines_observed_pct",
            "low_coverage_threshold",
            "high_coverage_threshold",
            "has_unusually_low_coverage_pct",
            "has_unusually_high_coverage_pct",
            "has_sudden_drop_vs_previous_date",
            "has_sudden_spike_vs_previous_date",
            "has_sudden_drop_vs_next_date",
            "has_sudden_spike_vs_next_date",
            "has_suspicious_coverage_percentage",
            "gtfs_load_date",
            "record_created_at",
        )
    )

    return audit_df
