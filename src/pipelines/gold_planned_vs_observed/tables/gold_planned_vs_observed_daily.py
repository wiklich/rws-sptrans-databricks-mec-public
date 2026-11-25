"""
Gold planned vs observed daily summary pipeline.

Purpose:
    Build the canonical daily KPI summary for the Planned vs Observed Service
    Comparison component.

Business role:
    This dataset represents the final business-facing summary output of the
    project. It aggregates the detailed daily line-level comparison into one
    row per analysis date, exposing the main coverage and consistency KPIs.

Input:
    - gold.gold_planned_vs_observed_line_status

Output:
    - gold.gold_planned_vs_observed_daily

Important rules:
    - one row per analysis_date
    - counts must be derived from the detailed comparison output
    - percentage calculations must use division-by-zero protection
    - no new comparison logic should be introduced in this table
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_planned_vs_observed_daily import (
    get_gold_planned_vs_observed_daily_schema,
)


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_line_status"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_daily"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical daily KPI summary of the planned versus observed comparison. "
        "Contains one row per analysis date with final business-facing metrics "
        "derived from gold_planned_vs_observed_line_status."
    ),
    schema=get_gold_planned_vs_observed_daily_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_planned_lines_count",
    "planned_lines_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observed_lines_count",
    "observed_lines_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_planned_lines_observed_count",
    "planned_lines_observed_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_planned_lines_not_observed_count",
    "planned_lines_not_observed_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observed_lines_not_in_gtfs_count",
    "observed_lines_not_in_gtfs_count IS NOT NULL",
)
def gold_planned_vs_observed_daily() -> DataFrame:
    """
    Materialize the canonical Gold planned vs observed daily summary table.

    Returns:
        DataFrame:
            Daily KPI summary derived from gold_planned_vs_observed_line_status.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_daily(source_df)


def _build_gold_planned_vs_observed_daily(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold planned vs observed daily summary dataset.

    Processing steps:
        1. Read the detailed planned vs observed line status table.
        2. Group records by analysis_date.
        3. Derive daily count metrics from planned/observed flags and match_status.
        4. Calculate planned line observed coverage percentage.
        5. Preserve contextual metadata when semantically valid.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Daily KPI summary output with one row per analysis_date.
    """
    aggregated_df = (
        source_df.groupBy("analysis_date")
        .agg(
            F.count(F.when(F.col("is_planned") == True, 1)).cast("long").alias("planned_lines_count"),
            F.count(F.when(F.col("is_observed") == True, 1)).cast("long").alias("observed_lines_count"),
            F.count(
                F.when(F.col("match_status") == "PLANNED_AND_OBSERVED", 1)
            ).cast("long").alias("planned_lines_observed_count"),
            F.count(
                F.when(F.col("match_status") == "PLANNED_NOT_OBSERVED", 1)
            ).cast("long").alias("planned_lines_not_observed_count"),
            F.count(
                F.when(F.col("match_status") == "OBSERVED_NOT_IN_GTFS", 1)
            ).cast("long").alias("observed_lines_not_in_gtfs_count"),
            F.first("gtfs_load_date", ignorenulls=True).alias("gtfs_load_date"),
        )
        .withColumn(
            "planned_lines_observed_pct",
            F.when(
                F.col("planned_lines_count") > 0,
                F.round(
                    (F.col("planned_lines_observed_count") / F.col("planned_lines_count")) * F.lit(100.0),
                    2,
                ),
            ).otherwise(F.lit(None)),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "analysis_date",
        "planned_lines_count",
        "observed_lines_count",
        "planned_lines_observed_count",
        "planned_lines_not_observed_count",
        "observed_lines_not_in_gtfs_count",
        "planned_lines_observed_pct",
        "gtfs_load_date",
        "record_created_at",
    )
