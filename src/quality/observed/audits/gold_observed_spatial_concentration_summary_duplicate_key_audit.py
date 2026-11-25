"""
Gold observed spatial concentration summary duplicate daily summary key audit pipeline.

Purpose:
    Identify cases where multiple output rows exist for the same daily summary
    key in gold_observed_spatial_concentration_summary.

Audit focus:
    - same analysis_date appearing more than once
    - accidental duplication introduced by summarization logic

Why this audit matters:
    This audit validates the stability of the declared Gold grain for
    gold_observed_spatial_concentration_summary.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_concentration_summary"
OUTPUT_TABLE_NAME = "gold.gold_observed_spatial_concentration_summary_duplicate_key_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying duplicate daily summary keys in "
        "gold_observed_spatial_concentration_summary. Detects cases where more "
        "than one row exists for the same analysis_date."
    ),
)
def gold_observed_spatial_concentration_summary_duplicate_key_audit() -> DataFrame:
    """
    Materialize the duplicate daily summary key audit for
    gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit rows for duplicate analysis_date keys.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_spatial_concentration_summary_duplicate_key_audit(
        source_df
    )


def _build_gold_observed_spatial_concentration_summary_duplicate_key_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the duplicate daily summary key audit dataset.

    Processing steps:
        1. Read gold_observed_spatial_concentration_summary.
        2. Group by analysis_date.
        3. Count rows per declared Gold grain.
        4. Keep only keys with more than one row.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit DataFrame containing duplicate daily summary keys.
    """
    audit_df = (
        source_df.groupBy("analysis_date")
        .agg(
            F.count(F.lit(1)).alias("duplicate_row_count"),
            F.collect_set("active_spatial_cells_count").alias(
                "active_spatial_cells_count_values"
            ),
            F.collect_set("total_observed_position_records_count").alias(
                "total_observed_position_records_count_values"
            ),
            F.collect_set("avg_positions_per_active_cell").alias(
                "avg_positions_per_active_cell_values"
            ),
            F.collect_set("max_positions_in_single_cell").alias(
                "max_positions_in_single_cell_values"
            ),
            F.collect_set("max_distinct_observed_vehicles_in_single_cell").alias(
                "max_distinct_observed_vehicles_in_single_cell_values"
            ),
            F.collect_set("max_distinct_observed_lines_in_single_cell").alias(
                "max_distinct_observed_lines_in_single_cell_values"
            ),
            F.collect_set("top_cell_position_share_ratio").alias(
                "top_cell_position_share_ratio_values"
            ),
            F.collect_set("top_5_cells_position_share_ratio").alias(
                "top_5_cells_position_share_ratio_values"
            ),
            F.min("record_created_at").alias("min_record_created_at"),
            F.max("record_created_at").alias("max_record_created_at"),
        )
        .withColumn(
            "has_duplicate_daily_summary_key",
            F.col("duplicate_row_count") > F.lit(1),
        )
        .where(F.col("has_duplicate_daily_summary_key") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "duplicate_row_count",
            "active_spatial_cells_count_values",
            "total_observed_position_records_count_values",
            "avg_positions_per_active_cell_values",
            "max_positions_in_single_cell_values",
            "max_distinct_observed_vehicles_in_single_cell_values",
            "max_distinct_observed_lines_in_single_cell_values",
            "top_cell_position_share_ratio_values",
            "top_5_cells_position_share_ratio_values",
            "has_duplicate_daily_summary_key",
            "min_record_created_at",
            "max_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
