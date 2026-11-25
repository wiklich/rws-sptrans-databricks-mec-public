"""
Gold observed spatial concentration summary pipeline.

Purpose:
    Build the canonical daily summary layer of observed spatial concentration
    used by the Observed Spatial Analysis component.

Business role:
    This dataset represents a business-ready daily summary of how concentrated
    the observed vehicle positions are across space.

Input:
    - gold.gold_observed_spatial_distribution

Output:
    - gold.gold_observed_spatial_concentration_summary

Declared grain:
    - one row per analysis_date

Important rules:
    - Gold should consume the canonical detailed spatial distribution Gold
    - the analysis must remain strictly observed-side only
    - one row per analysis_date is allowed
    - daily totals must reconcile with the detailed spatial distribution Gold
    - hotspot dominance metrics must describe observed concentration, not
      operational importance or official service coverage
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.gold_observed.schemas.schema_gold_observed_spatial_concentration_summary import (
    get_gold_observed_spatial_concentration_summary_schema,
)


GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_distribution"
OUTPUT_TABLE_NAME = "gold.gold_observed_spatial_concentration_summary"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical observed spatial concentration summary table derived from "
        "gold_observed_spatial_distribution. Contains one row per analysis_date "
        "with active spatial cell count, total observed position volume, "
        "average density per active cell, daily hotspot maxima, and top-cell "
        "concentration share ratios."
    ),
    schema=get_gold_observed_spatial_concentration_summary_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_active_spatial_cells_count",
    "active_spatial_cells_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_total_observed_position_records_count",
    "total_observed_position_records_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_avg_positions_per_active_cell",
    "avg_positions_per_active_cell IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_max_positions_in_single_cell",
    "max_positions_in_single_cell IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_top_cell_position_share_ratio",
    "top_cell_position_share_ratio IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_top_5_cells_position_share_ratio",
    "top_5_cells_position_share_ratio IS NOT NULL",
)
def gold_observed_spatial_concentration_summary() -> DataFrame:
    """
    Materialize the canonical Gold observed spatial concentration summary table.

    Returns:
        DataFrame:
            Daily observed spatial concentration summary derived from
            gold_observed_spatial_distribution.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_spatial_concentration_summary(source_df)


def _prepare_spatial_distribution_source(source_df: DataFrame) -> DataFrame:
    """
    Prepare the detailed spatial distribution Gold source for daily concentration
    summarization.

    Processing steps:
        1. Keep only fields needed for daily concentration summary logic.
        2. Cast fields to expected analytical types.
        3. Filter rows with invalid required values.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Prepared detailed spatial distribution dataset ready for
            concentration summary derivation.
    """
    return (
        source_df.select(
            F.col("analysis_date").cast("date").alias("analysis_date"),
            F.col("spatial_cell_id").cast("string").alias("spatial_cell_id"),
            F.col("observed_position_records_count").cast("long").alias(
                "observed_position_records_count"
            ),
            F.col("distinct_observed_vehicles_count").cast("long").alias(
                "distinct_observed_vehicles_count"
            ),
            F.col("distinct_observed_lines_count").cast("long").alias(
                "distinct_observed_lines_count"
            ),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("spatial_cell_id").isNotNull())
        .where(F.trim(F.col("spatial_cell_id")) != "")
        .where(F.col("observed_position_records_count").isNotNull())
        .where(F.col("distinct_observed_vehicles_count").isNotNull())
        .where(F.col("distinct_observed_lines_count").isNotNull())
    )


def _build_ranked_spatial_cells(prepared_df: DataFrame) -> DataFrame:
    """
    Rank spatial cells within each analysis_date by observed position volume.

    Ranking rule:
        - higher observed_position_records_count ranks first
        - spatial_cell_id is used as a deterministic tie-breaker

    Args:
        prepared_df:
            Prepared detailed spatial distribution dataset.

    Returns:
        DataFrame:
            Ranked spatial cells with one deterministic daily rank per cell.
    """
    ranking_window = Window.partitionBy("analysis_date").orderBy(
        F.col("observed_position_records_count").desc(),
        F.col("spatial_cell_id").asc(),
    )

    return prepared_df.withColumn(
        "spatial_concentration_rank",
        F.row_number().over(ranking_window),
    )


def _build_gold_observed_spatial_concentration_summary(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the canonical Gold observed spatial concentration summary dataset.

    Processing steps:
        1. Prepare the detailed spatial distribution Gold source.
        2. Rank spatial cells within each analysis date by observed position volume.
        3. Aggregate daily concentration summary metrics.
        4. Derive top-cell and top-5-cell position share ratios.
        5. Materialize the final Gold output.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Canonical observed spatial concentration summary output with one
            row per analysis_date.
    """
    prepared_df = _prepare_spatial_distribution_source(source_df)
    ranked_df = _build_ranked_spatial_cells(prepared_df)

    aggregated_df = (
        ranked_df.groupBy("analysis_date")
        .agg(
            F.countDistinct("spatial_cell_id").cast("long").alias(
                "active_spatial_cells_count"
            ),
            F.coalesce(F.sum("observed_position_records_count"), F.lit(0))
            .cast("long")
            .alias("total_observed_position_records_count"),
            F.max("observed_position_records_count").cast("long").alias(
                "max_positions_in_single_cell"
            ),
            F.max("distinct_observed_vehicles_count").cast("long").alias(
                "max_distinct_observed_vehicles_in_single_cell"
            ),
            F.max("distinct_observed_lines_count").cast("long").alias(
                "max_distinct_observed_lines_in_single_cell"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("spatial_concentration_rank") == 1,
                        F.col("observed_position_records_count"),
                    ).otherwise(F.lit(0))
                ),
                F.lit(0),
            )
            .cast("long")
            .alias("top_1_cells_observed_position_records_count"),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("spatial_concentration_rank") <= 5,
                        F.col("observed_position_records_count"),
                    ).otherwise(F.lit(0))
                ),
                F.lit(0),
            )
            .cast("long")
            .alias("top_5_cells_observed_position_records_count"),
        )
        .withColumn(
            "avg_positions_per_active_cell",
            F.when(
                F.col("active_spatial_cells_count") > 0,
                (
                    F.col("total_observed_position_records_count").cast("double")
                    / F.col("active_spatial_cells_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "top_cell_position_share_ratio",
            F.when(
                F.col("total_observed_position_records_count") > 0,
                (
                    F.col("top_1_cells_observed_position_records_count").cast("double")
                    / F.col("total_observed_position_records_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "top_5_cells_position_share_ratio",
            F.when(
                F.col("total_observed_position_records_count") > 0,
                (
                    F.col("top_5_cells_observed_position_records_count").cast("double")
                    / F.col("total_observed_position_records_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "analysis_date",
        "active_spatial_cells_count",
        "total_observed_position_records_count",
        "avg_positions_per_active_cell",
        "max_positions_in_single_cell",
        "max_distinct_observed_vehicles_in_single_cell",
        "max_distinct_observed_lines_in_single_cell",
        "top_cell_position_share_ratio",
        "top_5_cells_position_share_ratio",
        "record_created_at",
    )
