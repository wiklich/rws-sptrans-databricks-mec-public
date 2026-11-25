"""
Gold observed spatial concentration summary reconciliation anomaly audit pipeline.

Purpose:
    Identify analysis dates where the observed spatial concentration summary
    does not reconcile correctly with the detailed spatial distribution source.

Audit focus:
    - total position volume does not match the sum from the detailed spatial Gold
    - active spatial cell count does not match the number of distinct cells upstream

Why this audit matters:
    This audit is useful because reconciliation is central to trust in the
    daily spatial concentration summary output.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SUMMARY_GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_concentration_summary"
DETAIL_GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_distribution"
OUTPUT_TABLE_NAME = (
    "gold.gold_observed_spatial_concentration_summary_reconciliation_anomaly_audit"
)

FLOAT_TOLERANCE = 1e-9


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying reconciliation anomalies between "
        "gold_observed_spatial_concentration_summary and "
        "gold_observed_spatial_distribution, including mismatches in daily "
        "total observed position volume, active spatial cell count, and other "
        "derived summary metrics."
    ),
)
def gold_observed_spatial_concentration_summary_reconciliation_anomaly_audit() -> DataFrame:
    """
    Materialize the reconciliation anomaly audit for
    gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Audit rows for daily summaries that do not reconcile with the
            detailed spatial distribution Gold source.
    """
    summary_df = spark.read.table(SUMMARY_GOLD_SOURCE_TABLE)
    detail_df = spark.read.table(DETAIL_GOLD_SOURCE_TABLE)

    return _build_gold_observed_spatial_concentration_summary_reconciliation_anomaly_audit(
        summary_df,
        detail_df,
    )


def _prepare_summary_source(summary_df: DataFrame) -> DataFrame:
    """
    Prepare the summary Gold source for reconciliation checks.

    Args:
        summary_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Prepared summary dataset with the fields required for reconciliation.
    """
    return summary_df.select(
        F.col("analysis_date").cast("date").alias("analysis_date"),
        F.col("active_spatial_cells_count").cast("long").alias(
            "actual_active_spatial_cells_count"
        ),
        F.col("total_observed_position_records_count").cast("long").alias(
            "actual_total_observed_position_records_count"
        ),
        F.col("avg_positions_per_active_cell").cast("double").alias(
            "actual_avg_positions_per_active_cell"
        ),
        F.col("max_positions_in_single_cell").cast("long").alias(
            "actual_max_positions_in_single_cell"
        ),
        F.col("top_cell_position_share_ratio").cast("double").alias(
            "actual_top_cell_position_share_ratio"
        ),
        F.col("top_5_cells_position_share_ratio").cast("double").alias(
            "actual_top_5_cells_position_share_ratio"
        ),
        F.col("record_created_at").cast("timestamp").alias("summary_record_created_at"),
    )


def _prepare_detail_source(detail_df: DataFrame) -> DataFrame:
    """
    Prepare the detailed spatial distribution Gold source for reconciliation checks.

    Args:
        detail_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Prepared detailed spatial distribution dataset.
    """
    return detail_df.select(
        F.col("analysis_date").cast("date").alias("analysis_date"),
        F.col("spatial_cell_id").cast("string").alias("spatial_cell_id"),
        F.col("observed_position_records_count").cast("long").alias(
            "observed_position_records_count"
        ),
    )


def _build_expected_daily_summary_from_detail(detail_df: DataFrame) -> DataFrame:
    """
    Build the expected daily summary metrics directly from the detailed spatial
    distribution Gold source.

    Derived metrics:
        - active_spatial_cells_count
        - total_observed_position_records_count
        - avg_positions_per_active_cell
        - max_positions_in_single_cell
        - top_cell_position_share_ratio
        - top_5_cells_position_share_ratio

    Args:
        detail_df:
            Prepared detailed spatial distribution dataset.

    Returns:
        DataFrame:
            Expected daily summary metrics derived from the detailed Gold source.
    """
    ranked_df = detail_df.withColumn(
        "spatial_concentration_rank",
        F.row_number().over(
            Window.partitionBy("analysis_date").orderBy(
                F.col("observed_position_records_count").desc(),
                F.col("spatial_cell_id").asc(),
            )
        ),
    )

    expected_df = (
        ranked_df.groupBy("analysis_date")
        .agg(
            F.countDistinct("spatial_cell_id").cast("long").alias(
                "expected_active_spatial_cells_count"
            ),
            F.coalesce(F.sum("observed_position_records_count"), F.lit(0))
            .cast("long")
            .alias("expected_total_observed_position_records_count"),
            F.max("observed_position_records_count").cast("long").alias(
                "expected_max_positions_in_single_cell"
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
            .alias("expected_top_1_cells_observed_position_records_count"),
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
            .alias("expected_top_5_cells_observed_position_records_count"),
        )
        .withColumn(
            "expected_avg_positions_per_active_cell",
            F.when(
                F.col("expected_active_spatial_cells_count") > 0,
                (
                    F.col("expected_total_observed_position_records_count").cast("double")
                    / F.col("expected_active_spatial_cells_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "expected_top_cell_position_share_ratio",
            F.when(
                F.col("expected_total_observed_position_records_count") > 0,
                (
                    F.col(
                        "expected_top_1_cells_observed_position_records_count"
                    ).cast("double")
                    / F.col("expected_total_observed_position_records_count").cast(
                        "double"
                    )
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "expected_top_5_cells_position_share_ratio",
            F.when(
                F.col("expected_total_observed_position_records_count") > 0,
                (
                    F.col(
                        "expected_top_5_cells_observed_position_records_count"
                    ).cast("double")
                    / F.col("expected_total_observed_position_records_count").cast(
                        "double"
                    )
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    return expected_df


def _build_gold_observed_spatial_concentration_summary_reconciliation_anomaly_audit(
    summary_df: DataFrame,
    detail_df: DataFrame,
) -> DataFrame:
    """
    Build the reconciliation anomaly audit dataset.

    Processing steps:
        1. Read the summary Gold source.
        2. Read the detailed spatial distribution Gold source.
        3. Derive expected daily summary metrics from the detailed Gold source.
        4. Join actual and expected daily metrics by analysis_date.
        5. Flag reconciliation anomalies.
        6. Keep only rows with at least one reconciliation mismatch.

    Args:
        summary_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.
        detail_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Audit DataFrame containing analysis dates with reconciliation anomalies.
    """
    prepared_summary_df = _prepare_summary_source(summary_df)
    prepared_detail_df = _prepare_detail_source(detail_df)
    expected_daily_df = _build_expected_daily_summary_from_detail(prepared_detail_df)

    audit_df = (
        prepared_summary_df.join(expected_daily_df, on="analysis_date", how="full")
        .withColumn(
            "has_missing_summary_row",
            F.col("actual_total_observed_position_records_count").isNull(),
        )
        .withColumn(
            "has_missing_expected_detail_summary",
            F.col("expected_total_observed_position_records_count").isNull(),
        )
        .withColumn(
            "has_total_position_volume_mismatch",
            F.col("actual_total_observed_position_records_count").isNotNull()
            & F.col("expected_total_observed_position_records_count").isNotNull()
            & (
                F.col("actual_total_observed_position_records_count")
                != F.col("expected_total_observed_position_records_count")
            ),
        )
        .withColumn(
            "has_active_spatial_cells_count_mismatch",
            F.col("actual_active_spatial_cells_count").isNotNull()
            & F.col("expected_active_spatial_cells_count").isNotNull()
            & (
                F.col("actual_active_spatial_cells_count")
                != F.col("expected_active_spatial_cells_count")
            ),
        )
        .withColumn(
            "has_avg_positions_per_active_cell_mismatch",
            F.col("actual_avg_positions_per_active_cell").isNotNull()
            & F.col("expected_avg_positions_per_active_cell").isNotNull()
            & (
                F.abs(
                    F.col("actual_avg_positions_per_active_cell")
                    - F.col("expected_avg_positions_per_active_cell")
                )
                > F.lit(FLOAT_TOLERANCE)
            ),
        )
        .withColumn(
            "has_max_positions_in_single_cell_mismatch",
            F.col("actual_max_positions_in_single_cell").isNotNull()
            & F.col("expected_max_positions_in_single_cell").isNotNull()
            & (
                F.col("actual_max_positions_in_single_cell")
                != F.col("expected_max_positions_in_single_cell")
            ),
        )
        .withColumn(
            "has_top_cell_position_share_ratio_mismatch",
            F.col("actual_top_cell_position_share_ratio").isNotNull()
            & F.col("expected_top_cell_position_share_ratio").isNotNull()
            & (
                F.abs(
                    F.col("actual_top_cell_position_share_ratio")
                    - F.col("expected_top_cell_position_share_ratio")
                )
                > F.lit(FLOAT_TOLERANCE)
            ),
        )
        .withColumn(
            "has_top_5_cells_position_share_ratio_mismatch",
            F.col("actual_top_5_cells_position_share_ratio").isNotNull()
            & F.col("expected_top_5_cells_position_share_ratio").isNotNull()
            & (
                F.abs(
                    F.col("actual_top_5_cells_position_share_ratio")
                    - F.col("expected_top_5_cells_position_share_ratio")
                )
                > F.lit(FLOAT_TOLERANCE)
            ),
        )
        .withColumn(
            "has_reconciliation_anomaly",
            F.col("has_missing_summary_row")
            | F.col("has_missing_expected_detail_summary")
            | F.col("has_total_position_volume_mismatch")
            | F.col("has_active_spatial_cells_count_mismatch")
            | F.col("has_avg_positions_per_active_cell_mismatch")
            | F.col("has_max_positions_in_single_cell_mismatch")
            | F.col("has_top_cell_position_share_ratio_mismatch")
            | F.col("has_top_5_cells_position_share_ratio_mismatch"),
        )
        .where(F.col("has_reconciliation_anomaly") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "actual_active_spatial_cells_count",
            "expected_active_spatial_cells_count",
            "actual_total_observed_position_records_count",
            "expected_total_observed_position_records_count",
            "actual_avg_positions_per_active_cell",
            "expected_avg_positions_per_active_cell",
            "actual_max_positions_in_single_cell",
            "expected_max_positions_in_single_cell",
            "actual_top_cell_position_share_ratio",
            "expected_top_cell_position_share_ratio",
            "actual_top_5_cells_position_share_ratio",
            "expected_top_5_cells_position_share_ratio",
            "has_missing_summary_row",
            "has_missing_expected_detail_summary",
            "has_total_position_volume_mismatch",
            "has_active_spatial_cells_count_mismatch",
            "has_avg_positions_per_active_cell_mismatch",
            "has_max_positions_in_single_cell_mismatch",
            "has_top_cell_position_share_ratio_mismatch",
            "has_top_5_cells_position_share_ratio_mismatch",
            "has_reconciliation_anomaly",
            "summary_record_created_at",
            "record_created_at",
        )
    )

    return audit_df
