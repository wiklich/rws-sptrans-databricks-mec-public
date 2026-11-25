"""
Gold observed spatial distribution suspicious spatial outlier audit pipeline.

Purpose:
    Identify spatial cells with unusually high concentration relative to the
    broader spatial distribution in gold_observed_spatial_distribution.

Audit focus:
    - very large observed_position_records_count
    - very large distinct_observed_vehicles_count
    - abnormal daily concentration spikes

Why this audit matters:
    This audit is useful because it highlights spatial areas whose observed
    activity is unusually concentrated relative to the broader daily spatial
    distribution.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_distribution"
OUTPUT_TABLE_NAME = "gold.gold_observed_spatial_distribution_suspicious_outlier_audit"

STDDEV_MULTIPLIER = 2.0


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying spatial cells with suspicious concentration "
        "outliers in gold_observed_spatial_distribution, including unusually "
        "high observed position volume and unusually high distinct observed "
        "vehicle support relative to the broader daily spatial distribution."
    ),
)
def gold_observed_spatial_distribution_suspicious_outlier_audit() -> DataFrame:
    """
    Materialize the suspicious spatial outlier audit for
    gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Audit rows for spatial cells with suspiciously high concentration.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_spatial_distribution_suspicious_outlier_audit(
        source_df
    )


def _build_gold_observed_spatial_distribution_suspicious_outlier_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious spatial outlier audit dataset.

    Processing steps:
        1. Read gold_observed_spatial_distribution.
        2. Compute daily reference statistics for spatial concentration metrics.
        3. Compute high outlier thresholds per analysis_date.
        4. Flag spatial cells with suspiciously high concentration.
        5. Keep only rows that match at least one outlier condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Audit DataFrame containing spatial cells with suspiciously high
            concentration relative to the broader daily spatial distribution.
    """
    daily_stats_df = (
        source_df.groupBy("analysis_date")
        .agg(
            F.avg("observed_position_records_count").alias(
                "avg_observed_position_records_count"
            ),
            F.stddev_pop("observed_position_records_count").alias(
                "stddev_observed_position_records_count"
            ),
            F.min("observed_position_records_count").alias(
                "min_observed_position_records_count"
            ),
            F.max("observed_position_records_count").alias(
                "max_observed_position_records_count"
            ),
            F.avg("distinct_observed_vehicles_count").alias(
                "avg_distinct_observed_vehicles_count"
            ),
            F.stddev_pop("distinct_observed_vehicles_count").alias(
                "stddev_distinct_observed_vehicles_count"
            ),
            F.min("distinct_observed_vehicles_count").alias(
                "min_distinct_observed_vehicles_count"
            ),
            F.max("distinct_observed_vehicles_count").alias(
                "max_distinct_observed_vehicles_count"
            ),
            F.count(F.lit(1)).cast("long").alias("active_spatial_cells_count"),
            F.sum("observed_position_records_count").cast("long").alias(
                "total_daily_observed_position_records_count"
            ),
        )
    )

    audit_df = (
        source_df.join(daily_stats_df, on="analysis_date", how="inner")
        .withColumn(
            "high_position_records_threshold",
            F.col("avg_observed_position_records_count")
            + (
                F.coalesce(
                    F.col("stddev_observed_position_records_count"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "high_distinct_vehicles_threshold",
            F.col("avg_distinct_observed_vehicles_count")
            + (
                F.coalesce(
                    F.col("stddev_distinct_observed_vehicles_count"),
                    F.lit(0.0),
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "position_share_ratio",
            F.when(
                F.col("total_daily_observed_position_records_count") > 0,
                (
                    F.col("observed_position_records_count").cast("double")
                    / F.col("total_daily_observed_position_records_count").cast(
                        "double"
                    )
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "has_unusually_high_position_records_count",
            F.col("observed_position_records_count")
            > F.col("high_position_records_threshold"),
        )
        .withColumn(
            "has_unusually_high_distinct_observed_vehicles_count",
            F.col("distinct_observed_vehicles_count")
            > F.col("high_distinct_vehicles_threshold"),
        )
        .withColumn(
            "has_abnormal_daily_concentration_spike",
            F.col("max_observed_position_records_count").isNotNull()
            & (F.col("max_observed_position_records_count") > 0)
            & (
                F.col("observed_position_records_count")
                == F.col("max_observed_position_records_count")
            )
            & (F.col("active_spatial_cells_count") > 1)
            & F.col("position_share_ratio").isNotNull()
            & (
                F.col("position_share_ratio")
                > (
                    F.lit(1.0) / F.col("active_spatial_cells_count").cast("double")
                ) * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "has_suspicious_spatial_outlier",
            F.col("has_unusually_high_position_records_count")
            | F.col("has_unusually_high_distinct_observed_vehicles_count")
            | F.col("has_abnormal_daily_concentration_spike"),
        )
        .where(F.col("has_suspicious_spatial_outlier") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "spatial_cell_id",
            "spatial_resolution",
            "cell_center_latitude",
            "cell_center_longitude",
            "observed_position_records_count",
            "distinct_observed_vehicles_count",
            "distinct_observed_lines_count",
            "min_captured_at_local",
            "max_captured_at_local",
            "avg_observed_position_records_count",
            "stddev_observed_position_records_count",
            "min_observed_position_records_count",
            "max_observed_position_records_count",
            "avg_distinct_observed_vehicles_count",
            "stddev_distinct_observed_vehicles_count",
            "min_distinct_observed_vehicles_count",
            "max_distinct_observed_vehicles_count",
            "active_spatial_cells_count",
            "total_daily_observed_position_records_count",
            "high_position_records_threshold",
            "high_distinct_vehicles_threshold",
            "position_share_ratio",
            "has_unusually_high_position_records_count",
            "has_unusually_high_distinct_observed_vehicles_count",
            "has_abnormal_daily_concentration_spike",
            "has_suspicious_spatial_outlier",
            "record_created_at",
        )
    )

    return audit_df
