"""
Audit pipeline for edge cases in gold.gold_direction_mapping_reference.

Purpose:
    Detect unusual but possible scenarios that should be monitored in the
    Gold direction mapping reference table.

Audit scope:
    - lines with only one observed direction represented
    - lines where both observed directions map to the same gtfs_direction_id
    - unusually high share of low or none confidence mappings
    - mappings built from extremely low sample size
    - multiple GTFS route candidates for the same observed line_sign

Important:
    This is a non-blocking audit table.
    It exposes edge cases for investigation without dropping records from the
    main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_direction_mapping_reference"
TABLE_NAME = "gold.gold_direction_mapping_reference_edge_cases_audit"

EXTREMELY_LOW_SAMPLE_THRESHOLD = 2
LOW_OR_NONE_CONFIDENCE_SHARE_THRESHOLD = 0.50


def _base_source() -> DataFrame:
    """
    Read the source Gold direction mapping reference table.
    """
    return spark.read.table(SOURCE_TABLE)


def _lines_with_only_one_observed_direction(df: DataFrame) -> DataFrame:
    """
    Detect lines where only one observed direction is represented.
    """
    flagged_lines = (
        df.groupBy("line_sign")
        .agg(
            F.countDistinct("observed_direction").alias("observed_direction_count"),
        )
        .filter(F.col("observed_direction_count") == 1)
    )

    return (
        df.alias("src")
        .join(flagged_lines.alias("flag"), on="line_sign", how="inner")
        .select(
            F.col("src.*"),
            F.col("flag.observed_direction_count").alias("observed_direction_count"),
            F.lit(None).cast("int").alias("distinct_gtfs_direction_count"),
            F.lit(None).cast("double").alias("low_or_none_confidence_share"),
            F.lit(None).cast("array<string>").alias("candidate_confidence_values"),
            F.lit("ONLY_ONE_OBSERVED_DIRECTION").alias("audit_rule"),
            F.lit(
                "The line_sign has only one observed_direction represented in the Gold table."
            ).alias("audit_message"),
        )
    )


def _both_observed_directions_same_gtfs_direction(df: DataFrame) -> DataFrame:
    """
    Detect lines where multiple observed directions collapse into the same
    gtfs_direction_id.
    """
    flagged_lines = (
        df.groupBy("line_sign")
        .agg(
            F.countDistinct("observed_direction").alias("observed_direction_count"),
            F.countDistinct("gtfs_direction_id").alias("distinct_gtfs_direction_count"),
        )
        .filter(
            (F.col("observed_direction_count") > 1)
            & (F.col("distinct_gtfs_direction_count") == 1)
        )
    )

    return (
        df.alias("src")
        .join(flagged_lines.alias("flag"), on="line_sign", how="inner")
        .select(
            F.col("src.*"),
            F.col("flag.observed_direction_count").alias("observed_direction_count"),
            F.col("flag.distinct_gtfs_direction_count").alias(
                "distinct_gtfs_direction_count"
            ),
            F.lit(None).cast("double").alias("low_or_none_confidence_share"),
            F.lit(None).cast("array<string>").alias("candidate_confidence_values"),
            F.lit("OBSERVED_DIRECTIONS_COLLAPSE_TO_SAME_GTFS_DIRECTION").alias(
                "audit_rule"
            ),
            F.lit(
                "Multiple observed directions for the same line_sign map to the same gtfs_direction_id."
            ).alias("audit_message"),
        )
    )


def _high_share_low_or_none_confidence(df: DataFrame) -> DataFrame:
    """
    Detect lines where the share of low or none confidence mappings is unusually high.
    """
    flagged_lines = (
        df.groupBy("line_sign")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(
                F.when(F.col("mapping_confidence").isin("low", "none"), 1).otherwise(0)
            ).alias("low_or_none_confidence_count"),
            F.collect_set("mapping_confidence").alias("candidate_confidence_values"),
        )
        .withColumn(
            "low_or_none_confidence_share",
            F.col("low_or_none_confidence_count") / F.col("row_count"),
        )
        .filter(
            F.col("low_or_none_confidence_share")
            >= F.lit(LOW_OR_NONE_CONFIDENCE_SHARE_THRESHOLD)
        )
    )

    return (
        df.alias("src")
        .join(flagged_lines.alias("flag"), on="line_sign", how="inner")
        .select(
            F.col("src.*"),
            F.lit(None).cast("int").alias("observed_direction_count"),
            F.lit(None).cast("int").alias("distinct_gtfs_direction_count"),
            F.col("flag.low_or_none_confidence_share").alias(
                "low_or_none_confidence_share"
            ),
            F.col("flag.candidate_confidence_values").alias(
                "candidate_confidence_values"
            ),
            F.lit("HIGH_SHARE_LOW_OR_NONE_CONFIDENCE").alias("audit_rule"),
            F.lit(
                "The line_sign has an unusually high share of low or none confidence mappings."
            ).alias("audit_message"),
        )
    )


def _extremely_low_sample_size(df: DataFrame) -> DataFrame:
    """
    Detect mappings built from extremely low sample size.
    """
    return (
        df.filter(F.col("vehicle_count") <= F.lit(EXTREMELY_LOW_SAMPLE_THRESHOLD))
        .select(
            F.col("*"),
            F.lit(None).cast("int").alias("observed_direction_count"),
            F.lit(None).cast("int").alias("distinct_gtfs_direction_count"),
            F.lit(None).cast("double").alias("low_or_none_confidence_share"),
            F.lit(None).cast("array<string>").alias("candidate_confidence_values"),
            F.lit("EXTREMELY_LOW_SAMPLE_SIZE").alias("audit_rule"),
            F.lit(
                "The mapping is built from an extremely low number of observed vehicles."
            ).alias("audit_message"),
        )
    )


def _multiple_gtfs_candidates_per_line(df: DataFrame) -> DataFrame:
    """
    Detect lines with multiple GTFS direction candidates represented across the line.

    Note:
        In the current Gold schema there is no route_id or separate GTFS route candidate
        field. Therefore, this audit monitors multiple gtfs_direction_id values for the
        same line_sign as the closest available edge-case indicator.
    """
    flagged_lines = (
        df.groupBy("line_sign")
        .agg(
            F.countDistinct("gtfs_direction_id").alias("distinct_gtfs_direction_count"),
        )
        .filter(F.col("distinct_gtfs_direction_count") > 1)
    )

    return (
        df.alias("src")
        .join(flagged_lines.alias("flag"), on="line_sign", how="inner")
        .select(
            F.col("src.*"),
            F.lit(None).cast("int").alias("observed_direction_count"),
            F.col("flag.distinct_gtfs_direction_count").alias(
                "distinct_gtfs_direction_count"
            ),
            F.lit(None).cast("double").alias("low_or_none_confidence_share"),
            F.lit(None).cast("array<string>").alias("candidate_confidence_values"),
            F.lit("MULTIPLE_GTFS_DIRECTION_CANDIDATES_FOR_LINE").alias("audit_rule"),
            F.lit(
                "The line_sign is associated with multiple gtfs_direction_id values and should be monitored."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "gold.gold_direction_mapping_reference, including single observed "
        "direction lines, direction collapses, high low-confidence share, "
        "extremely low sample size, and multiple GTFS direction candidates."
    ),
)
def gold_direction_mapping_reference_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for edge cases in the Gold direction
    mapping reference.
    """
    source_df = _base_source()

    base_columns = [
        "line_sign",
        "observed_direction",
        "gtfs_direction_id",
        "evidence_type",
        "evidence_score",
        "mapping_confidence",
        "line_category",
        "dist_first_km",
        "dist_last_km",
        "vehicle_count",
        "low_sample_flag",
        "direction_collision_flag",
        "review_required_flag",
        "validated_flag",
        "record_created_at",
    ]

    only_one_observed_direction_df = _lines_with_only_one_observed_direction(
        source_df
    ).select(
        *base_columns,
        "observed_direction_count",
        "distinct_gtfs_direction_count",
        "low_or_none_confidence_share",
        "candidate_confidence_values",
        "audit_rule",
        "audit_message",
    )

    collapsed_direction_df = _both_observed_directions_same_gtfs_direction(
        source_df
    ).select(
        *base_columns,
        "observed_direction_count",
        "distinct_gtfs_direction_count",
        "low_or_none_confidence_share",
        "candidate_confidence_values",
        "audit_rule",
        "audit_message",
    )

    high_low_confidence_share_df = _high_share_low_or_none_confidence(source_df).select(
        *base_columns,
        "observed_direction_count",
        "distinct_gtfs_direction_count",
        "low_or_none_confidence_share",
        "candidate_confidence_values",
        "audit_rule",
        "audit_message",
    )

    extremely_low_sample_df = _extremely_low_sample_size(source_df).select(
        *base_columns,
        "observed_direction_count",
        "distinct_gtfs_direction_count",
        "low_or_none_confidence_share",
        "candidate_confidence_values",
        "audit_rule",
        "audit_message",
    )

    multiple_gtfs_candidates_df = _multiple_gtfs_candidates_per_line(source_df).select(
        *base_columns,
        "observed_direction_count",
        "distinct_gtfs_direction_count",
        "low_or_none_confidence_share",
        "candidate_confidence_values",
        "audit_rule",
        "audit_message",
    )

    audit_df = (
        only_one_observed_direction_df.unionByName(collapsed_direction_df)
        .unionByName(high_low_confidence_share_df)
        .unionByName(extremely_low_sample_df)
        .unionByName(multiple_gtfs_candidates_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
