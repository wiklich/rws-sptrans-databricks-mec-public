"""
Audit pipeline for duplicate grain violations in gold.gold_direction_mapping_reference.

Purpose:
    Detect unexpected duplication at the declared Gold grain.

Declared grain:
    One row per:
        line_sign + observed_direction

Audit rule:
    - COUNT(*) > 1 grouped by line_sign, observed_direction

Important:
    This is a non-blocking audit table.
    It exposes duplicate-grain violations for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_direction_mapping_reference"
TABLE_NAME = "gold.gold_direction_mapping_reference_duplicate_grain_violations_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for duplicate grain violations in "
        "gold.gold_direction_mapping_reference. Detects cases where more than "
        "one row exists for the same line_sign and observed_direction."
    ),
)
def gold_direction_mapping_reference_duplicate_grain_violations_audit() -> DataFrame:
    """
    Create a non-blocking audit table for duplicate grain violations in the
    Gold direction mapping reference.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy("line_sign", "observed_direction")
        .agg(F.count(F.lit(1)).alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
    )

    audit_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["line_sign", "observed_direction"],
            how="inner",
        )
        .select(
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.observed_direction").alias("observed_direction"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.evidence_type").alias("evidence_type"),
            F.col("src.evidence_score").alias("evidence_score"),
            F.col("src.mapping_confidence").alias("mapping_confidence"),
            F.col("src.line_category").alias("line_category"),
            F.col("src.dist_first_km").alias("dist_first_km"),
            F.col("src.dist_last_km").alias("dist_last_km"),
            F.col("src.vehicle_count").alias("vehicle_count"),
            F.col("src.low_sample_flag").alias("low_sample_flag"),
            F.col("src.direction_collision_flag").alias("direction_collision_flag"),
            F.col("src.review_required_flag").alias("review_required_flag"),
            F.col("src.validated_flag").alias("validated_flag"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("dup.duplicate_count").alias("duplicate_count"),
            F.lit("DUPLICATE_GRAIN_VIOLATION").alias("audit_rule"),
            F.lit(
                "More than one row exists for the same line_sign and "
                "observed_direction, which violates the declared Gold grain."
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
