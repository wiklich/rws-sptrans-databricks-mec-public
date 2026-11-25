"""
Audit pipeline for conflicting source attributes in gold.gold_direction_mapping_reference.

Purpose:
    Detect cases where multiple records collapse into the same Gold business key
    with conflicting descriptive values.

Declared grain:
    One row per:
        line_sign + observed_direction

Audit scope:
    - same line_sign + observed_direction with multiple gtfs_direction_id values
    - same line_sign + observed_direction with multiple mapping_confidence values

Important:
    This is a non-blocking audit table.
    It exposes conflicting business outcomes for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_direction_mapping_reference"
TABLE_NAME = "gold.gold_direction_mapping_reference_conflicting_source_attributes_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for conflicting source attributes in "
        "gold.gold_direction_mapping_reference. Detects cases where the same "
        "line_sign and observed_direction appear with multiple gtfs_direction_id "
        "values or multiple mapping_confidence values."
    ),
)
def gold_direction_mapping_reference_conflicting_source_attributes_audit() -> DataFrame:
    """
    Create a non-blocking audit table for conflicting descriptive values inside
    the declared Gold business key.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    conflicting_keys_df = (
        source_df.groupBy("line_sign", "observed_direction")
        .agg(
            F.countDistinct("gtfs_direction_id").alias("distinct_gtfs_direction_id_count"),
            F.countDistinct("mapping_confidence").alias("distinct_mapping_confidence_count"),
            F.collect_set("gtfs_direction_id").alias("gtfs_direction_id_values"),
            F.collect_set("mapping_confidence").alias("mapping_confidence_values"),
        )
        .withColumn(
            "conflict_gtfs_direction_id_flag",
            F.col("distinct_gtfs_direction_id_count") > 1,
        )
        .withColumn(
            "conflict_mapping_confidence_flag",
            F.col("distinct_mapping_confidence_count") > 1,
        )
        .filter(
            F.col("conflict_gtfs_direction_id_flag")
            | F.col("conflict_mapping_confidence_flag")
        )
    )

    audit_df = (
        source_df.alias("src")
        .join(
            conflicting_keys_df.alias("conf"),
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
            F.col("conf.distinct_gtfs_direction_id_count").alias(
                "distinct_gtfs_direction_id_count"
            ),
            F.col("conf.distinct_mapping_confidence_count").alias(
                "distinct_mapping_confidence_count"
            ),
            F.col("conf.gtfs_direction_id_values").alias("gtfs_direction_id_values"),
            F.col("conf.mapping_confidence_values").alias(
                "mapping_confidence_values"
            ),
            F.col("conf.conflict_gtfs_direction_id_flag").alias(
                "conflict_gtfs_direction_id_flag"
            ),
            F.col("conf.conflict_mapping_confidence_flag").alias(
                "conflict_mapping_confidence_flag"
            ),
            F.lit("CONFLICTING_SOURCE_ATTRIBUTES").alias("audit_rule"),
            F.when(
                F.col("conf.conflict_gtfs_direction_id_flag")
                & F.col("conf.conflict_mapping_confidence_flag"),
                F.lit(
                    "The same line_sign and observed_direction have multiple "
                    "gtfs_direction_id values and multiple mapping_confidence values."
                ),
            )
            .when(
                F.col("conf.conflict_gtfs_direction_id_flag"),
                F.lit(
                    "The same line_sign and observed_direction have multiple "
                    "gtfs_direction_id values."
                ),
            )
            .otherwise(
                F.lit(
                    "The same line_sign and observed_direction have multiple "
                    "mapping_confidence values."
                )
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
