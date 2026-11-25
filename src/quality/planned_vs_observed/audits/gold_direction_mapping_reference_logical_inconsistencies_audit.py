"""
Audit pipeline for logical inconsistencies in gold.gold_direction_mapping_reference.

Purpose:
    Detect business-logic violations in the Gold direction mapping reference table.

Audit scope:
    - vehicle_count < 0
    - dist_first_km < 0
    - dist_last_km < 0
    - validated_flag = true while review_required_flag = true

Important:
    This is a non-blocking audit table.
    It must expose inconsistent rows for investigation without dropping records
    from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_direction_mapping_reference"
TABLE_NAME = "gold.gold_direction_mapping_reference_logical_inconsistencies_audit"


def _vehicle_count_negative(df: DataFrame) -> DataFrame:
    """
    Detect rows where vehicle_count is negative.
    """
    return (
        df.filter(F.col("vehicle_count") < 0)
        .withColumn("audit_rule", F.lit("VEHICLE_COUNT_NEGATIVE"))
        .withColumn(
            "audit_message",
            F.lit("vehicle_count is negative, which is not logically valid."),
        )
    )


def _dist_first_negative(df: DataFrame) -> DataFrame:
    """
    Detect rows where dist_first_km is negative.
    """
    return (
        df.filter(F.col("dist_first_km") < 0)
        .withColumn("audit_rule", F.lit("DIST_FIRST_KM_NEGATIVE"))
        .withColumn(
            "audit_message",
            F.lit("dist_first_km is negative, which is not logically valid."),
        )
    )


def _dist_last_negative(df: DataFrame) -> DataFrame:
    """
    Detect rows where dist_last_km is negative.
    """
    return (
        df.filter(F.col("dist_last_km") < 0)
        .withColumn("audit_rule", F.lit("DIST_LAST_KM_NEGATIVE"))
        .withColumn(
            "audit_message",
            F.lit("dist_last_km is negative, which is not logically valid."),
        )
    )


def _validated_and_review_required(df: DataFrame) -> DataFrame:
    """
    Detect rows where validated_flag is true while review_required_flag is true.
    """
    return (
        df.filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("review_required_flag") == F.lit(True))
        )
        .withColumn("audit_rule", F.lit("VALIDATED_AND_REVIEW_REQUIRED"))
        .withColumn(
            "audit_message",
            F.lit(
                "validated_flag is true while review_required_flag is also true, "
                "which is logically inconsistent."
            ),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "gold.gold_direction_mapping_reference, including negative distance or "
        "vehicle metrics and contradictory validation flags."
    ),
)
def gold_direction_mapping_reference_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    Gold direction mapping reference.
    """
    source_df = spark.read.table(SOURCE_TABLE)

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

    vehicle_count_negative_df = _vehicle_count_negative(source_df).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    dist_first_negative_df = _dist_first_negative(source_df).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    dist_last_negative_df = _dist_last_negative(source_df).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    validated_and_review_required_df = _validated_and_review_required(source_df).select(
        *base_columns,
        "audit_rule",
        "audit_message",
    )

    audit_df = (
        vehicle_count_negative_df.unionByName(dist_first_negative_df)
        .unionByName(dist_last_negative_df)
        .unionByName(validated_and_review_required_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
