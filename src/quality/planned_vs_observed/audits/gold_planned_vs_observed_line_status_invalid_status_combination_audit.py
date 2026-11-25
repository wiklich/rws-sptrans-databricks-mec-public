"""
Gold planned vs observed line status invalid status combination audit pipeline.

Purpose:
    Identify cases where boolean flags and status values are inconsistent in
    gold_planned_vs_observed_line_status.

Audit focus:
    - match_status = PLANNED_AND_OBSERVED while is_planned = FALSE or is_observed = FALSE
    - match_status = PLANNED_NOT_OBSERVED while is_planned = FALSE or is_observed = TRUE
    - match_status = OBSERVED_NOT_IN_GTFS while is_planned = TRUE or is_observed = FALSE
    - rows where both flags are FALSE
    - rows with null or unsupported status values

Why this audit matters:
    This audit is useful because status assignment is central to the business
    meaning of the table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_line_status"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_line_status_invalid_status_combination_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying invalid combinations between match_status and "
        "boolean comparison flags in gold_planned_vs_observed_line_status."
    ),
)
def gold_planned_vs_observed_line_status_invalid_status_combination_audit() -> DataFrame:
    """
    Materialize the invalid status combination audit for
    gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Audit rows for inconsistent combinations between match_status,
            is_planned, and is_observed.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_line_status_invalid_status_combination_audit(source_df)


def _build_gold_planned_vs_observed_line_status_invalid_status_combination_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the invalid status combination audit dataset.

    Processing steps:
        1. Read gold_planned_vs_observed_line_status.
        2. Evaluate expected consistency between match_status and boolean flags.
        3. Flag invalid combinations.
        4. Keep only inconsistent rows.

    Args:
        source_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Audit DataFrame containing rows with invalid status combinations.
    """
    audit_df = (
        source_df.withColumn(
            "has_invalid_planned_and_observed_status",
            (
                (F.col("match_status") == "PLANNED_AND_OBSERVED")
                & (
                    (F.col("is_planned") != F.lit(True))
                    | (F.col("is_observed") != F.lit(True))
                )
            ),
        )
        .withColumn(
            "has_invalid_planned_not_observed_status",
            (
                (F.col("match_status") == "PLANNED_NOT_OBSERVED")
                & (
                    (F.col("is_planned") != F.lit(True))
                    | (F.col("is_observed") != F.lit(False))
                )
            ),
        )
        .withColumn(
            "has_invalid_observed_not_in_gtfs_status",
            (
                (F.col("match_status") == "OBSERVED_NOT_IN_GTFS")
                & (
                    (F.col("is_planned") != F.lit(False))
                    | (F.col("is_observed") != F.lit(True))
                )
            ),
        )
        .withColumn(
            "has_both_flags_false",
            (F.col("is_planned") == F.lit(False))
            & (F.col("is_observed") == F.lit(False)),
        )
        .withColumn(
            "has_null_match_status",
            F.col("match_status").isNull(),
        )
        .withColumn(
            "has_unsupported_match_status",
            F.col("match_status").isNotNull()
            & ~F.col("match_status").isin(
                "PLANNED_AND_OBSERVED",
                "PLANNED_NOT_OBSERVED",
                "OBSERVED_NOT_IN_GTFS",
            ),
        )
        .withColumn(
            "has_invalid_status_combination",
            F.col("has_invalid_planned_and_observed_status")
            | F.col("has_invalid_planned_not_observed_status")
            | F.col("has_invalid_observed_not_in_gtfs_status")
            | F.col("has_both_flags_false")
            | F.col("has_null_match_status")
            | F.col("has_unsupported_match_status"),
        )
        .where(F.col("has_invalid_status_combination") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "line_code",
            "planned_route_id",
            "observed_line_sign",
            "is_planned",
            "is_observed",
            "match_status",
            "has_invalid_planned_and_observed_status",
            "has_invalid_planned_not_observed_status",
            "has_invalid_observed_not_in_gtfs_status",
            "has_both_flags_false",
            "has_null_match_status",
            "has_unsupported_match_status",
            "has_invalid_status_combination",
            "record_created_at",
        )
    )

    return audit_df
