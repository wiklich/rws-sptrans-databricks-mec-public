"""
Gold planned vs observed line status validation pipeline.

Purpose:
    Validate whether gold_planned_vs_observed_line_status is structurally and
    logically acceptable for downstream use as the canonical detailed comparison
    layer in the Planned vs Observed Service Comparison component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Comparison consistency validation
    E. Reference usability validation

Output:
    - gold.gold_planned_vs_observed_line_status_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_planned_vs_observed_line_status"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_line_status_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_planned_vs_observed_line_status with one row "
        "per validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_planned_vs_observed_line_status_validation() -> DataFrame:
    """
    Materialize the validation results for gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_planned_vs_observed_line_status_validation(validated_df)


def _build_gold_planned_vs_observed_line_status_validation(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_planned_vs_observed_line_status.

    Args:
        validated_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_line_code_not_null(validated_df),
        _validate_line_code_not_blank(validated_df),
        _validate_is_planned_not_null(validated_df),
        _validate_is_observed_not_null(validated_df),
        _validate_match_status_not_null(validated_df),
        _validate_unique_analysis_date_line_code(validated_df),
        _validate_match_status_allowed_domain(validated_df),
        _validate_planned_and_observed_status_consistency(validated_df),
        _validate_planned_not_observed_status_consistency(validated_df),
        _validate_observed_not_in_gtfs_status_consistency(validated_df),
        _validate_planned_route_id_present_when_planned(validated_df),
        _validate_observed_line_sign_present_when_observed(validated_df),
        _validate_no_rows_with_both_flags_false(validated_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_analysis_date_count_greater_than_zero(validated_df),
        _validate_contains_at_least_one_valid_comparison_status(validated_df),
    ]

    return reduce(lambda left, right: left.unionByName(right), validation_dfs)


def _build_validation_result(
    validated_df: DataFrame,
    validation_category: str,
    validation_rule: str,
    validation_description: str,
    severity: str,
    violation_condition,
) -> DataFrame:
    """
    Build a single validation result row.

    Args:
        validated_df:
            DataFrame being validated.
        validation_category:
            Logical category of the validation.
        validation_rule:
            Short rule identifier.
        validation_description:
            Human-readable description of the validation.
        severity:
            Validation severity (ERROR, WARNING, INFO).
        violation_condition:
            Spark boolean column expression identifying violating rows.

    Returns:
        DataFrame:
            A single-row DataFrame with validation result information.
    """
    return (
        validated_df.agg(
            F.sum(F.when(violation_condition, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit(validation_category))
        .withColumn("validation_rule", F.lit(validation_rule))
        .withColumn("validation_description", F.lit(validation_description))
        .withColumn("severity", F.lit(severity))
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )


def _validate_analysis_date_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that analysis_date is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="analysis_date_not_null",
        validation_description="analysis_date must not be null.",
        severity="ERROR",
        violation_condition=F.col("analysis_date").isNull(),
    )


def _validate_line_code_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that line_code is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="line_code_not_null",
        validation_description="line_code must not be null.",
        severity="ERROR",
        violation_condition=F.col("line_code").isNull(),
    )


def _validate_line_code_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that line_code is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="line_code_not_blank",
        validation_description="line_code must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("line_code")) == "",
    )


def _validate_is_planned_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that is_planned is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="is_planned_not_null",
        validation_description="is_planned must not be null.",
        severity="ERROR",
        violation_condition=F.col("is_planned").isNull(),
    )


def _validate_is_observed_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that is_observed is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="is_observed_not_null",
        validation_description="is_observed must not be null.",
        severity="ERROR",
        violation_condition=F.col("is_observed").isNull(),
    )


def _validate_match_status_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that match_status is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="match_status_not_null",
        validation_description="match_status must not be null.",
        severity="ERROR",
        violation_condition=F.col("match_status").isNull(),
    )


def _validate_unique_analysis_date_line_code(validated_df: DataFrame) -> DataFrame:
    """
    Validate that analysis_date + line_code is unique at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy("analysis_date", "line_code")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn("validation_rule", F.lit("unique_analysis_date_line_code"))
        .withColumn(
            "validation_description",
            F.lit("There must be one and only one row per analysis_date and line_code."),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )


def _validate_match_status_allowed_domain(validated_df: DataFrame) -> DataFrame:
    """
    Validate that match_status belongs to the allowed domain.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="match_status_allowed_domain",
        validation_description=(
            "match_status must be one of PLANNED_AND_OBSERVED, "
            "PLANNED_NOT_OBSERVED, or OBSERVED_NOT_IN_GTFS."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("match_status").isNotNull()
            & ~F.col("match_status").isin(
                "PLANNED_AND_OBSERVED",
                "PLANNED_NOT_OBSERVED",
                "OBSERVED_NOT_IN_GTFS",
            )
        ),
    )


def _validate_planned_and_observed_status_consistency(validated_df: DataFrame) -> DataFrame:
    """
    Validate that PLANNED_AND_OBSERVED implies is_planned = TRUE and is_observed = TRUE.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="planned_and_observed_status_consistency",
        validation_description=(
            "PLANNED_AND_OBSERVED must imply is_planned = TRUE and is_observed = TRUE."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("match_status") == "PLANNED_AND_OBSERVED")
            & (
                (F.col("is_planned") != F.lit(True))
                | (F.col("is_observed") != F.lit(True))
            )
        ),
    )


def _validate_planned_not_observed_status_consistency(validated_df: DataFrame) -> DataFrame:
    """
    Validate that PLANNED_NOT_OBSERVED implies is_planned = TRUE and is_observed = FALSE.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="planned_not_observed_status_consistency",
        validation_description=(
            "PLANNED_NOT_OBSERVED must imply is_planned = TRUE and is_observed = FALSE."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("match_status") == "PLANNED_NOT_OBSERVED")
            & (
                (F.col("is_planned") != F.lit(True))
                | (F.col("is_observed") != F.lit(False))
            )
        ),
    )


def _validate_observed_not_in_gtfs_status_consistency(validated_df: DataFrame) -> DataFrame:
    """
    Validate that OBSERVED_NOT_IN_GTFS implies is_planned = FALSE and is_observed = TRUE.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="observed_not_in_gtfs_status_consistency",
        validation_description=(
            "OBSERVED_NOT_IN_GTFS must imply is_planned = FALSE and is_observed = TRUE."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("match_status") == "OBSERVED_NOT_IN_GTFS")
            & (
                (F.col("is_planned") != F.lit(False))
                | (F.col("is_observed") != F.lit(True))
            )
        ),
    )


def _validate_planned_route_id_present_when_planned(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_route_id is populated when is_planned = TRUE.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="COMPARISON_CONSISTENCY",
        validation_rule="planned_route_id_present_when_planned",
        validation_description=(
            "planned_route_id must be populated when is_planned = TRUE."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("is_planned") == F.lit(True))
            & (
                F.col("planned_route_id").isNull()
                | (F.trim(F.col("planned_route_id")) == "")
            )
        ),
    )


def _validate_observed_line_sign_present_when_observed(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_line_sign is populated when is_observed = TRUE.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="COMPARISON_CONSISTENCY",
        validation_rule="observed_line_sign_present_when_observed",
        validation_description=(
            "observed_line_sign must be populated when is_observed = TRUE."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("is_observed") == F.lit(True))
            & (
                F.col("observed_line_sign").isNull()
                | (F.trim(F.col("observed_line_sign")) == "")
            )
        ),
    )


def _validate_no_rows_with_both_flags_false(validated_df: DataFrame) -> DataFrame:
    """
    Validate that rows with is_planned = FALSE and is_observed = FALSE do not exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="COMPARISON_CONSISTENCY",
        validation_rule="no_rows_with_both_flags_false",
        validation_description=(
            "Rows with is_planned = FALSE and is_observed = FALSE must not exist."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("is_planned") == F.lit(False))
            & (F.col("is_observed") == F.lit(False))
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the detailed comparison table is not empty.
    """
    return (
        validated_df.agg(
            F.when(F.count(F.lit(1)) > 0, F.lit(0)).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn("validation_rule", F.lit("reference_table_not_empty"))
        .withColumn(
            "validation_description",
            F.lit("gold_planned_vs_observed_line_status must contain at least one row."),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )


def _validate_analysis_date_count_greater_than_zero(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the detailed comparison table contains at least one analysis_date.
    """
    return (
        validated_df.agg(
            F.when(F.countDistinct("analysis_date") > 0, F.lit(0)).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.countDistinct("analysis_date").cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn("validation_rule", F.lit("analysis_date_count_greater_than_zero"))
        .withColumn(
            "validation_description",
            F.lit("gold_planned_vs_observed_line_status must contain at least one analysis_date."),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )


def _validate_contains_at_least_one_valid_comparison_status(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the table contains at least one valid comparison status.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("match_status").isin(
                            "PLANNED_AND_OBSERVED",
                            "PLANNED_NOT_OBSERVED",
                            "OBSERVED_NOT_IN_GTFS",
                        ),
                        1,
                    )
                ) > 0,
                F.lit(0),
            )
            .otherwise(F.lit(1))
            .cast("long")
            .alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn("validation_rule", F.lit("contains_at_least_one_valid_comparison_status"))
        .withColumn(
            "validation_description",
            F.lit(
                "gold_planned_vs_observed_line_status must contain at least one valid comparison status."
            ),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("validated_table", F.lit(VALIDATED_TABLE_NAME))
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "validation_category",
            "validation_rule",
            "validation_description",
            "severity",
            "validation_status",
            "violation_count",
            "validated_row_count",
            "validated_table",
            "validated_at",
        )
    )
