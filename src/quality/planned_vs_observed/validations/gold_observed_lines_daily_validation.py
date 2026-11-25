"""
Gold observed lines daily validation pipeline.

Purpose:
    Validate whether gold_observed_lines_daily is structurally and logically
    acceptable for downstream use as the canonical observed-side daily reference
    in the Planned vs Observed Service Comparison component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reference usability validation

Output:
    - gold.gold_observed_lines_daily_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_observed_lines_daily"
OUTPUT_TABLE_NAME = "gold.gold_observed_lines_daily_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_observed_lines_daily with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_observed_lines_daily_validation() -> DataFrame:
    """
    Materialize the validation results for gold_observed_lines_daily.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_observed_lines_daily_validation(validated_df)


def _build_gold_observed_lines_daily_validation(validated_df: DataFrame) -> DataFrame:
    """
    Build the validation result table for gold_observed_lines_daily.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_lines_daily.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_line_code_not_null(validated_df),
        _validate_line_code_not_blank(validated_df),
        _validate_observed_line_sign_not_null(validated_df),
        _validate_observed_line_sign_not_blank(validated_df),
        _validate_unique_analysis_date_line_code(validated_df),
        _validate_line_code_matches_normalization_rule(validated_df),
        _validate_observed_events_count_minimum(validated_df),
        _validate_observed_vehicles_count_minimum_when_present(validated_df),
        _validate_captured_at_interval_order(validated_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_analysis_date_count_greater_than_zero(validated_df),
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


def _validate_observed_line_sign_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_line_sign is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_line_sign_not_null",
        validation_description="observed_line_sign must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_line_sign").isNull(),
    )


def _validate_observed_line_sign_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_line_sign is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_line_sign_not_blank",
        validation_description="observed_line_sign must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("observed_line_sign")) == "",
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


def _validate_line_code_matches_normalization_rule(validated_df: DataFrame) -> DataFrame:
    """
    Validate that line_code follows the official normalization rule:
    UPPER(TRIM(observed_line_sign)).
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="line_code_matches_normalization_rule",
        validation_description="line_code must equal UPPER(TRIM(observed_line_sign)).",
        severity="ERROR",
        violation_condition=F.col("line_code") != F.upper(F.trim(F.col("observed_line_sign"))),
    )


def _validate_observed_events_count_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_events_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_events_count_minimum",
        validation_description="observed_events_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=F.col("observed_events_count").isNull() | (F.col("observed_events_count") < 1),
    )


def _validate_observed_vehicles_count_minimum_when_present(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_vehicles_count is at least 1 when populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_vehicles_count_minimum_when_present",
        validation_description=(
            "observed_vehicles_count must be greater than or equal to 1 when populated."
        ),
        severity="ERROR",
        violation_condition=F.col("observed_vehicles_count").isNotNull() & (F.col("observed_vehicles_count") < 1),
    )


def _validate_captured_at_interval_order(validated_df: DataFrame) -> DataFrame:
    """
    Validate that min_captured_at_local is less than or equal to max_captured_at_local
    when both timestamps are populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="captured_at_interval_order",
        validation_description=(
            "min_captured_at_local must be less than or equal to max_captured_at_local "
            "when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("min_captured_at_local").isNotNull()
            & F.col("max_captured_at_local").isNotNull()
            & (F.col("min_captured_at_local") > F.col("max_captured_at_local"))
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the observed daily reference table is not empty.
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
            F.lit("gold_observed_lines_daily must contain at least one row."),
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
    Validate that the observed daily reference contains at least one analysis date.
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
            F.lit("gold_observed_lines_daily must contain at least one analysis_date."),
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
