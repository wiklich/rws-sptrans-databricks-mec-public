"""
Gold planned lines reference validation pipeline.

Purpose:
    Validate whether gold_planned_lines_reference is structurally and logically
    acceptable for downstream use as the canonical planned-side reference in the
    Planned vs Observed Service Comparison component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metadata validation
    E. Reference usability validation

Output:
    - gold.gold_planned_lines_reference_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_planned_lines_reference"
OUTPUT_TABLE_NAME = "gold.gold_planned_lines_reference_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_planned_lines_reference with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_planned_lines_reference_validation() -> DataFrame:
    """
    Materialize the validation results for gold_planned_lines_reference.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_planned_lines_reference_validation(validated_df)


def _build_gold_planned_lines_reference_validation(validated_df: DataFrame) -> DataFrame:
    """
    Build the validation result table for gold_planned_lines_reference.

    Args:
        validated_df:
            Input DataFrame from gold.gold_planned_lines_reference.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_line_code_not_null(validated_df),
        _validate_line_code_not_blank(validated_df),
        _validate_planned_route_id_not_null(validated_df),
        _validate_planned_route_id_not_blank(validated_df),
        _validate_unique_line_code(validated_df),
        _validate_line_code_matches_normalization_rule(validated_df),
        _validate_gtfs_load_date_metadata_only(validated_df),
        _validate_record_created_at_not_null(validated_df),
        _validate_reference_table_not_empty(validated_df),
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
            Spark SQL boolean column expression identifying violating rows.

    Returns:
        DataFrame:
            A single-row DataFrame with validation result information.
    """
    return (
        validated_df.agg(
            F.sum(F.when(violation_condition, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("violation_count"),
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


def _validate_planned_route_id_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_route_id is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="planned_route_id_not_null",
        validation_description="planned_route_id must not be null.",
        severity="ERROR",
        violation_condition=F.col("planned_route_id").isNull(),
    )


def _validate_planned_route_id_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_route_id is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="planned_route_id_not_blank",
        validation_description="planned_route_id must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("planned_route_id")) == "",
    )


def _validate_unique_line_code(validated_df: DataFrame) -> DataFrame:
    """
    Validate that line_code is unique at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy("line_code")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn("validation_rule", F.lit("unique_line_code"))
        .withColumn(
            "validation_description",
            F.lit("There must be one and only one row per line_code."),
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
    UPPER(TRIM(planned_route_id)).
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="BUSINESS_RULE",
        validation_rule="line_code_matches_normalization_rule",
        validation_description=(
            "line_code must equal UPPER(TRIM(planned_route_id))."
        ),
        severity="ERROR",
        violation_condition=F.col("line_code") != F.upper(F.trim(F.col("planned_route_id"))),
    )


def _validate_gtfs_load_date_metadata_only(validated_df: DataFrame) -> DataFrame:
    """
    Validate metadata presence of gtfs_load_date as a contextual field.

    This is a warning-level validation because gtfs_load_date is treated as
    metadata only for this Gold table.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METADATA",
        validation_rule="gtfs_load_date_present_when_available",
        validation_description=(
            "gtfs_load_date should be populated when available upstream, "
            "but it is treated as metadata only in this Gold table."
        ),
        severity="WARNING",
        violation_condition=F.col("gtfs_load_date").isNull(),
    )


def _validate_record_created_at_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that record_created_at is populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METADATA",
        validation_rule="record_created_at_not_null",
        validation_description="record_created_at must be populated for all Gold rows.",
        severity="ERROR",
        violation_condition=F.col("record_created_at").isNull(),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the planned reference table is not empty.
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
            F.lit("gold_planned_lines_reference must contain at least one row."),
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