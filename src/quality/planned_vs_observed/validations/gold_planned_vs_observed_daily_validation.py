"""
Gold planned vs observed daily validation pipeline.

Purpose:
    Validate whether gold_planned_vs_observed_daily is structurally and
    logically acceptable for downstream use as the canonical daily summary KPI
    layer in the Planned vs Observed Service Comparison component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Reconciliation validation
    E. Reference usability validation

Output:
    - gold.gold_planned_vs_observed_daily_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_planned_vs_observed_daily"
DETAILED_SOURCE_TABLE = "main.gold.gold_planned_vs_observed_line_status"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_daily_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_planned_vs_observed_daily with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_planned_vs_observed_daily_validation() -> DataFrame:
    """
    Materialize the validation results for gold_planned_vs_observed_daily.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    detailed_df = spark.read.table(DETAILED_SOURCE_TABLE)

    return _build_gold_planned_vs_observed_daily_validation(
        validated_df=validated_df,
        detailed_df=detailed_df,
    )


def _build_gold_planned_vs_observed_daily_validation(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_planned_vs_observed_daily.

    Args:
        validated_df:
            Input DataFrame from gold.gold_planned_vs_observed_daily.
        detailed_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_planned_lines_count_not_null(validated_df),
        _validate_observed_lines_count_not_null(validated_df),
        _validate_planned_lines_observed_count_not_null(validated_df),
        _validate_planned_lines_not_observed_count_not_null(validated_df),
        _validate_observed_lines_not_in_gtfs_count_not_null(validated_df),
        _validate_unique_analysis_date(validated_df),
        _validate_planned_lines_count_non_negative(validated_df),
        _validate_observed_lines_count_non_negative(validated_df),
        _validate_planned_lines_observed_count_non_negative(validated_df),
        _validate_planned_lines_not_observed_count_non_negative(validated_df),
        _validate_observed_lines_not_in_gtfs_count_non_negative(validated_df),
        _validate_planned_lines_observed_pct_range(validated_df),
        _validate_planned_lines_observed_pct_formula(validated_df),
        _validate_planned_identity_reconciliation(validated_df),
        _validate_observed_identity_reconciliation(validated_df),
        _validate_summary_matches_detailed_planned_lines_count(validated_df, detailed_df),
        _validate_summary_matches_detailed_observed_lines_count(validated_df, detailed_df),
        _validate_summary_matches_detailed_planned_lines_observed_count(validated_df, detailed_df),
        _validate_summary_matches_detailed_planned_lines_not_observed_count(validated_df, detailed_df),
        _validate_summary_matches_detailed_observed_lines_not_in_gtfs_count(validated_df, detailed_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_analysis_date_count_greater_than_zero(validated_df),
        _validate_contains_at_least_one_valid_kpi_row(validated_df),
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
    Build a single validation result row from a row-level condition.

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


def _build_joined_summary_vs_detailed_metrics(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Build a joined DataFrame containing summary metrics and the corresponding
    metrics re-derived from the detailed comparison table.

    Args:
        validated_df:
            Input DataFrame from gold.gold_planned_vs_observed_daily.
        detailed_df:
            Input DataFrame from gold.gold_planned_vs_observed_line_status.

    Returns:
        DataFrame:
            One row per analysis_date with summary metrics and detailed-derived metrics.
    """
    detailed_aggregated_df = (
        detailed_df.groupBy("analysis_date")
        .agg(
            F.count(F.when(F.col("is_planned") == True, 1)).cast("long").alias(
                "expected_planned_lines_count"
            ),
            F.count(F.when(F.col("is_observed") == True, 1)).cast("long").alias(
                "expected_observed_lines_count"
            ),
            F.count(
                F.when(F.col("match_status") == "PLANNED_AND_OBSERVED", 1)
            ).cast("long").alias("expected_planned_lines_observed_count"),
            F.count(
                F.when(F.col("match_status") == "PLANNED_NOT_OBSERVED", 1)
            ).cast("long").alias("expected_planned_lines_not_observed_count"),
            F.count(
                F.when(F.col("match_status") == "OBSERVED_NOT_IN_GTFS", 1)
            ).cast("long").alias("expected_observed_lines_not_in_gtfs_count"),
        )
    )

    return (
        validated_df.alias("summary")
        .join(
            detailed_aggregated_df.alias("detailed"),
            on="analysis_date",
            how="left",
        )
    )


def _build_validation_result_from_joined_metrics(
    joined_df: DataFrame,
    validation_rule: str,
    validation_description: str,
    violation_condition,
) -> DataFrame:
    """
    Build a validation result row from a joined summary-versus-detailed DataFrame.

    Args:
        joined_df:
            DataFrame containing summary metrics and detailed-derived metrics.
        validation_rule:
            Short rule identifier.
        validation_description:
            Human-readable description of the validation.
        violation_condition:
            Spark boolean column expression identifying violating rows.

    Returns:
        DataFrame:
            A single-row DataFrame with validation result information.
    """
    return (
        joined_df.agg(
            F.sum(F.when(violation_condition, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("RECONCILIATION"))
        .withColumn("validation_rule", F.lit(validation_rule))
        .withColumn("validation_description", F.lit(validation_description))
        .withColumn("severity", F.lit("ERROR"))
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


def _validate_planned_lines_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="planned_lines_count_not_null",
        validation_description="planned_lines_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("planned_lines_count").isNull(),
    )


def _validate_observed_lines_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_lines_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_lines_count_not_null",
        validation_description="observed_lines_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_lines_count").isNull(),
    )


def _validate_planned_lines_observed_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_observed_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="planned_lines_observed_count_not_null",
        validation_description="planned_lines_observed_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("planned_lines_observed_count").isNull(),
    )


def _validate_planned_lines_not_observed_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_not_observed_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="planned_lines_not_observed_count_not_null",
        validation_description="planned_lines_not_observed_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("planned_lines_not_observed_count").isNull(),
    )


def _validate_observed_lines_not_in_gtfs_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_lines_not_in_gtfs_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_lines_not_in_gtfs_count_not_null",
        validation_description="observed_lines_not_in_gtfs_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_lines_not_in_gtfs_count").isNull(),
    )


def _validate_unique_analysis_date(validated_df: DataFrame) -> DataFrame:
    """
    Validate that analysis_date is unique at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy("analysis_date")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn("validation_rule", F.lit("unique_analysis_date"))
        .withColumn(
            "validation_description",
            F.lit("There must be one and only one row per analysis_date."),
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


def _validate_planned_lines_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="planned_lines_count_non_negative",
        validation_description="planned_lines_count must be greater than or equal to zero.",
        severity="ERROR",
        violation_condition=F.col("planned_lines_count") < 0,
    )


def _validate_observed_lines_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_lines_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_lines_count_non_negative",
        validation_description="observed_lines_count must be greater than or equal to zero.",
        severity="ERROR",
        violation_condition=F.col("observed_lines_count") < 0,
    )


def _validate_planned_lines_observed_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_observed_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="planned_lines_observed_count_non_negative",
        validation_description="planned_lines_observed_count must be greater than or equal to zero.",
        severity="ERROR",
        violation_condition=F.col("planned_lines_observed_count") < 0,
    )


def _validate_planned_lines_not_observed_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_not_observed_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="planned_lines_not_observed_count_non_negative",
        validation_description="planned_lines_not_observed_count must be greater than or equal to zero.",
        severity="ERROR",
        violation_condition=F.col("planned_lines_not_observed_count") < 0,
    )


def _validate_observed_lines_not_in_gtfs_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_lines_not_in_gtfs_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_lines_not_in_gtfs_count_non_negative",
        validation_description="observed_lines_not_in_gtfs_count must be greater than or equal to zero.",
        severity="ERROR",
        violation_condition=F.col("observed_lines_not_in_gtfs_count") < 0,
    )


def _validate_planned_lines_observed_pct_range(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_observed_pct is between 0 and 100 when populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="planned_lines_observed_pct_range",
        validation_description=(
            "planned_lines_observed_pct must be between 0 and 100 when populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("planned_lines_observed_pct").isNotNull()
            & (
                (F.col("planned_lines_observed_pct") < 0.0)
                | (F.col("planned_lines_observed_pct") > 100.0)
            )
        ),
    )


def _validate_planned_lines_observed_pct_formula(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_observed_pct follows the expected formula with
    division-by-zero protection.
    """
    expected_pct = F.when(
        F.col("planned_lines_count") > 0,
        F.round(
            (F.col("planned_lines_observed_count") / F.col("planned_lines_count")) * F.lit(100.0),
            2,
        ),
    ).otherwise(F.lit(None))

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="planned_lines_observed_pct_formula",
        validation_description=(
            "planned_lines_observed_pct must match the expected formula "
            "planned_lines_observed_count / planned_lines_count * 100, "
            "using division-by-zero protection."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("planned_lines_count") > 0)
            & (F.col("planned_lines_observed_pct") != expected_pct)
        )
        | (
            (F.col("planned_lines_count") <= 0)
            & F.col("planned_lines_observed_pct").isNotNull()
        ),
    )


def _validate_planned_identity_reconciliation(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_observed_count + planned_lines_not_observed_count
    equals planned_lines_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="RECONCILIATION",
        validation_rule="planned_identity_reconciliation",
        validation_description=(
            "planned_lines_observed_count + planned_lines_not_observed_count "
            "must equal planned_lines_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("planned_lines_observed_count") + F.col("planned_lines_not_observed_count")
        ) != F.col("planned_lines_count"),
    )


def _validate_observed_identity_reconciliation(validated_df: DataFrame) -> DataFrame:
    """
    Validate that planned_lines_observed_count + observed_lines_not_in_gtfs_count
    equals observed_lines_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="RECONCILIATION",
        validation_rule="observed_identity_reconciliation",
        validation_description=(
            "planned_lines_observed_count + observed_lines_not_in_gtfs_count "
            "must equal observed_lines_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("planned_lines_observed_count") + F.col("observed_lines_not_in_gtfs_count")
        ) != F.col("observed_lines_count"),
    )


def _validate_summary_matches_detailed_planned_lines_count(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Validate that summary planned_lines_count matches the detailed-table derivation.
    """
    joined_df = _build_joined_summary_vs_detailed_metrics(validated_df, detailed_df)

    return _build_validation_result_from_joined_metrics(
        joined_df=joined_df,
        validation_rule="summary_matches_detailed_planned_lines_count",
        validation_description=(
            "planned_lines_count must match the count derived from "
            "gold_planned_vs_observed_line_status where is_planned = TRUE."
        ),
        violation_condition=(
            F.col("expected_planned_lines_count").isNull()
            | (F.col("planned_lines_count") != F.col("expected_planned_lines_count"))
        ),
    )


def _validate_summary_matches_detailed_observed_lines_count(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Validate that summary observed_lines_count matches the detailed-table derivation.
    """
    joined_df = _build_joined_summary_vs_detailed_metrics(validated_df, detailed_df)

    return _build_validation_result_from_joined_metrics(
        joined_df=joined_df,
        validation_rule="summary_matches_detailed_observed_lines_count",
        validation_description=(
            "observed_lines_count must match the count derived from "
            "gold_planned_vs_observed_line_status where is_observed = TRUE."
        ),
        violation_condition=(
            F.col("expected_observed_lines_count").isNull()
            | (F.col("observed_lines_count") != F.col("expected_observed_lines_count"))
        ),
    )


def _validate_summary_matches_detailed_planned_lines_observed_count(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Validate that summary planned_lines_observed_count matches the detailed-table derivation.
    """
    joined_df = _build_joined_summary_vs_detailed_metrics(validated_df, detailed_df)

    return _build_validation_result_from_joined_metrics(
        joined_df=joined_df,
        validation_rule="summary_matches_detailed_planned_lines_observed_count",
        validation_description=(
            "planned_lines_observed_count must match the count derived from "
            "gold_planned_vs_observed_line_status where "
            "match_status = 'PLANNED_AND_OBSERVED'."
        ),
        violation_condition=(
            F.col("expected_planned_lines_observed_count").isNull()
            | (
                F.col("planned_lines_observed_count")
                != F.col("expected_planned_lines_observed_count")
            )
        ),
    )


def _validate_summary_matches_detailed_planned_lines_not_observed_count(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Validate that summary planned_lines_not_observed_count matches the
    detailed-table derivation.
    """
    joined_df = _build_joined_summary_vs_detailed_metrics(validated_df, detailed_df)

    return _build_validation_result_from_joined_metrics(
        joined_df=joined_df,
        validation_rule="summary_matches_detailed_planned_lines_not_observed_count",
        validation_description=(
            "planned_lines_not_observed_count must match the count derived from "
            "gold_planned_vs_observed_line_status where "
            "match_status = 'PLANNED_NOT_OBSERVED'."
        ),
        violation_condition=(
            F.col("expected_planned_lines_not_observed_count").isNull()
            | (
                F.col("planned_lines_not_observed_count")
                != F.col("expected_planned_lines_not_observed_count")
            )
        ),
    )


def _validate_summary_matches_detailed_observed_lines_not_in_gtfs_count(
    validated_df: DataFrame,
    detailed_df: DataFrame,
) -> DataFrame:
    """
    Validate that summary observed_lines_not_in_gtfs_count matches the
    detailed-table derivation.
    """
    joined_df = _build_joined_summary_vs_detailed_metrics(validated_df, detailed_df)

    return _build_validation_result_from_joined_metrics(
        joined_df=joined_df,
        validation_rule="summary_matches_detailed_observed_lines_not_in_gtfs_count",
        validation_description=(
            "observed_lines_not_in_gtfs_count must match the count derived from "
            "gold_planned_vs_observed_line_status where "
            "match_status = 'OBSERVED_NOT_IN_GTFS'."
        ),
        violation_condition=(
            F.col("expected_observed_lines_not_in_gtfs_count").isNull()
            | (
                F.col("observed_lines_not_in_gtfs_count")
                != F.col("expected_observed_lines_not_in_gtfs_count")
            )
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the daily summary table is not empty.
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
            F.lit("gold_planned_vs_observed_daily must contain at least one row."),
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
    Validate that the daily summary table contains at least one analysis_date.
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
            F.lit("gold_planned_vs_observed_daily must contain at least one analysis_date."),
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


def _validate_contains_at_least_one_valid_kpi_row(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the table contains at least one valid KPI row.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("analysis_date").isNotNull()
                        & F.col("planned_lines_count").isNotNull()
                        & F.col("observed_lines_count").isNotNull()
                        & F.col("planned_lines_observed_count").isNotNull()
                        & F.col("planned_lines_not_observed_count").isNotNull()
                        & F.col("observed_lines_not_in_gtfs_count").isNotNull(),
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
        .withColumn("validation_rule", F.lit("contains_at_least_one_valid_kpi_row"))
        .withColumn(
            "validation_description",
            F.lit(
                "gold_planned_vs_observed_daily must contain at least one valid KPI row."
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
