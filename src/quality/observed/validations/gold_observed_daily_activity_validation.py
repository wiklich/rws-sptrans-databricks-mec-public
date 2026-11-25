"""
Gold observed daily activity validation pipeline.

Purpose:
    Validate whether gold_observed_daily_activity is structurally and
    logically acceptable for downstream use as the canonical daily observed
    activity layer in the Observed Temporal Activity Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Temporal consistency validation
    E. Reference usability validation

Output:
    - gold.gold_observed_daily_activity_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_observed_daily_activity"
OUTPUT_TABLE_NAME = "gold.gold_observed_daily_activity_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_observed_daily_activity with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_observed_daily_activity_validation() -> DataFrame:
    """
    Materialize the validation results for gold_observed_daily_activity.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_observed_daily_activity_validation(validated_df)


def _build_gold_observed_daily_activity_validation(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_observed_daily_activity.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_daily_activity.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_observed_position_records_count_not_null(validated_df),
        _validate_distinct_observed_vehicles_count_not_null(validated_df),
        _validate_distinct_observed_lines_count_not_null(validated_df),
        _validate_distinct_observed_hours_count_not_null(validated_df),
        _validate_distinct_observed_minutes_count_not_null(validated_df),
        _validate_unique_analysis_date(validated_df),
        _validate_observed_position_records_count_minimum(validated_df),
        _validate_distinct_observed_vehicles_count_non_negative(validated_df),
        _validate_distinct_observed_lines_count_non_negative(validated_df),
        _validate_distinct_observed_hours_count_minimum(validated_df),
        _validate_distinct_observed_minutes_count_minimum(validated_df),
        _validate_captured_at_interval_order(validated_df),
        _validate_min_captured_at_local_matches_analysis_date(validated_df),
        _validate_max_captured_at_local_matches_analysis_date(validated_df),
        _validate_observed_hours_count_compatible_with_timestamp_bounds(validated_df),
        _validate_observed_minutes_count_compatible_with_timestamp_bounds(validated_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_analysis_date_count_greater_than_zero(validated_df),
        _validate_contains_at_least_one_valid_daily_activity_row(validated_df),
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


def _validate_observed_position_records_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that observed_position_records_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_position_records_count_not_null",
        validation_description="observed_position_records_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_position_records_count").isNull(),
    )


def _validate_distinct_observed_vehicles_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_vehicles_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_observed_vehicles_count_not_null",
        validation_description="distinct_observed_vehicles_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_observed_vehicles_count").isNull(),
    )


def _validate_distinct_observed_lines_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_lines_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_observed_lines_count_not_null",
        validation_description="distinct_observed_lines_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_observed_lines_count").isNull(),
    )


def _validate_distinct_observed_hours_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_hours_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_observed_hours_count_not_null",
        validation_description="distinct_observed_hours_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_observed_hours_count").isNull(),
    )


def _validate_distinct_observed_minutes_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_minutes_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_observed_minutes_count_not_null",
        validation_description="distinct_observed_minutes_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_observed_minutes_count").isNull(),
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


def _validate_observed_position_records_count_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that observed_position_records_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_position_records_count_minimum",
        validation_description=(
            "observed_position_records_count must be greater than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("observed_position_records_count").isNull()
            | (F.col("observed_position_records_count") < 1)
        ),
    )


def _validate_distinct_observed_vehicles_count_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_vehicles_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_observed_vehicles_count_non_negative",
        validation_description=(
            "distinct_observed_vehicles_count must be greater than or equal to zero."
        ),
        severity="ERROR",
        violation_condition=F.col("distinct_observed_vehicles_count") < 0,
    )


def _validate_distinct_observed_lines_count_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_lines_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_observed_lines_count_non_negative",
        validation_description=(
            "distinct_observed_lines_count must be greater than or equal to zero."
        ),
        severity="ERROR",
        violation_condition=F.col("distinct_observed_lines_count") < 0,
    )


def _validate_distinct_observed_hours_count_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_hours_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_observed_hours_count_minimum",
        validation_description="distinct_observed_hours_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("distinct_observed_hours_count").isNull()
            | (F.col("distinct_observed_hours_count") < 1)
        ),
    )


def _validate_distinct_observed_minutes_count_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_minutes_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_observed_minutes_count_minimum",
        validation_description="distinct_observed_minutes_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("distinct_observed_minutes_count").isNull()
            | (F.col("distinct_observed_minutes_count") < 1)
        ),
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


def _validate_min_captured_at_local_matches_analysis_date(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that min_captured_at_local belongs to analysis_date when populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="min_captured_at_local_matches_analysis_date",
        validation_description=(
            "min_captured_at_local must belong to analysis_date when populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("min_captured_at_local").isNotNull()
            & (F.to_date(F.col("min_captured_at_local")) != F.col("analysis_date"))
        ),
    )


def _validate_max_captured_at_local_matches_analysis_date(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_captured_at_local belongs to analysis_date when populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="max_captured_at_local_matches_analysis_date",
        validation_description=(
            "max_captured_at_local must belong to analysis_date when populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_captured_at_local").isNotNull()
            & (F.to_date(F.col("max_captured_at_local")) != F.col("analysis_date"))
        ),
    )


def _validate_observed_hours_count_compatible_with_timestamp_bounds(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_hours_count is logically compatible with
    the timestamp bounds of the day.
    """
    inclusive_hour_span = (
        F.when(
            F.col("min_captured_at_local").isNotNull()
            & F.col("max_captured_at_local").isNotNull(),
            (
                F.hour(F.col("max_captured_at_local"))
                - F.hour(F.col("min_captured_at_local"))
                + F.lit(1)
            ),
        ).otherwise(F.lit(None))
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="observed_hours_count_compatible_with_timestamp_bounds",
        validation_description=(
            "distinct_observed_hours_count must be less than or equal to the inclusive "
            "hour span between min_captured_at_local and max_captured_at_local."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("min_captured_at_local").isNotNull()
            & F.col("max_captured_at_local").isNotNull()
            & (
                F.col("distinct_observed_hours_count")
                > inclusive_hour_span
            )
        ),
    )


def _validate_observed_minutes_count_compatible_with_timestamp_bounds(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_observed_minutes_count is logically compatible with
    the timestamp bounds of the day.
    """
    inclusive_minute_span = (
        F.when(
            F.col("min_captured_at_local").isNotNull()
            & F.col("max_captured_at_local").isNotNull(),
            (
                F.floor(
                    (
                        F.unix_timestamp(F.col("max_captured_at_local"))
                        - F.unix_timestamp(F.col("min_captured_at_local"))
                    ) / F.lit(60)
                ) + F.lit(1)
            ),
        ).otherwise(F.lit(None))
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="observed_minutes_count_compatible_with_timestamp_bounds",
        validation_description=(
            "distinct_observed_minutes_count must be less than or equal to the inclusive "
            "minute span between min_captured_at_local and max_captured_at_local."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("min_captured_at_local").isNotNull()
            & F.col("max_captured_at_local").isNotNull()
            & (
                F.col("distinct_observed_minutes_count")
                > inclusive_minute_span
            )
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the daily activity table is not empty.
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
            F.lit("gold_observed_daily_activity must contain at least one row."),
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
    Validate that the daily activity table contains at least one analysis_date.
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
            F.lit("gold_observed_daily_activity must contain at least one analysis_date."),
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


def _validate_contains_at_least_one_valid_daily_activity_row(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one valid daily activity row.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("analysis_date").isNotNull()
                        & F.col("observed_position_records_count").isNotNull()
                        & F.col("distinct_observed_vehicles_count").isNotNull()
                        & F.col("distinct_observed_lines_count").isNotNull()
                        & F.col("distinct_observed_hours_count").isNotNull()
                        & F.col("distinct_observed_minutes_count").isNotNull(),
                        1,
                    )
                ) > 0,
                F.lit(0),
            ).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_valid_daily_activity_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_observed_daily_activity must contain at least one valid daily activity row."
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
