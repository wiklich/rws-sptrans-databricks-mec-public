"""
Gold vehicle multi-day continuity validation pipeline.

Purpose:
    Validate whether gold_vehicle_multi_day_continuity is structurally and
    logically acceptable for downstream use as the canonical vehicle multi-day
    continuity layer in the Observed Update Frequency and Continuity Analysis
    component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Temporal consistency validation
    E. Reference usability validation

Output:
    - gold.gold_vehicle_multi_day_continuity_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_vehicle_multi_day_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_multi_day_continuity_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_vehicle_multi_day_continuity with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_vehicle_multi_day_continuity_validation() -> DataFrame:
    """
    Materialize the validation results for gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_vehicle_multi_day_continuity_validation(validated_df)


def _build_gold_vehicle_multi_day_continuity_validation(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_vehicle_multi_day_continuity.

    Args:
        validated_df:
            Input DataFrame from gold.gold_vehicle_multi_day_continuity.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_vehicle_prefix_not_null(validated_df),
        _validate_vehicle_prefix_not_blank(validated_df),
        _validate_observed_dates_count_not_null(validated_df),
        _validate_first_observed_date_not_null(validated_df),
        _validate_last_observed_date_not_null(validated_df),
        _validate_observed_date_span_days_not_null(validated_df),
        _validate_missing_dates_within_span_count_not_null(validated_df),
        _validate_date_presence_ratio_not_null(validated_df),
        _validate_total_observations_count_not_null(validated_df),
        _validate_avg_observations_per_observed_date_not_null(validated_df),
        _validate_max_consecutive_observed_dates_not_null(validated_df),
        _validate_max_consecutive_missing_dates_within_span_not_null(validated_df),
        _validate_unique_vehicle_prefix(validated_df),
        _validate_observed_dates_count_minimum(validated_df),
        _validate_observed_date_span_days_minimum(validated_df),
        _validate_missing_dates_within_span_count_non_negative(validated_df),
        _validate_date_presence_ratio_between_zero_and_one(validated_df),
        _validate_total_observations_count_greater_than_or_equal_to_observed_dates_count(
            validated_df
        ),
        _validate_avg_observations_per_observed_date_positive(validated_df),
        _validate_max_consecutive_observed_dates_minimum(validated_df),
        _validate_max_consecutive_missing_dates_within_span_non_negative(validated_df),
        _validate_first_observed_date_less_than_or_equal_to_last_observed_date(
            validated_df
        ),
        _validate_observed_date_span_days_matches_date_bounds(validated_df),
        _validate_missing_dates_within_span_count_matches_span_and_observed_dates_count(
            validated_df
        ),
        _validate_first_captured_at_local_less_than_or_equal_to_last_captured_at_local(
            validated_df
        ),
        _validate_max_consecutive_observed_dates_compatible_with_span(validated_df),
        _validate_max_consecutive_missing_dates_compatible_with_missing_dates_count(
            validated_df
        ),
        _validate_reference_table_not_empty(validated_df),
        _validate_contains_at_least_one_vehicle_observed_on_multiple_dates(validated_df),
        _validate_contains_at_least_one_valid_multi_day_continuity_row(validated_df),
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
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
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


def _validate_vehicle_prefix_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that vehicle_prefix is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="vehicle_prefix_not_null",
        validation_description="vehicle_prefix must not be null.",
        severity="ERROR",
        violation_condition=F.col("vehicle_prefix").isNull(),
    )


def _validate_vehicle_prefix_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that vehicle_prefix is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="vehicle_prefix_not_blank",
        validation_description="vehicle_prefix must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("vehicle_prefix")) == "",
    )


def _validate_observed_dates_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_dates_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_dates_count_not_null",
        validation_description="observed_dates_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_dates_count").isNull(),
    )


def _validate_first_observed_date_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that first_observed_date is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="first_observed_date_not_null",
        validation_description="first_observed_date must not be null.",
        severity="ERROR",
        violation_condition=F.col("first_observed_date").isNull(),
    )


def _validate_last_observed_date_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that last_observed_date is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="last_observed_date_not_null",
        validation_description="last_observed_date must not be null.",
        severity="ERROR",
        violation_condition=F.col("last_observed_date").isNull(),
    )


def _validate_observed_date_span_days_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_date_span_days is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_date_span_days_not_null",
        validation_description="observed_date_span_days must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_date_span_days").isNull(),
    )


def _validate_missing_dates_within_span_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that missing_dates_within_span_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="missing_dates_within_span_count_not_null",
        validation_description="missing_dates_within_span_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("missing_dates_within_span_count").isNull(),
    )


def _validate_date_presence_ratio_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that date_presence_ratio is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="date_presence_ratio_not_null",
        validation_description="date_presence_ratio must not be null.",
        severity="ERROR",
        violation_condition=F.col("date_presence_ratio").isNull(),
    )


def _validate_total_observations_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that total_observations_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="total_observations_count_not_null",
        validation_description="total_observations_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("total_observations_count").isNull(),
    )


def _validate_avg_observations_per_observed_date_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_observations_per_observed_date is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="avg_observations_per_observed_date_not_null",
        validation_description="avg_observations_per_observed_date must not be null.",
        severity="ERROR",
        violation_condition=F.col("avg_observations_per_observed_date").isNull(),
    )


def _validate_max_consecutive_observed_dates_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_consecutive_observed_dates is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="max_consecutive_observed_dates_not_null",
        validation_description="max_consecutive_observed_dates must not be null.",
        severity="ERROR",
        violation_condition=F.col("max_consecutive_observed_dates").isNull(),
    )


def _validate_max_consecutive_missing_dates_within_span_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_consecutive_missing_dates_within_span is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="max_consecutive_missing_dates_within_span_not_null",
        validation_description=(
            "max_consecutive_missing_dates_within_span must not be null."
        ),
        severity="ERROR",
        violation_condition=F.col("max_consecutive_missing_dates_within_span").isNull(),
    )


def _validate_unique_vehicle_prefix(validated_df: DataFrame) -> DataFrame:
    """
    Validate that vehicle_prefix is unique at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy("vehicle_prefix")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn("validation_rule", F.lit("unique_vehicle_prefix"))
        .withColumn(
            "validation_description",
            F.lit("There must be one and only one row per vehicle_prefix."),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
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


def _validate_observed_dates_count_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_dates_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_dates_count_minimum",
        validation_description="observed_dates_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("observed_dates_count").isNull()
            | (F.col("observed_dates_count") < 1)
        ),
    )


def _validate_observed_date_span_days_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_date_span_days is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_date_span_days_minimum",
        validation_description="observed_date_span_days must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("observed_date_span_days").isNull()
            | (F.col("observed_date_span_days") < 1)
        ),
    )


def _validate_missing_dates_within_span_count_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that missing_dates_within_span_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="missing_dates_within_span_count_non_negative",
        validation_description=(
            "missing_dates_within_span_count must be greater than or equal to 0."
        ),
        severity="ERROR",
        violation_condition=F.col("missing_dates_within_span_count") < 0,
    )


def _validate_date_presence_ratio_between_zero_and_one(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that date_presence_ratio is between 0 and 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="date_presence_ratio_between_zero_and_one",
        validation_description="date_presence_ratio must be between 0 and 1 inclusive.",
        severity="ERROR",
        violation_condition=(
            F.col("date_presence_ratio").isNull()
            | (F.col("date_presence_ratio") < 0)
            | (F.col("date_presence_ratio") > 1)
        ),
    )


def _validate_total_observations_count_greater_than_or_equal_to_observed_dates_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_observations_count is greater than or equal to observed_dates_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="total_observations_count_greater_than_or_equal_to_observed_dates_count",
        validation_description=(
            "total_observations_count must be greater than or equal to observed_dates_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_observations_count").isNull()
            | F.col("observed_dates_count").isNull()
            | (F.col("total_observations_count") < F.col("observed_dates_count"))
        ),
    )


def _validate_avg_observations_per_observed_date_positive(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_observations_per_observed_date is positive.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="avg_observations_per_observed_date_positive",
        validation_description=(
            "avg_observations_per_observed_date must be greater than 0."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("avg_observations_per_observed_date").isNull()
            | (F.col("avg_observations_per_observed_date") <= 0)
        ),
    )


def _validate_max_consecutive_observed_dates_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_consecutive_observed_dates is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_consecutive_observed_dates_minimum",
        validation_description=(
            "max_consecutive_observed_dates must be greater than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_consecutive_observed_dates").isNull()
            | (F.col("max_consecutive_observed_dates") < 1)
        ),
    )


def _validate_max_consecutive_missing_dates_within_span_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_consecutive_missing_dates_within_span is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_consecutive_missing_dates_within_span_non_negative",
        validation_description=(
            "max_consecutive_missing_dates_within_span must be greater than or equal to 0."
        ),
        severity="ERROR",
        violation_condition=F.col("max_consecutive_missing_dates_within_span") < 0,
    )


def _validate_first_observed_date_less_than_or_equal_to_last_observed_date(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that first_observed_date is less than or equal to last_observed_date.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="first_observed_date_less_than_or_equal_to_last_observed_date",
        validation_description=(
            "first_observed_date must be less than or equal to last_observed_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("first_observed_date").isNotNull()
            & F.col("last_observed_date").isNotNull()
            & (F.col("first_observed_date") > F.col("last_observed_date"))
        ),
    )


def _validate_observed_date_span_days_matches_date_bounds(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that observed_date_span_days matches the inclusive date span.
    """
    expected_span_days = (
        F.datediff(F.col("last_observed_date"), F.col("first_observed_date"))
        + F.lit(1)
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="observed_date_span_days_matches_date_bounds",
        validation_description=(
            "observed_date_span_days must match the inclusive day span between "
            "first_observed_date and last_observed_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("first_observed_date").isNotNull()
            & F.col("last_observed_date").isNotNull()
            & F.col("observed_date_span_days").isNotNull()
            & (F.col("observed_date_span_days") != expected_span_days)
        ),
    )


def _validate_missing_dates_within_span_count_matches_span_and_observed_dates_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that missing_dates_within_span_count matches span minus observed dates count.
    """
    expected_missing_dates = (
        F.col("observed_date_span_days") - F.col("observed_dates_count")
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="missing_dates_within_span_count_matches_span_and_observed_dates_count",
        validation_description=(
            "missing_dates_within_span_count must equal observed_date_span_days "
            "minus observed_dates_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("observed_date_span_days").isNotNull()
            & F.col("observed_dates_count").isNotNull()
            & F.col("missing_dates_within_span_count").isNotNull()
            & (F.col("missing_dates_within_span_count") != expected_missing_dates)
        ),
    )


def _validate_first_captured_at_local_less_than_or_equal_to_last_captured_at_local(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that first_captured_at_local is less than or equal to last_captured_at_local.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="first_captured_at_local_less_than_or_equal_to_last_captured_at_local",
        validation_description=(
            "first_captured_at_local must be less than or equal to "
            "last_captured_at_local when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("first_captured_at_local").isNotNull()
            & F.col("last_captured_at_local").isNotNull()
            & (F.col("first_captured_at_local") > F.col("last_captured_at_local"))
        ),
    )


def _validate_max_consecutive_observed_dates_compatible_with_span(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_consecutive_observed_dates is less than or equal to observed_date_span_days.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="max_consecutive_observed_dates_compatible_with_span",
        validation_description=(
            "max_consecutive_observed_dates must be less than or equal to observed_date_span_days."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_consecutive_observed_dates").isNotNull()
            & F.col("observed_date_span_days").isNotNull()
            & (F.col("max_consecutive_observed_dates") > F.col("observed_date_span_days"))
        ),
    )


def _validate_max_consecutive_missing_dates_compatible_with_missing_dates_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_consecutive_missing_dates_within_span is less than or equal to missing_dates_within_span_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="max_consecutive_missing_dates_compatible_with_missing_dates_count",
        validation_description=(
            "max_consecutive_missing_dates_within_span must be less than or equal to "
            "missing_dates_within_span_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_consecutive_missing_dates_within_span").isNotNull()
            & F.col("missing_dates_within_span_count").isNotNull()
            & (
                F.col("max_consecutive_missing_dates_within_span")
                > F.col("missing_dates_within_span_count")
            )
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the vehicle multi-day continuity table is not empty.
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
            F.lit("gold_vehicle_multi_day_continuity must contain at least one row."),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
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


def _validate_contains_at_least_one_vehicle_observed_on_multiple_dates(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one vehicle observed on multiple dates.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(F.when(F.col("observed_dates_count") > 1, 1)) > 0,
                F.lit(0),
            ).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_vehicle_observed_on_multiple_dates"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_vehicle_multi_day_continuity must contain at least one vehicle "
                "with observed_dates_count > 1."
            ),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
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


def _validate_contains_at_least_one_valid_multi_day_continuity_row(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one valid multi-day continuity row.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("vehicle_prefix").isNotNull()
                        & (F.trim(F.col("vehicle_prefix")) != "")
                        & F.col("observed_dates_count").isNotNull()
                        & F.col("first_observed_date").isNotNull()
                        & F.col("last_observed_date").isNotNull()
                        & F.col("observed_date_span_days").isNotNull()
                        & F.col("missing_dates_within_span_count").isNotNull()
                        & F.col("date_presence_ratio").isNotNull()
                        & F.col("total_observations_count").isNotNull(),
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
            F.lit("contains_at_least_one_valid_multi_day_continuity_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_vehicle_multi_day_continuity must contain at least one valid "
                "multi-day continuity row."
            ),
        )
        .withColumn("severity", F.lit("ERROR"))
        .withColumn(
            "validation_status",
            F.when(F.col("violation_count") == 0, F.lit("PASS")).otherwise(
                F.lit("FAIL")
            ),
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
