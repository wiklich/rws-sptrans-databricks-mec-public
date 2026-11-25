"""
Gold recurring update patterns validation pipeline.

Purpose:
    Validate whether gold_recurring_update_patterns is structurally and
    logically acceptable for downstream use as the canonical recurring
    update pattern layer in the Observed Update Frequency and Continuity
    Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Temporal consistency validation
    E. Reference usability validation

Output:
    - gold.gold_recurring_update_patterns_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_recurring_update_patterns"
OUTPUT_TABLE_NAME = "gold.gold_recurring_update_patterns_validation"

FLOAT_TOLERANCE = 1e-9


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_recurring_update_patterns with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_recurring_update_patterns_validation() -> DataFrame:
    """
    Materialize the validation results for gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_recurring_update_patterns_validation(validated_df)


def _build_gold_recurring_update_patterns_validation(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_recurring_update_patterns.

    Args:
        validated_df:
            Input DataFrame from gold.gold_recurring_update_patterns.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_day_of_week_not_null(validated_df),
        _validate_hour_of_day_not_null(validated_df),
        _validate_observations_count_not_null(validated_df),
        _validate_measured_intervals_count_not_null(validated_df),
        _validate_distinct_vehicles_count_not_null(validated_df),
        _validate_distinct_dates_count_not_null(validated_df),
        _validate_avg_update_interval_seconds_not_null_when_intervals_exist(
            validated_df
        ),
        _validate_min_update_interval_seconds_not_null_when_intervals_exist(
            validated_df
        ),
        _validate_max_update_interval_seconds_not_null_when_intervals_exist(
            validated_df
        ),
        _validate_avg_intervals_per_vehicle_not_null_when_vehicle_support_exists(
            validated_df
        ),
        _validate_avg_intervals_per_date_not_null_when_date_support_exists(
            validated_df
        ),
        _validate_unique_recurring_temporal_key(validated_df),
        _validate_observations_count_minimum(validated_df),
        _validate_measured_intervals_count_non_negative(validated_df),
        _validate_avg_update_interval_seconds_positive_when_intervals_exist(
            validated_df
        ),
        _validate_min_update_interval_seconds_positive_when_intervals_exist(
            validated_df
        ),
        _validate_max_update_interval_seconds_greater_than_or_equal_to_min(
            validated_df
        ),
        _validate_stddev_update_interval_seconds_non_negative_when_populated(
            validated_df
        ),
        _validate_distinct_vehicles_count_minimum(validated_df),
        _validate_distinct_dates_count_minimum(validated_df),
        _validate_day_of_week_valid_range(validated_df),
        _validate_hour_of_day_valid_range(validated_df),
        _validate_measured_intervals_count_compatible_with_observations_count(
            validated_df
        ),
        _validate_distinct_vehicles_count_compatible_with_observations_count(
            validated_df
        ),
        _validate_distinct_dates_count_compatible_with_observations_count(
            validated_df
        ),
        _validate_avg_intervals_per_vehicle_matches_ratio(validated_df),
        _validate_avg_intervals_per_date_matches_ratio(validated_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_contains_at_least_one_bucket_with_measurable_intervals(
            validated_df
        ),
        _validate_contains_at_least_one_valid_recurring_pattern_row(validated_df),
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


def _validate_day_of_week_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that day_of_week is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="day_of_week_not_null",
        validation_description="day_of_week must not be null.",
        severity="ERROR",
        violation_condition=F.col("day_of_week").isNull(),
    )


def _validate_hour_of_day_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that hour_of_day is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="hour_of_day_not_null",
        validation_description="hour_of_day must not be null.",
        severity="ERROR",
        violation_condition=F.col("hour_of_day").isNull(),
    )


def _validate_observations_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observations_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observations_count_not_null",
        validation_description="observations_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("observations_count").isNull(),
    )


def _validate_measured_intervals_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that measured_intervals_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="measured_intervals_count_not_null",
        validation_description="measured_intervals_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("measured_intervals_count").isNull(),
    )


def _validate_distinct_vehicles_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_vehicles_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_vehicles_count_not_null",
        validation_description="distinct_vehicles_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_vehicles_count").isNull(),
    )


def _validate_distinct_dates_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_dates_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_dates_count_not_null",
        validation_description="distinct_dates_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_dates_count").isNull(),
    )


def _validate_avg_update_interval_seconds_not_null_when_intervals_exist(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_update_interval_seconds is populated when measurable intervals exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="avg_update_interval_seconds_not_null_when_intervals_exist",
        validation_description=(
            "avg_update_interval_seconds must not be null when measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("measured_intervals_count") > 0)
            & F.col("avg_update_interval_seconds").isNull()
        ),
    )


def _validate_min_update_interval_seconds_not_null_when_intervals_exist(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that min_update_interval_seconds is populated when measurable intervals exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="min_update_interval_seconds_not_null_when_intervals_exist",
        validation_description=(
            "min_update_interval_seconds must not be null when measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("measured_intervals_count") > 0)
            & F.col("min_update_interval_seconds").isNull()
        ),
    )


def _validate_max_update_interval_seconds_not_null_when_intervals_exist(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_update_interval_seconds is populated when measurable intervals exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="max_update_interval_seconds_not_null_when_intervals_exist",
        validation_description=(
            "max_update_interval_seconds must not be null when measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("measured_intervals_count") > 0)
            & F.col("max_update_interval_seconds").isNull()
        ),
    )


def _validate_avg_intervals_per_vehicle_not_null_when_vehicle_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_intervals_per_vehicle is populated when vehicle support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="avg_intervals_per_vehicle_not_null_when_vehicle_support_exists",
        validation_description=(
            "avg_intervals_per_vehicle must not be null when distinct_vehicles_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("distinct_vehicles_count") > 0)
            & F.col("avg_intervals_per_vehicle").isNull()
        ),
    )


def _validate_avg_intervals_per_date_not_null_when_date_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_intervals_per_date is populated when date support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="avg_intervals_per_date_not_null_when_date_support_exists",
        validation_description=(
            "avg_intervals_per_date must not be null when distinct_dates_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("distinct_dates_count") > 0)
            & F.col("avg_intervals_per_date").isNull()
        ),
    )


def _validate_unique_recurring_temporal_key(validated_df: DataFrame) -> DataFrame:
    """
    Validate that day_of_week + hour_of_day is unique at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy("day_of_week", "hour_of_day")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn("validation_rule", F.lit("unique_recurring_temporal_key"))
        .withColumn(
            "validation_description",
            F.lit("There must be one and only one row per day_of_week and hour_of_day."),
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


def _validate_observations_count_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observations_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observations_count_minimum",
        validation_description="observations_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("observations_count").isNull()
            | (F.col("observations_count") < 1)
        ),
    )


def _validate_measured_intervals_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that measured_intervals_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="measured_intervals_count_non_negative",
        validation_description="measured_intervals_count must be greater than or equal to 0.",
        severity="ERROR",
        violation_condition=F.col("measured_intervals_count") < 0,
    )


def _validate_avg_update_interval_seconds_positive_when_intervals_exist(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_update_interval_seconds is positive when measurable intervals exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="avg_update_interval_seconds_positive_when_intervals_exist",
        validation_description=(
            "avg_update_interval_seconds must be greater than 0 when measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("measured_intervals_count") > 0)
            & (
                F.col("avg_update_interval_seconds").isNull()
                | (F.col("avg_update_interval_seconds") <= 0)
            )
        ),
    )


def _validate_min_update_interval_seconds_positive_when_intervals_exist(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that min_update_interval_seconds is positive when measurable intervals exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="min_update_interval_seconds_positive_when_intervals_exist",
        validation_description=(
            "min_update_interval_seconds must be greater than 0 when measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("measured_intervals_count") > 0)
            & (
                F.col("min_update_interval_seconds").isNull()
                | (F.col("min_update_interval_seconds") <= 0)
            )
        ),
    )


def _validate_max_update_interval_seconds_greater_than_or_equal_to_min(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_update_interval_seconds is greater than or equal to min_update_interval_seconds.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_update_interval_seconds_greater_than_or_equal_to_min",
        validation_description=(
            "max_update_interval_seconds must be greater than or equal to "
            "min_update_interval_seconds when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("min_update_interval_seconds").isNotNull()
            & F.col("max_update_interval_seconds").isNotNull()
            & (
                F.col("max_update_interval_seconds")
                < F.col("min_update_interval_seconds")
            )
        ),
    )


def _validate_stddev_update_interval_seconds_non_negative_when_populated(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that stddev_update_interval_seconds is non-negative when populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="stddev_update_interval_seconds_non_negative_when_populated",
        validation_description=(
            "stddev_update_interval_seconds must be greater than or equal to 0 when populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("stddev_update_interval_seconds").isNotNull()
            & (F.col("stddev_update_interval_seconds") < 0)
        ),
    )


def _validate_distinct_vehicles_count_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_vehicles_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_vehicles_count_minimum",
        validation_description="distinct_vehicles_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("distinct_vehicles_count").isNull()
            | (F.col("distinct_vehicles_count") < 1)
        ),
    )


def _validate_distinct_dates_count_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_dates_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_dates_count_minimum",
        validation_description="distinct_dates_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("distinct_dates_count").isNull()
            | (F.col("distinct_dates_count") < 1)
        ),
    )


def _validate_day_of_week_valid_range(validated_df: DataFrame) -> DataFrame:
    """
    Validate that day_of_week is inside Spark dayofweek valid range.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="day_of_week_valid_range",
        validation_description="day_of_week must be between 1 and 7 inclusive.",
        severity="ERROR",
        violation_condition=(
            F.col("day_of_week").isNull()
            | (F.col("day_of_week") < 1)
            | (F.col("day_of_week") > 7)
        ),
    )


def _validate_hour_of_day_valid_range(validated_df: DataFrame) -> DataFrame:
    """
    Validate that hour_of_day is inside valid 24-hour range.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="hour_of_day_valid_range",
        validation_description="hour_of_day must be between 0 and 23 inclusive.",
        severity="ERROR",
        violation_condition=(
            F.col("hour_of_day").isNull()
            | (F.col("hour_of_day") < 0)
            | (F.col("hour_of_day") > 23)
        ),
    )


def _validate_measured_intervals_count_compatible_with_observations_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that measured_intervals_count is logically compatible with observations_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="measured_intervals_count_compatible_with_observations_count",
        validation_description=(
            "measured_intervals_count must be less than or equal to observations_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("measured_intervals_count").isNotNull()
            & F.col("observations_count").isNotNull()
            & (F.col("measured_intervals_count") > F.col("observations_count"))
        ),
    )


def _validate_distinct_vehicles_count_compatible_with_observations_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_vehicles_count is logically compatible with observations_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="distinct_vehicles_count_compatible_with_observations_count",
        validation_description=(
            "distinct_vehicles_count must be less than or equal to observations_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("distinct_vehicles_count").isNotNull()
            & F.col("observations_count").isNotNull()
            & (F.col("distinct_vehicles_count") > F.col("observations_count"))
        ),
    )


def _validate_distinct_dates_count_compatible_with_observations_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_dates_count is logically compatible with observations_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="distinct_dates_count_compatible_with_observations_count",
        validation_description=(
            "distinct_dates_count must be less than or equal to observations_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("distinct_dates_count").isNotNull()
            & F.col("observations_count").isNotNull()
            & (F.col("distinct_dates_count") > F.col("observations_count"))
        ),
    )


def _validate_avg_intervals_per_vehicle_matches_ratio(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_intervals_per_vehicle matches measured_intervals_count / distinct_vehicles_count.
    """
    expected_ratio = (
        F.col("measured_intervals_count").cast("double")
        / F.col("distinct_vehicles_count").cast("double")
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="avg_intervals_per_vehicle_matches_ratio",
        validation_description=(
            "avg_intervals_per_vehicle must match measured_intervals_count divided by distinct_vehicles_count."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("distinct_vehicles_count") > 0)
            & F.col("avg_intervals_per_vehicle").isNotNull()
            & (F.abs(F.col("avg_intervals_per_vehicle") - expected_ratio) > F.lit(FLOAT_TOLERANCE))
        ),
    )


def _validate_avg_intervals_per_date_matches_ratio(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_intervals_per_date matches measured_intervals_count / distinct_dates_count.
    """
    expected_ratio = (
        F.col("measured_intervals_count").cast("double")
        / F.col("distinct_dates_count").cast("double")
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="avg_intervals_per_date_matches_ratio",
        validation_description=(
            "avg_intervals_per_date must match measured_intervals_count divided by distinct_dates_count."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("distinct_dates_count") > 0)
            & F.col("avg_intervals_per_date").isNotNull()
            & (F.abs(F.col("avg_intervals_per_date") - expected_ratio) > F.lit(FLOAT_TOLERANCE))
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the recurring update patterns table is not empty.
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
            F.lit("gold_recurring_update_patterns must contain at least one row."),
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


def _validate_contains_at_least_one_bucket_with_measurable_intervals(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one recurring bucket with measurable intervals.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(F.when(F.col("measured_intervals_count") > 0, 1)) > 0,
                F.lit(0),
            ).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_bucket_with_measurable_intervals"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_recurring_update_patterns must contain at least one bucket "
                "with measured_intervals_count > 0."
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


def _validate_contains_at_least_one_valid_recurring_pattern_row(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one valid recurring-pattern row.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("day_of_week").isNotNull()
                        & F.col("hour_of_day").isNotNull()
                        & F.col("observations_count").isNotNull()
                        & F.col("measured_intervals_count").isNotNull()
                        & F.col("distinct_vehicles_count").isNotNull()
                        & F.col("distinct_dates_count").isNotNull(),
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
            F.lit("contains_at_least_one_valid_recurring_pattern_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_recurring_update_patterns must contain at least one valid "
                "recurring-pattern row."
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
