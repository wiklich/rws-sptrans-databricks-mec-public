"""
Gold vehicle observation continuity validation pipeline.

Purpose:
    Validate whether gold_vehicle_observation_continuity is structurally and
    logically acceptable for downstream use as the canonical vehicle
    observation continuity layer in the Observed Update Frequency and
    Continuity Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Temporal consistency validation
    E. Reference usability validation

Output:
    - gold.gold_vehicle_observation_continuity_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_vehicle_observation_continuity"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observation_continuity_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_vehicle_observation_continuity with one row "
        "per validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_vehicle_observation_continuity_validation() -> DataFrame:
    """
    Materialize the validation results for gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    return _build_gold_vehicle_observation_continuity_validation(validated_df)


def _build_gold_vehicle_observation_continuity_validation(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_vehicle_observation_continuity.

    Args:
        validated_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_vehicle_prefix_not_null(validated_df),
        _validate_vehicle_prefix_not_blank(validated_df),
        _validate_observations_count_not_null(validated_df),
        _validate_measured_gaps_count_not_null(validated_df),
        _validate_first_captured_at_local_not_null(validated_df),
        _validate_last_captured_at_local_not_null(validated_df),
        _validate_observed_span_seconds_not_null(validated_df),
        _validate_distinct_minutes_with_data_not_null(validated_df),
        _validate_distinct_hours_with_data_not_null(validated_df),
        _validate_large_gaps_count_not_null(validated_df),
        _validate_continuity_support_ratio_not_null(validated_df),
        _validate_unique_vehicle_prefix(validated_df),
        _validate_observations_count_minimum(validated_df),
        _validate_measured_gaps_count_non_negative(validated_df),
        _validate_observed_span_seconds_non_negative(validated_df),
        _validate_distinct_minutes_with_data_minimum(validated_df),
        _validate_distinct_hours_with_data_minimum(validated_df),
        _validate_avg_gap_seconds_positive_when_gaps_exist(validated_df),
        _validate_max_gap_seconds_greater_than_or_equal_to_avg(validated_df),
        _validate_large_gaps_count_non_negative(validated_df),
        _validate_continuity_support_ratio_non_negative(validated_df),
        _validate_first_captured_at_local_less_than_or_equal_to_last(validated_df),
        _validate_observed_span_seconds_matches_timestamp_bounds(validated_df),
        _validate_distinct_minutes_with_data_compatible_with_timestamp_bounds(
            validated_df
        ),
        _validate_distinct_hours_with_data_compatible_with_timestamp_bounds(
            validated_df
        ),
        _validate_measured_gaps_count_compatible_with_observations_count(validated_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_contains_at_least_one_vehicle_with_measurable_gaps(validated_df),
        _validate_contains_at_least_one_valid_continuity_row(validated_df),
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


def _validate_measured_gaps_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that measured_gaps_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="measured_gaps_count_not_null",
        validation_description="measured_gaps_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("measured_gaps_count").isNull(),
    )


def _validate_first_captured_at_local_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that first_captured_at_local is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="first_captured_at_local_not_null",
        validation_description="first_captured_at_local must not be null.",
        severity="ERROR",
        violation_condition=F.col("first_captured_at_local").isNull(),
    )


def _validate_last_captured_at_local_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that last_captured_at_local is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="last_captured_at_local_not_null",
        validation_description="last_captured_at_local must not be null.",
        severity="ERROR",
        violation_condition=F.col("last_captured_at_local").isNull(),
    )


def _validate_observed_span_seconds_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_span_seconds is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="observed_span_seconds_not_null",
        validation_description="observed_span_seconds must not be null.",
        severity="ERROR",
        violation_condition=F.col("observed_span_seconds").isNull(),
    )


def _validate_distinct_minutes_with_data_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_minutes_with_data is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_minutes_with_data_not_null",
        validation_description="distinct_minutes_with_data must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_minutes_with_data").isNull(),
    )


def _validate_distinct_hours_with_data_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_hours_with_data is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="distinct_hours_with_data_not_null",
        validation_description="distinct_hours_with_data must not be null.",
        severity="ERROR",
        violation_condition=F.col("distinct_hours_with_data").isNull(),
    )


def _validate_large_gaps_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that large_gaps_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="large_gaps_count_not_null",
        validation_description="large_gaps_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("large_gaps_count").isNull(),
    )


def _validate_continuity_support_ratio_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that continuity_support_ratio is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="continuity_support_ratio_not_null",
        validation_description="continuity_support_ratio must not be null.",
        severity="ERROR",
        violation_condition=F.col("continuity_support_ratio").isNull(),
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


def _validate_measured_gaps_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that measured_gaps_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="measured_gaps_count_non_negative",
        validation_description="measured_gaps_count must be greater than or equal to 0.",
        severity="ERROR",
        violation_condition=F.col("measured_gaps_count") < 0,
    )


def _validate_observed_span_seconds_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that observed_span_seconds is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="observed_span_seconds_non_negative",
        validation_description="observed_span_seconds must be greater than or equal to 0.",
        severity="ERROR",
        violation_condition=(
            F.col("observed_span_seconds").isNull()
            | (F.col("observed_span_seconds") < 0)
        ),
    )


def _validate_distinct_minutes_with_data_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_minutes_with_data is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_minutes_with_data_minimum",
        validation_description="distinct_minutes_with_data must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("distinct_minutes_with_data").isNull()
            | (F.col("distinct_minutes_with_data") < 1)
        ),
    )


def _validate_distinct_hours_with_data_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that distinct_hours_with_data is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="distinct_hours_with_data_minimum",
        validation_description="distinct_hours_with_data must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("distinct_hours_with_data").isNull()
            | (F.col("distinct_hours_with_data") < 1)
        ),
    )


def _validate_avg_gap_seconds_positive_when_gaps_exist(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_gap_seconds is positive when measurable gaps exist.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="avg_gap_seconds_positive_when_gaps_exist",
        validation_description=(
            "avg_gap_seconds must be greater than 0 when measured_gaps_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("measured_gaps_count") > 0)
            & (
                F.col("avg_gap_seconds").isNull()
                | (F.col("avg_gap_seconds") <= 0)
            )
        ),
    )


def _validate_max_gap_seconds_greater_than_or_equal_to_avg(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that max_gap_seconds is greater than or equal to avg_gap_seconds.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_gap_seconds_greater_than_or_equal_to_avg",
        validation_description=(
            "max_gap_seconds must be greater than or equal to avg_gap_seconds "
            "when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("avg_gap_seconds").isNotNull()
            & F.col("max_gap_seconds").isNotNull()
            & (F.col("max_gap_seconds") < F.col("avg_gap_seconds"))
        ),
    )


def _validate_large_gaps_count_non_negative(validated_df: DataFrame) -> DataFrame:
    """
    Validate that large_gaps_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="large_gaps_count_non_negative",
        validation_description="large_gaps_count must be greater than or equal to 0.",
        severity="ERROR",
        violation_condition=F.col("large_gaps_count") < 0,
    )


def _validate_continuity_support_ratio_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that continuity_support_ratio is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="continuity_support_ratio_non_negative",
        validation_description="continuity_support_ratio must be greater than or equal to 0.",
        severity="ERROR",
        violation_condition=(
            F.col("continuity_support_ratio").isNull()
            | (F.col("continuity_support_ratio") < 0)
        ),
    )


def _validate_first_captured_at_local_less_than_or_equal_to_last(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that first_captured_at_local is less than or equal to last_captured_at_local.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="first_captured_at_local_less_than_or_equal_to_last",
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


def _validate_observed_span_seconds_matches_timestamp_bounds(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that observed_span_seconds matches the difference between last and first timestamps.
    """
    expected_span_seconds = (
        F.unix_timestamp(F.col("last_captured_at_local"))
        - F.unix_timestamp(F.col("first_captured_at_local"))
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="observed_span_seconds_matches_timestamp_bounds",
        validation_description=(
            "observed_span_seconds must match the difference between "
            "last_captured_at_local and first_captured_at_local when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("first_captured_at_local").isNotNull()
            & F.col("last_captured_at_local").isNotNull()
            & F.col("observed_span_seconds").isNotNull()
            & (F.col("observed_span_seconds") != expected_span_seconds)
        ),
    )


def _validate_distinct_minutes_with_data_compatible_with_timestamp_bounds(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_minutes_with_data is logically compatible with timestamp bounds.
    """
    inclusive_minute_span = (
        F.floor(
            (
                F.unix_timestamp(F.col("last_captured_at_local"))
                - F.unix_timestamp(F.col("first_captured_at_local"))
            ) / F.lit(60)
        ) + F.lit(1)
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="distinct_minutes_with_data_compatible_with_timestamp_bounds",
        validation_description=(
            "distinct_minutes_with_data must be less than or equal to the inclusive "
            "minute span between first_captured_at_local and last_captured_at_local."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("first_captured_at_local").isNotNull()
            & F.col("last_captured_at_local").isNotNull()
            & (F.col("distinct_minutes_with_data") > inclusive_minute_span)
        ),
    )


def _validate_distinct_hours_with_data_compatible_with_timestamp_bounds(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that distinct_hours_with_data is logically compatible with timestamp bounds.
    """
    inclusive_hour_span = (
        F.floor(
            (
                F.unix_timestamp(F.col("last_captured_at_local"))
                - F.unix_timestamp(F.col("first_captured_at_local"))
            ) / F.lit(3600)
        ) + F.lit(1)
    )

    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="distinct_hours_with_data_compatible_with_timestamp_bounds",
        validation_description=(
            "distinct_hours_with_data must be less than or equal to the inclusive "
            "hour span between first_captured_at_local and last_captured_at_local."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("first_captured_at_local").isNotNull()
            & F.col("last_captured_at_local").isNotNull()
            & (F.col("distinct_hours_with_data") > inclusive_hour_span)
        ),
    )


def _validate_measured_gaps_count_compatible_with_observations_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that measured_gaps_count is logically compatible with observations_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="TEMPORAL_CONSISTENCY",
        validation_rule="measured_gaps_count_compatible_with_observations_count",
        validation_description=(
            "measured_gaps_count must be less than or equal to observations_count - 1 "
            "when observations_count is at least 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("observations_count").isNotNull()
            & (F.col("observations_count") >= 1)
            & (
                F.col("measured_gaps_count")
                > (F.col("observations_count") - F.lit(1))
            )
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the vehicle observation continuity table is not empty.
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
            F.lit(
                "gold_vehicle_observation_continuity must contain at least one row."
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


def _validate_contains_at_least_one_vehicle_with_measurable_gaps(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one vehicle with measurable gaps.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(F.when(F.col("measured_gaps_count") > 0, 1)) > 0,
                F.lit(0),
            ).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_vehicle_with_measurable_gaps"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_vehicle_observation_continuity must contain at least one vehicle "
                "with measured_gaps_count > 0."
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


def _validate_contains_at_least_one_valid_continuity_row(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one valid continuity row.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("vehicle_prefix").isNotNull()
                        & (F.trim(F.col("vehicle_prefix")) != "")
                        & F.col("observations_count").isNotNull()
                        & F.col("measured_gaps_count").isNotNull()
                        & F.col("observed_span_seconds").isNotNull()
                        & F.col("distinct_minutes_with_data").isNotNull()
                        & F.col("distinct_hours_with_data").isNotNull(),
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
            F.lit("contains_at_least_one_valid_continuity_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_vehicle_observation_continuity must contain at least one valid continuity row."
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
