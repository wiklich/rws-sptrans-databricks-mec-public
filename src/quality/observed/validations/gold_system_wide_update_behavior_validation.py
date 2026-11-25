"""
Gold system-wide update behavior validation pipeline.

Purpose:
    Validate whether gold_system_wide_update_behavior is structurally and
    logically acceptable for downstream use as the canonical system-wide
    update behavior layer in the Observed Update Frequency and Continuity
    Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Aggregation consistency validation
    E. Reference usability validation

Output:
    - gold.gold_system_wide_update_behavior_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALIDATED_TABLE_NAME = "main.gold.gold_system_wide_update_behavior"
UPDATE_FREQUENCY_SOURCE_TABLE = "main.gold.gold_vehicle_update_frequency"
OBSERVATION_CONTINUITY_SOURCE_TABLE = "main.gold.gold_vehicle_observation_continuity"
OUTPUT_TABLE_NAME = "gold.gold_system_wide_update_behavior_validation"

FLOAT_TOLERANCE = 1e-9


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_system_wide_update_behavior with one row per "
        "validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_system_wide_update_behavior_validation() -> DataFrame:
    """
    Materialize the validation results for gold_system_wide_update_behavior.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    update_frequency_df = spark.read.table(UPDATE_FREQUENCY_SOURCE_TABLE)
    observation_continuity_df = spark.read.table(OBSERVATION_CONTINUITY_SOURCE_TABLE)

    return _build_gold_system_wide_update_behavior_validation(
        validated_df,
        update_frequency_df,
        observation_continuity_df,
    )


def _build_gold_system_wide_update_behavior_validation(
    validated_df: DataFrame,
    update_frequency_df: DataFrame,
    observation_continuity_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_system_wide_update_behavior.

    Args:
        validated_df:
            Input DataFrame from gold.gold_system_wide_update_behavior.
        update_frequency_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.
        observation_continuity_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    consistency_df = _build_system_wide_consistency_df(
        validated_df,
        update_frequency_df,
        observation_continuity_df,
    )

    validation_dfs = [
        _validate_aggregation_scope_not_null(validated_df),
        _validate_aggregation_scope_not_blank(validated_df),
        _validate_vehicles_count_not_null(validated_df),
        _validate_total_observations_count_not_null(validated_df),
        _validate_total_measured_intervals_count_not_null(validated_df),
        _validate_system_avg_update_interval_seconds_not_null_when_interval_support_exists(
            validated_df
        ),
        _validate_system_min_update_interval_seconds_not_null_when_interval_support_exists(
            validated_df
        ),
        _validate_system_max_update_interval_seconds_not_null_when_interval_support_exists(
            validated_df
        ),
        _validate_total_distinct_minutes_with_data_not_null(validated_df),
        _validate_total_large_gaps_count_not_null(validated_df),
        _validate_avg_continuity_support_ratio_not_null_when_vehicle_support_exists(
            validated_df
        ),
        _validate_unique_aggregation_scope(validated_df),
        _validate_vehicles_count_minimum(validated_df),
        _validate_total_observations_count_greater_than_or_equal_to_vehicles_count(
            validated_df
        ),
        _validate_total_measured_intervals_count_non_negative(validated_df),
        _validate_system_avg_update_interval_seconds_positive_when_interval_support_exists(
            validated_df
        ),
        _validate_system_max_update_interval_seconds_greater_than_or_equal_to_min(
            validated_df
        ),
        _validate_system_avg_vehicle_stddev_interval_seconds_non_negative_when_populated(
            validated_df
        ),
        _validate_avg_continuity_support_ratio_non_negative(validated_df),
        _validate_avg_continuity_support_ratio_less_than_or_equal_to_one(validated_df),
        _validate_vehicles_count_reconciles_with_lower_level_inputs(consistency_df),
        _validate_total_observations_count_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_total_measured_intervals_count_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_system_avg_update_interval_seconds_reconciles_with_weighted_inputs(
            consistency_df
        ),
        _validate_system_min_update_interval_seconds_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_system_max_update_interval_seconds_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_system_avg_vehicle_stddev_interval_seconds_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_total_distinct_minutes_with_data_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_avg_continuity_support_ratio_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_total_large_gaps_count_reconciles_with_lower_level_inputs(
            consistency_df
        ),
        _validate_reference_table_not_empty(validated_df),
        _validate_contains_at_least_one_row_with_measurable_interval_support(
            validated_df
        ),
        _validate_contains_at_least_one_valid_system_wide_summary_row(validated_df),
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


def _build_system_wide_consistency_df(
    validated_df: DataFrame,
    update_frequency_df: DataFrame,
    observation_continuity_df: DataFrame,
) -> DataFrame:
    """
    Build the consistency DataFrame used to reconcile the system-wide Gold table
    against its lower-level Gold inputs.

    Args:
        validated_df:
            Input DataFrame from gold.gold_system_wide_update_behavior.
        update_frequency_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.
        observation_continuity_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Single-row consistency DataFrame with actual and expected system-wide metrics.
    """
    prepared_update_frequency_df = (
        update_frequency_df.select(
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("updates_count").cast("long").alias("updates_count"),
            F.col("measured_intervals_count").cast("long").alias(
                "measured_intervals_count"
            ),
            F.col("avg_update_interval_seconds").cast("double").alias(
                "avg_update_interval_seconds"
            ),
            F.col("min_update_interval_seconds").cast("double").alias(
                "min_update_interval_seconds"
            ),
            F.col("max_update_interval_seconds").cast("double").alias(
                "max_update_interval_seconds"
            ),
            F.col("stddev_update_interval_seconds").cast("double").alias(
                "stddev_update_interval_seconds"
            ),
        )
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
        .where(F.col("updates_count").isNotNull())
        .where(F.col("measured_intervals_count").isNotNull())
    )

    prepared_observation_continuity_df = (
        observation_continuity_df.select(
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("distinct_minutes_with_data").cast("long").alias(
                "distinct_minutes_with_data"
            ),
            F.col("large_gaps_count").cast("long").alias("large_gaps_count"),
            F.col("continuity_support_ratio").cast("double").alias(
                "continuity_support_ratio"
            ),
        )
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
        .where(F.col("distinct_minutes_with_data").isNotNull())
        .where(F.col("large_gaps_count").isNotNull())
    )

    vehicle_base_df = prepared_update_frequency_df.join(
        prepared_observation_continuity_df,
        on="vehicle_prefix",
        how="inner",
    )

    expected_metrics_df = (
        vehicle_base_df.agg(
            F.countDistinct("vehicle_prefix").cast("long").alias(
                "expected_vehicles_count"
            ),
            F.coalesce(F.sum("updates_count"), F.lit(0)).cast("long").alias(
                "expected_total_observations_count"
            ),
            F.coalesce(F.sum("measured_intervals_count"), F.lit(0)).cast("long").alias(
                "expected_total_measured_intervals_count"
            ),
            F.sum(
                F.col("avg_update_interval_seconds") * F.col("measured_intervals_count")
            ).cast("double").alias("expected_weighted_interval_seconds_sum"),
            F.min("min_update_interval_seconds").cast("double").alias(
                "expected_system_min_update_interval_seconds"
            ),
            F.max("max_update_interval_seconds").cast("double").alias(
                "expected_system_max_update_interval_seconds"
            ),
            F.avg("stddev_update_interval_seconds").cast("double").alias(
                "expected_system_avg_vehicle_stddev_interval_seconds"
            ),
            F.coalesce(F.sum("distinct_minutes_with_data"), F.lit(0)).cast("long").alias(
                "expected_total_distinct_minutes_with_data"
            ),
            F.avg("continuity_support_ratio").cast("double").alias(
                "expected_avg_continuity_support_ratio"
            ),
            F.coalesce(F.sum("large_gaps_count"), F.lit(0)).cast("long").alias(
                "expected_total_large_gaps_count"
            ),
        )
        .withColumn(
            "expected_system_avg_update_interval_seconds",
            F.when(
                F.col("expected_total_measured_intervals_count") > 0,
                (
                    F.col("expected_weighted_interval_seconds_sum")
                    / F.col("expected_total_measured_intervals_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    return validated_df.crossJoin(expected_metrics_df)


def _validate_aggregation_scope_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that aggregation_scope is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="aggregation_scope_not_null",
        validation_description="aggregation_scope must not be null.",
        severity="ERROR",
        violation_condition=F.col("aggregation_scope").isNull(),
    )


def _validate_aggregation_scope_not_blank(validated_df: DataFrame) -> DataFrame:
    """
    Validate that aggregation_scope is not blank.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="aggregation_scope_not_blank",
        validation_description="aggregation_scope must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("aggregation_scope")) == "",
    )


def _validate_vehicles_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that vehicles_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="vehicles_count_not_null",
        validation_description="vehicles_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("vehicles_count").isNull(),
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


def _validate_total_measured_intervals_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_measured_intervals_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="total_measured_intervals_count_not_null",
        validation_description="total_measured_intervals_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("total_measured_intervals_count").isNull(),
    )


def _validate_system_avg_update_interval_seconds_not_null_when_interval_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_avg_update_interval_seconds is populated when interval support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="system_avg_update_interval_seconds_not_null_when_interval_support_exists",
        validation_description=(
            "system_avg_update_interval_seconds must not be null when "
            "total_measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("total_measured_intervals_count") > 0)
            & F.col("system_avg_update_interval_seconds").isNull()
        ),
    )


def _validate_system_min_update_interval_seconds_not_null_when_interval_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_min_update_interval_seconds is populated when interval support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="system_min_update_interval_seconds_not_null_when_interval_support_exists",
        validation_description=(
            "system_min_update_interval_seconds must not be null when "
            "total_measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("total_measured_intervals_count") > 0)
            & F.col("system_min_update_interval_seconds").isNull()
        ),
    )


def _validate_system_max_update_interval_seconds_not_null_when_interval_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_max_update_interval_seconds is populated when interval support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="system_max_update_interval_seconds_not_null_when_interval_support_exists",
        validation_description=(
            "system_max_update_interval_seconds must not be null when "
            "total_measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("total_measured_intervals_count") > 0)
            & F.col("system_max_update_interval_seconds").isNull()
        ),
    )


def _validate_total_distinct_minutes_with_data_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_distinct_minutes_with_data is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="total_distinct_minutes_with_data_not_null",
        validation_description="total_distinct_minutes_with_data must not be null.",
        severity="ERROR",
        violation_condition=F.col("total_distinct_minutes_with_data").isNull(),
    )


def _validate_total_large_gaps_count_not_null(validated_df: DataFrame) -> DataFrame:
    """
    Validate that total_large_gaps_count is not null.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="total_large_gaps_count_not_null",
        validation_description="total_large_gaps_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("total_large_gaps_count").isNull(),
    )


def _validate_avg_continuity_support_ratio_not_null_when_vehicle_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_continuity_support_ratio is populated when vehicle support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="avg_continuity_support_ratio_not_null_when_vehicle_support_exists",
        validation_description=(
            "avg_continuity_support_ratio must not be null when vehicles_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("vehicles_count") > 0)
            & F.col("avg_continuity_support_ratio").isNull()
        ),
    )


def _validate_unique_aggregation_scope(validated_df: DataFrame) -> DataFrame:
    """
    Validate that aggregation_scope is unique at the declared grain.
    """
    duplicates_df = (
        validated_df.groupBy("aggregation_scope")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn("validation_rule", F.lit("unique_aggregation_scope"))
        .withColumn(
            "validation_description",
            F.lit("There must be one and only one row per aggregation_scope."),
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


def _validate_vehicles_count_minimum(validated_df: DataFrame) -> DataFrame:
    """
    Validate that vehicles_count is at least 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="vehicles_count_minimum",
        validation_description="vehicles_count must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("vehicles_count").isNull()
            | (F.col("vehicles_count") < 1)
        ),
    )


def _validate_total_observations_count_greater_than_or_equal_to_vehicles_count(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_observations_count is greater than or equal to vehicles_count.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="total_observations_count_greater_than_or_equal_to_vehicles_count",
        validation_description=(
            "total_observations_count must be greater than or equal to vehicles_count."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_observations_count").isNull()
            | F.col("vehicles_count").isNull()
            | (F.col("total_observations_count") < F.col("vehicles_count"))
        ),
    )


def _validate_total_measured_intervals_count_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_measured_intervals_count is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="total_measured_intervals_count_non_negative",
        validation_description=(
            "total_measured_intervals_count must be greater than or equal to 0."
        ),
        severity="ERROR",
        violation_condition=F.col("total_measured_intervals_count") < 0,
    )


def _validate_system_avg_update_interval_seconds_positive_when_interval_support_exists(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_avg_update_interval_seconds is positive when interval support exists.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="system_avg_update_interval_seconds_positive_when_interval_support_exists",
        validation_description=(
            "system_avg_update_interval_seconds must be greater than 0 when "
            "total_measured_intervals_count > 0."
        ),
        severity="ERROR",
        violation_condition=(
            (F.col("total_measured_intervals_count") > 0)
            & (
                F.col("system_avg_update_interval_seconds").isNull()
                | (F.col("system_avg_update_interval_seconds") <= 0)
            )
        ),
    )


def _validate_system_max_update_interval_seconds_greater_than_or_equal_to_min(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_max_update_interval_seconds is greater than or equal to system_min_update_interval_seconds.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="system_max_update_interval_seconds_greater_than_or_equal_to_min",
        validation_description=(
            "system_max_update_interval_seconds must be greater than or equal to "
            "system_min_update_interval_seconds when both are populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("system_min_update_interval_seconds").isNotNull()
            & F.col("system_max_update_interval_seconds").isNotNull()
            & (
                F.col("system_max_update_interval_seconds")
                < F.col("system_min_update_interval_seconds")
            )
        ),
    )


def _validate_system_avg_vehicle_stddev_interval_seconds_non_negative_when_populated(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_avg_vehicle_stddev_interval_seconds is non-negative when populated.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="system_avg_vehicle_stddev_interval_seconds_non_negative_when_populated",
        validation_description=(
            "system_avg_vehicle_stddev_interval_seconds must be greater than or "
            "equal to 0 when populated."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("system_avg_vehicle_stddev_interval_seconds").isNotNull()
            & (F.col("system_avg_vehicle_stddev_interval_seconds") < 0)
        ),
    )


def _validate_avg_continuity_support_ratio_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_continuity_support_ratio is non-negative.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="avg_continuity_support_ratio_non_negative",
        validation_description=(
            "avg_continuity_support_ratio must be greater than or equal to 0."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("avg_continuity_support_ratio").isNull()
            | (F.col("avg_continuity_support_ratio") < 0)
        ),
    )


def _validate_avg_continuity_support_ratio_less_than_or_equal_to_one(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_continuity_support_ratio is less than or equal to 1.
    """
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="avg_continuity_support_ratio_less_than_or_equal_to_one",
        validation_description=(
            "avg_continuity_support_ratio must be less than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("avg_continuity_support_ratio").isNotNull()
            & (F.col("avg_continuity_support_ratio") > 1)
        ),
    )


def _validate_vehicles_count_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that vehicles_count reconciles with the lower-level joined inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="vehicles_count_reconciles_with_lower_level_inputs",
        validation_description=(
            "vehicles_count must reconcile with the distinct vehicle count of "
            "the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=F.col("vehicles_count") != F.col("expected_vehicles_count"),
    )


def _validate_total_observations_count_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_observations_count reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="total_observations_count_reconciles_with_lower_level_inputs",
        validation_description=(
            "total_observations_count must reconcile with the sum of updates_count "
            "from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_observations_count")
            != F.col("expected_total_observations_count")
        ),
    )


def _validate_total_measured_intervals_count_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_measured_intervals_count reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="total_measured_intervals_count_reconciles_with_lower_level_inputs",
        validation_description=(
            "total_measured_intervals_count must reconcile with the sum of "
            "measured_intervals_count from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_measured_intervals_count")
            != F.col("expected_total_measured_intervals_count")
        ),
    )


def _validate_system_avg_update_interval_seconds_reconciles_with_weighted_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_avg_update_interval_seconds reconciles with the weighted lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="system_avg_update_interval_seconds_reconciles_with_weighted_inputs",
        validation_description=(
            "system_avg_update_interval_seconds must reconcile with the weighted "
            "average derived from lower-level inputs using measured_intervals_count."
        ),
        severity="ERROR",
        violation_condition=(
            (
                (F.col("total_measured_intervals_count") > 0)
                & (
                    F.col("system_avg_update_interval_seconds").isNull()
                    | F.col("expected_system_avg_update_interval_seconds").isNull()
                    | (
                        F.abs(
                            F.col("system_avg_update_interval_seconds")
                            - F.col("expected_system_avg_update_interval_seconds")
                        )
                        > F.lit(FLOAT_TOLERANCE)
                    )
                )
            )
            | (
                (F.col("total_measured_intervals_count") == 0)
                & F.col("system_avg_update_interval_seconds").isNotNull()
            )
        ),
    )


def _validate_system_min_update_interval_seconds_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_min_update_interval_seconds reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="system_min_update_interval_seconds_reconciles_with_lower_level_inputs",
        validation_description=(
            "system_min_update_interval_seconds must reconcile with the minimum "
            "vehicle-level interval from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            (
                (F.col("total_measured_intervals_count") > 0)
                & (
                    F.col("system_min_update_interval_seconds").isNull()
                    | F.col("expected_system_min_update_interval_seconds").isNull()
                    | (
                        F.abs(
                            F.col("system_min_update_interval_seconds")
                            - F.col("expected_system_min_update_interval_seconds")
                        )
                        > F.lit(FLOAT_TOLERANCE)
                    )
                )
            )
            | (
                (F.col("total_measured_intervals_count") == 0)
                & F.col("system_min_update_interval_seconds").isNotNull()
            )
        ),
    )


def _validate_system_max_update_interval_seconds_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_max_update_interval_seconds reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="system_max_update_interval_seconds_reconciles_with_lower_level_inputs",
        validation_description=(
            "system_max_update_interval_seconds must reconcile with the maximum "
            "vehicle-level interval from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            (
                (F.col("total_measured_intervals_count") > 0)
                & (
                    F.col("system_max_update_interval_seconds").isNull()
                    | F.col("expected_system_max_update_interval_seconds").isNull()
                    | (
                        F.abs(
                            F.col("system_max_update_interval_seconds")
                            - F.col("expected_system_max_update_interval_seconds")
                        )
                        > F.lit(FLOAT_TOLERANCE)
                    )
                )
            )
            | (
                (F.col("total_measured_intervals_count") == 0)
                & F.col("system_max_update_interval_seconds").isNotNull()
            )
        ),
    )


def _validate_system_avg_vehicle_stddev_interval_seconds_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that system_avg_vehicle_stddev_interval_seconds reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="system_avg_vehicle_stddev_interval_seconds_reconciles_with_lower_level_inputs",
        validation_description=(
            "system_avg_vehicle_stddev_interval_seconds must reconcile with the "
            "average of vehicle-level stddev_update_interval_seconds from the "
            "lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("system_avg_vehicle_stddev_interval_seconds").isNull()
            != F.col("expected_system_avg_vehicle_stddev_interval_seconds").isNull()
        )
        | (
            F.col("system_avg_vehicle_stddev_interval_seconds").isNotNull()
            & F.col("expected_system_avg_vehicle_stddev_interval_seconds").isNotNull()
            & (
                F.abs(
                    F.col("system_avg_vehicle_stddev_interval_seconds")
                    - F.col("expected_system_avg_vehicle_stddev_interval_seconds")
                )
                > F.lit(FLOAT_TOLERANCE)
            )
        ),
    )


def _validate_total_distinct_minutes_with_data_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_distinct_minutes_with_data reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="total_distinct_minutes_with_data_reconciles_with_lower_level_inputs",
        validation_description=(
            "total_distinct_minutes_with_data must reconcile with the sum of "
            "distinct_minutes_with_data from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_distinct_minutes_with_data")
            != F.col("expected_total_distinct_minutes_with_data")
        ),
    )


def _validate_avg_continuity_support_ratio_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that avg_continuity_support_ratio reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="avg_continuity_support_ratio_reconciles_with_lower_level_inputs",
        validation_description=(
            "avg_continuity_support_ratio must reconcile with the average "
            "continuity_support_ratio from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("avg_continuity_support_ratio").isNull()
            != F.col("expected_avg_continuity_support_ratio").isNull()
        )
        | (
            F.col("avg_continuity_support_ratio").isNotNull()
            & F.col("expected_avg_continuity_support_ratio").isNotNull()
            & (
                F.abs(
                    F.col("avg_continuity_support_ratio")
                    - F.col("expected_avg_continuity_support_ratio")
                )
                > F.lit(FLOAT_TOLERANCE)
            )
        ),
    )


def _validate_total_large_gaps_count_reconciles_with_lower_level_inputs(
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Validate that total_large_gaps_count reconciles with lower-level inputs.
    """
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="AGGREGATION_CONSISTENCY",
        validation_rule="total_large_gaps_count_reconciles_with_lower_level_inputs",
        validation_description=(
            "total_large_gaps_count must reconcile with the sum of large_gaps_count "
            "from the lower-level joined inputs."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_large_gaps_count")
            != F.col("expected_total_large_gaps_count")
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    """
    Validate that the system-wide update behavior table is not empty.
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
            F.lit("gold_system_wide_update_behavior must contain at least one row."),
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


def _validate_contains_at_least_one_row_with_measurable_interval_support(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one row with measurable interval support.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(F.when(F.col("total_measured_intervals_count") > 0, 1)) > 0,
                F.lit(0),
            ).otherwise(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_row_with_measurable_interval_support"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_system_wide_update_behavior must contain at least one row "
                "with total_measured_intervals_count > 0."
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


def _validate_contains_at_least_one_valid_system_wide_summary_row(
    validated_df: DataFrame,
) -> DataFrame:
    """
    Validate that the table contains at least one valid system-wide summary row.
    """
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("aggregation_scope").isNotNull()
                        & (F.trim(F.col("aggregation_scope")) != "")
                        & F.col("vehicles_count").isNotNull()
                        & F.col("total_observations_count").isNotNull()
                        & F.col("total_measured_intervals_count").isNotNull()
                        & F.col("total_distinct_minutes_with_data").isNotNull()
                        & F.col("total_large_gaps_count").isNotNull(),
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
            F.lit("contains_at_least_one_valid_system_wide_summary_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_system_wide_update_behavior must contain at least one valid "
                "system-wide summary row."
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
