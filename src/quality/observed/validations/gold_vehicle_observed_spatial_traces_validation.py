"""
Gold vehicle observed spatial traces validation pipeline.

Purpose:
    Validate whether gold_vehicle_observed_spatial_traces is structurally and
    logically acceptable for downstream use as the canonical observed trace
    layer in the Observed Spatial Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric and sequencing validation
    E. Reference usability validation

Output:
    - gold.gold_vehicle_observed_spatial_traces_validation

Important note:
    This pipeline does not modify or block the Gold table itself.
    It produces a formal validation output with one row per validation rule,
    including severity, status, and violation count.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


VALIDATED_TABLE_NAME = "main.gold.gold_vehicle_observed_spatial_traces"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observed_spatial_traces_validation"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_vehicle_observed_spatial_traces with one row "
        "per validation rule, including category, severity, pass/fail status, "
        "violation count, and execution timestamp."
    ),
)
def gold_vehicle_observed_spatial_traces_validation() -> DataFrame:
    """
    Materialize the validation results for gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    consistency_df = _build_trace_consistency_df(validated_df)

    return _build_gold_vehicle_observed_spatial_traces_validation(
        validated_df,
        consistency_df,
    )


def _build_gold_vehicle_observed_spatial_traces_validation(
    validated_df: DataFrame,
    consistency_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for gold_vehicle_observed_spatial_traces.

    Args:
        validated_df:
            Input DataFrame from gold.gold_vehicle_observed_spatial_traces.
        consistency_df:
            Trace-consistency DataFrame derived from the validated table.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_vehicle_prefix_not_null(validated_df),
        _validate_vehicle_prefix_not_blank(validated_df),
        _validate_trace_sequence_not_null(validated_df),
        _validate_captured_at_local_not_null(validated_df),
        _validate_latitude_not_null(validated_df),
        _validate_longitude_not_null(validated_df),
        _validate_unique_analysis_date_vehicle_prefix_trace_sequence(validated_df),
        _validate_trace_sequence_assigned_in_local_timestamp_order(consistency_df),
        _validate_analysis_date_matches_captured_at_local_date(consistency_df),
        _validate_latitude_valid_range(consistency_df),
        _validate_longitude_valid_range(consistency_df),
        _validate_trace_sequence_minimum(validated_df),
        _validate_captured_at_local_non_decreasing_by_trace_sequence(consistency_df),
        _validate_previous_captured_at_local_matches_actual_prior_row(consistency_df),
        _validate_previous_latitude_matches_actual_prior_row(consistency_df),
        _validate_previous_longitude_matches_actual_prior_row(consistency_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_contains_at_least_one_vehicle_with_more_than_one_trace_point(
            validated_df
        ),
        _validate_contains_at_least_one_valid_trace_row(validated_df),
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


def _build_trace_consistency_df(validated_df: DataFrame) -> DataFrame:
    """
    Build a trace-consistency DataFrame used to validate ordering semantics and
    previous-point helper fields.

    Logic:
        1. Recompute the expected trace_sequence using the same deterministic
           ordering rule used by the Gold table.
        2. Recompute the actual prior-row helper values using trace_sequence
           order inside each analysis_date + vehicle_prefix trace.
        3. Expose comparison fields for validation rules.

    Deterministic expected-order rule:
        - captured_at_local ascending
        - latitude ascending
        - longitude ascending
        - line_sign ascending
        - direction ascending
        - direction_description ascending

    Args:
        validated_df:
            Input DataFrame from gold.gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Consistency DataFrame with expected sequencing and prior-row fields.
    """
    sequence_window = Window.partitionBy("analysis_date", "vehicle_prefix").orderBy(
        F.col("trace_sequence").asc()
    )

    expected_order_window = Window.partitionBy("analysis_date", "vehicle_prefix").orderBy(
        F.col("captured_at_local").asc(),
        F.col("latitude").asc(),
        F.col("longitude").asc(),
        F.col("line_sign").asc_nulls_last(),
        F.col("direction").asc_nulls_last(),
        F.col("direction_description").asc_nulls_last(),
    )

    return (
        validated_df.withColumn(
            "expected_trace_sequence",
            F.row_number().over(expected_order_window).cast("long"),
        )
        .withColumn(
            "expected_previous_captured_at_local",
            F.lag("captured_at_local").over(sequence_window),
        )
        .withColumn(
            "expected_previous_latitude",
            F.lag("latitude").over(sequence_window),
        )
        .withColumn(
            "expected_previous_longitude",
            F.lag("longitude").over(sequence_window),
        )
        .withColumn(
            "trace_points_in_vehicle_day",
            F.count(F.lit(1)).over(
                Window.partitionBy("analysis_date", "vehicle_prefix")
            ).cast("long"),
        )
        .withColumn(
            "captured_at_local_date",
            F.to_date(F.col("captured_at_local")),
        )
    )


def _validate_analysis_date_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="analysis_date_not_null",
        validation_description="analysis_date must not be null.",
        severity="ERROR",
        violation_condition=F.col("analysis_date").isNull(),
    )


def _validate_vehicle_prefix_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="vehicle_prefix_not_null",
        validation_description="vehicle_prefix must not be null.",
        severity="ERROR",
        violation_condition=F.col("vehicle_prefix").isNull(),
    )


def _validate_vehicle_prefix_not_blank(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="vehicle_prefix_not_blank",
        validation_description="vehicle_prefix must not be blank after trimming.",
        severity="ERROR",
        violation_condition=F.trim(F.col("vehicle_prefix")) == "",
    )


def _validate_trace_sequence_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="trace_sequence_not_null",
        validation_description="trace_sequence must not be null.",
        severity="ERROR",
        violation_condition=F.col("trace_sequence").isNull(),
    )


def _validate_captured_at_local_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="captured_at_local_not_null",
        validation_description="captured_at_local must not be null.",
        severity="ERROR",
        violation_condition=F.col("captured_at_local").isNull(),
    )


def _validate_latitude_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="latitude_not_null",
        validation_description="latitude must not be null.",
        severity="ERROR",
        violation_condition=F.col("latitude").isNull(),
    )


def _validate_longitude_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="longitude_not_null",
        validation_description="longitude must not be null.",
        severity="ERROR",
        violation_condition=F.col("longitude").isNull(),
    )


def _validate_unique_analysis_date_vehicle_prefix_trace_sequence(
    validated_df: DataFrame,
) -> DataFrame:
    duplicates_df = (
        validated_df.groupBy("analysis_date", "vehicle_prefix", "trace_sequence")
        .count()
        .where(F.col("count") > 1)
    )

    return (
        duplicates_df.agg(
            F.count(F.lit(1)).cast("long").alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("GRAIN"))
        .withColumn(
            "validation_rule",
            F.lit("unique_analysis_date_vehicle_prefix_trace_sequence"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "There must be one and only one row per analysis_date, "
                "vehicle_prefix, and trace_sequence."
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


def _validate_trace_sequence_assigned_in_local_timestamp_order(
    consistency_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="trace_sequence_assigned_in_local_timestamp_order",
        validation_description=(
            "trace_sequence must match the deterministic local timestamp ordering "
            "used by the Gold trace logic."
        ),
        severity="ERROR",
        violation_condition=F.col("trace_sequence") != F.col("expected_trace_sequence"),
    )


def _validate_analysis_date_matches_captured_at_local_date(
    consistency_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="analysis_date_matches_captured_at_local_date",
        validation_description=(
            "analysis_date must match the local date derived from captured_at_local."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("captured_at_local").isNotNull()
            & F.col("analysis_date").isNotNull()
            & (F.col("analysis_date") != F.col("captured_at_local_date"))
        ),
    )


def _validate_latitude_valid_range(consistency_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="latitude_valid_range",
        validation_description="latitude must be between -90 and 90 inclusive.",
        severity="ERROR",
        violation_condition=(
            F.col("latitude").isNull() | ~F.col("latitude").between(-90.0, 90.0)
        ),
    )


def _validate_longitude_valid_range(consistency_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="BUSINESS_RULE",
        validation_rule="longitude_valid_range",
        validation_description="longitude must be between -180 and 180 inclusive.",
        severity="ERROR",
        violation_condition=(
            F.col("longitude").isNull() | ~F.col("longitude").between(-180.0, 180.0)
        ),
    )


def _validate_trace_sequence_minimum(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC_AND_SEQUENCING",
        validation_rule="trace_sequence_minimum",
        validation_description="trace_sequence must be greater than or equal to 1.",
        severity="ERROR",
        violation_condition=(
            F.col("trace_sequence").isNull() | (F.col("trace_sequence") < 1)
        ),
    )


def _validate_captured_at_local_non_decreasing_by_trace_sequence(
    consistency_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="METRIC_AND_SEQUENCING",
        validation_rule="captured_at_local_non_decreasing_by_trace_sequence",
        validation_description=(
            "Within the same analysis_date + vehicle_prefix trace, "
            "captured_at_local must be non-decreasing by trace_sequence."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("expected_previous_captured_at_local").isNotNull()
            & (F.col("captured_at_local") < F.col("expected_previous_captured_at_local"))
        ),
    )


def _validate_previous_captured_at_local_matches_actual_prior_row(
    consistency_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="METRIC_AND_SEQUENCING",
        validation_rule="previous_captured_at_local_matches_actual_prior_row",
        validation_description=(
            "previous_captured_at_local must match the actual prior trace row "
            "within analysis_date + vehicle_prefix."
        ),
        severity="ERROR",
        violation_condition=(
            (
                F.col("expected_previous_captured_at_local").isNull()
                & F.col("previous_captured_at_local").isNotNull()
            )
            | (
                F.col("expected_previous_captured_at_local").isNotNull()
                & F.col("previous_captured_at_local").isNull()
            )
            | (
                F.col("expected_previous_captured_at_local").isNotNull()
                & F.col("previous_captured_at_local").isNotNull()
                & (
                    F.col("previous_captured_at_local")
                    != F.col("expected_previous_captured_at_local")
                )
            )
        ),
    )


def _validate_previous_latitude_matches_actual_prior_row(
    consistency_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="METRIC_AND_SEQUENCING",
        validation_rule="previous_latitude_matches_actual_prior_row",
        validation_description=(
            "previous_latitude must match the latitude of the actual prior trace row "
            "within analysis_date + vehicle_prefix."
        ),
        severity="ERROR",
        violation_condition=(
            (
                F.col("expected_previous_latitude").isNull()
                & F.col("previous_latitude").isNotNull()
            )
            | (
                F.col("expected_previous_latitude").isNotNull()
                & F.col("previous_latitude").isNull()
            )
            | (
                F.col("expected_previous_latitude").isNotNull()
                & F.col("previous_latitude").isNotNull()
                & (F.col("previous_latitude") != F.col("expected_previous_latitude"))
            )
        ),
    )


def _validate_previous_longitude_matches_actual_prior_row(
    consistency_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=consistency_df,
        validation_category="METRIC_AND_SEQUENCING",
        validation_rule="previous_longitude_matches_actual_prior_row",
        validation_description=(
            "previous_longitude must match the longitude of the actual prior trace row "
            "within analysis_date + vehicle_prefix."
        ),
        severity="ERROR",
        violation_condition=(
            (
                F.col("expected_previous_longitude").isNull()
                & F.col("previous_longitude").isNotNull()
            )
            | (
                F.col("expected_previous_longitude").isNotNull()
                & F.col("previous_longitude").isNull()
            )
            | (
                F.col("expected_previous_longitude").isNotNull()
                & F.col("previous_longitude").isNotNull()
                & (F.col("previous_longitude") != F.col("expected_previous_longitude"))
            )
        ),
    )


def _validate_reference_table_not_empty(validated_df: DataFrame) -> DataFrame:
    return (
        validated_df.agg(
            F.when(F.count(F.lit(1)) > 0, F.lit(0))
            .otherwise(F.lit(1))
            .cast("long")
            .alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn("validation_rule", F.lit("reference_table_not_empty"))
        .withColumn(
            "validation_description",
            F.lit("gold_vehicle_observed_spatial_traces must contain at least one row."),
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


def _validate_contains_at_least_one_vehicle_with_more_than_one_trace_point(
    validated_df: DataFrame,
) -> DataFrame:
    vehicle_day_counts_df = (
        validated_df.groupBy("analysis_date", "vehicle_prefix")
        .agg(F.count(F.lit(1)).cast("long").alias("trace_points_count"))
    )

    return (
        vehicle_day_counts_df.agg(
            F.when(
                F.count(F.when(F.col("trace_points_count") > 1, 1)) > 0,
                F.lit(0),
            )
            .otherwise(F.lit(1))
            .cast("long")
            .alias("violation_count"),
            F.count(F.lit(1)).cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_vehicle_with_more_than_one_trace_point"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "The table must contain at least one analysis_date + vehicle_prefix "
                "trace with more than one trace point."
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


def _validate_contains_at_least_one_valid_trace_row(
    validated_df: DataFrame,
) -> DataFrame:
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("analysis_date").isNotNull()
                        & F.col("vehicle_prefix").isNotNull()
                        & (F.trim(F.col("vehicle_prefix")) != "")
                        & F.col("trace_sequence").isNotNull()
                        & (F.col("trace_sequence") >= 1)
                        & F.col("captured_at_local").isNotNull()
                        & F.col("latitude").isNotNull()
                        & F.col("longitude").isNotNull(),
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
        .withColumn(
            "validation_rule",
            F.lit("contains_at_least_one_valid_trace_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_vehicle_observed_spatial_traces must contain at least one "
                "valid observed trace row."
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
