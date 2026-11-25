"""
Gold observed spatial concentration summary validation pipeline.

Purpose:
    Validate whether gold_observed_spatial_concentration_summary is structurally
    and logically acceptable for downstream use as the canonical daily spatial
    concentration summary layer in the Observed Spatial Analysis component.

Validation scope:
    A. Structural validation
    B. Grain validation
    C. Metric validation
    D. Reconciliation validation
    E. Reference usability validation

Output:
    - gold.gold_observed_spatial_concentration_summary_validation

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


VALIDATED_TABLE_NAME = "main.gold.gold_observed_spatial_concentration_summary"
DETAIL_GOLD_SOURCE_TABLE = "main.gold.gold_observed_spatial_distribution"
OUTPUT_TABLE_NAME = "gold.gold_observed_spatial_concentration_summary_validation"

FLOAT_TOLERANCE = 1e-9


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Validation table for gold_observed_spatial_concentration_summary with "
        "one row per validation rule, including category, severity, pass/fail "
        "status, violation count, and execution timestamp."
    ),
)
def gold_observed_spatial_concentration_summary_validation() -> DataFrame:
    """
    Materialize the validation results for
    gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Validation results with one row per validation rule.
    """
    validated_df = spark.read.table(VALIDATED_TABLE_NAME)
    detail_df = spark.read.table(DETAIL_GOLD_SOURCE_TABLE)

    return _build_gold_observed_spatial_concentration_summary_validation(
        validated_df,
        detail_df,
    )


def _build_gold_observed_spatial_concentration_summary_validation(
    validated_df: DataFrame,
    detail_df: DataFrame,
) -> DataFrame:
    """
    Build the validation result table for
    gold_observed_spatial_concentration_summary.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.
        detail_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Validation result rows with pass/fail status and violation counts.
    """
    expected_daily_df = _build_expected_daily_summary_from_detail(detail_df)
    reconciliation_df = _build_reconciliation_df(validated_df, expected_daily_df)

    validation_dfs = [
        _validate_analysis_date_not_null(validated_df),
        _validate_active_spatial_cells_count_not_null(validated_df),
        _validate_total_observed_position_records_count_not_null(validated_df),
        _validate_avg_positions_per_active_cell_not_null(validated_df),
        _validate_max_positions_in_single_cell_not_null(validated_df),
        _validate_max_distinct_observed_vehicles_in_single_cell_not_null(validated_df),
        _validate_max_distinct_observed_lines_in_single_cell_not_null(validated_df),
        _validate_top_cell_position_share_ratio_not_null(validated_df),
        _validate_top_5_cells_position_share_ratio_not_null(validated_df),
        _validate_unique_analysis_date(validated_df),
        _validate_active_spatial_cells_count_minimum(validated_df),
        _validate_total_observed_position_records_count_minimum(validated_df),
        _validate_avg_positions_per_active_cell_positive(validated_df),
        _validate_max_positions_in_single_cell_minimum(validated_df),
        _validate_max_distinct_observed_vehicles_in_single_cell_non_negative(
            validated_df
        ),
        _validate_max_distinct_observed_lines_in_single_cell_non_negative(
            validated_df
        ),
        _validate_top_cell_position_share_ratio_between_zero_and_one(validated_df),
        _validate_top_5_cells_position_share_ratio_between_zero_and_one(validated_df),
        _validate_top_5_cells_position_share_ratio_greater_than_or_equal_to_top_cell(
            validated_df
        ),
        _validate_total_observed_position_records_count_reconciles(
            reconciliation_df
        ),
        _validate_active_spatial_cells_count_reconciles(reconciliation_df),
        _validate_max_positions_in_single_cell_reconciles(reconciliation_df),
        _validate_avg_positions_per_active_cell_reconciles(reconciliation_df),
        _validate_top_cell_position_share_ratio_reconciles(reconciliation_df),
        _validate_top_5_cells_position_share_ratio_reconciles(reconciliation_df),
        _validate_reference_table_not_empty(validated_df),
        _validate_total_analysis_date_count_greater_than_zero(validated_df),
        _validate_contains_at_least_one_valid_daily_concentration_summary_row(
            validated_df
        ),
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


def _prepare_summary_source(summary_df: DataFrame) -> DataFrame:
    """
    Prepare the summary Gold source for reconciliation checks.

    Args:
        summary_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.

    Returns:
        DataFrame:
            Prepared summary dataset with the fields required for reconciliation.
    """
    return summary_df.select(
        F.col("analysis_date").cast("date").alias("analysis_date"),
        F.col("active_spatial_cells_count").cast("long").alias(
            "actual_active_spatial_cells_count"
        ),
        F.col("total_observed_position_records_count").cast("long").alias(
            "actual_total_observed_position_records_count"
        ),
        F.col("avg_positions_per_active_cell").cast("double").alias(
            "actual_avg_positions_per_active_cell"
        ),
        F.col("max_positions_in_single_cell").cast("long").alias(
            "actual_max_positions_in_single_cell"
        ),
        F.col("top_cell_position_share_ratio").cast("double").alias(
            "actual_top_cell_position_share_ratio"
        ),
        F.col("top_5_cells_position_share_ratio").cast("double").alias(
            "actual_top_5_cells_position_share_ratio"
        ),
    )


def _prepare_detail_source(detail_df: DataFrame) -> DataFrame:
    """
    Prepare the detailed spatial distribution Gold source for reconciliation checks.

    Args:
        detail_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Prepared detailed spatial distribution dataset.
    """
    return detail_df.select(
        F.col("analysis_date").cast("date").alias("analysis_date"),
        F.col("spatial_cell_id").cast("string").alias("spatial_cell_id"),
        F.col("observed_position_records_count").cast("long").alias(
            "observed_position_records_count"
        ),
    )


def _build_expected_daily_summary_from_detail(detail_df: DataFrame) -> DataFrame:
    """
    Build the expected daily summary metrics directly from the detailed spatial
    distribution Gold source.

    Args:
        detail_df:
            Input DataFrame from gold.gold_observed_spatial_distribution.

    Returns:
        DataFrame:
            Expected daily summary metrics derived from the detailed Gold source.
    """
    prepared_detail_df = _prepare_detail_source(detail_df)

    ranked_df = prepared_detail_df.withColumn(
        "spatial_concentration_rank",
        F.row_number().over(
            Window.partitionBy("analysis_date").orderBy(
                F.col("observed_position_records_count").desc(),
                F.col("spatial_cell_id").asc(),
            )
        ),
    )

    return (
        ranked_df.groupBy("analysis_date")
        .agg(
            F.countDistinct("spatial_cell_id").cast("long").alias(
                "expected_active_spatial_cells_count"
            ),
            F.coalesce(F.sum("observed_position_records_count"), F.lit(0))
            .cast("long")
            .alias("expected_total_observed_position_records_count"),
            F.max("observed_position_records_count").cast("long").alias(
                "expected_max_positions_in_single_cell"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("spatial_concentration_rank") == 1,
                        F.col("observed_position_records_count"),
                    ).otherwise(F.lit(0))
                ),
                F.lit(0),
            )
            .cast("long")
            .alias("expected_top_1_cells_observed_position_records_count"),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("spatial_concentration_rank") <= 5,
                        F.col("observed_position_records_count"),
                    ).otherwise(F.lit(0))
                ),
                F.lit(0),
            )
            .cast("long")
            .alias("expected_top_5_cells_observed_position_records_count"),
        )
        .withColumn(
            "expected_avg_positions_per_active_cell",
            F.when(
                F.col("expected_active_spatial_cells_count") > 0,
                (
                    F.col("expected_total_observed_position_records_count").cast(
                        "double"
                    )
                    / F.col("expected_active_spatial_cells_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "expected_top_cell_position_share_ratio",
            F.when(
                F.col("expected_total_observed_position_records_count") > 0,
                (
                    F.col(
                        "expected_top_1_cells_observed_position_records_count"
                    ).cast("double")
                    / F.col("expected_total_observed_position_records_count").cast(
                        "double"
                    )
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "expected_top_5_cells_position_share_ratio",
            F.when(
                F.col("expected_total_observed_position_records_count") > 0,
                (
                    F.col(
                        "expected_top_5_cells_observed_position_records_count"
                    ).cast("double")
                    / F.col("expected_total_observed_position_records_count").cast(
                        "double"
                    )
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def _build_reconciliation_df(
    validated_df: DataFrame,
    expected_daily_df: DataFrame,
) -> DataFrame:
    """
    Build a reconciliation DataFrame between actual summary output and the
    expected summary derived from the detailed Gold source.

    Args:
        validated_df:
            Input DataFrame from gold.gold_observed_spatial_concentration_summary.
        expected_daily_df:
            Expected daily summary metrics derived from the detailed Gold source.

    Returns:
        DataFrame:
            Reconciliation DataFrame with actual and expected fields.
    """
    actual_df = _prepare_summary_source(validated_df)

    return actual_df.join(
        expected_daily_df,
        on="analysis_date",
        how="full",
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


def _validate_active_spatial_cells_count_not_null(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="active_spatial_cells_count_not_null",
        validation_description="active_spatial_cells_count must not be null.",
        severity="ERROR",
        violation_condition=F.col("active_spatial_cells_count").isNull(),
    )


def _validate_total_observed_position_records_count_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="total_observed_position_records_count_not_null",
        validation_description=(
            "total_observed_position_records_count must not be null."
        ),
        severity="ERROR",
        violation_condition=F.col("total_observed_position_records_count").isNull(),
    )


def _validate_avg_positions_per_active_cell_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="avg_positions_per_active_cell_not_null",
        validation_description="avg_positions_per_active_cell must not be null.",
        severity="ERROR",
        violation_condition=F.col("avg_positions_per_active_cell").isNull(),
    )


def _validate_max_positions_in_single_cell_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="max_positions_in_single_cell_not_null",
        validation_description="max_positions_in_single_cell must not be null.",
        severity="ERROR",
        violation_condition=F.col("max_positions_in_single_cell").isNull(),
    )


def _validate_max_distinct_observed_vehicles_in_single_cell_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="max_distinct_observed_vehicles_in_single_cell_not_null",
        validation_description=(
            "max_distinct_observed_vehicles_in_single_cell must not be null."
        ),
        severity="ERROR",
        violation_condition=F.col("max_distinct_observed_vehicles_in_single_cell").isNull(),
    )


def _validate_max_distinct_observed_lines_in_single_cell_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="max_distinct_observed_lines_in_single_cell_not_null",
        validation_description=(
            "max_distinct_observed_lines_in_single_cell must not be null."
        ),
        severity="ERROR",
        violation_condition=F.col("max_distinct_observed_lines_in_single_cell").isNull(),
    )


def _validate_top_cell_position_share_ratio_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="top_cell_position_share_ratio_not_null",
        validation_description="top_cell_position_share_ratio must not be null.",
        severity="ERROR",
        violation_condition=F.col("top_cell_position_share_ratio").isNull(),
    )


def _validate_top_5_cells_position_share_ratio_not_null(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="STRUCTURAL",
        validation_rule="top_5_cells_position_share_ratio_not_null",
        validation_description="top_5_cells_position_share_ratio must not be null.",
        severity="ERROR",
        violation_condition=F.col("top_5_cells_position_share_ratio").isNull(),
    )


def _validate_unique_analysis_date(validated_df: DataFrame) -> DataFrame:
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


def _validate_active_spatial_cells_count_minimum(validated_df: DataFrame) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="active_spatial_cells_count_minimum",
        validation_description=(
            "active_spatial_cells_count must be greater than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("active_spatial_cells_count").isNull()
            | (F.col("active_spatial_cells_count") < 1)
        ),
    )


def _validate_total_observed_position_records_count_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="total_observed_position_records_count_minimum",
        validation_description=(
            "total_observed_position_records_count must be greater than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("total_observed_position_records_count").isNull()
            | (F.col("total_observed_position_records_count") < 1)
        ),
    )


def _validate_avg_positions_per_active_cell_positive(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="avg_positions_per_active_cell_positive",
        validation_description="avg_positions_per_active_cell must be greater than 0.",
        severity="ERROR",
        violation_condition=(
            F.col("avg_positions_per_active_cell").isNull()
            | (F.col("avg_positions_per_active_cell") <= 0)
        ),
    )


def _validate_max_positions_in_single_cell_minimum(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_positions_in_single_cell_minimum",
        validation_description=(
            "max_positions_in_single_cell must be greater than or equal to 1."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_positions_in_single_cell").isNull()
            | (F.col("max_positions_in_single_cell") < 1)
        ),
    )


def _validate_max_distinct_observed_vehicles_in_single_cell_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_distinct_observed_vehicles_in_single_cell_non_negative",
        validation_description=(
            "max_distinct_observed_vehicles_in_single_cell must be greater than "
            "or equal to 0."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_distinct_observed_vehicles_in_single_cell").isNull()
            | (F.col("max_distinct_observed_vehicles_in_single_cell") < 0)
        ),
    )


def _validate_max_distinct_observed_lines_in_single_cell_non_negative(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="max_distinct_observed_lines_in_single_cell_non_negative",
        validation_description=(
            "max_distinct_observed_lines_in_single_cell must be greater than or "
            "equal to 0."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("max_distinct_observed_lines_in_single_cell").isNull()
            | (F.col("max_distinct_observed_lines_in_single_cell") < 0)
        ),
    )


def _validate_top_cell_position_share_ratio_between_zero_and_one(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="top_cell_position_share_ratio_between_zero_and_one",
        validation_description=(
            "top_cell_position_share_ratio must be between 0 and 1 inclusive."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("top_cell_position_share_ratio").isNull()
            | (F.col("top_cell_position_share_ratio") < 0)
            | (F.col("top_cell_position_share_ratio") > 1)
        ),
    )


def _validate_top_5_cells_position_share_ratio_between_zero_and_one(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="top_5_cells_position_share_ratio_between_zero_and_one",
        validation_description=(
            "top_5_cells_position_share_ratio must be between 0 and 1 inclusive."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("top_5_cells_position_share_ratio").isNull()
            | (F.col("top_5_cells_position_share_ratio") < 0)
            | (F.col("top_5_cells_position_share_ratio") > 1)
        ),
    )


def _validate_top_5_cells_position_share_ratio_greater_than_or_equal_to_top_cell(
    validated_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=validated_df,
        validation_category="METRIC",
        validation_rule="top_5_cells_position_share_ratio_greater_than_or_equal_to_top_cell",
        validation_description=(
            "top_5_cells_position_share_ratio must be greater than or equal to "
            "top_cell_position_share_ratio."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("top_5_cells_position_share_ratio").isNotNull()
            & F.col("top_cell_position_share_ratio").isNotNull()
            & (
                F.col("top_5_cells_position_share_ratio")
                < F.col("top_cell_position_share_ratio")
            )
        ),
    )


def _validate_total_observed_position_records_count_reconciles(
    reconciliation_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="total_observed_position_records_count_reconciles",
        validation_description=(
            "total_observed_position_records_count must equal the sum of "
            "observed_position_records_count from the detailed spatial distribution "
            "source for the same analysis_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_total_observed_position_records_count").isNull()
            | F.col("expected_total_observed_position_records_count").isNull()
            | (
                F.col("actual_total_observed_position_records_count")
                != F.col("expected_total_observed_position_records_count")
            )
        ),
    )


def _validate_active_spatial_cells_count_reconciles(
    reconciliation_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="active_spatial_cells_count_reconciles",
        validation_description=(
            "active_spatial_cells_count must equal the number of distinct "
            "spatial_cell_id values upstream for the same analysis_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_active_spatial_cells_count").isNull()
            | F.col("expected_active_spatial_cells_count").isNull()
            | (
                F.col("actual_active_spatial_cells_count")
                != F.col("expected_active_spatial_cells_count")
            )
        ),
    )


def _validate_max_positions_in_single_cell_reconciles(
    reconciliation_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="max_positions_in_single_cell_reconciles",
        validation_description=(
            "max_positions_in_single_cell must match the upstream maximum "
            "observed_position_records_count for the same analysis_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_max_positions_in_single_cell").isNull()
            | F.col("expected_max_positions_in_single_cell").isNull()
            | (
                F.col("actual_max_positions_in_single_cell")
                != F.col("expected_max_positions_in_single_cell")
            )
        ),
    )


def _validate_avg_positions_per_active_cell_reconciles(
    reconciliation_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="avg_positions_per_active_cell_reconciles",
        validation_description=(
            "avg_positions_per_active_cell must reconcile with the detailed "
            "spatial distribution source for the same analysis_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_avg_positions_per_active_cell").isNull()
            | F.col("expected_avg_positions_per_active_cell").isNull()
            | (
                F.abs(
                    F.col("actual_avg_positions_per_active_cell")
                    - F.col("expected_avg_positions_per_active_cell")
                )
                > F.lit(FLOAT_TOLERANCE)
            )
        ),
    )


def _validate_top_cell_position_share_ratio_reconciles(
    reconciliation_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="top_cell_position_share_ratio_reconciles",
        validation_description=(
            "top_cell_position_share_ratio must reconcile with the detailed "
            "spatial distribution source for the same analysis_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_top_cell_position_share_ratio").isNull()
            | F.col("expected_top_cell_position_share_ratio").isNull()
            | (
                F.abs(
                    F.col("actual_top_cell_position_share_ratio")
                    - F.col("expected_top_cell_position_share_ratio")
                )
                > F.lit(FLOAT_TOLERANCE)
            )
        ),
    )


def _validate_top_5_cells_position_share_ratio_reconciles(
    reconciliation_df: DataFrame,
) -> DataFrame:
    return _build_validation_result(
        validated_df=reconciliation_df,
        validation_category="RECONCILIATION",
        validation_rule="top_5_cells_position_share_ratio_reconciles",
        validation_description=(
            "top_5_cells_position_share_ratio must reconcile with the detailed "
            "spatial distribution source for the same analysis_date."
        ),
        severity="ERROR",
        violation_condition=(
            F.col("actual_top_5_cells_position_share_ratio").isNull()
            | F.col("expected_top_5_cells_position_share_ratio").isNull()
            | (
                F.abs(
                    F.col("actual_top_5_cells_position_share_ratio")
                    - F.col("expected_top_5_cells_position_share_ratio")
                )
                > F.lit(FLOAT_TOLERANCE)
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
            F.lit(
                "gold_observed_spatial_concentration_summary must contain at least one row."
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


def _validate_total_analysis_date_count_greater_than_zero(
    validated_df: DataFrame,
) -> DataFrame:
    return (
        validated_df.agg(
            F.when(F.countDistinct("analysis_date") > 0, F.lit(0))
            .otherwise(F.lit(1))
            .cast("long")
            .alias("violation_count"),
            F.countDistinct("analysis_date").cast("long").alias("validated_row_count"),
        )
        .withColumn("validation_category", F.lit("REFERENCE_USABILITY"))
        .withColumn(
            "validation_rule",
            F.lit("total_analysis_date_count_greater_than_zero"),
        )
        .withColumn(
            "validation_description",
            F.lit("The table must contain at least one analysis_date."),
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


def _validate_contains_at_least_one_valid_daily_concentration_summary_row(
    validated_df: DataFrame,
) -> DataFrame:
    return (
        validated_df.agg(
            F.when(
                F.count(
                    F.when(
                        F.col("analysis_date").isNotNull()
                        & F.col("active_spatial_cells_count").isNotNull()
                        & F.col("total_observed_position_records_count").isNotNull()
                        & F.col("avg_positions_per_active_cell").isNotNull()
                        & F.col("max_positions_in_single_cell").isNotNull()
                        & F.col("top_cell_position_share_ratio").isNotNull()
                        & F.col("top_5_cells_position_share_ratio").isNotNull(),
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
            F.lit("contains_at_least_one_valid_daily_concentration_summary_row"),
        )
        .withColumn(
            "validation_description",
            F.lit(
                "gold_observed_spatial_concentration_summary must contain at "
                "least one valid daily concentration summary row."
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
