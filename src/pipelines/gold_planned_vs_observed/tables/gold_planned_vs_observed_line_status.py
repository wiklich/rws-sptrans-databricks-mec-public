"""
Gold planned vs observed line status pipeline.

Purpose:
    Build the canonical detailed daily comparison between the GTFS planned line
    catalog and the SPTrans observed daily line catalog.

Business role:
    This dataset represents the main detailed analytical output of the
    Planned vs Observed Service Comparison component. It classifies each
    analysis_date and normalized line code as planned and observed, planned
    but not observed, or observed but not found in the GTFS planned catalog.

Inputs:
    - gold.gold_planned_lines_reference
    - gold.gold_observed_lines_daily

Output:
    - gold.gold_planned_vs_observed_line_status

Official business key:
    - comparison key: line_code

Important rules:
    - one row per analysis_date and normalized line_code
    - planned-only cases must be preserved
    - observed-only cases must be preserved
    - matched cases must be preserved explicitly
    - no trip-level, stop-level, or delay-level inference is allowed
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_planned_vs_observed_line_status import (
    get_gold_planned_vs_observed_line_status_schema,
)


PLANNED_SOURCE_TABLE = "main.gold.gold_planned_lines_reference"
OBSERVED_SOURCE_TABLE = "main.gold.gold_observed_lines_daily"
OUTPUT_TABLE_NAME = "gold.gold_planned_vs_observed_line_status"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical detailed daily comparison between the GTFS planned line "
        "catalog and the SPTrans observed daily line catalog. Contains one row "
        "per analysis date and normalized line code with explicit planned vs "
        "observed classification."
    ),
    schema=get_gold_planned_vs_observed_line_status_schema(),
)
@dp.expect_or_drop(
    "valid_analysis_date",
    "analysis_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_line_code",
    "line_code IS NOT NULL AND TRIM(line_code) <> ''",
)
@dp.expect_or_drop(
    "valid_is_planned",
    "is_planned IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_is_observed",
    "is_observed IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_match_status",
    "match_status IN ('PLANNED_AND_OBSERVED', 'PLANNED_NOT_OBSERVED', 'OBSERVED_NOT_IN_GTFS')",
)
def gold_planned_vs_observed_line_status() -> DataFrame:
    """
    Materialize the canonical Gold planned vs observed line status table.

    Returns:
        DataFrame:
            Detailed daily comparison output derived from the planned and
            observed Gold reference tables.
    """
    planned_df = spark.read.table(PLANNED_SOURCE_TABLE)
    observed_df = spark.read.table(OBSERVED_SOURCE_TABLE)
    return _build_gold_planned_vs_observed_line_status(planned_df, observed_df)


def _build_analysis_dates(observed_df: DataFrame) -> DataFrame:
    """
    Build the distinct analysis date scaffold from the observed daily dataset.

    Args:
        observed_df:
            Input DataFrame from gold.gold_observed_lines_daily.

    Returns:
        DataFrame:
            One row per analysis_date.
    """
    return (
        observed_df.select(F.col("analysis_date").cast("date").alias("analysis_date"))
        .where(F.col("analysis_date").isNotNull())
        .distinct()
    )


def _build_planned_lines_by_date(
    planned_df: DataFrame,
    analysis_dates_df: DataFrame,
) -> DataFrame:
    """
    Replicate the planned line catalog across the observed date scope.

    Args:
        planned_df:
            Input DataFrame from gold.gold_planned_lines_reference.
        analysis_dates_df:
            Distinct analysis dates derived from the observed daily dataset.

    Returns:
        DataFrame:
            Planned line catalog replicated for each analysis_date.
    """
    prepared_planned_df = planned_df.select(
        F.col("line_code").cast("string").alias("line_code"),
        F.col("planned_route_id").cast("string").alias("planned_route_id"),
        F.col("planned_route_short_name").cast("string").alias("planned_route_short_name"),
        F.col("planned_route_long_name").cast("string").alias("planned_route_long_name"),
        F.col("planned_route_name_display").cast("string").alias("planned_route_name_display"),
        F.col("route_type").cast("int").alias("route_type"),
        F.col("route_type_description").cast("string").alias("route_type_description"),
        F.col("route_color").cast("string").alias("route_color"),
        F.col("route_color_name").cast("string").alias("route_color_name"),
        F.col("route_text_color").cast("string").alias("route_text_color"),
        F.col("route_text_color_name").cast("string").alias("route_text_color_name"),
        F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
    )

    return analysis_dates_df.crossJoin(prepared_planned_df)


def _build_gold_planned_vs_observed_line_status(
    planned_df: DataFrame,
    observed_df: DataFrame,
) -> DataFrame:
    """
    Build the canonical Gold planned vs observed line status dataset.

    Processing steps:
        1. Build the distinct analysis date scaffold from the observed daily data.
        2. Replicate the planned line catalog across all analysis dates.
        3. Join planned-by-date and observed daily lines using analysis_date and line_code.
        4. Derive planned and observed boolean flags.
        5. Assign the official match_status.
        6. Preserve planned-side descriptive fields and observed-side daily metrics.

    Args:
        planned_df:
            Input DataFrame from gold.gold_planned_lines_reference.
        observed_df:
            Input DataFrame from gold.gold_observed_lines_daily.

    Returns:
        DataFrame:
            Detailed daily planned vs observed comparison output.
    """
    analysis_dates_df = _build_analysis_dates(observed_df)
    planned_by_date_df = _build_planned_lines_by_date(planned_df, analysis_dates_df)

    prepared_observed_df = observed_df.select(
        F.col("analysis_date").cast("date").alias("analysis_date"),
        F.col("line_code").cast("string").alias("line_code"),
        F.col("observed_line_sign").cast("string").alias("observed_line_sign"),
        F.col("observed_events_count").cast("long").alias("observed_events_count"),
        F.col("observed_vehicles_count").cast("long").alias("observed_vehicles_count"),
        F.col("min_captured_at_local").cast("timestamp").alias("min_captured_at_local"),
        F.col("max_captured_at_local").cast("timestamp").alias("max_captured_at_local"),
    )

    comparison_df = (
        planned_by_date_df.alias("planned")
        .join(
            prepared_observed_df.alias("observed"),
            on=[
                F.col("planned.analysis_date") == F.col("observed.analysis_date"),
                F.col("planned.line_code") == F.col("observed.line_code"),
            ],
            how="full_outer",
        )
        .select(
            F.coalesce(
                F.col("planned.analysis_date"),
                F.col("observed.analysis_date"),
            ).alias("analysis_date"),
            F.coalesce(
                F.col("planned.line_code"),
                F.col("observed.line_code"),
            ).alias("line_code"),
            F.col("planned.planned_route_id").alias("planned_route_id"),
            F.col("observed.observed_line_sign").alias("observed_line_sign"),
            F.col("planned.planned_route_short_name").alias("planned_route_short_name"),
            F.col("planned.planned_route_long_name").alias("planned_route_long_name"),
            F.col("planned.planned_route_name_display").alias("planned_route_name_display"),
            F.col("planned.route_type").alias("route_type"),
            F.col("planned.route_type_description").alias("route_type_description"),
            F.col("planned.route_color").alias("route_color"),
            F.col("planned.route_color_name").alias("route_color_name"),
            F.col("planned.route_text_color").alias("route_text_color"),
            F.col("planned.route_text_color_name").alias("route_text_color_name"),
            F.col("observed.observed_events_count").alias("observed_events_count"),
            F.col("observed.observed_vehicles_count").alias("observed_vehicles_count"),
            F.col("observed.min_captured_at_local").alias("min_captured_at_local"),
            F.col("observed.max_captured_at_local").alias("max_captured_at_local"),
            F.col("planned.gtfs_load_date").alias("gtfs_load_date"),
        )
        .withColumn("is_planned", F.col("planned_route_id").isNotNull())
        .withColumn("is_observed", F.col("observed_line_sign").isNotNull())
        .withColumn(
            "match_status",
            F.when(
                F.col("planned_route_id").isNotNull()
                & F.col("observed_line_sign").isNotNull(),
                F.lit("PLANNED_AND_OBSERVED"),
            )
            .when(
                F.col("planned_route_id").isNotNull()
                & F.col("observed_line_sign").isNull(),
                F.lit("PLANNED_NOT_OBSERVED"),
            )
            .when(
                F.col("planned_route_id").isNull()
                & F.col("observed_line_sign").isNotNull(),
                F.lit("OBSERVED_NOT_IN_GTFS"),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return comparison_df.select(
        "analysis_date",
        "line_code",
        "planned_route_id",
        "observed_line_sign",
        "is_planned",
        "is_observed",
        "match_status",
        "planned_route_short_name",
        "planned_route_long_name",
        "planned_route_name_display",
        "route_type",
        "route_type_description",
        "route_color",
        "route_color_name",
        "route_text_color",
        "route_text_color_name",
        "observed_events_count",
        "observed_vehicles_count",
        "min_captured_at_local",
        "max_captured_at_local",
        "gtfs_load_date",
        "record_created_at",
    )
