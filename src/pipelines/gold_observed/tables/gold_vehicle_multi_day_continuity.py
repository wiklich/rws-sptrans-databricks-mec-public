"""
Gold vehicle multi-day continuity pipeline.

Purpose:
    Build the canonical multi-day vehicle continuity layer used by the
    Observed Update Frequency and Continuity Analysis component.

Business role:
    This dataset represents a business-ready view of multi-day observation
    continuity per vehicle, derived from SPTrans vehicle position data.

Input:
    - silver.silver_sptrans_positions

Output:
    - gold.gold_vehicle_multi_day_continuity

Declared grain:
    - one row per vehicle_prefix

Important rules:
    - Gold must consume the cleaned Silver dataset
    - the analysis must remain strictly observed-side only
    - one row per vehicle_prefix is allowed
    - day-level continuity must be based on distinct local observed dates
    - missing-day logic must be calculated only inside the observed date span
    - the table must be interpreted only across dates actually present in the dataset
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.gold_observed.schemas.schema_gold_vehicle_multi_day_continuity import (
    get_gold_vehicle_multi_day_continuity_schema,
)


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_multi_day_continuity"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical vehicle-level multi-day continuity table derived from "
        "silver_sptrans_positions. Contains one row per vehicle_prefix with "
        "observed date counts, date span, missing dates within span, "
        "date presence ratio, observation volume, consecutive observed-date "
        "streaks, and consecutive missing-date streaks."
    ),
    schema=get_gold_vehicle_multi_day_continuity_schema(),
)
@dp.expect_or_drop(
    "valid_vehicle_prefix",
    "vehicle_prefix IS NOT NULL AND TRIM(vehicle_prefix) <> ''",
)
@dp.expect_or_drop(
    "valid_observed_dates_count",
    "observed_dates_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_first_observed_date",
    "first_observed_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_last_observed_date",
    "last_observed_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_observed_date_span_days",
    "observed_date_span_days IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_missing_dates_within_span_count",
    "missing_dates_within_span_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_date_presence_ratio",
    "date_presence_ratio IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_total_observations_count",
    "total_observations_count IS NOT NULL",
)
def gold_vehicle_multi_day_continuity() -> DataFrame:
    """
    Materialize the canonical Gold vehicle multi-day continuity table.

    Returns:
        DataFrame:
            Vehicle-level multi-day continuity output derived from Silver
            SPTrans positions.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_vehicle_multi_day_continuity(source_df)


def _prepare_vehicle_date_stream(source_df: DataFrame) -> DataFrame:
    """
    Prepare the Silver SPTrans positions stream for vehicle-level multi-day
    continuity calculation.

    Processing steps:
        1. Keep vehicle_prefix, local observed date, and local timestamp.
        2. Cast fields to the expected analytical types.
        3. Filter rows with invalid vehicle identifiers, dates, or timestamps.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Prepared vehicle/date/timestamp stream ready for multi-day
            continuity calculation.
    """
    return (
        source_df.select(
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.coalesce(
                F.col("captured_date_local").cast("date"),
                F.to_date(F.col("captured_at_local")),
            ).alias("observed_date"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
        )
        .where(F.col("vehicle_prefix").isNotNull())
        .where(F.trim(F.col("vehicle_prefix")) != "")
        .where(F.col("observed_date").isNotNull())
        .where(F.col("captured_at_local").isNotNull())
    )


def _build_vehicle_date_presence(prepared_df: DataFrame) -> DataFrame:
    """
    Reduce event-level data to vehicle-date presence.

    Processing steps:
        1. Group by vehicle_prefix and observed_date.
        2. Count observations on each observed date.

    Args:
        prepared_df:
            Prepared vehicle/date/timestamp stream.

    Returns:
        DataFrame:
            Vehicle-date presence table with one row per vehicle and date.
    """
    return (
        prepared_df.groupBy("vehicle_prefix", "observed_date")
        .agg(
            F.count(F.lit(1)).cast("long").alias("observations_on_date"),
        )
    )


def _build_vehicle_event_aggregation(prepared_df: DataFrame) -> DataFrame:
    """
    Build vehicle-level event aggregation.

    Args:
        prepared_df:
            Prepared vehicle/date/timestamp stream.

    Returns:
        DataFrame:
            Vehicle-level event summary with total observations and timestamp bounds.
    """
    return (
        prepared_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).cast("long").alias("total_observations_count"),
            F.min("captured_at_local").alias("first_captured_at_local"),
            F.max("captured_at_local").alias("last_captured_at_local"),
        )
    )


def _build_vehicle_date_aggregation(vehicle_date_presence_df: DataFrame) -> DataFrame:
    """
    Build vehicle-level date aggregation.

    Args:
        vehicle_date_presence_df:
            Vehicle-date presence table.

    Returns:
        DataFrame:
            Vehicle-level date summary with observed date counts and span fields.
    """
    return (
        vehicle_date_presence_df.groupBy("vehicle_prefix")
        .agg(
            F.count(F.lit(1)).cast("long").alias("observed_dates_count"),
            F.min("observed_date").alias("first_observed_date"),
            F.max("observed_date").alias("last_observed_date"),
        )
        .withColumn(
            "observed_date_span_days",
            (
                F.datediff(F.col("last_observed_date"), F.col("first_observed_date"))
                + F.lit(1)
            ).cast("long"),
        )
        .withColumn(
            "missing_dates_within_span_count",
            (
                F.col("observed_date_span_days") - F.col("observed_dates_count")
            ).cast("long"),
        )
        .withColumn(
            "date_presence_ratio",
            (
                F.col("observed_dates_count").cast("double")
                / F.col("observed_date_span_days").cast("double")
            ).cast("double"),
        )
    )


def _build_max_consecutive_observed_dates(
    vehicle_date_presence_df: DataFrame,
) -> DataFrame:
    """
    Build maximum consecutive observed-date streak per vehicle.

    Args:
        vehicle_date_presence_df:
            Vehicle-date presence table.

    Returns:
        DataFrame:
            One row per vehicle_prefix with max_consecutive_observed_dates.
    """
    window_spec = Window.partitionBy("vehicle_prefix").orderBy("observed_date")

    streaks_df = (
        vehicle_date_presence_df.withColumn("rn", F.row_number().over(window_spec))
        .withColumn(
            "streak_anchor_date",
            F.expr("date_sub(observed_date, rn)"),
        )
        .groupBy("vehicle_prefix", "streak_anchor_date")
        .agg(F.count(F.lit(1)).cast("long").alias("observed_streak_length"))
    )

    return (
        streaks_df.groupBy("vehicle_prefix")
        .agg(
            F.max("observed_streak_length").cast("long").alias(
                "max_consecutive_observed_dates"
            )
        )
    )


def _build_max_consecutive_missing_dates_within_span(
    vehicle_date_agg_df: DataFrame,
    vehicle_date_presence_df: DataFrame,
) -> DataFrame:
    """
    Build maximum consecutive missing-date streak inside the observed span per vehicle.

    Args:
        vehicle_date_agg_df:
            Vehicle-level date summary.
        vehicle_date_presence_df:
            Vehicle-date presence table.

    Returns:
        DataFrame:
            One row per vehicle_prefix with max_consecutive_missing_dates_within_span.
    """
    full_calendar_df = (
        vehicle_date_agg_df.select(
            "vehicle_prefix",
            F.explode(
                F.sequence("first_observed_date", "last_observed_date")
            ).alias("calendar_date"),
        )
    )

    observed_dates_df = vehicle_date_presence_df.select(
        "vehicle_prefix",
        F.col("observed_date").alias("calendar_date"),
    )

    missing_dates_df = (
        full_calendar_df.join(
            observed_dates_df,
            on=["vehicle_prefix", "calendar_date"],
            how="left_anti",
        )
    )

    window_spec = Window.partitionBy("vehicle_prefix").orderBy("calendar_date")

    missing_streaks_df = (
        missing_dates_df.withColumn("rn", F.row_number().over(window_spec))
        .withColumn(
            "missing_streak_anchor_date",
            F.expr("date_sub(calendar_date, rn)"),
        )
        .groupBy("vehicle_prefix", "missing_streak_anchor_date")
        .agg(F.count(F.lit(1)).cast("long").alias("missing_streak_length"))
    )

    max_missing_df = (
        missing_streaks_df.groupBy("vehicle_prefix")
        .agg(
            F.max("missing_streak_length").cast("long").alias(
                "max_consecutive_missing_dates_within_span"
            )
        )
    )

    return (
        vehicle_date_agg_df.select("vehicle_prefix")
        .join(max_missing_df, on="vehicle_prefix", how="left")
        .withColumn(
            "max_consecutive_missing_dates_within_span",
            F.coalesce(
                F.col("max_consecutive_missing_dates_within_span"),
                F.lit(0).cast("long"),
            ),
        )
    )


def _build_gold_vehicle_multi_day_continuity(source_df: DataFrame) -> DataFrame:
    """
    Build the canonical Gold vehicle multi-day continuity dataset.

    Processing steps:
        1. Prepare the Silver SPTrans positions stream.
        2. Reduce event-level data to vehicle-date presence.
        3. Build vehicle-level event aggregation.
        4. Build vehicle-level date aggregation.
        5. Derive max consecutive observed-date streaks.
        6. Derive max consecutive missing-date streaks within span.
        7. Join all multi-day continuity metrics.
        8. Materialize the final Gold output.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Canonical vehicle-level multi-day continuity output with one row
            per vehicle_prefix.
    """
    prepared_df = _prepare_vehicle_date_stream(source_df)
    vehicle_date_presence_df = _build_vehicle_date_presence(prepared_df)
    vehicle_event_agg_df = _build_vehicle_event_aggregation(prepared_df)
    vehicle_date_agg_df = _build_vehicle_date_aggregation(vehicle_date_presence_df)
    max_observed_streak_df = _build_max_consecutive_observed_dates(
        vehicle_date_presence_df
    )
    max_missing_streak_df = _build_max_consecutive_missing_dates_within_span(
        vehicle_date_agg_df,
        vehicle_date_presence_df,
    )

    final_df = (
        vehicle_date_agg_df.join(
            vehicle_event_agg_df,
            on="vehicle_prefix",
            how="inner",
        )
        .join(
            max_observed_streak_df,
            on="vehicle_prefix",
            how="left",
        )
        .join(
            max_missing_streak_df,
            on="vehicle_prefix",
            how="left",
        )
        .withColumn(
            "avg_observations_per_observed_date",
            (
                F.col("total_observations_count").cast("double")
                / F.col("observed_dates_count").cast("double")
            ).cast("double"),
        )
        .withColumn(
            "max_consecutive_observed_dates",
            F.coalesce(
                F.col("max_consecutive_observed_dates"),
                F.lit(0).cast("long"),
            ),
        )
        .withColumn(
            "max_consecutive_missing_dates_within_span",
            F.coalesce(
                F.col("max_consecutive_missing_dates_within_span"),
                F.lit(0).cast("long"),
            ),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return final_df.select(
        "vehicle_prefix",
        "observed_dates_count",
        "first_observed_date",
        "last_observed_date",
        "observed_date_span_days",
        "missing_dates_within_span_count",
        "date_presence_ratio",
        "first_captured_at_local",
        "last_captured_at_local",
        "total_observations_count",
        "avg_observations_per_observed_date",
        "max_consecutive_observed_dates",
        "max_consecutive_missing_dates_within_span",
        "record_created_at",
    )
