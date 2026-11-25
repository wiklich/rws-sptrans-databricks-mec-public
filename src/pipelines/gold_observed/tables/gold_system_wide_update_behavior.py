"""
Gold system-wide update behavior pipeline.

Purpose:
    Build the canonical system-wide update behavior layer used by the
    Observed Update Frequency and Continuity Analysis component.

Business role:
    This dataset represents a business-ready aggregate view of update behavior
    across the observed system, derived from lower-level Gold vehicle tables.

Input:
    - gold.gold_vehicle_update_frequency
    - gold.gold_vehicle_observation_continuity

Output:
    - gold.gold_system_wide_update_behavior

Declared grain:
    - one row per declared aggregation scope

First-version aggregation scope:
    - FULL_OBSERVED_DATASET_WINDOW

Important rules:
    - Gold should preferably consume lower-level Gold tables rather than raw Silver
    - the analysis must remain strictly observed-side only
    - the aggregation grain must be explicitly declared and stable
    - weighted aggregation must be used for system_avg_update_interval_seconds
      using measured_intervals_count as support weight
    - the resulting metrics must be interpreted only within the actually
      observed dataset scope
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_observed.schemas.schema_gold_system_wide_update_behavior import (
    get_gold_system_wide_update_behavior_schema,
)


UPDATE_FREQUENCY_SOURCE_TABLE = "main.gold.gold_vehicle_update_frequency"
OBSERVATION_CONTINUITY_SOURCE_TABLE = "main.gold.gold_vehicle_observation_continuity"
OUTPUT_TABLE_NAME = "gold.gold_system_wide_update_behavior"
AGGREGATION_SCOPE = "FULL_OBSERVED_DATASET_WINDOW"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Canonical system-wide update behavior table derived from "
        "gold_vehicle_update_frequency and gold_vehicle_observation_continuity. "
        "Contains one row for the full observed dataset window with aggregate "
        "update interval metrics, support indicators, and continuity summary metrics."
    ),
    schema=get_gold_system_wide_update_behavior_schema(),
)
@dp.expect_or_drop(
    "valid_aggregation_scope",
    "aggregation_scope IS NOT NULL AND TRIM(aggregation_scope) <> ''",
)
@dp.expect_or_drop(
    "valid_vehicles_count",
    "vehicles_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_total_observations_count",
    "total_observations_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_total_measured_intervals_count",
    "total_measured_intervals_count IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_system_avg_update_interval_seconds_when_supported",
    (
        "total_measured_intervals_count = 0 "
        "OR system_avg_update_interval_seconds IS NOT NULL"
    ),
)
@dp.expect_or_drop(
    "valid_system_min_update_interval_seconds_when_supported",
    (
        "total_measured_intervals_count = 0 "
        "OR system_min_update_interval_seconds IS NOT NULL"
    ),
)
@dp.expect_or_drop(
    "valid_system_max_update_interval_seconds_when_supported",
    (
        "total_measured_intervals_count = 0 "
        "OR system_max_update_interval_seconds IS NOT NULL"
    ),
)
def gold_system_wide_update_behavior() -> DataFrame:
    """
    Materialize the canonical Gold system-wide update behavior table.

    Returns:
        DataFrame:
            System-wide update behavior output derived from lower-level Gold
            vehicle update frequency and observation continuity tables.
    """
    update_frequency_df = spark.read.table(UPDATE_FREQUENCY_SOURCE_TABLE)
    observation_continuity_df = spark.read.table(OBSERVATION_CONTINUITY_SOURCE_TABLE)

    return _build_gold_system_wide_update_behavior(
        update_frequency_df,
        observation_continuity_df,
    )


def _prepare_system_wide_vehicle_base(
    update_frequency_df: DataFrame,
    observation_continuity_df: DataFrame,
) -> DataFrame:
    """
    Prepare the joined vehicle-level base used for system-wide aggregation.

    Processing steps:
        1. Select only the fields required for system-wide aggregation from
           gold_vehicle_update_frequency.
        2. Select only the fields required for system-wide aggregation from
           gold_vehicle_observation_continuity.
        3. Keep only structurally valid lower-level Gold records.
        4. Join both lower-level Gold tables on vehicle_prefix.

    Args:
        update_frequency_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.
        observation_continuity_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Joined vehicle-level base ready for system-wide aggregation.
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

    return prepared_update_frequency_df.join(
        prepared_observation_continuity_df,
        on="vehicle_prefix",
        how="inner",
    )


def _build_gold_system_wide_update_behavior(
    update_frequency_df: DataFrame,
    observation_continuity_df: DataFrame,
) -> DataFrame:
    """
    Build the canonical Gold system-wide update behavior dataset.

    Processing steps:
        1. Prepare the joined vehicle-level aggregation base.
        2. Aggregate update interval metrics across vehicles.
        3. Aggregate support and continuity indicators across vehicles.
        4. Calculate the weighted system average update interval.
        5. Materialize one row for the declared aggregation scope.

    Args:
        update_frequency_df:
            Input DataFrame from gold.gold_vehicle_update_frequency.
        observation_continuity_df:
            Input DataFrame from gold.gold_vehicle_observation_continuity.

    Returns:
        DataFrame:
            Canonical system-wide update behavior output with one row for the
            full observed dataset window.
    """
    vehicle_base_df = _prepare_system_wide_vehicle_base(
        update_frequency_df,
        observation_continuity_df,
    )

    aggregated_df = (
        vehicle_base_df.agg(
            F.countDistinct("vehicle_prefix").cast("long").alias("vehicles_count"),
            F.coalesce(F.sum("updates_count"), F.lit(0)).cast("long").alias(
                "total_observations_count"
            ),
            F.coalesce(F.sum("measured_intervals_count"), F.lit(0)).cast("long").alias(
                "total_measured_intervals_count"
            ),
            F.sum(
                F.col("avg_update_interval_seconds") * F.col("measured_intervals_count")
            ).cast("double").alias("weighted_interval_seconds_sum"),
            F.min("min_update_interval_seconds").cast("double").alias(
                "system_min_update_interval_seconds"
            ),
            F.max("max_update_interval_seconds").cast("double").alias(
                "system_max_update_interval_seconds"
            ),
            F.avg("stddev_update_interval_seconds").cast("double").alias(
                "system_avg_vehicle_stddev_interval_seconds"
            ),
            F.coalesce(F.sum("distinct_minutes_with_data"), F.lit(0)).cast("long").alias(
                "total_distinct_minutes_with_data"
            ),
            F.avg("continuity_support_ratio").cast("double").alias(
                "avg_continuity_support_ratio"
            ),
            F.coalesce(F.sum("large_gaps_count"), F.lit(0)).cast("long").alias(
                "total_large_gaps_count"
            ),
        )
        .withColumn("aggregation_scope", F.lit(AGGREGATION_SCOPE))
        .withColumn(
            "system_avg_update_interval_seconds",
            F.when(
                F.col("total_measured_intervals_count") > 0,
                (
                    F.col("weighted_interval_seconds_sum")
                    / F.col("total_measured_intervals_count").cast("double")
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("record_created_at", F.current_timestamp())
    )

    return aggregated_df.select(
        "aggregation_scope",
        "vehicles_count",
        "total_observations_count",
        "total_measured_intervals_count",
        "system_avg_update_interval_seconds",
        "system_min_update_interval_seconds",
        "system_max_update_interval_seconds",
        "system_avg_vehicle_stddev_interval_seconds",
        "total_distinct_minutes_with_data",
        "avg_continuity_support_ratio",
        "total_large_gaps_count",
        "record_created_at",
    )
