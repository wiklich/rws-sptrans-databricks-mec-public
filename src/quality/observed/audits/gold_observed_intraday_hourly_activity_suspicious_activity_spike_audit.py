"""
Gold observed intraday hourly activity suspicious hourly activity spike audit pipeline.

Purpose:
    Identify hours with unusually high or low observed activity volume in
    gold_observed_intraday_hourly_activity.

Audit focus:
    - sudden spikes in observed_position_records_count
    - sudden drops in distinct_observed_vehicles_count
    - strong deviations from surrounding hours

Why this audit matters:
    This audit is useful for interpretation and downstream confidence analysis.
    Large spikes or drops may indicate unusual operational conditions, partial
    collection windows, or weak comparability between adjacent hourly buckets.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


GOLD_SOURCE_TABLE = "main.gold.gold_observed_intraday_hourly_activity"
OUTPUT_TABLE_NAME = (
    "gold.gold_observed_intraday_hourly_activity_suspicious_activity_spike_audit"
)

STDDEV_MULTIPLIER = 2.0
POSITION_DELTA_THRESHOLD_RATIO = 0.50
VEHICLE_DELTA_THRESHOLD_RATIO = 0.30


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying hours with unusually high or low observed "
        "activity volume, including sudden spikes in position records, sudden "
        "drops in distinct observed vehicles, and strong deviations from "
        "surrounding hours in gold_observed_intraday_hourly_activity."
    ),
)
def gold_observed_intraday_hourly_activity_suspicious_activity_spike_audit() -> DataFrame:
    """
    Materialize the suspicious hourly activity spike audit for
    gold_observed_intraday_hourly_activity.

    Returns:
        DataFrame:
            Audit rows for hours with suspicious activity spikes or drops.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_observed_intraday_hourly_activity_suspicious_activity_spike_audit(
        source_df
    )


def _build_gold_observed_intraday_hourly_activity_suspicious_activity_spike_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the suspicious hourly activity spike audit dataset.

    Processing steps:
        1. Read gold_observed_intraday_hourly_activity.
        2. Compute global reference statistics for hourly position and vehicle counts.
        3. Compute previous and next hourly values.
        4. Detect unusually high or low hourly values using global thresholds.
        5. Detect sudden spikes or drops relative to surrounding hours.
        6. Keep only rows flagged as suspicious.

    Args:
        source_df:
            Input DataFrame from gold.gold_observed_intraday_hourly_activity.

    Returns:
        DataFrame:
            Audit DataFrame containing suspicious hourly activity rows.
    """
    ordered_window = Window.orderBy("analysis_date", "analysis_hour_local")

    stats_df = source_df.agg(
        F.avg("observed_position_records_count").alias(
            "avg_observed_position_records_count"
        ),
        F.stddev_pop("observed_position_records_count").alias(
            "stddev_observed_position_records_count"
        ),
        F.min("observed_position_records_count").alias(
            "min_observed_position_records_count"
        ),
        F.max("observed_position_records_count").alias(
            "max_observed_position_records_count"
        ),
        F.avg("distinct_observed_vehicles_count").alias(
            "avg_distinct_observed_vehicles_count"
        ),
        F.stddev_pop("distinct_observed_vehicles_count").alias(
            "stddev_distinct_observed_vehicles_count"
        ),
        F.min("distinct_observed_vehicles_count").alias(
            "min_distinct_observed_vehicles_count"
        ),
        F.max("distinct_observed_vehicles_count").alias(
            "max_distinct_observed_vehicles_count"
        ),
    )

    prepared_df = (
        source_df.withColumn(
            "previous_observed_position_records_count",
            F.lag("observed_position_records_count").over(ordered_window),
        )
        .withColumn(
            "next_observed_position_records_count",
            F.lead("observed_position_records_count").over(ordered_window),
        )
        .withColumn(
            "previous_distinct_observed_vehicles_count",
            F.lag("distinct_observed_vehicles_count").over(ordered_window),
        )
        .withColumn(
            "next_distinct_observed_vehicles_count",
            F.lead("distinct_observed_vehicles_count").over(ordered_window),
        )
        .withColumn(
            "delta_vs_previous_position_records",
            F.when(
                F.col("previous_observed_position_records_count").isNotNull(),
                F.col("observed_position_records_count")
                - F.col("previous_observed_position_records_count"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "delta_vs_next_position_records",
            F.when(
                F.col("next_observed_position_records_count").isNotNull(),
                F.col("observed_position_records_count")
                - F.col("next_observed_position_records_count"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "delta_vs_previous_distinct_vehicles",
            F.when(
                F.col("previous_distinct_observed_vehicles_count").isNotNull(),
                F.col("distinct_observed_vehicles_count")
                - F.col("previous_distinct_observed_vehicles_count"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "delta_vs_next_distinct_vehicles",
            F.when(
                F.col("next_distinct_observed_vehicles_count").isNotNull(),
                F.col("distinct_observed_vehicles_count")
                - F.col("next_distinct_observed_vehicles_count"),
            ).otherwise(F.lit(None)),
        )
    )

    audit_df = (
        prepared_df.crossJoin(stats_df)
        .withColumn(
            "high_position_records_threshold",
            F.col("avg_observed_position_records_count")
            + (
                F.coalesce(
                    F.col("stddev_observed_position_records_count"), F.lit(0.0)
                )
                * F.lit(STDDEV_MULTIPLIER)
            ),
        )
        .withColumn(
            "low_vehicle_count_threshold",
            F.greatest(
                F.lit(0.0),
                F.col("avg_distinct_observed_vehicles_count")
                - (
                    F.coalesce(
                        F.col("stddev_distinct_observed_vehicles_count"), F.lit(0.0)
                    )
                    * F.lit(STDDEV_MULTIPLIER)
                ),
            ),
        )
        .withColumn(
            "has_unusually_high_position_volume",
            F.col("observed_position_records_count")
            > F.col("high_position_records_threshold"),
        )
        .withColumn(
            "has_unusually_low_vehicle_volume",
            F.col("distinct_observed_vehicles_count")
            < F.col("low_vehicle_count_threshold"),
        )
        .withColumn(
            "has_sudden_spike_vs_previous_hour",
            F.col("previous_observed_position_records_count").isNotNull()
            & (
                F.col("observed_position_records_count")
                > F.col("previous_observed_position_records_count")
                * F.lit(1.0 + POSITION_DELTA_THRESHOLD_RATIO)
            ),
        )
        .withColumn(
            "has_sudden_spike_vs_next_hour",
            F.col("next_observed_position_records_count").isNotNull()
            & (
                F.col("observed_position_records_count")
                > F.col("next_observed_position_records_count")
                * F.lit(1.0 + POSITION_DELTA_THRESHOLD_RATIO)
            ),
        )
        .withColumn(
            "has_sudden_drop_vs_previous_hour",
            F.col("previous_distinct_observed_vehicles_count").isNotNull()
            & (
                F.col("distinct_observed_vehicles_count")
                < F.col("previous_distinct_observed_vehicles_count")
                * F.lit(1.0 - VEHICLE_DELTA_THRESHOLD_RATIO)
            ),
        )
        .withColumn(
            "has_sudden_drop_vs_next_hour",
            F.col("next_distinct_observed_vehicles_count").isNotNull()
            & (
                F.col("distinct_observed_vehicles_count")
                < F.col("next_distinct_observed_vehicles_count")
                * F.lit(1.0 - VEHICLE_DELTA_THRESHOLD_RATIO)
            ),
        )
        .withColumn(
            "has_suspicious_hourly_activity_spike",
            F.col("has_unusually_high_position_volume")
            | F.col("has_unusually_low_vehicle_volume")
            | F.col("has_sudden_spike_vs_previous_hour")
            | F.col("has_sudden_spike_vs_next_hour")
            | F.col("has_sudden_drop_vs_previous_hour")
            | F.col("has_sudden_drop_vs_next_hour"),
        )
        .where(F.col("has_suspicious_hourly_activity_spike") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "analysis_hour_local",
            "observed_position_records_count",
            "distinct_observed_vehicles_count",
            "distinct_observed_lines_count",
            "distinct_minutes_with_data",
            "min_captured_at_local",
            "max_captured_at_local",
            "previous_observed_position_records_count",
            "next_observed_position_records_count",
            "previous_distinct_observed_vehicles_count",
            "next_distinct_observed_vehicles_count",
            "delta_vs_previous_position_records",
            "delta_vs_next_position_records",
            "delta_vs_previous_distinct_vehicles",
            "delta_vs_next_distinct_vehicles",
            "avg_observed_position_records_count",
            "stddev_observed_position_records_count",
            "min_observed_position_records_count",
            "max_observed_position_records_count",
            "avg_distinct_observed_vehicles_count",
            "stddev_distinct_observed_vehicles_count",
            "min_distinct_observed_vehicles_count",
            "max_distinct_observed_vehicles_count",
            "high_position_records_threshold",
            "low_vehicle_count_threshold",
            "has_unusually_high_position_volume",
            "has_unusually_low_vehicle_volume",
            "has_sudden_spike_vs_previous_hour",
            "has_sudden_spike_vs_next_hour",
            "has_sudden_drop_vs_previous_hour",
            "has_sudden_drop_vs_next_hour",
            "has_suspicious_hourly_activity_spike",
            "record_created_at",
        )
    )

    return audit_df
