"""
Audit pipeline for conflicting source attributes in
gold.gold_planned_vs_observed_service_adherence_hourly.

Purpose:
    Detect cases where multiple records collapse into the same Gold business key
    with conflicting descriptive or metric values.

Declared grain:
    One row per:
        service_date + line_sign + gtfs_direction_id + hour

Audit scope:
    - same Gold key associated with multiple planned_headway_minutes values
    - same Gold key associated with multiple adherence_type values
    - same Gold key associated with multiple adherence_note values

Important:
    This is a non-blocking audit table.
    It exposes conflicting business outcomes for investigation without dropping
    records from the main Gold table.

Note:
    The current Gold schema does not include an explicit route interpretation
    field such as route_id or route_long_name. Therefore, conflicting route
    interpretations cannot be directly validated from this final table alone.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_planned_vs_observed_service_adherence_hourly"
TABLE_NAME = (
    "gold.gold_planned_vs_observed_service_adherence_hourly_"
    "conflicting_source_attributes_audit"
)


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for conflicting source attributes in "
        "gold.gold_planned_vs_observed_service_adherence_hourly. Detects cases "
        "where the same Gold key appears with conflicting planned headway or "
        "interpretation fields."
    ),
)
def gold_planned_vs_observed_service_adherence_hourly_conflicting_source_attributes_audit() -> DataFrame:
    """
    Create a non-blocking audit table for conflicting descriptive or metric
    values inside the declared Gold business key.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    conflicting_keys_df = (
        source_df.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
        )
        .agg(
            F.countDistinct("planned_headway_minutes").alias(
                "distinct_planned_headway_minutes_count"
            ),
            F.countDistinct("adherence_type").alias("distinct_adherence_type_count"),
            F.countDistinct("adherence_note").alias("distinct_adherence_note_count"),
            F.collect_set("planned_headway_minutes").alias(
                "planned_headway_minutes_values"
            ),
            F.collect_set("adherence_type").alias("adherence_type_values"),
            F.collect_set("adherence_note").alias("adherence_note_values"),
        )
        .withColumn(
            "conflict_planned_headway_flag",
            F.col("distinct_planned_headway_minutes_count") > 1,
        )
        .withColumn(
            "conflict_adherence_type_flag",
            F.col("distinct_adherence_type_count") > 1,
        )
        .withColumn(
            "conflict_adherence_note_flag",
            F.col("distinct_adherence_note_count") > 1,
        )
        .filter(
            F.col("conflict_planned_headway_flag")
            | F.col("conflict_adherence_type_flag")
            | F.col("conflict_adherence_note_flag")
        )
    )

    audit_df = (
        source_df.alias("src")
        .join(
            conflicting_keys_df.alias("conf"),
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="inner",
        )
        .select(
            F.col("src.service_date").alias("service_date"),
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.hour").alias("hour"),
            F.col("src.planned_headway_minutes").alias("planned_headway_minutes"),
            F.col("src.observed_event_count_proxy").alias("observed_event_count_proxy"),
            F.col("src.observed_vehicle_count").alias("observed_vehicle_count"),
            F.col("src.observed_headway_avg_minutes").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("src.observed_headway_p50_minutes").alias(
                "observed_headway_p50_minutes"
            ),
            F.col("src.observed_headway_p90_minutes").alias(
                "observed_headway_p90_minutes"
            ),
            F.col("src.headway_deviation_minutes").alias(
                "headway_deviation_minutes"
            ),
            F.col("src.headway_deviation_pct").alias("headway_deviation_pct"),
            F.col("src.planned_service_present_flag").alias(
                "planned_service_present_flag"
            ),
            F.col("src.observed_service_present_flag").alias(
                "observed_service_present_flag"
            ),
            F.col("src.missed_service_flag").alias("missed_service_flag"),
            F.col("src.adherence_type").alias("adherence_type"),
            F.col("src.adherence_note").alias("adherence_note"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("conf.distinct_planned_headway_minutes_count").alias(
                "distinct_planned_headway_minutes_count"
            ),
            F.col("conf.distinct_adherence_type_count").alias(
                "distinct_adherence_type_count"
            ),
            F.col("conf.distinct_adherence_note_count").alias(
                "distinct_adherence_note_count"
            ),
            F.col("conf.planned_headway_minutes_values").alias(
                "planned_headway_minutes_values"
            ),
            F.col("conf.adherence_type_values").alias("adherence_type_values"),
            F.col("conf.adherence_note_values").alias("adherence_note_values"),
            F.col("conf.conflict_planned_headway_flag").alias(
                "conflict_planned_headway_flag"
            ),
            F.col("conf.conflict_adherence_type_flag").alias(
                "conflict_adherence_type_flag"
            ),
            F.col("conf.conflict_adherence_note_flag").alias(
                "conflict_adherence_note_flag"
            ),
            F.lit("CONFLICTING_SOURCE_ATTRIBUTES").alias("audit_rule"),
            F.when(
                F.col("conf.conflict_planned_headway_flag")
                & F.col("conf.conflict_adherence_type_flag")
                & F.col("conf.conflict_adherence_note_flag"),
                F.lit(
                    "The same Gold key has multiple planned_headway_minutes values, "
                    "multiple adherence_type values, and multiple adherence_note values."
                ),
            )
            .when(
                F.col("conf.conflict_planned_headway_flag")
                & F.col("conf.conflict_adherence_type_flag"),
                F.lit(
                    "The same Gold key has multiple planned_headway_minutes values "
                    "and multiple adherence_type values."
                ),
            )
            .when(
                F.col("conf.conflict_planned_headway_flag")
                & F.col("conf.conflict_adherence_note_flag"),
                F.lit(
                    "The same Gold key has multiple planned_headway_minutes values "
                    "and multiple adherence_note values."
                ),
            )
            .when(
                F.col("conf.conflict_adherence_type_flag")
                & F.col("conf.conflict_adherence_note_flag"),
                F.lit(
                    "The same Gold key has multiple adherence_type values and "
                    "multiple adherence_note values."
                ),
            )
            .when(
                F.col("conf.conflict_planned_headway_flag"),
                F.lit(
                    "The same Gold key has multiple incompatible planned_headway_minutes values."
                ),
            )
            .when(
                F.col("conf.conflict_adherence_type_flag"),
                F.lit(
                    "The same Gold key has multiple adherence_type values."
                ),
            )
            .otherwise(
                F.lit(
                    "The same Gold key has multiple adherence_note values."
                )
            ).alias("audit_message"),
            F.current_timestamp().alias("audit_created_at"),
        )
    )

    return audit_df
