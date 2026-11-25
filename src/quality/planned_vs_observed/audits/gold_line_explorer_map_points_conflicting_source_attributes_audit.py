"""
Audit pipeline for conflicting source attributes in
gold.gold_line_explorer_map_points.

Purpose:
    Detect cases where multiple source attributes conflict within the declared
    observed point context.

Audit scope:
    - same observed point grain associated with multiple gtfs_direction_id values
    - same observed point grain associated with conflicting adherence metrics

Important:
    This is a non-blocking audit table.
    It exposes conflicting source attributes for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_line_explorer_map_points"
TABLE_NAME = "gold.gold_line_explorer_map_points_conflicting_source_attributes_audit"

SOURCE_COLUMNS = [
    "event_id",
    "service_date",
    "captured_at_local",
    "hour",
    "line_sign",
    "observed_direction",
    "gtfs_direction_id",
    "direction_description",
    "vehicle_prefix",
    "latitude",
    "longitude",
    "planned_headway_minutes",
    "observed_headway_avg_minutes",
    "headway_deviation_minutes",
    "headway_deviation_pct",
    "missed_service_flag",
    "adherence_band",
    "record_created_at",
    "record_hash",
]


def _conflicting_gtfs_direction_id(df: DataFrame) -> DataFrame:
    """
    Detect observed point contexts associated with multiple gtfs_direction_id values.

    Conflict grain:
        service_date
        + captured_at_local
        + line_sign
        + observed_direction
        + vehicle_prefix
        + latitude
        + longitude
    """
    conflicting_directions = (
        df.groupBy(
            "service_date",
            "captured_at_local",
            "line_sign",
            "observed_direction",
            "vehicle_prefix",
            "latitude",
            "longitude",
        )
        .agg(
            F.countDistinct("gtfs_direction_id").alias("distinct_gtfs_direction_id_count"),
            F.collect_set(F.col("gtfs_direction_id").cast("string")).alias(
                "gtfs_direction_ids"
            ),
        )
        .filter(F.col("distinct_gtfs_direction_id_count") > 1)
    )

    return (
        df.alias("src")
        .join(
            conflicting_directions.alias("conf"),
            on=[
                "service_date",
                "captured_at_local",
                "line_sign",
                "observed_direction",
                "vehicle_prefix",
                "latitude",
                "longitude",
            ],
            how="inner",
        )
        .select(
            *[F.col(f"src.{column_name}").alias(column_name) for column_name in SOURCE_COLUMNS],
            F.col("conf.distinct_gtfs_direction_id_count")
            .cast("long")
            .alias("conflict_count"),
            F.to_json(F.col("conf.gtfs_direction_ids")).alias("conflict_values"),
            F.lit("CONFLICTING_GTFS_DIRECTION_ID").alias("audit_rule"),
            F.lit(
                "The same observed point context is associated with multiple "
                "gtfs_direction_id values."
            ).alias("audit_message"),
        )
    )


def _conflicting_adherence_metrics(df: DataFrame) -> DataFrame:
    """
    Detect observed point grains associated with conflicting adherence metrics.

    Conflict grain:
        service_date
        + captured_at_local
        + line_sign
        + gtfs_direction_id
        + vehicle_prefix
        + latitude
        + longitude

    Conflict definition:
        More than one distinct adherence metric signature exists for the same
        observed point grain.
    """
    metric_signature = F.concat_ws(
        "||",
        F.coalesce(F.col("planned_headway_minutes").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("observed_headway_avg_minutes").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("headway_deviation_minutes").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("headway_deviation_pct").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("missed_service_flag").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("adherence_band"), F.lit("NULL")),
    )

    conflicting_metrics = (
        df.withColumn("adherence_metric_signature", metric_signature)
        .groupBy(
            "service_date",
            "captured_at_local",
            "line_sign",
            "gtfs_direction_id",
            "vehicle_prefix",
            "latitude",
            "longitude",
        )
        .agg(
            F.countDistinct("adherence_metric_signature").alias(
                "distinct_adherence_metric_signature_count"
            ),
            F.collect_set("adherence_metric_signature").alias(
                "adherence_metric_signatures"
            ),
        )
        .filter(F.col("distinct_adherence_metric_signature_count") > 1)
    )

    return (
        df.alias("src")
        .join(
            conflicting_metrics.alias("conf"),
            on=[
                "service_date",
                "captured_at_local",
                "line_sign",
                "gtfs_direction_id",
                "vehicle_prefix",
                "latitude",
                "longitude",
            ],
            how="inner",
        )
        .select(
            *[F.col(f"src.{column_name}").alias(column_name) for column_name in SOURCE_COLUMNS],
            F.col("conf.distinct_adherence_metric_signature_count")
            .cast("long")
            .alias("conflict_count"),
            F.to_json(F.col("conf.adherence_metric_signatures")).alias(
                "conflict_values"
            ),
            F.lit("CONFLICTING_ADHERENCE_METRICS").alias("audit_rule"),
            F.lit(
                "The same observed point grain is associated with conflicting "
                "adherence metrics."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for conflicting source attributes in "
        "gold.gold_line_explorer_map_points, including conflicting "
        "gtfs_direction_id values and conflicting adherence metrics within "
        "the same observed point context."
    ),
)
def gold_line_explorer_map_points_conflicting_source_attributes_audit() -> DataFrame:
    """
    Create a non-blocking audit table for conflicting source attributes in the
    Gold line explorer map points table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    conflicting_gtfs_direction_id_df = _conflicting_gtfs_direction_id(source_df)
    conflicting_adherence_metrics_df = _conflicting_adherence_metrics(source_df)

    audit_df = (
        conflicting_gtfs_direction_id_df.unionByName(conflicting_adherence_metrics_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
