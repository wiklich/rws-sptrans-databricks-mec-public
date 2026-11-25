"""
Audit pipeline for conflicting source attributes in
gold.gold_route_terminal_reference.

Purpose:
    Detect cases where multiple source attributes conflict within the declared
    Gold grain after representative endpoint selection.

Audit scope:
    - same line_sign + gtfs_direction_id + terminal_role associated with multiple stop_id values
    - same line_sign + gtfs_direction_id associated with multiple route_long_name values

Important:
    This is a non-blocking audit table.
    It exposes conflicting source attributes for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_terminal_reference"
TABLE_NAME = "gold.gold_route_terminal_reference_conflicting_source_attributes_audit"


def _conflicting_stop_ids(df: DataFrame) -> DataFrame:
    """
    Detect line_sign + gtfs_direction_id + terminal_role groups associated with
    multiple stop_id values.
    """
    conflicting_stop_ids = (
        df.groupBy("line_sign", "gtfs_direction_id", "terminal_role")
        .agg(
            F.countDistinct("stop_id").alias("distinct_stop_id_count"),
            F.collect_set("stop_id").alias("stop_ids"),
        )
        .filter(F.col("distinct_stop_id_count") > 1)
    )

    return (
        df.alias("src")
        .join(
            conflicting_stop_ids.alias("conf"),
            on=["line_sign", "gtfs_direction_id", "terminal_role"],
            how="inner",
        )
        .select(
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.route_id").alias("route_id"),
            F.col("src.route_long_name").alias("route_long_name"),
            F.col("src.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.direction_description").alias("direction_description"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.terminal_role").alias("terminal_role"),
            F.col("src.stop_id").alias("stop_id"),
            F.col("src.stop_name").alias("stop_name"),
            F.col("src.latitude").alias("latitude"),
            F.col("src.longitude").alias("longitude"),
            F.col("src.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("src.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("src.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("src.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("conf.distinct_stop_id_count").cast("long").alias("conflict_count"),
            F.to_json(F.col("conf.stop_ids")).alias("conflict_values"),
            F.lit("CONFLICTING_STOP_IDS").alias("audit_rule"),
            F.lit(
                "The same line_sign, gtfs_direction_id, and terminal_role are "
                "associated with multiple stop_id values in the final Gold table."
            ).alias("audit_message"),
        )
    )


def _conflicting_route_long_names(df: DataFrame) -> DataFrame:
    """
    Detect line_sign + gtfs_direction_id groups associated with multiple
    route_long_name values.
    """
    conflicting_route_names = (
        df.groupBy("line_sign", "gtfs_direction_id")
        .agg(
            F.countDistinct("route_long_name").alias("distinct_route_long_name_count"),
            F.collect_set("route_long_name").alias("route_long_names"),
        )
        .filter(F.col("distinct_route_long_name_count") > 1)
    )

    return (
        df.alias("src")
        .join(
            conflicting_route_names.alias("conf"),
            on=["line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .select(
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.route_id").alias("route_id"),
            F.col("src.route_long_name").alias("route_long_name"),
            F.col("src.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.direction_description").alias("direction_description"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.terminal_role").alias("terminal_role"),
            F.col("src.stop_id").alias("stop_id"),
            F.col("src.stop_name").alias("stop_name"),
            F.col("src.latitude").alias("latitude"),
            F.col("src.longitude").alias("longitude"),
            F.col("src.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("src.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("src.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("src.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_created_at").alias("record_created_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("conf.distinct_route_long_name_count")
            .cast("long")
            .alias("conflict_count"),
            F.to_json(F.col("conf.route_long_names")).alias("conflict_values"),
            F.lit("CONFLICTING_ROUTE_LONG_NAMES").alias("audit_rule"),
            F.lit(
                "The same line_sign and gtfs_direction_id are associated with "
                "multiple route_long_name values after representative endpoint selection."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for conflicting source attributes in "
        "gold.gold_route_terminal_reference, including conflicting terminal stop_id "
        "values and incompatible route_long_name values within the same Gold grain."
    ),
)
def gold_route_terminal_reference_conflicting_source_attributes_audit() -> DataFrame:
    """
    Create a non-blocking audit table for conflicting source attributes in the
    Gold route terminal reference table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    conflicting_stop_ids_df = _conflicting_stop_ids(source_df)
    conflicting_route_long_names_df = _conflicting_route_long_names(source_df)

    audit_df = (
        conflicting_stop_ids_df.unionByName(conflicting_route_long_names_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
