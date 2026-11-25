"""
Audit pipeline for logical inconsistencies in
gold.gold_route_terminal_reference.

Purpose:
    Detect violations in core Gold route terminal logic.

Audit scope:
    - endpoint_pair_rank <> 1 for a selected representative terminal pair
    - endpoint_pair_trip_count <= 0
    - terminal_role outside approved domain
    - representative_first_stop_id = representative_last_stop_id

Important:
    This is a non-blocking audit table.
    It exposes inconsistent Gold route terminal records for investigation
    without dropping records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_terminal_reference"
TABLE_NAME = "gold.gold_route_terminal_reference_logical_inconsistencies_audit"

APPROVED_TERMINAL_ROLES = ["ORIGIN", "DESTINATION"]


def _endpoint_pair_rank_not_one(df: DataFrame) -> DataFrame:
    """
    Detect rows where the selected representative endpoint pair does not have
    endpoint_pair_rank = 1.
    """
    return (
        df.filter(F.col("endpoint_pair_rank") != 1)
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("ENDPOINT_PAIR_RANK_NOT_ONE").alias("audit_rule"),
            F.lit(
                "A selected representative terminal pair has endpoint_pair_rank "
                "different from 1, which violates the expected representative "
                "endpoint selection logic."
            ).alias("audit_message"),
        )
    )


def _endpoint_pair_trip_count_non_positive(df: DataFrame) -> DataFrame:
    """
    Detect rows where endpoint_pair_trip_count is less than or equal to 0.
    """
    return (
        df.filter(F.col("endpoint_pair_trip_count") <= 0)
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("ENDPOINT_PAIR_TRIP_COUNT_NON_POSITIVE").alias("audit_rule"),
            F.lit(
                "endpoint_pair_trip_count is less than or equal to 0, which "
                "violates the expected representative endpoint usage logic."
            ).alias("audit_message"),
        )
    )


def _terminal_role_outside_approved_domain(df: DataFrame) -> DataFrame:
    """
    Detect rows where terminal_role is outside the approved domain.
    """
    return (
        df.filter(~F.col("terminal_role").isin(APPROVED_TERMINAL_ROLES))
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("TERMINAL_ROLE_OUTSIDE_APPROVED_DOMAIN").alias("audit_rule"),
            F.lit(
                "terminal_role is outside the approved domain. Expected values "
                "are ORIGIN or DESTINATION."
            ).alias("audit_message"),
        )
    )


def _representative_first_stop_equals_last_stop(df: DataFrame) -> DataFrame:
    """
    Detect rows where representative_first_stop_id equals representative_last_stop_id.

    This may be valid in rare loop-like scenarios, but is treated as a logical
    inconsistency candidate that should be reviewed.
    """
    return (
        df.filter(
            F.col("representative_first_stop_id")
            == F.col("representative_last_stop_id")
        )
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("terminal_role").alias("terminal_role"),
            F.col("stop_id").alias("stop_id"),
            F.col("stop_name").alias("stop_name"),
            F.col("latitude").alias("latitude"),
            F.col("longitude").alias("longitude"),
            F.col("representative_first_stop_id").alias("representative_first_stop_id"),
            F.col("representative_last_stop_id").alias("representative_last_stop_id"),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit("REPRESENTATIVE_FIRST_STOP_EQUALS_LAST_STOP").alias("audit_rule"),
            F.lit(
                "representative_first_stop_id equals representative_last_stop_id. "
                "This may be valid in rare scenarios, but should be reviewed."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "gold.gold_route_terminal_reference, including representative endpoint "
        "rank issues, non-positive endpoint pair trip counts, invalid terminal "
        "roles, and cases where the representative first and last stop are equal."
    ),
)
def gold_route_terminal_reference_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    Gold route terminal reference table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    endpoint_pair_rank_not_one_df = _endpoint_pair_rank_not_one(source_df)
    endpoint_pair_trip_count_non_positive_df = _endpoint_pair_trip_count_non_positive(
        source_df
    )
    terminal_role_outside_domain_df = _terminal_role_outside_approved_domain(source_df)
    representative_first_stop_equals_last_stop_df = (
        _representative_first_stop_equals_last_stop(source_df)
    )

    audit_df = (
        endpoint_pair_rank_not_one_df.unionByName(
            endpoint_pair_trip_count_non_positive_df
        )
        .unionByName(terminal_role_outside_domain_df)
        .unionByName(representative_first_stop_equals_last_stop_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
