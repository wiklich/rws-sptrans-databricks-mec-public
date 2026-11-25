"""
Gold route terminal reference pipeline.

Purpose:
    Build a business-ready, map-ready representative terminal reference for each:
    - line_sign
    - gtfs_direction_id
    - terminal_role

Source:
    - main.silver.silver_gtfs_routes
    - main.silver.silver_gtfs_trips
    - main.silver.silver_gtfs_stop_times
    - main.silver.silver_gtfs_stops

Important implementation note:
    This implementation materializes only the latest common GTFS snapshot
    available across:
    - routes
    - trips
    - stop_times
    - stops

Grain:
    One row per:
        line_sign + gtfs_direction_id + terminal_role
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.gold_planned_reference.schemas.schema_gold_route_terminal_reference import (
    GOLD_ROUTE_TERMINAL_REFERENCE_SCHEMA,
)

TABLE_NAME = "gold.gold_route_terminal_reference"

GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_STOP_TIMES_SOURCE = "main.silver.silver_gtfs_stop_times"
GTFS_STOPS_SOURCE = "main.silver.silver_gtfs_stops"


def _build_latest_common_snapshot() -> DataFrame:
    """
    Build a one-row DataFrame containing the latest common GTFS snapshot date
    shared across routes, trips, stop_times, and stops.
    """
    routes_dates = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    trips_dates = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    stop_times_dates = (
        spark.read.table(GTFS_STOP_TIMES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    stops_dates = (
        spark.read.table(GTFS_STOPS_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    return (
        routes_dates.join(trips_dates, on="gtfs_load_date", how="inner")
        .join(stop_times_dates, on="gtfs_load_date", how="inner")
        .join(stops_dates, on="gtfs_load_date", how="inner")
        .agg(F.max("gtfs_load_date").alias("gtfs_load_date"))
        .filter(F.col("gtfs_load_date").isNotNull())
    )


def _build_routes_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("route_id"),
            F.col("route_short_name").alias("line_sign"),
            F.col("route_long_name"),
            F.col("route_name_display"),
            F.col("gtfs_loaded_at").alias("route_gtfs_loaded_at"),
        )
        .filter(
            F.col("route_id").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
        )
    )


def _build_trips_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("route_id"),
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("trip_headsign"),
            F.col("gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
    )


def _build_stop_times_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_STOP_TIMES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("stop_id"),
            F.col("stop_sequence").cast("int").alias("stop_sequence"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("stop_id").isNotNull()
            & F.col("stop_sequence").isNotNull()
        )
    )


def _build_stops_latest() -> DataFrame:
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_STOPS_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("stop_id"),
            F.col("stop_name"),
            F.col("stop_lat").alias("latitude"),
            F.col("stop_lon").alias("longitude"),
            F.col("gtfs_loaded_at").alias("stop_gtfs_loaded_at"),
        )
        .filter(
            F.col("stop_id").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
    )


def _build_route_trip_base() -> DataFrame:
    routes_latest = _build_routes_latest()
    trips_latest = _build_trips_latest()

    return routes_latest.join(
        trips_latest,
        on=["gtfs_load_date", "route_id"],
        how="inner",
    ).select(
        F.col("gtfs_load_date"),
        F.col("line_sign"),
        F.col("route_id"),
        F.col("route_long_name"),
        F.col("route_name_display"),
        F.col("trip_id"),
        F.col("gtfs_direction_id"),
        F.col("direction_description"),
        F.col("trip_headsign"),
        F.col("route_gtfs_loaded_at"),
        F.col("trip_gtfs_loaded_at"),
    )


def _build_first_trip_stops() -> DataFrame:
    stop_times_latest = _build_stop_times_latest()

    first_stop_window = Window.partitionBy("gtfs_load_date", "trip_id").orderBy(
        F.col("stop_sequence").asc()
    )

    return (
        stop_times_latest.withColumn(
            "first_stop_row_number",
            F.row_number().over(first_stop_window),
        )
        .filter(F.col("first_stop_row_number") == 1)
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("stop_id").alias("first_stop_id"),
        )
    )


def _build_last_trip_stops() -> DataFrame:
    stop_times_latest = _build_stop_times_latest()

    last_stop_window = Window.partitionBy("gtfs_load_date", "trip_id").orderBy(
        F.col("stop_sequence").desc()
    )

    return (
        stop_times_latest.withColumn(
            "last_stop_row_number",
            F.row_number().over(last_stop_window),
        )
        .filter(F.col("last_stop_row_number") == 1)
        .select(
            F.col("gtfs_load_date"),
            F.col("trip_id"),
            F.col("stop_id").alias("last_stop_id"),
        )
    )


def _build_trip_endpoints() -> DataFrame:
    route_trip_base = _build_route_trip_base()
    first_trip_stops = _build_first_trip_stops()
    last_trip_stops = _build_last_trip_stops()

    return (
        route_trip_base.join(
            first_trip_stops,
            on=["gtfs_load_date", "trip_id"],
            how="inner",
        )
        .join(
            last_trip_stops,
            on=["gtfs_load_date", "trip_id"],
            how="inner",
        )
        .select(
            F.col("gtfs_load_date"),
            F.col("line_sign"),
            F.col("route_id"),
            F.col("route_long_name"),
            F.col("route_name_display"),
            F.col("trip_id"),
            F.col("gtfs_direction_id"),
            F.col("direction_description"),
            F.col("trip_headsign"),
            F.col("first_stop_id"),
            F.col("last_stop_id"),
            F.col("route_gtfs_loaded_at"),
            F.col("trip_gtfs_loaded_at"),
        )
    )


def _build_endpoint_pair_counts() -> DataFrame:
    trip_endpoints = _build_trip_endpoints()

    return (
        trip_endpoints.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "first_stop_id",
            "last_stop_id",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias("endpoint_pair_trip_count"),
        )
    )


def _build_endpoint_pair_reference_metadata() -> DataFrame:
    """
    Choose deterministic representative route metadata for each endpoint pair.
    """
    trip_endpoints = _build_trip_endpoints()

    pair_reference_candidates = (
        trip_endpoints.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "first_stop_id",
            "last_stop_id",
            "route_id",
            "route_long_name",
            "route_name_display",
            "direction_description",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias(
                "route_trip_count_using_endpoint_pair"
            ),
            F.min("trip_headsign").alias("trip_headsign_reference"),
            F.max("route_gtfs_loaded_at").alias("route_gtfs_loaded_at"),
            F.max("trip_gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
    )

    pair_reference_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
        "first_stop_id",
        "last_stop_id",
    ).orderBy(
        F.col("route_trip_count_using_endpoint_pair").desc(),
        F.col("route_id").asc(),
    )

    return (
        pair_reference_candidates.withColumn(
            "pair_reference_rank",
            F.row_number().over(pair_reference_window),
        )
        .filter(F.col("pair_reference_rank") == 1)
        .drop("pair_reference_rank")
        .withColumn(
            "route_trip_gtfs_loaded_at",
            F.greatest(
                F.col("route_gtfs_loaded_at"),
                F.col("trip_gtfs_loaded_at"),
            ),
        )
        .drop("route_gtfs_loaded_at", "trip_gtfs_loaded_at")
    )


def _build_representative_endpoint_pairs() -> DataFrame:
    """
    Select one representative endpoint pair per line_sign + gtfs_direction_id
    using deterministic ranking.
    """
    endpoint_pair_counts = _build_endpoint_pair_counts()

    pair_rank_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
    ).orderBy(
        F.col("endpoint_pair_trip_count").desc(),
        F.col("first_stop_id").asc(),
        F.col("last_stop_id").asc(),
    )

    return (
        endpoint_pair_counts.withColumn(
            "endpoint_pair_rank",
            F.row_number().over(pair_rank_window).cast("int"),
        )
        .filter(F.col("endpoint_pair_rank") == 1)
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Gold spatial terminal reference table that provides one "
        "representative terminal row per line_sign, gtfs_direction_id, and "
        "terminal_role, ready for direct terminal-map consumption and "
        "downstream analytical joins."
    ),
    schema=GOLD_ROUTE_TERMINAL_REFERENCE_SCHEMA,
)
@dp.expect_or_drop("valid_line_sign", "line_sign IS NOT NULL")
@dp.expect_or_drop("valid_gtfs_direction_id", "gtfs_direction_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_terminal_role",
    "terminal_role IN ('ORIGIN', 'DESTINATION')",
)
@dp.expect_or_drop("valid_stop_id", "stop_id IS NOT NULL")
@dp.expect_or_drop("valid_latitude", "latitude IS NOT NULL")
@dp.expect_or_drop("valid_longitude", "longitude IS NOT NULL")
def gold_route_terminal_reference() -> DataFrame:
    """
    Create a business-ready representative route terminal reference table.

    Final output:
        One row per:
            line_sign + gtfs_direction_id + terminal_role
    """
    representative_pairs = _build_representative_endpoint_pairs()
    pair_reference_metadata = _build_endpoint_pair_reference_metadata()
    stops_latest = _build_stops_latest()

    selected_pairs = (
        representative_pairs.alias("pair")
        .join(
            pair_reference_metadata.alias("meta"),
            on=[
                "gtfs_load_date",
                "line_sign",
                "gtfs_direction_id",
                "first_stop_id",
                "last_stop_id",
            ],
            how="inner",
        )
        .select(
            F.col("pair.gtfs_load_date").alias("gtfs_load_date"),
            F.col("pair.line_sign").alias("line_sign"),
            F.col("meta.route_id").alias("route_id"),
            F.col("meta.route_long_name").alias("route_long_name"),
            F.col("meta.route_name_display").alias("route_name_display"),
            F.col("pair.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("meta.direction_description").alias("direction_description"),
            F.col("meta.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("pair.first_stop_id").alias("representative_first_stop_id"),
            F.col("pair.last_stop_id").alias("representative_last_stop_id"),
            F.col("pair.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("pair.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("meta.route_trip_gtfs_loaded_at").alias("route_trip_gtfs_loaded_at"),
        )
    )

    origin_terminals = (
        selected_pairs.alias("sel")
        .join(
            stops_latest.alias("stp"),
            on=[
                F.col("sel.gtfs_load_date") == F.col("stp.gtfs_load_date"),
                F.col("sel.representative_first_stop_id") == F.col("stp.stop_id"),
            ],
            how="inner",
        )
        .select(
            F.col("sel.line_sign").alias("line_sign"),
            F.col("sel.route_id").alias("route_id"),
            F.col("sel.route_long_name").alias("route_long_name"),
            F.col("sel.route_name_display").alias("route_name_display"),
            F.col("sel.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("sel.direction_description").alias("direction_description"),
            F.col("sel.trip_headsign_reference").alias("trip_headsign_reference"),
            F.lit("ORIGIN").alias("terminal_role"),
            F.col("stp.stop_id").alias("stop_id"),
            F.col("stp.stop_name").alias("stop_name"),
            F.col("stp.latitude").alias("latitude"),
            F.col("stp.longitude").alias("longitude"),
            F.col("sel.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("sel.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("sel.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("sel.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("sel.gtfs_load_date").alias("gtfs_load_date"),
            F.greatest(
                F.col("sel.route_trip_gtfs_loaded_at"),
                F.col("stp.stop_gtfs_loaded_at"),
            ).alias("gtfs_loaded_at"),
        )
    )

    destination_terminals = (
        selected_pairs.alias("sel")
        .join(
            stops_latest.alias("stp"),
            on=[
                F.col("sel.gtfs_load_date") == F.col("stp.gtfs_load_date"),
                F.col("sel.representative_last_stop_id") == F.col("stp.stop_id"),
            ],
            how="inner",
        )
        .select(
            F.col("sel.line_sign").alias("line_sign"),
            F.col("sel.route_id").alias("route_id"),
            F.col("sel.route_long_name").alias("route_long_name"),
            F.col("sel.route_name_display").alias("route_name_display"),
            F.col("sel.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("sel.direction_description").alias("direction_description"),
            F.col("sel.trip_headsign_reference").alias("trip_headsign_reference"),
            F.lit("DESTINATION").alias("terminal_role"),
            F.col("stp.stop_id").alias("stop_id"),
            F.col("stp.stop_name").alias("stop_name"),
            F.col("stp.latitude").alias("latitude"),
            F.col("stp.longitude").alias("longitude"),
            F.col("sel.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("sel.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("sel.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("sel.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("sel.gtfs_load_date").alias("gtfs_load_date"),
            F.greatest(
                F.col("sel.route_trip_gtfs_loaded_at"),
                F.col("stp.stop_gtfs_loaded_at"),
            ).alias("gtfs_loaded_at"),
        )
    )

    final_df = (
        origin_terminals.unionByName(destination_terminals)
        .withColumn("record_created_at", F.current_timestamp())
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("line_sign"),
                    F.col("gtfs_direction_id").cast("string"),
                    F.col("terminal_role"),
                    F.col("stop_id"),
                    F.col("representative_first_stop_id"),
                    F.col("representative_last_stop_id"),
                    F.col("gtfs_load_date").cast("string"),
                ),
                256,
            ),
        )
        .select(
            F.col("line_sign").cast("string").alias("line_sign"),
            F.col("route_id").cast("string").alias("route_id"),
            F.col("route_long_name").cast("string").alias("route_long_name"),
            F.col("route_name_display").cast("string").alias("route_name_display"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("direction_description").cast("string").alias(
                "direction_description"
            ),
            F.col("trip_headsign_reference").cast("string").alias(
                "trip_headsign_reference"
            ),
            F.col("terminal_role").cast("string").alias("terminal_role"),
            F.col("stop_id").cast("string").alias("stop_id"),
            F.col("stop_name").cast("string").alias("stop_name"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("representative_first_stop_id").cast("string").alias(
                "representative_first_stop_id"
            ),
            F.col("representative_last_stop_id").cast("string").alias(
                "representative_last_stop_id"
            ),
            F.col("endpoint_pair_trip_count").cast("long").alias(
                "endpoint_pair_trip_count"
            ),
            F.col("endpoint_pair_rank").cast("int").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
            F.col("record_created_at").cast("timestamp").alias("record_created_at"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )
        .orderBy(
            F.col("line_sign").asc(),
            F.col("gtfs_direction_id").asc(),
            F.col("terminal_role").asc(),
        )
    )

    return final_df
