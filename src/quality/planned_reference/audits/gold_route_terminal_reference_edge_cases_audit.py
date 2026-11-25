"""
Audit pipeline for edge cases in
gold.gold_route_terminal_reference.

Purpose:
    Detect unusual but possible route-terminal scenarios that should be monitored.

Audit scope:
    - a route-direction has only one endpoint pair candidate supported by very few trips
    - a route-direction has no valid terminal despite upstream presence
    - origin and destination collapse to the same stop
    - terminal coordinates are missing or suspiciously duplicated across many unrelated routes

Important:
    This is a non-blocking audit table.
    It exposes unusual route terminal cases for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_terminal_reference"
TABLE_NAME = "gold.gold_route_terminal_reference_edge_cases_audit"

GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_STOP_TIMES_SOURCE = "main.silver.silver_gtfs_stop_times"
GTFS_STOPS_SOURCE = "main.silver.silver_gtfs_stops"

VERY_FEW_TRIPS_SINGLE_ENDPOINT_PAIR_CANDIDATE_THRESHOLD = 2
SUSPICIOUS_DUPLICATED_TERMINAL_COORDINATES_ROUTE_COUNT_THRESHOLD = 10


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
    Select one representative endpoint pair per line_sign + gtfs_direction_id.
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


def _build_expected_terminals() -> DataFrame:
    """
    Build expected representative terminals from upstream logic, even before
    enforcing valid terminal presence in Gold.
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
            how="left",
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
            F.col("sel.representative_first_stop_id").alias("stop_id"),
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
            how="left",
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
            F.col("sel.representative_last_stop_id").alias("stop_id"),
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

    return origin_terminals.unionByName(destination_terminals)


def _single_endpoint_pair_candidate_supported_by_few_trips(
    gold_df: DataFrame,
) -> DataFrame:
    """
    Detect route-directions that have only one endpoint pair candidate and that
    candidate is supported by very few trips.
    """
    endpoint_pair_counts = _build_endpoint_pair_counts()

    candidate_stats = (
        endpoint_pair_counts.groupBy("gtfs_load_date", "line_sign", "gtfs_direction_id")
        .agg(
            F.countDistinct(
                F.concat_ws("||", F.col("first_stop_id"), F.col("last_stop_id"))
            ).alias("candidate_pair_count"),
            F.max("endpoint_pair_trip_count").cast("long").alias(
                "top_endpoint_pair_trip_count"
            ),
        )
        .filter(
            (F.col("candidate_pair_count") == 1)
            & (
                F.col("top_endpoint_pair_trip_count")
                <= F.lit(VERY_FEW_TRIPS_SINGLE_ENDPOINT_PAIR_CANDIDATE_THRESHOLD)
            )
        )
    )

    return (
        gold_df.alias("g")
        .join(
            candidate_stats.alias("c"),
            on=["gtfs_load_date", "line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .select(
            F.col("g.line_sign").alias("line_sign"),
            F.col("g.route_id").alias("route_id"),
            F.col("g.route_long_name").alias("route_long_name"),
            F.col("g.route_name_display").alias("route_name_display"),
            F.col("g.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("g.direction_description").alias("direction_description"),
            F.col("g.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("g.terminal_role").alias("terminal_role"),
            F.col("g.stop_id").alias("stop_id"),
            F.col("g.stop_name").alias("stop_name"),
            F.col("g.latitude").alias("latitude"),
            F.col("g.longitude").alias("longitude"),
            F.col("g.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("g.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("g.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("g.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("g.gtfs_load_date").alias("gtfs_load_date"),
            F.col("g.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("g.record_created_at").alias("record_created_at"),
            F.col("g.record_hash").alias("record_hash"),
            F.col("c.top_endpoint_pair_trip_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(
                F.struct(
                    F.col("c.candidate_pair_count").cast("long").alias(
                        "candidate_pair_count"
                    ),
                    F.col("c.top_endpoint_pair_trip_count").cast("long").alias(
                        "top_endpoint_pair_trip_count"
                    ),
                    F.lit(
                        VERY_FEW_TRIPS_SINGLE_ENDPOINT_PAIR_CANDIDATE_THRESHOLD
                    ).alias("few_trips_threshold"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("SINGLE_ENDPOINT_PAIR_CANDIDATE_FEW_TRIPS").alias("audit_rule"),
            F.lit(
                "A route-direction has only one endpoint pair candidate and that "
                "candidate is supported by very few trips."
            ).alias("audit_message"),
        )
    )


def _upstream_presence_without_valid_gold_terminal(gold_df: DataFrame) -> DataFrame:
    """
    Detect route-direction-terminal combinations expected from upstream GTFS logic
    that do not have a valid terminal row in Gold.
    """
    expected_terminals = _build_expected_terminals()

    joined_df = (
        expected_terminals.alias("exp")
        .join(
            gold_df.alias("g"),
            on=["line_sign", "gtfs_direction_id", "terminal_role", "gtfs_load_date"],
            how="left",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("g.stop_id").isNull()
            | F.col("g.latitude").isNull()
            | F.col("g.longitude").isNull()
        )
    )

    return (
        joined_df.select(
            F.col("exp.line_sign").alias("line_sign"),
            F.col("exp.route_id").alias("route_id"),
            F.col("exp.route_long_name").alias("route_long_name"),
            F.col("exp.route_name_display").alias("route_name_display"),
            F.col("exp.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("exp.direction_description").alias("direction_description"),
            F.col("exp.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("exp.terminal_role").alias("terminal_role"),
            F.col("exp.stop_id").alias("stop_id"),
            F.col("exp.stop_name").alias("stop_name"),
            F.col("exp.latitude").alias("latitude"),
            F.col("exp.longitude").alias("longitude"),
            F.col("exp.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("exp.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("exp.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("exp.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("exp.gtfs_load_date").alias("gtfs_load_date"),
            F.coalesce(F.col("g.gtfs_loaded_at"), F.col("exp.gtfs_loaded_at")).alias(
                "gtfs_loaded_at"
            ),
            F.col("g.record_created_at").alias("record_created_at"),
            F.col("g.record_hash").alias("record_hash"),
            F.lit(None).cast("long").alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.when(F.col("g.line_sign").isNull(), F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("gold_row_missing"),
                    F.when(F.col("g.stop_id").isNull(), F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("gold_stop_id_missing"),
                    F.when(F.col("g.latitude").isNull(), F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("gold_latitude_missing"),
                    F.when(F.col("g.longitude").isNull(), F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("gold_longitude_missing"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("UPSTREAM_PRESENCE_WITHOUT_VALID_GOLD_TERMINAL").alias(
                "audit_rule"
            ),
            F.lit(
                "A route-direction-terminal exists in upstream GTFS endpoint logic "
                "but does not have a valid terminal row in the Gold table."
            ).alias("audit_message"),
        )
    )


def _origin_destination_collapse_to_same_stop(gold_df: DataFrame) -> DataFrame:
    """
    Detect cases where origin and destination collapse to the same stop_id
    within the same line_sign + gtfs_direction_id.
    """
    collapsed_route_directions = (
        gold_df.groupBy("line_sign", "gtfs_direction_id", "gtfs_load_date")
        .agg(
            F.countDistinct("terminal_role").alias("distinct_terminal_role_count"),
            F.countDistinct("stop_id").alias("distinct_stop_id_count"),
            F.collect_set("stop_id").alias("stop_ids"),
        )
        .filter(
            (F.col("distinct_terminal_role_count") == 2)
            & (F.col("distinct_stop_id_count") == 1)
        )
    )

    return (
        gold_df.alias("g")
        .join(
            collapsed_route_directions.alias("c"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="inner",
        )
        .select(
            F.col("g.line_sign").alias("line_sign"),
            F.col("g.route_id").alias("route_id"),
            F.col("g.route_long_name").alias("route_long_name"),
            F.col("g.route_name_display").alias("route_name_display"),
            F.col("g.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("g.direction_description").alias("direction_description"),
            F.col("g.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("g.terminal_role").alias("terminal_role"),
            F.col("g.stop_id").alias("stop_id"),
            F.col("g.stop_name").alias("stop_name"),
            F.col("g.latitude").alias("latitude"),
            F.col("g.longitude").alias("longitude"),
            F.col("g.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("g.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("g.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("g.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("g.gtfs_load_date").alias("gtfs_load_date"),
            F.col("g.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("g.record_created_at").alias("record_created_at"),
            F.col("g.record_hash").alias("record_hash"),
            F.col("c.distinct_stop_id_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(F.col("c.stop_ids")).alias("edge_case_metric_values"),
            F.lit("ORIGIN_DESTINATION_COLLAPSE_TO_SAME_STOP").alias("audit_rule"),
            F.lit(
                "Origin and destination collapse to the same stop_id within the "
                "same route-direction."
            ).alias("audit_message"),
        )
    )


def _missing_terminal_coordinates(gold_df: DataFrame) -> DataFrame:
    """
    Detect rows where terminal coordinates are missing in the Gold table.
    This should normally be zero because inline expectations already enforce it.
    """
    return (
        gold_df.filter(F.col("latitude").isNull() | F.col("longitude").isNull())
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
            F.col("representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit(None).cast("long").alias("edge_case_metric_count"),
            F.lit(None).cast("string").alias("edge_case_metric_values"),
            F.lit("MISSING_TERMINAL_COORDINATES").alias("audit_rule"),
            F.lit(
                "Terminal coordinates are missing in the Gold table. This should "
                "normally be prevented by inline expectations."
            ).alias("audit_message"),
        )
    )


def _suspiciously_duplicated_terminal_coordinates(gold_df: DataFrame) -> DataFrame:
    """
    Detect terminal coordinates reused across many distinct line_sign values.

    This is a monitoring rule. High reuse may be valid for large terminals,
    but should still be visible.
    """
    duplicated_coordinates = (
        gold_df.groupBy("latitude", "longitude")
        .agg(
            F.countDistinct("line_sign").alias("distinct_line_sign_count"),
            F.countDistinct(
                F.concat_ws(
                    "||",
                    F.col("line_sign"),
                    F.col("gtfs_direction_id").cast("string"),
                    F.col("terminal_role"),
                )
            ).alias("distinct_route_direction_terminal_count"),
            F.collect_set("stop_id").alias("stop_ids"),
        )
        .filter(
            F.col("distinct_line_sign_count")
            > F.lit(SUSPICIOUS_DUPLICATED_TERMINAL_COORDINATES_ROUTE_COUNT_THRESHOLD)
        )
    )

    return (
        gold_df.alias("g")
        .join(
            duplicated_coordinates.alias("d"),
            on=["latitude", "longitude"],
            how="inner",
        )
        .select(
            F.col("g.line_sign").alias("line_sign"),
            F.col("g.route_id").alias("route_id"),
            F.col("g.route_long_name").alias("route_long_name"),
            F.col("g.route_name_display").alias("route_name_display"),
            F.col("g.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("g.direction_description").alias("direction_description"),
            F.col("g.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("g.terminal_role").alias("terminal_role"),
            F.col("g.stop_id").alias("stop_id"),
            F.col("g.stop_name").alias("stop_name"),
            F.col("g.latitude").alias("latitude"),
            F.col("g.longitude").alias("longitude"),
            F.col("g.representative_first_stop_id").alias(
                "representative_first_stop_id"
            ),
            F.col("g.representative_last_stop_id").alias(
                "representative_last_stop_id"
            ),
            F.col("g.endpoint_pair_trip_count").alias("endpoint_pair_trip_count"),
            F.col("g.endpoint_pair_rank").alias("endpoint_pair_rank"),
            F.col("g.gtfs_load_date").alias("gtfs_load_date"),
            F.col("g.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("g.record_created_at").alias("record_created_at"),
            F.col("g.record_hash").alias("record_hash"),
            F.col("d.distinct_line_sign_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(
                F.struct(
                    F.col("d.distinct_line_sign_count").cast("long").alias(
                        "distinct_line_sign_count"
                    ),
                    F.col("d.distinct_route_direction_terminal_count")
                    .cast("long")
                    .alias("distinct_route_direction_terminal_count"),
                    F.col("d.stop_ids").alias("stop_ids"),
                    F.lit(
                        SUSPICIOUS_DUPLICATED_TERMINAL_COORDINATES_ROUTE_COUNT_THRESHOLD
                    ).alias("route_count_threshold"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("SUSPICIOUSLY_DUPLICATED_TERMINAL_COORDINATES").alias(
                "audit_rule"
            ),
            F.lit(
                "Terminal coordinates are duplicated across many distinct routes. "
                "This may be valid for large terminals, but should be monitored."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "gold.gold_route_terminal_reference, including weak single-candidate "
        "endpoint pairs, upstream presence without valid Gold terminal rows, "
        "origin-destination collapse to the same stop, and suspicious terminal "
        "coordinate reuse."
    ),
)
def gold_route_terminal_reference_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for unusual but possible edge cases
    in the Gold route terminal reference table.
    """
    gold_df = spark.read.table(SOURCE_TABLE)

    single_candidate_few_trips_df = (
        _single_endpoint_pair_candidate_supported_by_few_trips(gold_df)
    )
    upstream_without_valid_gold_df = _upstream_presence_without_valid_gold_terminal(
        gold_df
    )
    collapsed_origin_destination_df = _origin_destination_collapse_to_same_stop(
        gold_df
    )
    missing_coordinates_df = _missing_terminal_coordinates(gold_df)
    duplicated_coordinates_df = _suspiciously_duplicated_terminal_coordinates(gold_df)

    audit_df = (
        single_candidate_few_trips_df.unionByName(upstream_without_valid_gold_df)
        .unionByName(collapsed_origin_destination_df)
        .unionByName(missing_coordinates_df)
        .unionByName(duplicated_coordinates_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
