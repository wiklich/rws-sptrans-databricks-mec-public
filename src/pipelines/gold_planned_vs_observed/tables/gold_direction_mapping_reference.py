"""
Gold pipeline for validated observed-to-GTFS direction mapping.

Purpose:
    Build a Gold analytical reference table that maps:
    - observed SPTrans direction values
    - GTFS direction_id values

Business interpretation:
    This dataset is an analytical direction reference.
    It is not an official operational truth table.

Grain:
    One row per:
        line_sign + observed_direction

Main logic:
    1. Read observed SPTrans positions.
    2. For each vehicle/day/line/direction, identify first and last observed points.
    3. Aggregate first/last observed points into an observed endpoint pattern.
    4. Build GTFS direction endpoints from first/last stop of trips.
    5. Compare observed endpoints to GTFS endpoints using spatial distance.
    6. Rank candidate GTFS directions per observed line/direction.
    7. Compute confidence and production safety flags.

Important notes:
    - This version intentionally does not use GTFS shapes.
    - The mapping is based on endpoint inference using:
        - observed first/last vehicle behavior
        - GTFS first/last stop behavior
    - Downstream production joins should use only:
        validated_flag = true and review_required_flag = false
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_direction_mapping_reference import (
    GOLD_DIRECTION_MAPPING_REFERENCE_SCHEMA,
)

TABLE_NAME = "gold.gold_direction_mapping_reference"

OBSERVED_SOURCE = "main.silver.silver_sptrans_positions"
GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_STOP_TIMES_SOURCE = "main.silver.silver_gtfs_stop_times"
GTFS_STOPS_SOURCE = "main.silver.silver_gtfs_stops"

EVIDENCE_TYPE = "SPATIAL_ENDPOINT_INFERENCE"
MIN_VALIDATED_VEHICLE_COUNT = 5
HIGH_CONFIDENCE_TOTAL_DISTANCE_KM = 1.5
MODERATE_CONFIDENCE_TOTAL_DISTANCE_KM = 3.0
HIGH_CONFIDENCE_GAP_KM = 1.0
MODERATE_CONFIDENCE_GAP_KM = 0.3
NO_MATCH_GTFS_DIRECTION_ID = -1


def _haversine_km(
    lat1_col: str,
    lon1_col: str,
    lat2_col: str,
    lon2_col: str,
) -> F.Column:
    """
    Compute haversine distance in kilometers between two latitude/longitude pairs.
    """
    earth_radius_km = 6371.0

    lat1 = F.radians(F.col(lat1_col))
    lon1 = F.radians(F.col(lon1_col))
    lat2 = F.radians(F.col(lat2_col))
    lon2 = F.radians(F.col(lon2_col))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        F.pow(F.sin(dlat / F.lit(2.0)), 2)
        + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / F.lit(2.0)), 2)
    )

    c = F.lit(2.0) * F.asin(F.sqrt(a))

    return F.lit(earth_radius_km) * c


def _build_observed_endpoint_patterns() -> DataFrame:
    """
    Build observed endpoint patterns by line_sign + observed_direction.

    Logic:
        - For each service_date + line_sign + observed_direction + vehicle_id,
          capture the first and last observed ping.
        - Aggregate those vehicle-level endpoints into one observed pattern
          per line_sign + observed_direction.
    """
    observed = (
        spark.read.table(OBSERVED_SOURCE)
        .select(
            F.col("line_sign"),
            F.col("direction").cast("int").alias("observed_direction"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_id"),
            F.col("captured_date_local").alias("service_date"),
            F.col("captured_at_local").alias("event_timestamp"),
            F.col("latitude"),
            F.col("longitude"),
        )
        .filter(
            F.col("line_sign").isNotNull()
            & F.col("observed_direction").isNotNull()
            & F.col("vehicle_id").isNotNull()
            & F.col("service_date").isNotNull()
            & F.col("event_timestamp").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
    )

    first_window = Window.partitionBy(
        "service_date",
        "line_sign",
        "observed_direction",
        "vehicle_id",
    ).orderBy("event_timestamp")

    last_window = Window.partitionBy(
        "service_date",
        "line_sign",
        "observed_direction",
        "vehicle_id",
    ).orderBy(F.col("event_timestamp").desc())

    first_points = (
        observed.withColumn("rn_first", F.row_number().over(first_window))
        .filter(F.col("rn_first") == 1)
        .select(
            "service_date",
            "line_sign",
            "observed_direction",
            "vehicle_id",
            F.col("latitude").alias("first_lat"),
            F.col("longitude").alias("first_lon"),
        )
    )

    last_points = (
        observed.withColumn("rn_last", F.row_number().over(last_window))
        .filter(F.col("rn_last") == 1)
        .select(
            "service_date",
            "line_sign",
            "observed_direction",
            "vehicle_id",
            F.col("latitude").alias("last_lat"),
            F.col("longitude").alias("last_lon"),
        )
    )

    vehicle_endpoints = (
        first_points.join(
            last_points,
            on=["service_date", "line_sign", "observed_direction", "vehicle_id"],
            how="inner",
        )
    )

    observed_patterns = (
        vehicle_endpoints.groupBy("line_sign", "observed_direction")
        .agg(
            F.avg("first_lat").alias("observed_first_lat"),
            F.avg("first_lon").alias("observed_first_lon"),
            F.avg("last_lat").alias("observed_last_lat"),
            F.avg("last_lon").alias("observed_last_lon"),
            F.countDistinct("vehicle_id").cast("long").alias("vehicle_count"),
        )
        .filter(
            F.col("line_sign").isNotNull()
            & F.col("observed_direction").isNotNull()
            & F.col("vehicle_count").isNotNull()
        )
    )

    return observed_patterns


def _build_gtfs_direction_endpoints() -> DataFrame:
    """
    Build representative GTFS endpoints by line_sign + gtfs_direction_id.

    Logic:
        - Derive first and last stop per trip from stop_sequence.
        - Join trips and routes to connect endpoints to line_sign.
        - For each line_sign + gtfs_direction_id, keep the most frequent
          first_stop_id + last_stop_id pair as the representative endpoint pair.
    """
    routes = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select(
            F.col("route_id"),
            F.col("route_short_name").alias("line_sign"),
            F.col("route_long_name"),
        )
        .filter(F.col("route_id").isNotNull() & F.col("line_sign").isNotNull())
    )

    trips = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select(
            F.col("trip_id"),
            F.col("route_id"),
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("trip_headsign"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
    )

    stop_times = (
        spark.read.table(GTFS_STOP_TIMES_SOURCE)
        .select(
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

    stops = (
        spark.read.table(GTFS_STOPS_SOURCE)
        .select(
            F.col("stop_id"),
            F.col("stop_name"),
            F.col("stop_lat"),
            F.col("stop_lon"),
        )
        .filter(
            F.col("stop_id").isNotNull()
            & F.col("stop_lat").isNotNull()
            & F.col("stop_lon").isNotNull()
        )
    )

    first_stop_window = Window.partitionBy("trip_id").orderBy("stop_sequence")
    last_stop_window = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").desc())

    first_trip_stops = (
        stop_times.withColumn("rn_first", F.row_number().over(first_stop_window))
        .filter(F.col("rn_first") == 1)
        .select(
            "trip_id",
            F.col("stop_id").alias("first_stop_id"),
        )
    )

    last_trip_stops = (
        stop_times.withColumn("rn_last", F.row_number().over(last_stop_window))
        .filter(F.col("rn_last") == 1)
        .select(
            "trip_id",
            F.col("stop_id").alias("last_stop_id"),
        )
    )

    trip_endpoints = (
        trips.join(first_trip_stops, on="trip_id", how="inner")
        .join(last_trip_stops, on="trip_id", how="inner")
        .join(routes, on="route_id", how="inner")
        .join(
            stops.select(
                F.col("stop_id").alias("first_stop_id"),
                F.col("stop_name").alias("first_stop_name"),
                F.col("stop_lat").alias("gtfs_first_lat"),
                F.col("stop_lon").alias("gtfs_first_lon"),
            ),
            on="first_stop_id",
            how="inner",
        )
        .join(
            stops.select(
                F.col("stop_id").alias("last_stop_id"),
                F.col("stop_name").alias("last_stop_name"),
                F.col("stop_lat").alias("gtfs_last_lat"),
                F.col("stop_lon").alias("gtfs_last_lon"),
            ),
            on="last_stop_id",
            how="inner",
        )
        .select(
            F.col("line_sign"),
            F.col("gtfs_direction_id"),
            F.col("trip_id"),
            F.col("trip_headsign"),
            F.col("route_long_name"),
            F.col("first_stop_id"),
            F.col("last_stop_id"),
            F.col("first_stop_name"),
            F.col("last_stop_name"),
            F.col("gtfs_first_lat"),
            F.col("gtfs_first_lon"),
            F.col("gtfs_last_lat"),
            F.col("gtfs_last_lon"),
        )
    )

    representative_pair_window = Window.partitionBy(
        "line_sign", "gtfs_direction_id"
    ).orderBy(
        F.col("trip_count").desc(),
        F.col("first_stop_id").asc(),
        F.col("last_stop_id").asc(),
    )

    representative_gtfs_endpoints = (
        trip_endpoints.groupBy(
            "line_sign",
            "gtfs_direction_id",
            "first_stop_id",
            "last_stop_id",
            "first_stop_name",
            "last_stop_name",
            "gtfs_first_lat",
            "gtfs_first_lon",
            "gtfs_last_lat",
            "gtfs_last_lon",
        )
        .agg(
            F.countDistinct("trip_id").alias("trip_count"),
            F.first("route_long_name", ignorenulls=True).alias("route_long_name"),
            F.first("trip_headsign", ignorenulls=True).alias("trip_headsign_reference"),
        )
        .withColumn("rn_best_pair", F.row_number().over(representative_pair_window))
        .filter(F.col("rn_best_pair") == 1)
        .select(
            "line_sign",
            "gtfs_direction_id",
            "route_long_name",
            "trip_headsign_reference",
            "first_stop_name",
            "last_stop_name",
            "gtfs_first_lat",
            "gtfs_first_lon",
            "gtfs_last_lat",
            "gtfs_last_lon",
            "trip_count",
        )
    )

    return representative_gtfs_endpoints


def _build_candidate_mappings() -> DataFrame:
    """
    Build candidate mappings between observed directions and GTFS directions.

    Returns one candidate row per:
        line_sign + observed_direction + candidate_gtfs_direction_id
    """
    observed_patterns = _build_observed_endpoint_patterns()
    gtfs_endpoints = _build_gtfs_direction_endpoints()

    candidates = (
        observed_patterns.alias("obs")
        .join(
            gtfs_endpoints.alias("gtfs"),
            on="line_sign",
            how="left",
        )
        .withColumn(
            "dist_first_km",
            F.when(
                F.col("gtfs.gtfs_direction_id").isNotNull(),
                _haversine_km(
                    "obs.observed_first_lat",
                    "obs.observed_first_lon",
                    "gtfs.gtfs_first_lat",
                    "gtfs.gtfs_first_lon",
                ),
            ),
        )
        .withColumn(
            "dist_last_km",
            F.when(
                F.col("gtfs.gtfs_direction_id").isNotNull(),
                _haversine_km(
                    "obs.observed_last_lat",
                    "obs.observed_last_lon",
                    "gtfs.gtfs_last_lat",
                    "gtfs.gtfs_last_lon",
                ),
            ),
        )
        .withColumn(
            "total_dist_km",
            F.when(
                F.col("gtfs.gtfs_direction_id").isNotNull(),
                F.col("dist_first_km") + F.col("dist_last_km"),
            ),
        )
        .withColumn(
            "evidence_score",
            F.when(
                F.col("gtfs.gtfs_direction_id").isNotNull()
                & F.col("total_dist_km").isNotNull(),
                F.lit(100.0) / (F.lit(1.0) + F.col("total_dist_km")),
            ),
        )
        .select(
            F.col("line_sign"),
            F.col("obs.observed_direction").alias("observed_direction"),
            F.col("obs.vehicle_count").alias("vehicle_count"),
            F.col("gtfs.gtfs_direction_id").alias("candidate_gtfs_direction_id"),
            F.col("gtfs.route_long_name").alias("route_long_name"),
            F.col("gtfs.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("dist_first_km"),
            F.col("dist_last_km"),
            F.col("total_dist_km"),
            F.col("evidence_score"),
        )
    )

    return candidates


def _build_ranked_best_mapping() -> DataFrame:
    """
    Rank candidate mappings and keep the best candidate per
    line_sign + observed_direction.
    """
    candidates = _build_candidate_mappings()

    candidate_ranking_window = Window.partitionBy(
        "line_sign", "observed_direction"
    ).orderBy(
        F.col("total_dist_km").asc_nulls_last(),
        F.col("candidate_gtfs_direction_id").asc_nulls_last(),
    )

    candidate_count_window = Window.partitionBy("line_sign", "observed_direction")

    ranked = (
        candidates.withColumn(
            "candidate_count",
            F.count("candidate_gtfs_direction_id").over(candidate_count_window),
        )
        .withColumn(
            "candidate_rank",
            F.row_number().over(candidate_ranking_window),
        )
        .withColumn(
            "second_best_total_dist_km",
            F.lead("total_dist_km").over(candidate_ranking_window),
        )
        .withColumn(
            "distance_gap_km",
            F.when(
                F.col("second_best_total_dist_km").isNotNull()
                & F.col("total_dist_km").isNotNull(),
                F.col("second_best_total_dist_km") - F.col("total_dist_km"),
            ),
        )
    )

    best_mapping = (
        ranked.filter(F.col("candidate_rank") == 1)
        .withColumn(
            "gtfs_direction_id",
            F.coalesce(
                F.col("candidate_gtfs_direction_id").cast("int"),
                F.lit(NO_MATCH_GTFS_DIRECTION_ID),
            ),
        )
        .withColumn("evidence_type", F.lit(EVIDENCE_TYPE))
    )

    return best_mapping


def _with_mapping_confidence(best_mapping: DataFrame) -> DataFrame:
    """
    Assign mapping confidence based on total distance and ranking separation.
    """
    return (
        best_mapping.withColumn(
            "mapping_confidence",
            F.when(
                F.col("candidate_gtfs_direction_id").isNull(),
                F.lit("none"),
            )
            .when(
                (F.col("total_dist_km") <= F.lit(HIGH_CONFIDENCE_TOTAL_DISTANCE_KM))
                & (
                    (F.col("candidate_count") == 1)
                    | (F.col("distance_gap_km") >= F.lit(HIGH_CONFIDENCE_GAP_KM))
                ),
                F.lit("high"),
            )
            .when(
                (F.col("total_dist_km") <= F.lit(MODERATE_CONFIDENCE_TOTAL_DISTANCE_KM))
                & (
                    (F.col("candidate_count") == 1)
                    | (F.col("distance_gap_km") >= F.lit(MODERATE_CONFIDENCE_GAP_KM))
                ),
                F.lit("moderate"),
            )
            .when(
                F.col("candidate_gtfs_direction_id").isNotNull(),
                F.lit("low"),
            )
            .otherwise(F.lit("none")),
        )
    )


def _with_direction_collision_flag(best_mapping: DataFrame) -> DataFrame:
    """
    Flag lines where multiple observed directions collapse into fewer
    GTFS directions than expected.

    Typical case:
        Both observed directions of the same line map to the same GTFS direction_id.
    """
    collision_by_line = (
        best_mapping.groupBy("line_sign")
        .agg(
            F.countDistinct("observed_direction").alias("observed_direction_count"),
            F.countDistinct(
                F.when(F.col("gtfs_direction_id") >= 0, F.col("gtfs_direction_id"))
            ).alias("distinct_gtfs_direction_count"),
        )
        .withColumn(
            "direction_collision_flag",
            (F.col("observed_direction_count") > 1)
            & (F.col("distinct_gtfs_direction_count") < F.col("observed_direction_count")),
        )
        .select("line_sign", "direction_collision_flag")
    )

    return best_mapping.join(collision_by_line, on="line_sign", how="left")


def _with_production_flags(best_mapping: DataFrame) -> DataFrame:
    """
    Add low-sample, review-required, validated, and line-category flags.
    """
    flagged = (
        best_mapping.withColumn(
            "low_sample_flag",
            F.col("vehicle_count") < F.lit(MIN_VALIDATED_VEHICLE_COUNT),
        )
        .withColumn(
            "review_required_flag",
            (F.col("mapping_confidence").isin("low", "none"))
            | F.col("low_sample_flag")
            | F.col("direction_collision_flag"),
        )
        .withColumn(
            "validated_flag",
            (F.col("mapping_confidence").isin("high", "moderate"))
            & (~F.col("low_sample_flag"))
            & (~F.col("direction_collision_flag"))
            & (F.col("gtfs_direction_id") >= F.lit(0)),
        )
        .withColumn(
            "line_category",
            F.when(F.col("gtfs_direction_id") == F.lit(NO_MATCH_GTFS_DIRECTION_ID), F.lit("NO_MATCH"))
            .when(F.col("direction_collision_flag"), F.lit("DIRECTION_COLLISION"))
            .when(F.col("low_sample_flag"), F.lit("LOW_SAMPLE"))
            .when(F.col("mapping_confidence") == F.lit("high"), F.lit("HIGH_CONFIDENCE"))
            .when(F.col("mapping_confidence") == F.lit("moderate"), F.lit("MODERATE_CONFIDENCE"))
            .when(F.col("mapping_confidence") == F.lit("low"), F.lit("LOW_CONFIDENCE"))
            .otherwise(F.lit("UNCLASSIFIED")),
        )
    )

    return flagged


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Gold analytical reference table for mapping observed SPTrans direction "
        "values to GTFS direction_id using spatial endpoint inference. "
        "One row per line_sign + observed_direction. "
        "Not an official operational truth table."
    ),
    schema=GOLD_DIRECTION_MAPPING_REFERENCE_SCHEMA,
)
def gold_direction_mapping_reference() -> DataFrame:
    """
    Create the final Gold direction mapping reference.

    Final interpretation:
        - This table provides the best available analytical mapping between
          observed SPTrans directions and GTFS directions.
        - Downstream production joins should use only:
              validated_flag = true
              and review_required_flag = false
        - This table preserves ambiguity and review requirements explicitly.
    """
    best_mapping = _build_ranked_best_mapping()
    best_mapping = _with_mapping_confidence(best_mapping)
    best_mapping = _with_direction_collision_flag(best_mapping)
    best_mapping = _with_production_flags(best_mapping)

    final_df = (
        best_mapping.withColumn("record_created_at", F.current_timestamp())
        .select(
            F.col("line_sign").cast("string").alias("line_sign"),
            F.col("observed_direction").cast("int").alias("observed_direction"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("evidence_type").cast("string").alias("evidence_type"),
            F.col("evidence_score").cast("double").alias("evidence_score"),
            F.col("mapping_confidence").cast("string").alias("mapping_confidence"),
            F.col("line_category").cast("string").alias("line_category"),
            F.col("dist_first_km").cast("double").alias("dist_first_km"),
            F.col("dist_last_km").cast("double").alias("dist_last_km"),
            F.col("vehicle_count").cast("long").alias("vehicle_count"),
            F.col("low_sample_flag").cast("boolean").alias("low_sample_flag"),
            F.col("direction_collision_flag").cast("boolean").alias(
                "direction_collision_flag"
            ),
            F.col("review_required_flag").cast("boolean").alias(
                "review_required_flag"
            ),
            F.col("validated_flag").cast("boolean").alias("validated_flag"),
            F.col("record_created_at").cast("timestamp").alias("record_created_at"),
        )
        .filter(
            F.col("line_sign").isNotNull()
            & F.col("observed_direction").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("mapping_confidence").isNotNull()
        )
    )

    return final_df