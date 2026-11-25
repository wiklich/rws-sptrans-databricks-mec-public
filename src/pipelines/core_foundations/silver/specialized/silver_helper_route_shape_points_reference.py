"""
Silver helper pipeline for route-direction GTFS shape points reference.

Purpose:
    Build a reusable point-level spatial helper table that links:
    - business-facing line_sign
    - GTFS route_id
    - GTFS direction_id
    - GTFS shape_id
    - ordered GTFS shape points

Important implementation note:
    This implementation materializes only the latest common GTFS snapshot
    available across:
    - silver_gtfs_routes
    - silver_gtfs_trips
    - silver_gtfs_shapes

    This keeps the documented helper grain stable:
        line_sign + gtfs_direction_id + shape_id + shape_pt_sequence

Grain:
    One row per:
        line_sign + gtfs_direction_id + shape_id + shape_pt_sequence
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.core_foundations.schemas.schema_silver_helper_route_shape_points_reference import (
    SILVER_HELPER_ROUTE_SHAPE_POINTS_REFERENCE_SCHEMA,
)

TABLE_NAME = "silver.silver_helper_route_shape_points_reference"

GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_SHAPES_SOURCE = "main.silver.silver_gtfs_shapes"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Silver helper table that links business-facing route identity and "
        "GTFS direction to ordered GTFS shape points for the latest common "
        "GTFS snapshot. Intended for downstream spatial modeling and map-ready "
        "geometry construction."
    ),
    schema=SILVER_HELPER_ROUTE_SHAPE_POINTS_REFERENCE_SCHEMA,
)
@dp.expect_or_drop("valid_line_sign", "line_sign IS NOT NULL")
@dp.expect_or_drop("valid_route_id", "route_id IS NOT NULL")
@dp.expect_or_drop("valid_gtfs_direction_id", "gtfs_direction_id IS NOT NULL")
@dp.expect_or_drop("valid_shape_id", "shape_id IS NOT NULL")
@dp.expect_or_drop("valid_shape_pt_sequence", "shape_pt_sequence IS NOT NULL")
@dp.expect_or_drop("valid_shape_pt_lat", "shape_pt_lat IS NOT NULL")
@dp.expect_or_drop("valid_shape_pt_lon", "shape_pt_lon IS NOT NULL")
def silver_helper_route_shape_points_reference() -> DataFrame:
    """
    Create a reusable point-level route shape helper table.

    Final output:
        One row per:
            line_sign + gtfs_direction_id + shape_id + shape_pt_sequence

    Snapshot strategy:
        Uses only the latest common gtfs_load_date available across routes,
        trips, and shapes.
    """
    shape_usage = _build_shape_usage()
    shape_reference_metadata = _build_shape_reference_metadata()
    shape_point_counts = _build_shape_point_counts()
    shapes_latest = _build_shapes_latest()

    final_df = (
        shape_usage.alias("usage")
        .join(
            shape_reference_metadata.alias("meta"),
            on=[
                "gtfs_load_date",
                "line_sign",
                "gtfs_direction_id",
                "shape_id",
            ],
            how="inner",
        )
        .join(
            shape_point_counts.alias("cnt"),
            on=["gtfs_load_date", "shape_id"],
            how="inner",
        )
        .join(
            shapes_latest.alias("shp"),
            on=["gtfs_load_date", "shape_id"],
            how="inner",
        )
        .withColumn(
            "gtfs_loaded_at",
            F.greatest(
                F.col("meta.route_trip_gtfs_loaded_at"),
                F.col("cnt.shape_gtfs_loaded_at"),
            ),
        )
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("line_sign"),
                    F.col("route_id"),
                    F.col("gtfs_direction_id").cast("string"),
                    F.col("shape_id"),
                    F.col("shp.shape_pt_sequence").cast("string"),
                    F.col("shp.shape_pt_lat").cast("string"),
                    F.col("shp.shape_pt_lon").cast("string"),
                    F.coalesce(
                        F.col("shp.shape_dist_traveled").cast("string"),
                        F.lit("NULL"),
                    ),
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
            F.col("shape_id").cast("string").alias("shape_id"),
            F.col("trip_headsign_reference").cast("string").alias(
                "trip_headsign_reference"
            ),
            F.col("trip_count_using_shape").cast("long").alias(
                "trip_count_using_shape"
            ),
            F.col("shape_usage_rank").cast("int").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate")
            .cast("boolean")
            .alias("is_representative_shape_candidate"),
            F.col("shape_point_count").cast("long").alias("shape_point_count"),
            F.col("shp.shape_pt_sequence").cast("int").alias("shape_pt_sequence"),
            F.col("shp.shape_pt_lat").cast("double").alias("shape_pt_lat"),
            F.col("shp.shape_pt_lon").cast("double").alias("shape_pt_lon"),
            F.col("shp.shape_dist_traveled").cast("double").alias(
                "shape_dist_traveled"
            ),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )
        .orderBy(
            F.col("line_sign").asc(),
            F.col("gtfs_direction_id").asc(),
            F.col("shape_usage_rank").asc(),
            F.col("shape_id").asc(),
            F.col("shape_pt_sequence").asc(),
        )
    )

    return final_df


def _build_latest_common_snapshot() -> DataFrame:
    """
    Build a one-row DataFrame containing the latest common GTFS snapshot date
    shared across routes, trips, and shapes.
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

    shapes_dates = (
        spark.read.table(GTFS_SHAPES_SOURCE)
        .select("gtfs_load_date")
        .filter(F.col("gtfs_load_date").isNotNull())
        .distinct()
    )

    return (
        routes_dates.join(trips_dates, on="gtfs_load_date", how="inner")
        .join(shapes_dates, on="gtfs_load_date", how="inner")
        .agg(F.max("gtfs_load_date").alias("gtfs_load_date"))
        .filter(F.col("gtfs_load_date").isNotNull())
    )


def _build_routes_latest() -> DataFrame:
    """
    Read routes for the latest common GTFS snapshot.
    """
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
    """
    Read trips for the latest common GTFS snapshot.
    """
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
            F.col("shape_id"),
            F.col("gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("shape_id").isNotNull()
        )
    )


def _build_shapes_latest() -> DataFrame:
    """
    Read shapes for the latest common GTFS snapshot.
    """
    latest_snapshot = _build_latest_common_snapshot()

    return (
        spark.read.table(GTFS_SHAPES_SOURCE)
        .join(latest_snapshot, on="gtfs_load_date", how="inner")
        .select(
            F.col("gtfs_load_date"),
            F.col("shape_id"),
            F.col("shape_pt_lat"),
            F.col("shape_pt_lon"),
            F.col("shape_pt_sequence").cast("int").alias("shape_pt_sequence"),
            F.col("shape_dist_traveled"),
            F.col("gtfs_loaded_at").alias("shape_gtfs_loaded_at"),
        )
        .filter(
            F.col("shape_id").isNotNull()
            & F.col("shape_pt_lat").isNotNull()
            & F.col("shape_pt_lon").isNotNull()
            & F.col("shape_pt_sequence").isNotNull()
        )
    )


def _build_route_trip_base() -> DataFrame:
    """
    Join routes and trips for the latest common GTFS snapshot.
    """
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
        F.col("shape_id"),
        F.col("route_gtfs_loaded_at"),
        F.col("trip_gtfs_loaded_at"),
    )


def _build_shape_usage() -> DataFrame:
    """
    Compute shape usage counts and ranks for each line_sign and direction.
    """
    route_trip_base = _build_route_trip_base()

    shape_usage = (
        route_trip_base.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "shape_id",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias("trip_count_using_shape"),
        )
    )

    shape_rank_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
    ).orderBy(
        F.col("trip_count_using_shape").desc(),
        F.col("shape_id").asc(),
    )

    return (
        shape_usage.withColumn(
            "shape_usage_rank",
            F.row_number().over(shape_rank_window).cast("int"),
        )
        .withColumn(
            "is_representative_shape_candidate",
            (F.col("shape_usage_rank") == F.lit(1)).cast("boolean"),
        )
    )


def _build_shape_reference_metadata() -> DataFrame:
    """
    Choose a deterministic representative route metadata record for each
    line_sign + gtfs_direction_id + shape_id.
    """
    route_trip_base = _build_route_trip_base()

    route_reference_candidates = (
        route_trip_base.groupBy(
            "gtfs_load_date",
            "line_sign",
            "gtfs_direction_id",
            "shape_id",
            "route_id",
            "route_long_name",
            "route_name_display",
            "direction_description",
        )
        .agg(
            F.countDistinct("trip_id").cast("long").alias(
                "route_trip_count_using_shape"
            ),
            F.min("trip_headsign").alias("trip_headsign_reference"),
            F.max("route_gtfs_loaded_at").alias("route_gtfs_loaded_at"),
            F.max("trip_gtfs_loaded_at").alias("trip_gtfs_loaded_at"),
        )
    )

    route_reference_window = Window.partitionBy(
        "gtfs_load_date",
        "line_sign",
        "gtfs_direction_id",
        "shape_id",
    ).orderBy(
        F.col("route_trip_count_using_shape").desc(),
        F.col("route_id").asc(),
    )

    return (
        route_reference_candidates.withColumn(
            "route_reference_rank",
            F.row_number().over(route_reference_window),
        )
        .filter(F.col("route_reference_rank") == 1)
        .drop("route_reference_rank")
        .withColumn(
            "route_trip_gtfs_loaded_at",
            F.greatest(
                F.col("route_gtfs_loaded_at"),
                F.col("trip_gtfs_loaded_at"),
            ),
        )
        .drop("route_gtfs_loaded_at", "trip_gtfs_loaded_at")
    )


def _build_shape_point_counts() -> DataFrame:
    """
    Compute the number of points available for each shape in the latest
    common GTFS snapshot.
    """
    shapes_latest = _build_shapes_latest()

    return shapes_latest.groupBy(
        "gtfs_load_date",
        "shape_id",
    ).agg(
        F.count(F.lit(1)).cast("long").alias("shape_point_count"),
        F.max("shape_gtfs_loaded_at").alias("shape_gtfs_loaded_at"),
    )
