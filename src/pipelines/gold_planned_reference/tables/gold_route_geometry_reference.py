"""
Gold route geometry reference pipeline.

Purpose:
    Build a business-ready, map-ready representative route geometry for each:
    - line_sign
    - gtfs_direction_id

Source:
    main.silver.silver_helper_route_shape_points_reference

Important implementation note:
    This implementation selects the latest gtfs_load_date available from the
    helper table, then chooses one representative shape per line_sign and
    gtfs_direction_id using the documented deterministic rule hierarchy.

Grain:
    One row per:
        line_sign + gtfs_direction_id
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.gold_planned_reference.schemas.schema_gold_route_geometry_reference import (
    GOLD_ROUTE_GEOMETRY_REFERENCE_SCHEMA,
)

TABLE_NAME = "gold.gold_route_geometry_reference"
HELPER_SOURCE = "main.silver.silver_helper_route_shape_points_reference"


def _build_helper_latest() -> DataFrame:
    """
    Read helper rows for the latest GTFS snapshot available in the helper table.
    """
    latest_snapshot = (
        spark.read.table(HELPER_SOURCE)
        .select(F.max("gtfs_load_date").alias("gtfs_load_date"))
        .filter(F.col("gtfs_load_date").isNotNull())
    )

    return spark.read.table(HELPER_SOURCE).join(
        latest_snapshot,
        on="gtfs_load_date",
        how="inner",
    )


def _build_shape_level_candidates() -> DataFrame:
    """
    Collapse the helper table to one row per candidate shape for each
    line_sign + gtfs_direction_id.
    """
    helper_latest = _build_helper_latest()

    return helper_latest.select(
        F.col("line_sign"),
        F.col("route_id"),
        F.col("route_long_name"),
        F.col("route_name_display"),
        F.col("gtfs_direction_id"),
        F.col("direction_description"),
        F.col("shape_id"),
        F.col("trip_headsign_reference"),
        F.col("trip_count_using_shape"),
        F.col("shape_usage_rank"),
        F.col("is_representative_shape_candidate"),
        F.col("shape_point_count"),
        F.col("gtfs_load_date"),
        F.col("gtfs_loaded_at"),
    ).dropDuplicates(
        ["line_sign", "gtfs_direction_id", "shape_id"]
    )


def _build_representative_shapes() -> DataFrame:
    """
    Select one representative shape per line_sign + gtfs_direction_id using
    the documented deterministic rule hierarchy.
    """
    shape_level = _build_shape_level_candidates()

    representative_window = Window.partitionBy(
        "line_sign",
        "gtfs_direction_id",
    ).orderBy(
        F.when(F.col("is_representative_shape_candidate"), F.lit(1))
        .otherwise(F.lit(0))
        .desc(),
        F.col("shape_usage_rank").asc(),
        F.col("trip_count_using_shape").desc(),
        F.col("shape_id").asc(),
    )

    return (
        shape_level.withColumn(
            "representative_shape_row_number",
            F.row_number().over(representative_window),
        )
        .filter(F.col("representative_shape_row_number") == 1)
        .drop("representative_shape_row_number")
        .withColumnRenamed("shape_id", "representative_shape_id")
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Gold spatial reference table that provides one representative GTFS "
        "route geometry per line_sign and gtfs_direction_id, ready for direct "
        "map consumption and downstream analytical joins."
    ),
    schema=GOLD_ROUTE_GEOMETRY_REFERENCE_SCHEMA,
)
@dp.expect_or_drop("valid_line_sign", "line_sign IS NOT NULL")
@dp.expect_or_drop("valid_gtfs_direction_id", "gtfs_direction_id IS NOT NULL")
@dp.expect_or_drop("valid_representative_shape_id", "representative_shape_id IS NOT NULL")
@dp.expect_or_drop("valid_geometry_wkt", "geometry_wkt IS NOT NULL")
@dp.expect_or_drop("valid_geometry_point_count", "geometry_point_count >= 2")
def gold_route_geometry_reference() -> DataFrame:
    """
    Create a business-ready representative route geometry table.

    Final output:
        One row per:
            line_sign + gtfs_direction_id
    """
    helper_latest = _build_helper_latest()
    representative_shapes = _build_representative_shapes()

    selected_shape_points = (
        helper_latest.alias("src")
        .join(
            representative_shapes.alias("rep"),
            on=[
                "line_sign",
                "gtfs_direction_id",
                "gtfs_load_date",
            ],
            how="inner",
        )
        .filter(F.col("src.shape_id") == F.col("rep.representative_shape_id"))
    )

    grouped_df = (
        selected_shape_points.groupBy(
            F.col("src.line_sign").alias("line_sign"),
            F.col("rep.route_id").alias("route_id"),
            F.col("rep.route_long_name").alias("route_long_name"),
            F.col("rep.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("rep.direction_description").alias("direction_description"),
            F.col("rep.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("rep.representative_shape_id").alias("representative_shape_id"),
            F.col("rep.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("rep.shape_usage_rank").alias("shape_usage_rank"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
        )
        .agg(
            F.max("src.shape_point_count").cast("long").alias("shape_point_count"),
            F.count(F.lit(1)).cast("long").alias("geometry_point_count"),
            F.max("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.array_sort(
                F.collect_list(
                    F.struct(
                        F.col("src.shape_pt_sequence").alias("shape_pt_sequence"),
                        F.col("src.shape_pt_lon").alias("shape_pt_lon"),
                        F.col("src.shape_pt_lat").alias("shape_pt_lat"),
                    )
                )
            ).alias("ordered_points"),
        )
    )

    final_df = (
        grouped_df.withColumn(
            "geometry_wkt",
            F.expr(
                """
                concat(
                  'LINESTRING(',
                  concat_ws(
                    ', ',
                    transform(
                      ordered_points,
                      p -> concat(
                        cast(p.shape_pt_lon as string),
                        ' ',
                        cast(p.shape_pt_lat as string)
                      )
                    )
                  ),
                  ')'
                )
                """
            ),
        )
        .withColumn("record_created_at", F.current_timestamp())
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("line_sign"),
                    F.col("gtfs_direction_id").cast("string"),
                    F.col("representative_shape_id"),
                    F.col("geometry_wkt"),
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
            F.col("representative_shape_id").cast("string").alias(
                "representative_shape_id"
            ),
            F.col("trip_count_using_shape").cast("long").alias(
                "trip_count_using_shape"
            ),
            F.col("shape_usage_rank").cast("int").alias("shape_usage_rank"),
            F.col("shape_point_count").cast("long").alias("shape_point_count"),
            F.col("geometry_point_count").cast("long").alias("geometry_point_count"),
            F.col("geometry_wkt").cast("string").alias("geometry_wkt"),
            F.col("gtfs_load_date").cast("date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").cast("timestamp").alias("gtfs_loaded_at"),
            F.col("record_created_at").cast("timestamp").alias("record_created_at"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )
        .orderBy(
            F.col("line_sign").asc(),
            F.col("gtfs_direction_id").asc(),
        )
    )

    return final_df
