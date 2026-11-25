"""
Audit pipeline for edge cases in
gold.gold_route_geometry_reference.

Purpose:
    Detect unusual but possible route-geometry scenarios that should be monitored.

Audit scope:
    - a route-direction has only a very small number of points
    - the selected representative shape has a low trip_count relative to competing candidates
    - a route-direction has no valid geometry despite helper presence
    - geometry path appears unusually dense or unusually sparse

Important:
    This is a non-blocking audit table.
    It exposes unusual route geometry cases for investigation without dropping
    records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_route_geometry_reference"
HELPER_SOURCE = "main.silver.silver_helper_route_shape_points_reference"
TABLE_NAME = "gold.gold_route_geometry_reference_edge_cases_audit"

VERY_SMALL_GEOMETRY_POINT_COUNT_THRESHOLD = 10
WEAK_REPRESENTATIVE_SHAPE_SHARE_THRESHOLD = 0.50
UNUSUALLY_SPARSE_GEOMETRY_PERCENTILE = 0.01
UNUSUALLY_DENSE_GEOMETRY_PERCENTILE = 0.99


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


def _build_helper_shape_level_candidates() -> DataFrame:
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


def _build_expected_representative_shapes() -> DataFrame:
    """
    Select one expected representative shape per line_sign + gtfs_direction_id
    using the same deterministic hierarchy used by the Gold pipeline.
    """
    shape_level = _build_helper_shape_level_candidates()

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


def _very_small_geometry_point_count(df: DataFrame) -> DataFrame:
    """
    Detect route-directions with a very small number of geometry points.
    """
    threshold_json = F.to_json(
        F.struct(
            F.lit(VERY_SMALL_GEOMETRY_POINT_COUNT_THRESHOLD).alias(
                "very_small_geometry_point_count_threshold"
            )
        )
    )

    return (
        df.filter(
            F.col("geometry_point_count")
            <= F.lit(VERY_SMALL_GEOMETRY_POINT_COUNT_THRESHOLD)
        )
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("representative_shape_id").alias("representative_shape_id"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("geometry_point_count").alias("geometry_point_count"),
            F.col("geometry_wkt").alias("geometry_wkt"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.col("geometry_point_count").cast("long").alias("edge_case_metric_count"),
            threshold_json.alias("edge_case_metric_values"),
            F.lit("VERY_SMALL_GEOMETRY_POINT_COUNT").alias("audit_rule"),
            F.lit(
                "A route-direction has a very small number of geometry points. "
                "This may be valid, but should be monitored."
            ).alias("audit_message"),
        )
    )


def _weak_representative_shape_relative_to_competitors(gold_df: DataFrame) -> DataFrame:
    """
    Detect cases where the selected representative shape has low trip_count
    relative to competing shape candidates in the helper table.

    Rule:
        - candidate_shape_count > 1
        - selected_shape_trip_share <= threshold
    """
    helper_shape_level = _build_helper_shape_level_candidates()

    candidate_stats = (
        helper_shape_level.groupBy("line_sign", "gtfs_direction_id", "gtfs_load_date")
        .agg(
            F.countDistinct("shape_id").alias("candidate_shape_count"),
            F.sum("trip_count_using_shape").cast("long").alias(
                "total_trip_count_across_candidates"
            ),
        )
    )

    joined_df = (
        gold_df.alias("g")
        .join(
            candidate_stats.alias("c"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="inner",
        )
        .withColumn(
            "selected_shape_trip_share",
            F.when(
                F.col("c.total_trip_count_across_candidates") > 0,
                F.col("g.trip_count_using_shape")
                / F.col("c.total_trip_count_across_candidates"),
            ),
        )
        .filter(
            (F.col("c.candidate_shape_count") > 1)
            & (
                F.col("selected_shape_trip_share")
                <= F.lit(WEAK_REPRESENTATIVE_SHAPE_SHARE_THRESHOLD)
            )
        )
    )

    return (
        joined_df.select(
            F.col("g.line_sign").alias("line_sign"),
            F.col("g.route_id").alias("route_id"),
            F.col("g.route_long_name").alias("route_long_name"),
            F.col("g.route_name_display").alias("route_name_display"),
            F.col("g.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("g.direction_description").alias("direction_description"),
            F.col("g.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("g.representative_shape_id").alias("representative_shape_id"),
            F.col("g.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("g.shape_usage_rank").alias("shape_usage_rank"),
            F.col("g.shape_point_count").alias("shape_point_count"),
            F.col("g.geometry_point_count").alias("geometry_point_count"),
            F.col("g.geometry_wkt").alias("geometry_wkt"),
            F.col("g.gtfs_load_date").alias("gtfs_load_date"),
            F.col("g.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("g.record_created_at").alias("record_created_at"),
            F.col("g.record_hash").alias("record_hash"),
            F.col("c.candidate_shape_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(
                F.struct(
                    F.col("g.trip_count_using_shape").cast("long").alias(
                        "selected_shape_trip_count"
                    ),
                    F.col("c.total_trip_count_across_candidates").cast("long").alias(
                        "total_trip_count_across_candidates"
                    ),
                    F.col("selected_shape_trip_share").cast("double").alias(
                        "selected_shape_trip_share"
                    ),
                    F.lit(WEAK_REPRESENTATIVE_SHAPE_SHARE_THRESHOLD).alias(
                        "weak_shape_share_threshold"
                    ),
                )
            ).alias("edge_case_metric_values"),
            F.lit("WEAK_REPRESENTATIVE_SHAPE_RELATIVE_TO_COMPETITORS").alias(
                "audit_rule"
            ),
            F.lit(
                "The selected representative shape has a low trip_count share "
                "relative to competing candidate shapes for the same route-direction."
            ).alias("audit_message"),
        )
    )


def _helper_presence_without_valid_gold_geometry(gold_df: DataFrame) -> DataFrame:
    """
    Detect line_sign + gtfs_direction_id combinations that exist in the helper
    but do not have a valid geometry row in Gold.

    Valid Gold row means:
        - Gold row exists
        - geometry_wkt is not null
        - geometry_point_count >= 2
    """
    expected_representative_shapes = _build_expected_representative_shapes()

    joined_df = (
        expected_representative_shapes.alias("exp")
        .join(
            gold_df.alias("g"),
            on=["line_sign", "gtfs_direction_id", "gtfs_load_date"],
            how="left",
        )
        .filter(
            F.col("g.line_sign").isNull()
            | F.col("g.geometry_wkt").isNull()
            | (F.col("g.geometry_point_count") < 2)
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
            F.col("exp.representative_shape_id").alias("representative_shape_id"),
            F.col("exp.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("exp.shape_usage_rank").alias("shape_usage_rank"),
            F.col("exp.shape_point_count").alias("shape_point_count"),
            F.col("g.geometry_point_count").alias("geometry_point_count"),
            F.col("g.geometry_wkt").alias("geometry_wkt"),
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
                    F.when(F.col("g.geometry_wkt").isNull(), F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("geometry_wkt_missing"),
                    F.when(F.col("g.geometry_point_count") < 2, F.lit(True))
                    .otherwise(F.lit(False))
                    .alias("geometry_point_count_invalid"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("HELPER_PRESENCE_WITHOUT_VALID_GOLD_GEOMETRY").alias("audit_rule"),
            F.lit(
                "A route-direction exists in the helper table but has no valid "
                "geometry in the final Gold route geometry table."
            ).alias("audit_message"),
        )
    )


def _unusually_dense_or_sparse_geometry(df: DataFrame) -> DataFrame:
    """
    Detect unusually dense or sparse route geometries using dynamic p01/p99
    thresholds derived from the current Gold dataset.

    Sparse rule:
        geometry_point_count < p01 threshold
        and geometry_point_count > very small threshold
        to avoid duplicating the dedicated very-small-points rule too heavily.

    Dense rule:
        geometry_point_count > p99 threshold
    """
    threshold_row = (
        df.agg(
            F.percentile_approx(
                "geometry_point_count",
                UNUSUALLY_SPARSE_GEOMETRY_PERCENTILE,
            ).alias("p01_geometry_point_count_threshold"),
            F.percentile_approx(
                "geometry_point_count",
                UNUSUALLY_DENSE_GEOMETRY_PERCENTILE,
            ).alias("p99_geometry_point_count_threshold"),
        ).collect()
    )

    p01_threshold = (
        threshold_row[0]["p01_geometry_point_count_threshold"]
        if len(threshold_row) > 0
        else None
    )
    p99_threshold = (
        threshold_row[0]["p99_geometry_point_count_threshold"]
        if len(threshold_row) > 0
        else None
    )

    if p01_threshold is None:
        sparse_df = df.filter(F.lit(False))
    else:
        sparse_df = df.filter(
            (F.col("geometry_point_count") < F.lit(p01_threshold))
            & (
                F.col("geometry_point_count")
                > F.lit(VERY_SMALL_GEOMETRY_POINT_COUNT_THRESHOLD)
            )
        ).select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("representative_shape_id").alias("representative_shape_id"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("geometry_point_count").alias("geometry_point_count"),
            F.col("geometry_wkt").alias("geometry_wkt"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.col("geometry_point_count").cast("long").alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.lit(p01_threshold).cast("long").alias(
                        "p01_geometry_point_count_threshold"
                    )
                )
            ).alias("edge_case_metric_values"),
            F.lit("UNUSUALLY_SPARSE_GEOMETRY").alias("audit_rule"),
            F.lit(
                "The route geometry appears unusually sparse relative to the "
                "current Gold dataset."
            ).alias("audit_message"),
        )

    if p99_threshold is None:
        dense_df = df.filter(F.lit(False))
    else:
        dense_df = df.filter(
            F.col("geometry_point_count") > F.lit(p99_threshold)
        ).select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("representative_shape_id").alias("representative_shape_id"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("shape_point_count").alias("shape_point_count"),
            F.col("geometry_point_count").alias("geometry_point_count"),
            F.col("geometry_wkt").alias("geometry_wkt"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_created_at").alias("record_created_at"),
            F.col("record_hash").alias("record_hash"),
            F.col("geometry_point_count").cast("long").alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.lit(p99_threshold).cast("long").alias(
                        "p99_geometry_point_count_threshold"
                    )
                )
            ).alias("edge_case_metric_values"),
            F.lit("UNUSUALLY_DENSE_GEOMETRY").alias("audit_rule"),
            F.lit(
                "The route geometry appears unusually dense relative to the "
                "current Gold dataset."
            ).alias("audit_message"),
        )

    return sparse_df.unionByName(dense_df)


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "gold.gold_route_geometry_reference, including very small route "
        "geometries, weak representative shapes relative to competing "
        "candidates, helper presence without valid Gold geometry, and "
        "unusually dense or sparse route paths."
    ),
)
def gold_route_geometry_reference_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for unusual but possible edge cases
    in the Gold route geometry reference table.
    """
    gold_df = spark.read.table(SOURCE_TABLE)

    very_small_geometry_df = _very_small_geometry_point_count(gold_df)
    weak_representative_shape_df = _weak_representative_shape_relative_to_competitors(
        gold_df
    )
    helper_without_valid_gold_df = _helper_presence_without_valid_gold_geometry(
        gold_df
    )
    dense_or_sparse_geometry_df = _unusually_dense_or_sparse_geometry(gold_df)

    audit_df = (
        very_small_geometry_df.unionByName(weak_representative_shape_df)
        .unionByName(helper_without_valid_gold_df)
        .unionByName(dense_or_sparse_geometry_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
