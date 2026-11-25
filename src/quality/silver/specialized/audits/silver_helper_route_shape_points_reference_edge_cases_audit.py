"""
Audit pipeline for edge cases in
silver.silver_helper_route_shape_points_reference.

Purpose:
    Detect unusual but possible spatial scenarios that should be monitored.

Audit scope:
    - a single line_sign + gtfs_direction_id has many competing shape_id values
    - the top-ranked shape has very low trip_count_using_shape
    - the same shape_id appears across multiple line-direction combinations
    - shape paths with abnormally high point counts

Important:
    This is a non-blocking audit table.
    It exposes edge cases for investigation without dropping records from the
    main Silver helper table.

Important calibration note:
    - TOP_RANKED_SHAPE_LOW_TRIP_COUNT is intentionally classified as INFO,
      because low trip_count_using_shape may be expected in the current GTFS
      reference model.
    - ABNORMALLY_HIGH_SHAPE_POINT_COUNT uses a dynamic p99 threshold derived
      from the current helper dataset instead of a fixed static threshold.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_helper_route_shape_points_reference"
TABLE_NAME = "silver.silver_helper_route_shape_points_reference_edge_cases_audit"

MANY_COMPETING_SHAPES_THRESHOLD = 3
LOW_TOP_RANKED_TRIP_COUNT_THRESHOLD = 2
ABNORMALLY_HIGH_POINT_COUNT_PERCENTILE = 0.99


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "silver.silver_helper_route_shape_points_reference, including many "
        "competing shapes, informational low trip-count top-ranked shapes, "
        "reused shapes across multiple line-directions, and abnormally high "
        "shape point counts based on a dynamic p99 threshold."
    ),
)
def silver_helper_route_shape_points_reference_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for unusual but possible spatial edge cases
    in the route-shape point helper table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    many_competing_shapes_df = _many_competing_shapes(source_df)
    top_ranked_shape_low_trip_count_df = _top_ranked_shape_low_trip_count(source_df)
    shape_reused_across_multiple_line_directions_df = (
        _shape_reused_across_multiple_line_directions(source_df)
    )
    abnormally_high_shape_point_count_df = _abnormally_high_shape_point_count(source_df)

    audit_df = (
        many_competing_shapes_df.unionByName(top_ranked_shape_low_trip_count_df)
        .unionByName(shape_reused_across_multiple_line_directions_df)
        .unionByName(abnormally_high_shape_point_count_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df


def _shape_level(df: DataFrame) -> DataFrame:
    """
    Collapse the point-level helper table into one row per
    line_sign + gtfs_direction_id + shape_id for shape-level validations.
    """
    return (
        df.select(
            "line_sign",
            "route_id",
            "route_long_name",
            "route_name_display",
            "gtfs_direction_id",
            "direction_description",
            "shape_id",
            "trip_headsign_reference",
            "trip_count_using_shape",
            "shape_usage_rank",
            "is_representative_shape_candidate",
            "shape_point_count",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
        )
        .dropDuplicates(["line_sign", "gtfs_direction_id", "shape_id"])
    )


def _many_competing_shapes(df: DataFrame) -> DataFrame:
    """
    Detect line_sign + gtfs_direction_id combinations with many competing shape_id values.
    """
    shape_level = _shape_level(df)

    competing_shapes = (
        shape_level.groupBy("line_sign", "gtfs_direction_id")
        .agg(
            F.countDistinct("shape_id").alias("distinct_shape_id_count"),
            F.collect_set("shape_id").alias("competing_shape_ids"),
        )
        .filter(F.col("distinct_shape_id_count") > F.lit(MANY_COMPETING_SHAPES_THRESHOLD))
    )

    return (
        shape_level.alias("src")
        .join(
            competing_shapes.alias("cmp"),
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
            F.col("src.shape_id").alias("shape_id"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("src.shape_usage_rank").alias("shape_usage_rank"),
            F.col("src.is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("src.shape_point_count").alias("shape_point_count"),
            F.lit(None).cast("int").alias("shape_pt_sequence"),
            F.lit(None).cast("double").alias("shape_pt_lat"),
            F.lit(None).cast("double").alias("shape_pt_lon"),
            F.lit(None).cast("double").alias("shape_dist_traveled"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("cmp.distinct_shape_id_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(F.col("cmp.competing_shape_ids")).alias(
                "edge_case_metric_values"
            ),
            F.lit("MANY_COMPETING_SHAPES_FOR_LINE_DIRECTION").alias("audit_rule"),
            F.lit("WARNING").alias("audit_severity"),
            F.lit(
                "A single line_sign and gtfs_direction_id has many competing shape_id "
                "values. This may be valid, but should be monitored."
            ).alias("audit_message"),
        )
    )


def _top_ranked_shape_low_trip_count(df: DataFrame) -> DataFrame:
    """
    Detect top-ranked shapes with very low trip_count_using_shape.

    This rule is intentionally classified as INFO because low trip_count_using_shape
    may be expected in the current GTFS reference model.
    """
    shape_level = _shape_level(df)

    threshold_json = F.to_json(
        F.struct(
            F.lit(LOW_TOP_RANKED_TRIP_COUNT_THRESHOLD).alias(
                "low_trip_count_threshold"
            )
        )
    )

    return (
        shape_level.filter(
            (F.col("shape_usage_rank") == 1)
            & (F.col("trip_count_using_shape") <= F.lit(LOW_TOP_RANKED_TRIP_COUNT_THRESHOLD))
        )
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("shape_id").alias("shape_id"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("shape_point_count").alias("shape_point_count"),
            F.lit(None).cast("int").alias("shape_pt_sequence"),
            F.lit(None).cast("double").alias("shape_pt_lat"),
            F.lit(None).cast("double").alias("shape_pt_lon"),
            F.lit(None).cast("double").alias("shape_dist_traveled"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_hash").alias("record_hash"),
            F.col("trip_count_using_shape").cast("long").alias("edge_case_metric_count"),
            threshold_json.alias("edge_case_metric_values"),
            F.lit("TOP_RANKED_SHAPE_LOW_TRIP_COUNT").alias("audit_rule"),
            F.lit("INFO").alias("audit_severity"),
            F.lit(
                "The top-ranked shape has a very low trip_count_using_shape. "
                "This is monitored as informational context because it may be "
                "expected in the current GTFS reference model."
            ).alias("audit_message"),
        )
    )


def _shape_reused_across_multiple_line_directions(df: DataFrame) -> DataFrame:
    """
    Detect shape_id values reused across multiple line_sign + gtfs_direction_id combinations.
    """
    shape_level = _shape_level(df)

    reused_shapes = (
        shape_level.groupBy("shape_id")
        .agg(
            F.countDistinct(
                F.concat_ws(
                    "||",
                    F.col("line_sign"),
                    F.col("gtfs_direction_id").cast("string"),
                )
            ).alias("line_direction_combination_count"),
            F.collect_set(
                F.concat_ws(
                    " | ",
                    F.col("line_sign"),
                    F.col("gtfs_direction_id").cast("string"),
                )
            ).alias("line_direction_combinations"),
        )
        .filter(F.col("line_direction_combination_count") > 1)
    )

    return (
        shape_level.alias("src")
        .join(
            reused_shapes.alias("r"),
            on="shape_id",
            how="inner",
        )
        .select(
            F.col("src.line_sign").alias("line_sign"),
            F.col("src.route_id").alias("route_id"),
            F.col("src.route_long_name").alias("route_long_name"),
            F.col("src.route_name_display").alias("route_name_display"),
            F.col("src.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("src.direction_description").alias("direction_description"),
            F.col("src.shape_id").alias("shape_id"),
            F.col("src.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("src.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("src.shape_usage_rank").alias("shape_usage_rank"),
            F.col("src.is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("src.shape_point_count").alias("shape_point_count"),
            F.lit(None).cast("int").alias("shape_pt_sequence"),
            F.lit(None).cast("double").alias("shape_pt_lat"),
            F.lit(None).cast("double").alias("shape_pt_lon"),
            F.lit(None).cast("double").alias("shape_dist_traveled"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("src.record_hash").alias("record_hash"),
            F.col("r.line_direction_combination_count").cast("long").alias(
                "edge_case_metric_count"
            ),
            F.to_json(F.col("r.line_direction_combinations")).alias(
                "edge_case_metric_values"
            ),
            F.lit("SHAPE_REUSED_ACROSS_MULTIPLE_LINE_DIRECTIONS").alias("audit_rule"),
            F.lit("WARNING").alias("audit_severity"),
            F.lit(
                "The same shape_id appears across multiple line_sign and "
                "gtfs_direction_id combinations. This may be valid, but should be monitored."
            ).alias("audit_message"),
        )
    )


def _abnormally_high_shape_point_count(df: DataFrame) -> DataFrame:
    """
    Detect shapes with abnormally high point counts using a dynamic p99 threshold
    derived from the current helper dataset.
    """
    shape_level = _shape_level(df)

    threshold_row = (
        shape_level.agg(
            F.percentile_approx(
                "shape_point_count",
                ABNORMALLY_HIGH_POINT_COUNT_PERCENTILE,
            ).alias("p99_shape_point_count_threshold")
        ).collect()
    )

    dynamic_threshold = (
        threshold_row[0]["p99_shape_point_count_threshold"]
        if len(threshold_row) > 0
        and threshold_row[0]["p99_shape_point_count_threshold"] is not None
        else None
    )

    if dynamic_threshold is None:
        return shape_level.filter(F.lit(False)).select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("shape_id").alias("shape_id"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("shape_point_count").alias("shape_point_count"),
            F.lit(None).cast("int").alias("shape_pt_sequence"),
            F.lit(None).cast("double").alias("shape_pt_lat"),
            F.lit(None).cast("double").alias("shape_pt_lon"),
            F.lit(None).cast("double").alias("shape_dist_traveled"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_hash").alias("record_hash"),
            F.lit(None).cast("long").alias("edge_case_metric_count"),
            F.lit(None).cast("string").alias("edge_case_metric_values"),
            F.lit("ABNORMALLY_HIGH_SHAPE_POINT_COUNT").alias("audit_rule"),
            F.lit("WARNING").alias("audit_severity"),
            F.lit(
                "Unable to derive dynamic p99 threshold for shape_point_count."
            ).alias("audit_message"),
        )

    threshold_json = F.to_json(
        F.struct(
            F.lit(dynamic_threshold).cast("long").alias(
                "p99_shape_point_count_threshold"
            )
        )
    )

    return (
        shape_level.filter(F.col("shape_point_count") > F.lit(dynamic_threshold))
        .select(
            F.col("line_sign").alias("line_sign"),
            F.col("route_id").alias("route_id"),
            F.col("route_long_name").alias("route_long_name"),
            F.col("route_name_display").alias("route_name_display"),
            F.col("gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("direction_description").alias("direction_description"),
            F.col("shape_id").alias("shape_id"),
            F.col("trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("shape_usage_rank").alias("shape_usage_rank"),
            F.col("is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("shape_point_count").alias("shape_point_count"),
            F.lit(None).cast("int").alias("shape_pt_sequence"),
            F.lit(None).cast("double").alias("shape_pt_lat"),
            F.lit(None).cast("double").alias("shape_pt_lon"),
            F.lit(None).cast("double").alias("shape_dist_traveled"),
            F.col("gtfs_load_date").alias("gtfs_load_date"),
            F.col("gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.col("record_hash").alias("record_hash"),
            F.col("shape_point_count").cast("long").alias("edge_case_metric_count"),
            threshold_json.alias("edge_case_metric_values"),
            F.lit("ABNORMALLY_HIGH_SHAPE_POINT_COUNT").alias("audit_rule"),
            F.lit("WARNING").alias("audit_severity"),
            F.lit(
                "The shape path has an abnormally high point count relative to the "
                "current helper dataset p99 threshold. This may be valid, but should "
                "be monitored for rendering or geometry complexity concerns."
            ).alias("audit_message"),
        )
    )
