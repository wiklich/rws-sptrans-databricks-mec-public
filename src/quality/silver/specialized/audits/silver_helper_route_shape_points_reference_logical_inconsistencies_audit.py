"""
Audit pipeline for logical inconsistencies in
silver.silver_helper_route_shape_points_reference.

Purpose:
    Detect violations in core helper logic for the route-shape point reference.

Audit scope:
    - shape_usage_rank = 1 assigned to a shape whose trip_count_using_shape is not
      the maximum within the same line_sign + gtfs_direction_id
    - shape_point_count <= 1
    - shape_pt_sequence repeats or is not strictly unique within the same
      line_sign + gtfs_direction_id + shape_id

Important:
    This is a non-blocking audit table.
    It exposes inconsistent helper records for investigation without dropping
    records from the main Silver helper table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_helper_route_shape_points_reference"
TABLE_NAME = (
    "silver.silver_helper_route_shape_points_reference_"
    "logical_inconsistencies_audit"
)


def _base_shape_level(df: DataFrame) -> DataFrame:
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
        )
        .dropDuplicates(["line_sign", "gtfs_direction_id", "shape_id"])
    )


def _rank_one_without_highest_trip_count(df: DataFrame) -> DataFrame:
    """
    Detect shape candidates marked with shape_usage_rank = 1 whose
    trip_count_using_shape is not the maximum within the same
    line_sign + gtfs_direction_id.
    """
    shape_level = _base_shape_level(df)

    max_trip_counts = (
        shape_level.groupBy("line_sign", "gtfs_direction_id")
        .agg(
            F.max("trip_count_using_shape").alias("max_trip_count_using_shape"),
        )
    )

    inconsistent_shapes = (
        shape_level.alias("s")
        .join(
            max_trip_counts.alias("m"),
            on=["line_sign", "gtfs_direction_id"],
            how="inner",
        )
        .filter(
            (F.col("s.shape_usage_rank") == 1)
            & (
                F.col("s.trip_count_using_shape")
                != F.col("m.max_trip_count_using_shape")
            )
        )
        .select(
            F.col("s.line_sign").alias("line_sign"),
            F.col("s.route_id").alias("route_id"),
            F.col("s.route_long_name").alias("route_long_name"),
            F.col("s.route_name_display").alias("route_name_display"),
            F.col("s.gtfs_direction_id").alias("gtfs_direction_id"),
            F.col("s.direction_description").alias("direction_description"),
            F.col("s.shape_id").alias("shape_id"),
            F.col("s.trip_headsign_reference").alias("trip_headsign_reference"),
            F.col("s.trip_count_using_shape").alias("trip_count_using_shape"),
            F.col("m.max_trip_count_using_shape").alias("max_trip_count_using_shape"),
            F.col("s.shape_usage_rank").alias("shape_usage_rank"),
            F.col("s.is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("s.shape_point_count").alias("shape_point_count"),
            F.lit(None).cast("int").alias("shape_pt_sequence"),
            F.lit(None).cast("double").alias("shape_pt_lat"),
            F.lit(None).cast("double").alias("shape_pt_lon"),
            F.lit(None).cast("double").alias("shape_dist_traveled"),
            F.col("s.gtfs_load_date").alias("gtfs_load_date"),
            F.col("s.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.lit("RANK_ONE_NOT_HIGHEST_TRIP_COUNT").alias("audit_rule"),
            F.lit(
                "shape_usage_rank = 1 is assigned to a shape whose "
                "trip_count_using_shape is not the highest within the same "
                "line_sign and gtfs_direction_id."
            ).alias("audit_message"),
        )
    )

    return inconsistent_shapes


def _shape_point_count_too_low(df: DataFrame) -> DataFrame:
    """
    Detect shapes whose shape_point_count is less than or equal to 1.
    """
    shape_level = _base_shape_level(df)

    return (
        shape_level.filter(F.col("shape_point_count") <= 1)
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
            F.lit(None).cast("long").alias("max_trip_count_using_shape"),
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
            F.lit("SHAPE_POINT_COUNT_TOO_LOW").alias("audit_rule"),
            F.lit(
                "shape_point_count is less than or equal to 1, which is not "
                "sufficient for a valid route path."
            ).alias("audit_message"),
        )
    )


def _shape_pt_sequence_repeats(df: DataFrame) -> DataFrame:
    """
    Detect repeated shape_pt_sequence values within the same
    line_sign + gtfs_direction_id + shape_id.

    Note:
        At this table grain, repeated sequence values are the practical signal
        of unexpected non-strict sequencing.
    """
    duplicated_sequences = (
        df.groupBy(
            "line_sign",
            "gtfs_direction_id",
            "shape_id",
            "shape_pt_sequence",
        )
        .agg(F.count(F.lit(1)).alias("sequence_duplicate_count"))
        .filter(F.col("sequence_duplicate_count") > 1)
    )

    return (
        df.alias("src")
        .join(
            duplicated_sequences.alias("dup"),
            on=[
                "line_sign",
                "gtfs_direction_id",
                "shape_id",
                "shape_pt_sequence",
            ],
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
            F.lit(None).cast("long").alias("max_trip_count_using_shape"),
            F.col("src.shape_usage_rank").alias("shape_usage_rank"),
            F.col("src.is_representative_shape_candidate").alias(
                "is_representative_shape_candidate"
            ),
            F.col("src.shape_point_count").alias("shape_point_count"),
            F.col("src.shape_pt_sequence").alias("shape_pt_sequence"),
            F.col("src.shape_pt_lat").alias("shape_pt_lat"),
            F.col("src.shape_pt_lon").alias("shape_pt_lon"),
            F.col("src.shape_dist_traveled").alias("shape_dist_traveled"),
            F.col("src.gtfs_load_date").alias("gtfs_load_date"),
            F.col("src.gtfs_loaded_at").alias("gtfs_loaded_at"),
            F.lit("SHAPE_PT_SEQUENCE_REPEATS").alias("audit_rule"),
            F.lit(
                "shape_pt_sequence repeats within the same line_sign, "
                "gtfs_direction_id, and shape_id, indicating unexpected "
                "non-strict point sequencing."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "silver.silver_helper_route_shape_points_reference, including invalid "
        "rank assignment, insufficient shape point counts, and repeated shape "
        "point sequence values."
    ),
)
def silver_helper_route_shape_points_reference_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    route-shape point helper table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    rank_one_without_highest_trip_count_df = _rank_one_without_highest_trip_count(
        source_df
    )

    shape_point_count_too_low_df = _shape_point_count_too_low(source_df)

    shape_pt_sequence_repeats_df = _shape_pt_sequence_repeats(source_df)

    audit_df = (
        rank_one_without_highest_trip_count_df.unionByName(
            shape_point_count_too_low_df
        )
        .unionByName(shape_pt_sequence_repeats_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
