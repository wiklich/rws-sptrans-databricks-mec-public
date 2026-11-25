"""
Audit pipeline for logical inconsistencies in
gold.gold_line_explorer_map_points.

Purpose:
    Detect violations in core Gold line explorer map point logic.

Audit scope:
    - adherence_band = 'No Gold Match' for unexpectedly large volumes
    - points with impossible classification transitions within the same
      service_date + line_sign + gtfs_direction_id + hour + vehicle_prefix context
    - rows with mapped gtfs_direction_id but missing expected descriptive fields

Important:
    This is a non-blocking audit table.
    It exposes inconsistent Gold line explorer map point records for
    investigation without dropping records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_line_explorer_map_points"
TABLE_NAME = "gold.gold_line_explorer_map_points_logical_inconsistencies_audit"

UNEXPECTED_NO_GOLD_MATCH_POINT_COUNT_THRESHOLD = 100
UNEXPECTED_NO_GOLD_MATCH_SHARE_THRESHOLD = 0.50

SOURCE_COLUMNS = [
    "event_id",
    "service_date",
    "captured_at_local",
    "hour",
    "line_sign",
    "observed_direction",
    "gtfs_direction_id",
    "direction_description",
    "vehicle_prefix",
    "latitude",
    "longitude",
    "planned_headway_minutes",
    "observed_headway_avg_minutes",
    "headway_deviation_minutes",
    "headway_deviation_pct",
    "missed_service_flag",
    "adherence_band",
    "record_created_at",
    "record_hash",
]


def _source_projection(alias_name: str) -> list:
    """
    Build a reusable projection of source columns from the given alias.
    """
    return [F.col(f"{alias_name}.{column_name}").alias(column_name) for column_name in SOURCE_COLUMNS]


def _unexpectedly_large_no_gold_match_volume(df: DataFrame) -> DataFrame:
    """
    Detect hourly route-direction groups with unexpectedly large No Gold Match volume.
    """
    grouped_df = (
        df.groupBy("service_date", "line_sign", "gtfs_direction_id", "hour")
        .agg(
            F.count(F.lit(1)).cast("long").alias("group_point_count"),
            F.sum(
                F.when(F.col("adherence_band") == F.lit("No Gold Match"), F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("no_gold_match_point_count"),
        )
        .withColumn(
            "no_gold_match_share",
            F.when(
                F.col("group_point_count") > 0,
                F.col("no_gold_match_point_count") / F.col("group_point_count"),
            ),
        )
        .filter(
            (F.col("no_gold_match_point_count") >= F.lit(UNEXPECTED_NO_GOLD_MATCH_POINT_COUNT_THRESHOLD))
            & (F.col("no_gold_match_share") >= F.lit(UNEXPECTED_NO_GOLD_MATCH_SHARE_THRESHOLD))
        )
    )

    threshold_json = F.to_json(
        F.struct(
            F.lit(UNEXPECTED_NO_GOLD_MATCH_POINT_COUNT_THRESHOLD).alias(
                "no_gold_match_point_count_threshold"
            ),
            F.lit(UNEXPECTED_NO_GOLD_MATCH_SHARE_THRESHOLD).alias(
                "no_gold_match_share_threshold"
            ),
        )
    )

    return (
        df.alias("src")
        .join(
            grouped_df.alias("grp"),
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="inner",
        )
        .filter(F.col("src.adherence_band") == F.lit("No Gold Match"))
        .select(
            *_source_projection("src"),
            F.col("grp.no_gold_match_point_count").cast("long").alias("audit_metric_count"),
            F.to_json(
                F.struct(
                    F.col("grp.group_point_count").cast("long").alias("group_point_count"),
                    F.col("grp.no_gold_match_point_count").cast("long").alias(
                        "no_gold_match_point_count"
                    ),
                    F.col("grp.no_gold_match_share").cast("double").alias(
                        "no_gold_match_share"
                    ),
                    threshold_json.alias("thresholds"),
                )
            ).alias("audit_metric_values"),
            F.lit("UNEXPECTEDLY_LARGE_NO_GOLD_MATCH_VOLUME").alias("audit_rule"),
            F.lit(
                "adherence_band = 'No Gold Match' appears in unexpectedly large volume "
                "for the same service_date, line_sign, gtfs_direction_id, and hour."
            ).alias("audit_message"),
        )
    )


def _impossible_classification_transitions(df: DataFrame) -> DataFrame:
    """
    Detect cases where a vehicle has multiple adherence_band values within the same
    service_date + line_sign + gtfs_direction_id + hour context.

    Because adherence enrichment is joined at the hourly line-direction grain,
    multiple adherence bands within the same hourly vehicle context should not occur.
    """
    grouped_df = (
        df.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
            "vehicle_prefix",
        )
        .agg(
            F.countDistinct("adherence_band").cast("long").alias(
                "distinct_adherence_band_count"
            ),
            F.collect_set("adherence_band").alias("adherence_bands"),
        )
        .filter(F.col("distinct_adherence_band_count") > 1)
    )

    return (
        df.alias("src")
        .join(
            grouped_df.alias("grp"),
            on=[
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "hour",
                "vehicle_prefix",
            ],
            how="inner",
        )
        .select(
            *_source_projection("src"),
            F.col("grp.distinct_adherence_band_count")
            .cast("long")
            .alias("audit_metric_count"),
            F.to_json(
                F.struct(
                    F.col("grp.distinct_adherence_band_count")
                    .cast("long")
                    .alias("distinct_adherence_band_count"),
                    F.col("grp.adherence_bands").alias("adherence_bands"),
                )
            ).alias("audit_metric_values"),
            F.lit("IMPOSSIBLE_CLASSIFICATION_TRANSITIONS").alias("audit_rule"),
            F.lit(
                "The same vehicle has multiple adherence_band values within the same "
                "service_date, line_sign, gtfs_direction_id, and hour context."
            ).alias("audit_message"),
        )
    )


def _mapped_direction_missing_descriptive_fields(df: DataFrame) -> DataFrame:
    """
    Detect rows where gtfs_direction_id is mapped but expected descriptive fields
    are missing.

    Current rule:
        - direction_description is null or blank while gtfs_direction_id is not null
    """
    return (
        df.filter(
            F.col("gtfs_direction_id").isNotNull()
            & (
                F.col("direction_description").isNull()
                | (F.trim(F.col("direction_description")) == F.lit(""))
            )
        )
        .select(
            *[F.col(column_name).alias(column_name) for column_name in SOURCE_COLUMNS],
            F.lit(None).cast("long").alias("audit_metric_count"),
            F.lit(None).cast("string").alias("audit_metric_values"),
            F.lit("MAPPED_DIRECTION_MISSING_DESCRIPTIVE_FIELDS").alias("audit_rule"),
            F.lit(
                "gtfs_direction_id is mapped but expected descriptive fields are missing, "
                "such as direction_description."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for logical inconsistencies in "
        "gold.gold_line_explorer_map_points, including unexpectedly large "
        "No Gold Match volumes, impossible classification transitions, and "
        "mapped direction rows missing expected descriptive fields."
    ),
)
def gold_line_explorer_map_points_logical_inconsistencies_audit() -> DataFrame:
    """
    Create a non-blocking audit table for logical inconsistencies in the
    Gold line explorer map points table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    unexpectedly_large_no_gold_match_volume_df = (
        _unexpectedly_large_no_gold_match_volume(source_df)
    )
    impossible_classification_transitions_df = _impossible_classification_transitions(
        source_df
    )
    mapped_direction_missing_descriptive_fields_df = (
        _mapped_direction_missing_descriptive_fields(source_df)
    )

    audit_df = (
        unexpectedly_large_no_gold_match_volume_df.unionByName(
            impossible_classification_transitions_df
        )
        .unionByName(mapped_direction_missing_descriptive_fields_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
