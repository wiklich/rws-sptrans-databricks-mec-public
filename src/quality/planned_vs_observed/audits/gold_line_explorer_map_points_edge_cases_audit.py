"""
Audit pipeline for edge cases in
gold.gold_line_explorer_map_points.

Purpose:
    Detect unusual but possible observed-point scenarios that should be monitored.

Audit scope:
    - extremely high point density for one vehicle in a narrow time window
    - unusually large share of No Gold Match
    - one line-direction-hour producing very sparse or very dense point volume
    - points outside expected map coverage

Important:
    This is a non-blocking audit table.
    It exposes unusual Gold line explorer map point cases for investigation
    without dropping records from the main Gold table.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "gold.gold_line_explorer_map_points"
TABLE_NAME = "gold.gold_line_explorer_map_points_edge_cases_audit"

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

NARROW_WINDOW_MINUTES = 5
EXTREME_VEHICLE_POINT_DENSITY_THRESHOLD = 30

NO_GOLD_MATCH_POINT_COUNT_THRESHOLD = 100
NO_GOLD_MATCH_SHARE_THRESHOLD = 0.50

EXPECTED_MIN_LATITUDE = -24.20
EXPECTED_MAX_LATITUDE = -23.20
EXPECTED_MIN_LONGITUDE = -47.20
EXPECTED_MAX_LONGITUDE = -46.20


def _source_projection(alias_name: str) -> list:
    """
    Build a reusable projection of source columns from the given alias.
    """
    return [
        F.col(f"{alias_name}.{column_name}").alias(column_name)
        for column_name in SOURCE_COLUMNS
    ]


def _extremely_high_vehicle_point_density(df: DataFrame) -> DataFrame:
    """
    Detect extremely high point density for one vehicle in a narrow time window.

    Narrow window:
        fixed N-minute bucket over captured_at_local
    """
    window_seconds = NARROW_WINDOW_MINUTES * 60

    with_window = df.withColumn(
        "window_bucket_start_epoch",
        (
            F.floor(F.unix_timestamp("captured_at_local") / F.lit(window_seconds))
            * F.lit(window_seconds)
        ).cast("long"),
    ).withColumn(
        "window_bucket_start",
        F.from_unixtime(F.col("window_bucket_start_epoch")).cast("timestamp"),
    )

    grouped_df = (
        with_window.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "vehicle_prefix",
            "window_bucket_start",
        )
        .agg(F.count(F.lit(1)).cast("long").alias("vehicle_window_point_count"))
        .filter(
            F.col("vehicle_window_point_count")
            >= F.lit(EXTREME_VEHICLE_POINT_DENSITY_THRESHOLD)
        )
    )

    return (
        with_window.alias("src")
        .join(
            grouped_df.alias("grp"),
            on=[
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "vehicle_prefix",
                "window_bucket_start",
            ],
            how="inner",
        )
        .select(
            *_source_projection("src"),
            F.col("grp.vehicle_window_point_count")
            .cast("long")
            .alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.col("grp.window_bucket_start").alias("window_bucket_start"),
                    F.lit(NARROW_WINDOW_MINUTES).alias("window_minutes"),
                    F.col("grp.vehicle_window_point_count")
                    .cast("long")
                    .alias("vehicle_window_point_count"),
                    F.lit(EXTREME_VEHICLE_POINT_DENSITY_THRESHOLD).alias(
                        "point_density_threshold"
                    ),
                )
            ).alias("edge_case_metric_values"),
            F.lit("EXTREMELY_HIGH_VEHICLE_POINT_DENSITY").alias("audit_rule"),
            F.lit(
                "A vehicle has an extremely high number of observed points within "
                "a narrow time window."
            ).alias("audit_message"),
        )
    )


def _unusually_large_no_gold_match_share(df: DataFrame) -> DataFrame:
    """
    Detect hourly line-direction groups with unusually large share of No Gold Match.
    """
    grouped_df = (
        df.groupBy("service_date", "line_sign", "gtfs_direction_id", "hour")
        .agg(
            F.count(F.lit(1)).cast("long").alias("group_point_count"),
            F.sum(
                F.when(F.col("adherence_band") == F.lit("No Gold Match"), F.lit(1))
                .otherwise(F.lit(0))
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
            (F.col("no_gold_match_point_count") >= F.lit(NO_GOLD_MATCH_POINT_COUNT_THRESHOLD))
            & (F.col("no_gold_match_share") >= F.lit(NO_GOLD_MATCH_SHARE_THRESHOLD))
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
            F.col("grp.no_gold_match_point_count")
            .cast("long")
            .alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.col("grp.group_point_count").cast("long").alias("group_point_count"),
                    F.col("grp.no_gold_match_point_count")
                    .cast("long")
                    .alias("no_gold_match_point_count"),
                    F.col("grp.no_gold_match_share").cast("double").alias(
                        "no_gold_match_share"
                    ),
                    F.lit(NO_GOLD_MATCH_POINT_COUNT_THRESHOLD).alias(
                        "point_count_threshold"
                    ),
                    F.lit(NO_GOLD_MATCH_SHARE_THRESHOLD).alias("share_threshold"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("UNUSUALLY_LARGE_NO_GOLD_MATCH_SHARE").alias("audit_rule"),
            F.lit(
                "A line-direction-hour has an unusually large share of No Gold Match points."
            ).alias("audit_message"),
        )
    )


def _sparse_or_dense_line_direction_hour(df: DataFrame) -> DataFrame:
    """
    Detect line-direction-hour groups with unusually sparse or unusually dense point volume
    using dynamic p01 / p99 thresholds.
    """
    grouped_counts = (
        df.groupBy("service_date", "line_sign", "gtfs_direction_id", "hour")
        .agg(F.count(F.lit(1)).cast("long").alias("line_direction_hour_point_count"))
    )

    threshold_row = (
        grouped_counts.agg(
            F.percentile_approx(
                "line_direction_hour_point_count",
                0.01,
            ).alias("p01_point_count_threshold"),
            F.percentile_approx(
                "line_direction_hour_point_count",
                0.99,
            ).alias("p99_point_count_threshold"),
        ).collect()
    )

    p01_threshold = (
        threshold_row[0]["p01_point_count_threshold"] if len(threshold_row) > 0 else None
    )
    p99_threshold = (
        threshold_row[0]["p99_point_count_threshold"] if len(threshold_row) > 0 else None
    )

    sparse_groups = (
        grouped_counts.filter(F.col("line_direction_hour_point_count") < F.lit(p01_threshold))
        if p01_threshold is not None
        else grouped_counts.filter(F.lit(False))
    )

    dense_groups = (
        grouped_counts.filter(F.col("line_direction_hour_point_count") > F.lit(p99_threshold))
        if p99_threshold is not None
        else grouped_counts.filter(F.lit(False))
    )

    sparse_df = (
        df.alias("src")
        .join(
            sparse_groups.alias("grp"),
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="inner",
        )
        .select(
            *_source_projection("src"),
            F.col("grp.line_direction_hour_point_count")
            .cast("long")
            .alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.col("grp.line_direction_hour_point_count")
                    .cast("long")
                    .alias("line_direction_hour_point_count"),
                    F.lit(p01_threshold).cast("long").alias("p01_point_count_threshold"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("UNUSUALLY_SPARSE_LINE_DIRECTION_HOUR").alias("audit_rule"),
            F.lit(
                "A line-direction-hour produces an unusually sparse point volume "
                "relative to the current Gold dataset."
            ).alias("audit_message"),
        )
    )

    dense_df = (
        df.alias("src")
        .join(
            dense_groups.alias("grp"),
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="inner",
        )
        .select(
            *_source_projection("src"),
            F.col("grp.line_direction_hour_point_count")
            .cast("long")
            .alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.col("grp.line_direction_hour_point_count")
                    .cast("long")
                    .alias("line_direction_hour_point_count"),
                    F.lit(p99_threshold).cast("long").alias("p99_point_count_threshold"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("UNUSUALLY_DENSE_LINE_DIRECTION_HOUR").alias("audit_rule"),
            F.lit(
                "A line-direction-hour produces an unusually dense point volume "
                "relative to the current Gold dataset."
            ).alias("audit_message"),
        )
    )

    return sparse_df.unionByName(dense_df)


def _points_outside_expected_map_coverage(df: DataFrame) -> DataFrame:
    """
    Detect points outside expected map coverage.

    Expected bounding box is intentionally broad and should be calibrated later
    if the operational coverage expands or contracts.
    """
    return (
        df.filter(
            (F.col("latitude") < F.lit(EXPECTED_MIN_LATITUDE))
            | (F.col("latitude") > F.lit(EXPECTED_MAX_LATITUDE))
            | (F.col("longitude") < F.lit(EXPECTED_MIN_LONGITUDE))
            | (F.col("longitude") > F.lit(EXPECTED_MAX_LONGITUDE))
        )
        .select(
            *[F.col(column_name).alias(column_name) for column_name in SOURCE_COLUMNS],
            F.lit(None).cast("long").alias("edge_case_metric_count"),
            F.to_json(
                F.struct(
                    F.lit(EXPECTED_MIN_LATITUDE).alias("expected_min_latitude"),
                    F.lit(EXPECTED_MAX_LATITUDE).alias("expected_max_latitude"),
                    F.lit(EXPECTED_MIN_LONGITUDE).alias("expected_min_longitude"),
                    F.lit(EXPECTED_MAX_LONGITUDE).alias("expected_max_longitude"),
                )
            ).alias("edge_case_metric_values"),
            F.lit("POINT_OUTSIDE_EXPECTED_MAP_COVERAGE").alias("audit_rule"),
            F.lit(
                "Observed point lies outside the expected map coverage bounding box."
            ).alias("audit_message"),
        )
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Non-blocking audit table for edge cases in "
        "gold.gold_line_explorer_map_points, including extreme vehicle point "
        "density, unusually large No Gold Match share, unusually sparse or "
        "dense line-direction-hour point volume, and points outside expected "
        "map coverage."
    ),
)
def gold_line_explorer_map_points_edge_cases_audit() -> DataFrame:
    """
    Create a non-blocking audit table for unusual but possible edge cases
    in the Gold line explorer map points table.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    extremely_high_vehicle_point_density_df = _extremely_high_vehicle_point_density(
        source_df
    )
    unusually_large_no_gold_match_share_df = _unusually_large_no_gold_match_share(
        source_df
    )
    sparse_or_dense_line_direction_hour_df = _sparse_or_dense_line_direction_hour(
        source_df
    )
    points_outside_expected_map_coverage_df = _points_outside_expected_map_coverage(
        source_df
    )

    audit_df = (
        extremely_high_vehicle_point_density_df.unionByName(
            unusually_large_no_gold_match_share_df
        )
        .unionByName(sparse_or_dense_line_direction_hour_df)
        .unionByName(points_outside_expected_map_coverage_df)
        .withColumn("audit_created_at", F.current_timestamp())
    )

    return audit_df
