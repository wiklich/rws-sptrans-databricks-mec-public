"""
Gold line explorer map points pipeline.

Purpose:
    Build a dashboard-ready observed spatial point table for the Line Explorer Map.

Source:
    - main.silver.silver_sptrans_positions
    - main.gold.gold_direction_mapping_reference
    - main.gold.gold_planned_vs_observed_service_adherence_hourly

Grain:
    One row per observed position event after validated direction mapping enrichment.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_line_explorer_map_points import (
    GOLD_LINE_EXPLORER_MAP_POINTS_SCHEMA,
)

TABLE_NAME = "gold.gold_line_explorer_map_points"

SPTRANS_POSITIONS_SOURCE = "main.silver.silver_sptrans_positions"
DIRECTION_MAPPING_SOURCE = "main.gold.gold_direction_mapping_reference"
SERVICE_ADHERENCE_SOURCE = "main.gold.gold_planned_vs_observed_service_adherence_hourly"


def _build_positions_base() -> DataFrame:
    """
    Read observed SPTrans positions and keep only structurally valid map points.
    """
    return (
        spark.read.table(SPTRANS_POSITIONS_SOURCE)
        .select(
            F.col("event_id"),
            F.col("captured_date_local").cast("date").alias("service_date"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("captured_hour_local").cast("int").alias("hour"),
            F.col("line_sign"),
            F.col("direction").cast("int").alias("observed_direction"),
            F.col("direction_description"),
            F.col("vehicle_prefix").cast("long").alias("vehicle_prefix"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
        )
        .filter(
            F.col("event_id").isNotNull()
            & F.col("service_date").isNotNull()
            & F.col("captured_at_local").isNotNull()
            & F.col("hour").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("observed_direction").isNotNull()
            & F.col("vehicle_prefix").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .dropDuplicates(["event_id"])
    )


def _build_validated_direction_mapping() -> DataFrame:
    """
    Read only production-safe direction mappings.

    Approved mappings:
        - validated_flag = true
        - review_required_flag = false
    """
    return (
        spark.read.table(DIRECTION_MAPPING_SOURCE)
        .select(
            F.col("line_sign"),
            F.col("observed_direction").cast("int").alias("observed_direction"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("validated_flag"),
            F.col("review_required_flag"),
        )
        .filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("review_required_flag") == F.lit(False))
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("observed_direction").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
        .dropDuplicates(["line_sign", "observed_direction"])
    )


def _build_hourly_service_adherence() -> DataFrame:
    """
    Read hourly planned vs observed service adherence metrics at the dashboard join grain.
    """
    return (
        spark.read.table(SERVICE_ADHERENCE_SOURCE)
        .select(
            F.col("service_date").cast("date").alias("service_date"),
            F.col("line_sign"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("hour").cast("int").alias("hour"),
            F.col("planned_headway_minutes").cast("double").alias(
                "planned_headway_minutes"
            ),
            F.col("observed_headway_avg_minutes").cast("double").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("headway_deviation_minutes").cast("double").alias(
                "headway_deviation_minutes"
            ),
            F.col("headway_deviation_pct").cast("double").alias(
                "headway_deviation_pct"
            ),
            F.col("missed_service_flag").cast("boolean").alias("missed_service_flag"),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & (F.trim(F.col("line_sign")) != "")
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("hour").isNotNull()
        )
        .dropDuplicates(["service_date", "line_sign", "gtfs_direction_id", "hour"])
    )


def _build_mapped_points() -> DataFrame:
    """
    Join observed points with validated direction mapping.
    """
    positions_base = _build_positions_base()
    validated_mapping = _build_validated_direction_mapping()

    return positions_base.join(
        validated_mapping,
        on=["line_sign", "observed_direction"],
        how="inner",
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Gold observed spatial points serving table for the Line Explorer Map. "
        "Materializes observed SPTrans vehicle positions already enriched with "
        "validated direction mapping and hourly planned vs observed service "
        "adherence metrics."
    ),
    schema=GOLD_LINE_EXPLORER_MAP_POINTS_SCHEMA,
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_service_date", "service_date IS NOT NULL")
@dp.expect_or_drop("valid_captured_at_local", "captured_at_local IS NOT NULL")
@dp.expect_or_drop("valid_hour", "hour IS NOT NULL")
@dp.expect_or_drop("valid_line_sign", "line_sign IS NOT NULL")
@dp.expect_or_drop("valid_gtfs_direction_id", "gtfs_direction_id IS NOT NULL")
@dp.expect_or_drop("valid_vehicle_prefix", "vehicle_prefix IS NOT NULL")
@dp.expect_or_drop("valid_latitude", "latitude IS NOT NULL")
@dp.expect_or_drop("valid_longitude", "longitude IS NOT NULL")
def gold_line_explorer_map_points() -> DataFrame:
    """
    Create a dashboard-ready observed point table for the Line Explorer Map.

    Final output:
        One row per observed position event after validated direction mapping enrichment.
    """
    mapped_points = _build_mapped_points()
    hourly_adherence = _build_hourly_service_adherence()

    final_df = (
        mapped_points.alias("p")
        .join(
            hourly_adherence.alias("a"),
            on=[
                "service_date",
                "line_sign",
                "gtfs_direction_id",
                "hour",
            ],
            how="left",
        )
        .withColumn(
            "adherence_band",
            F.when(
                F.col("a.headway_deviation_minutes").isNull(),
                F.lit("No Gold Match"),
            )
            .when(
                F.col("a.headway_deviation_minutes") >= F.lit(10.0),
                F.lit("High Deviation"),
            )
            .when(
                F.col("a.headway_deviation_minutes") >= F.lit(3.0),
                F.lit("Moderate Deviation"),
            )
            .when(
                F.col("a.headway_deviation_minutes") > F.lit(0.0),
                F.lit("Low Deviation"),
            )
            .otherwise(F.lit("Aligned or Better")),
        )
        .withColumn("record_created_at", F.current_timestamp())
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("event_id"),
                    F.col("service_date").cast("string"),
                    F.col("captured_at_local").cast("string"),
                    F.col("line_sign"),
                    F.col("observed_direction").cast("string"),
                    F.col("gtfs_direction_id").cast("string"),
                    F.col("vehicle_prefix").cast("string"),
                    F.col("latitude").cast("string"),
                    F.col("longitude").cast("string"),
                ),
                256,
            ),
        )
        .select(
            F.col("event_id").cast("string").alias("event_id"),
            F.col("service_date").cast("date").alias("service_date"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
            F.col("hour").cast("int").alias("hour"),
            F.col("line_sign").cast("string").alias("line_sign"),
            F.col("observed_direction").cast("int").alias("observed_direction"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("direction_description").cast("string").alias(
                "direction_description"
            ),
            F.col("vehicle_prefix").cast("long").alias("vehicle_prefix"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("planned_headway_minutes").cast("double").alias(
                "planned_headway_minutes"
            ),
            F.col("observed_headway_avg_minutes").cast("double").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("headway_deviation_minutes").cast("double").alias(
                "headway_deviation_minutes"
            ),
            F.col("headway_deviation_pct").cast("double").alias(
                "headway_deviation_pct"
            ),
            F.col("missed_service_flag").cast("boolean").alias("missed_service_flag"),
            F.col("adherence_band").cast("string").alias("adherence_band"),
            F.col("record_created_at").cast("timestamp").alias("record_created_at"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )
    )

    return final_df
