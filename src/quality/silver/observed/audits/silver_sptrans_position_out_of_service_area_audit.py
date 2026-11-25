from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.constants_utils import (
    BOUNDING_BOX_MIN_LAT,
    BOUNDING_BOX_MAX_LAT,
    BOUNDING_BOX_MIN_LON,
    BOUNDING_BOX_MAX_LON,
)

SOURCE_TABLE = "silver.silver_sptrans_positions"
TABLE_NAME = "silver.silver_sptrans_position_out_of_service_area_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for positions that are globally valid but fall "
        "outside the approved SPTrans service bounding box in "
        "silver.silver_sptrans_positions."
    ),
)
def silver_sptrans_position_out_of_service_area_audit() -> DataFrame:
    """
    Audit rows from silver.silver_sptrans_positions that contain coordinates
    outside the approved SPTrans service area.

    Important:
        - This audit is intentionally narrower than
          silver_sptrans_position_invalid_coordinates_audit.
        - It only captures rows whose coordinates are globally valid
          but outside the approved SPTrans bounding box.
        - Null coordinates and globally invalid coordinates are expected
          to be captured by the invalid-coordinates audit instead.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    globally_valid_coordinates_df = source_df.filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("latitude").between(F.lit(-90.0), F.lit(90.0))
        & F.col("longitude").between(F.lit(-180.0), F.lit(180.0))
    )

    audited_df = (
        globally_valid_coordinates_df.withColumn(
            "audit_reason",
            F.when(
                (F.col("latitude") < F.lit(BOUNDING_BOX_MIN_LAT))
                & (F.col("longitude") < F.lit(BOUNDING_BOX_MIN_LON)),
                F.lit("OUTSIDE_SERVICE_AREA_SOUTHWEST"),
            )
            .when(
                (F.col("latitude") < F.lit(BOUNDING_BOX_MIN_LAT))
                & (F.col("longitude") > F.lit(BOUNDING_BOX_MAX_LON)),
                F.lit("OUTSIDE_SERVICE_AREA_SOUTHEAST"),
            )
            .when(
                (F.col("latitude") > F.lit(BOUNDING_BOX_MAX_LAT))
                & (F.col("longitude") < F.lit(BOUNDING_BOX_MIN_LON)),
                F.lit("OUTSIDE_SERVICE_AREA_NORTHWEST"),
            )
            .when(
                (F.col("latitude") > F.lit(BOUNDING_BOX_MAX_LAT))
                & (F.col("longitude") > F.lit(BOUNDING_BOX_MAX_LON)),
                F.lit("OUTSIDE_SERVICE_AREA_NORTHEAST"),
            )
            .when(
                F.col("latitude") < F.lit(BOUNDING_BOX_MIN_LAT),
                F.lit("OUTSIDE_SERVICE_AREA_SOUTH"),
            )
            .when(
                F.col("latitude") > F.lit(BOUNDING_BOX_MAX_LAT),
                F.lit("OUTSIDE_SERVICE_AREA_NORTH"),
            )
            .when(
                F.col("longitude") < F.lit(BOUNDING_BOX_MIN_LON),
                F.lit("OUTSIDE_SERVICE_AREA_WEST"),
            )
            .when(
                F.col("longitude") > F.lit(BOUNDING_BOX_MAX_LON),
                F.lit("OUTSIDE_SERVICE_AREA_EAST"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("POSITIONS_OUTSIDE_APPROVED_SERVICE_AREA"))
        .withColumn("audit_severity", F.lit("WARNING"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "event_id",
            "line_id",
            "line_sign",
            "vehicle_prefix",
            "direction",
            "direction_description",
            "captured_at_utc",
            "captured_at_local",
            "captured_date_local",
            "captured_hour_local",
            "latitude",
            "longitude",
            "vehicles_count",
            "ingestion_ts",
            "ingestion_date",
            "audited_at",
        )
    )

    return audited_df
