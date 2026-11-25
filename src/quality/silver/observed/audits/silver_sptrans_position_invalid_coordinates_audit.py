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
TABLE_NAME = "silver.silver_sptrans_position_invalid_coordinates_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or suspicious coordinates in "
        "silver.silver_sptrans_positions. Includes globally invalid latitude/"
        "longitude values and coordinates outside the approved SPTrans "
        "bounding box."
    ),
)
def silver_sptrans_position_invalid_coordinates_audit() -> DataFrame:
    """
    Audit rows from silver.silver_sptrans_positions that contain invalid,
    missing, or out-of-scope coordinates.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("latitude").isNull(), F.lit("LATITUDE_IS_NULL"))
            .when(F.col("longitude").isNull(), F.lit("LONGITUDE_IS_NULL"))
            .when(
                (F.col("latitude") < F.lit(-90.0)) | (F.col("latitude") > F.lit(90.0)),
                F.lit("LATITUDE_OUTSIDE_GLOBAL_DOMAIN"),
            )
            .when(
                (F.col("longitude") < F.lit(-180.0))
                | (F.col("longitude") > F.lit(180.0)),
                F.lit("LONGITUDE_OUTSIDE_GLOBAL_DOMAIN"),
            )
            .when(
                (F.col("latitude") < F.lit(BOUNDING_BOX_MIN_LAT))
                | (F.col("latitude") > F.lit(BOUNDING_BOX_MAX_LAT))
                | (F.col("longitude") < F.lit(BOUNDING_BOX_MIN_LON))
                | (F.col("longitude") > F.lit(BOUNDING_BOX_MAX_LON)),
                F.lit("OUTSIDE_SPTRANS_BOUNDING_BOX"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("INVALID_OR_OUT_OF_SCOPE_COORDINATES"))
        .withColumn("audit_severity", F.lit("ERROR"))
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
