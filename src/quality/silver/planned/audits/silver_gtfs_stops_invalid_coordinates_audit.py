from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_stops"
TABLE_NAME = "silver.silver_gtfs_stops_invalid_coordinates_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or missing coordinates in "
        "silver.silver_gtfs_stops."
    ),
)
def silver_gtfs_stops_invalid_coordinates_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stops that contain invalid,
    missing, or globally out-of-domain coordinates.

    Purpose:
        Identify stops whose latitude or longitude is null or falls
        outside the valid global geographic domain.

    Important:
        - This audit checks global geographic validity only.
        - It does not evaluate municipal or service-area bounding-box rules.
        - It is intended to protect downstream stop mapping, stop reference,
          and spatial analysis from malformed coordinates.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("stop_lat").isNull(), F.lit("STOP_LAT_IS_NULL"))
            .when(F.col("stop_lon").isNull(), F.lit("STOP_LON_IS_NULL"))
            .when(
                (F.col("stop_lat") < F.lit(-90.0))
                | (F.col("stop_lat") > F.lit(90.0)),
                F.lit("STOP_LAT_OUTSIDE_GLOBAL_DOMAIN"),
            )
            .when(
                (F.col("stop_lon") < F.lit(-180.0))
                | (F.col("stop_lon") > F.lit(180.0)),
                F.lit("STOP_LON_OUTSIDE_GLOBAL_DOMAIN"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("INVALID_OR_MISSING_COORDINATES"))
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "stop_id",
            "stop_name",
            "stop_desc",
            "stop_lat",
            "stop_lon",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
