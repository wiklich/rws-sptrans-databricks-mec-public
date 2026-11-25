from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_stops"
TABLE_NAME = "silver.silver_gtfs_stops_missing_critical_fields_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for missing critical fields in "
        "silver.silver_gtfs_stops. Identifies records where required "
        "identity, geographic, or lineage attributes are null or blank."
    ),
)
def silver_gtfs_stops_missing_critical_fields_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stops that contain missing
    critical fields required for downstream analytical usage.

    Purpose:
        Identify stops with missing identity, rider-facing, geographic,
        or lineage attributes that can compromise stop reference,
        stop_times linkage, spatial analysis, and downstream joins.

    Important:
        - stop_desc is not treated as a critical field here because it is optional.
        - This audit focuses on the minimum set of attributes required for
          stable analytical consumption in this project.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("stop_id").isNull(), F.lit("STOP_ID_IS_NULL"))
            .when(F.trim(F.col("stop_id")) == "", F.lit("STOP_ID_IS_BLANK"))
            .when(F.col("stop_name").isNull(), F.lit("STOP_NAME_IS_NULL"))
            .when(F.trim(F.col("stop_name")) == "", F.lit("STOP_NAME_IS_BLANK"))
            .when(F.col("stop_lat").isNull(), F.lit("STOP_LAT_IS_NULL"))
            .when(F.col("stop_lon").isNull(), F.lit("STOP_LON_IS_NULL"))
            .when(F.col("gtfs_load_date").isNull(), F.lit("GTFS_LOAD_DATE_IS_NULL"))
            .when(F.col("gtfs_loaded_at").isNull(), F.lit("GTFS_LOADED_AT_IS_NULL"))
            .when(F.col("record_hash").isNull(), F.lit("RECORD_HASH_IS_NULL"))
            .when(F.trim(F.col("record_hash")) == "", F.lit("RECORD_HASH_IS_BLANK"))
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("STRUCTURAL"))
        .withColumn("audit_rule", F.lit("MISSING_CRITICAL_FIELDS"))
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
