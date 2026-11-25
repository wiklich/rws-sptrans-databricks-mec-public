from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_sptrans_positions"
TABLE_NAME = "silver.silver_sptrans_position_missing_critical_fields_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for missing critical fields in "
        "silver.silver_sptrans_positions. Identifies records where required "
        "operational, temporal, or spatial attributes are null or blank."
    ),
)
def silver_sptrans_position_missing_critical_fields_audit() -> DataFrame:
    """
    Audit rows from silver.silver_sptrans_positions that contain missing
    critical fields required for downstream analytical usage.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("event_id").isNull(), F.lit("EVENT_ID_IS_NULL"))
            .when(F.trim(F.col("event_id")) == "", F.lit("EVENT_ID_IS_BLANK"))
            .when(F.col("line_id").isNull(), F.lit("LINE_ID_IS_NULL"))
            .when(F.col("vehicle_prefix").isNull(), F.lit("VEHICLE_PREFIX_IS_NULL"))
            .when(
                F.trim(F.col("vehicle_prefix").cast("string")) == "",
                F.lit("VEHICLE_PREFIX_IS_BLANK"),
            )
            .when(F.col("direction").isNull(), F.lit("DIRECTION_IS_NULL"))
            .when(
                F.col("direction_description").isNull(),
                F.lit("DIRECTION_DESCRIPTION_IS_NULL"),
            )
            .when(
                F.trim(F.col("direction_description")) == "",
                F.lit("DIRECTION_DESCRIPTION_IS_BLANK"),
            )
            .when(F.col("captured_at_utc").isNull(), F.lit("CAPTURED_AT_UTC_IS_NULL"))
            .when(
                F.col("captured_at_local").isNull(),
                F.lit("CAPTURED_AT_LOCAL_IS_NULL"),
            )
            .when(
                F.col("captured_date_local").isNull(),
                F.lit("CAPTURED_DATE_LOCAL_IS_NULL"),
            )
            .when(
                F.col("captured_hour_local").isNull(),
                F.lit("CAPTURED_HOUR_LOCAL_IS_NULL"),
            )
            .when(F.col("latitude").isNull(), F.lit("LATITUDE_IS_NULL"))
            .when(F.col("longitude").isNull(), F.lit("LONGITUDE_IS_NULL"))
            .when(F.col("ingestion_ts").isNull(), F.lit("INGESTION_TS_IS_NULL"))
            .when(F.col("ingestion_date").isNull(), F.lit("INGESTION_DATE_IS_NULL"))
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
