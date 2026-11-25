from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_calendar"
TABLE_NAME = "silver.silver_gtfs_calendar_missing_critical_fields_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for missing critical fields in "
        "silver.silver_gtfs_calendar. Identifies records where required "
        "identity, service-window, or lineage attributes are null or blank."
    ),
)
def silver_gtfs_calendar_missing_critical_fields_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_calendar that contain missing
    critical fields required for downstream analytical usage.

    Purpose:
        Identify service calendar records with missing identity, service-window,
        or lineage attributes that can compromise trip linkage, planned-service
        interpretation, and downstream analysis.

    Important:
        - Daily activity flags are not treated as critical fields here because
          they are evaluated separately as part of weekly-profile quality.
        - This audit focuses on the minimum set of attributes required for
          stable analytical consumption in this project.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("service_id").isNull(), F.lit("SERVICE_ID_IS_NULL"))
            .when(F.trim(F.col("service_id")) == "", F.lit("SERVICE_ID_IS_BLANK"))
            .when(F.col("start_date").isNull(), F.lit("START_DATE_IS_NULL"))
            .when(F.col("end_date").isNull(), F.lit("END_DATE_IS_NULL"))
            .when(
                F.col("service_duration_days").isNull(),
                F.lit("SERVICE_DURATION_DAYS_IS_NULL"),
            )
            .when(
                F.col("active_weekdays_count").isNull(),
                F.lit("ACTIVE_WEEKDAYS_COUNT_IS_NULL"),
            )
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
            "service_id",
            "monday_active",
            "tuesday_active",
            "wednesday_active",
            "thursday_active",
            "friday_active",
            "saturday_active",
            "sunday_active",
            "start_date",
            "end_date",
            "service_duration_days",
            "active_weekdays_count",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
