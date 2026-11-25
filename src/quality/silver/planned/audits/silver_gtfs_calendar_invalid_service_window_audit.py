from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_calendar"
TABLE_NAME = "silver.silver_gtfs_calendar_invalid_service_window_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent service window "
        "attributes in silver.silver_gtfs_calendar."
    ),
)
def silver_gtfs_calendar_invalid_service_window_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_calendar where the service window
    is invalid or does not reconcile with start_date and end_date.

    Purpose:
        Identify malformed service-date windows that can compromise
        downstream planned-service interpretation.

    Important:
        - This audit focuses on:
            * start_date later than end_date
            * non-positive service_duration_days
            * service_duration_days inconsistent with start_date/end_date
    """
    source_df = spark.read.table(SOURCE_TABLE)

    enriched_df = (
        source_df.withColumn(
            "expected_service_duration_days",
            F.when(
                F.col("start_date").isNotNull() & F.col("end_date").isNotNull(),
                F.datediff(F.col("end_date"), F.col("start_date")) + F.lit(1),
            ),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("start_date").isNull(),
                F.lit("START_DATE_IS_NULL"),
            )
            .when(
                F.col("end_date").isNull(),
                F.lit("END_DATE_IS_NULL"),
            )
            .when(
                F.col("start_date") > F.col("end_date"),
                F.lit("START_DATE_GREATER_THAN_END_DATE"),
            )
            .when(
                F.col("service_duration_days").isNull(),
                F.lit("SERVICE_DURATION_DAYS_IS_NULL"),
            )
            .when(
                F.col("service_duration_days") <= F.lit(0),
                F.lit("SERVICE_DURATION_DAYS_NOT_POSITIVE"),
            )
            .when(
                F.col("expected_service_duration_days").isNotNull()
                & (
                    F.col("service_duration_days")
                    != F.col("expected_service_duration_days")
                ),
                F.lit(
                    "SERVICE_DURATION_DAYS_DOES_NOT_RECONCILE_WITH_START_AND_END_DATE"
                ),
            )
            .otherwise(F.lit(None)),
        )
    )

    audited_df = (
        enriched_df.filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("RECONCILIATION"))
        .withColumn("audit_rule", F.lit("INVALID_SERVICE_WINDOW"))
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
            "expected_service_duration_days",
            "active_weekdays_count",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
