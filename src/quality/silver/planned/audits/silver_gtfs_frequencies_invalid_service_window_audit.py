from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_frequencies"
TABLE_NAME = "silver.silver_gtfs_frequencies_invalid_service_window_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent service window "
        "attributes in silver.silver_gtfs_frequencies."
    ),
)
def silver_gtfs_frequencies_invalid_service_window_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_frequencies where the service window
    is invalid or does not reconcile with start/end time fields.

    Purpose:
        Identify malformed frequency service windows that can compromise
        downstream planned-service interpretation.

    Important:
        - This audit focuses on:
            * end_time earlier than start_time
            * non-positive service windows
            * service_window_seconds inconsistent with start/end times
            * service_window_minutes inconsistent with service_window_seconds
        - Headway-specific logic should be handled by a dedicated audit.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    enriched_df = (
        source_df.withColumn(
            "expected_service_window_seconds",
            F.when(
                F.col("start_time_seconds").isNotNull()
                & F.col("end_time_seconds").isNotNull(),
                F.col("end_time_seconds") - F.col("start_time_seconds"),
            ),
        )
        .withColumn(
            "expected_service_window_minutes",
            F.when(
                F.col("expected_service_window_seconds").isNotNull(),
                F.col("expected_service_window_seconds") / F.lit(60.0),
            ),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("start_time_seconds").isNotNull()
                & F.col("end_time_seconds").isNotNull()
                & (F.col("start_time_seconds") > F.col("end_time_seconds")),
                F.lit("START_TIME_SECONDS_GREATER_THAN_END_TIME_SECONDS"),
            )
            .when(
                F.col("service_window_seconds").isNull(),
                F.lit("SERVICE_WINDOW_SECONDS_IS_NULL"),
            )
            .when(
                F.col("service_window_seconds") <= F.lit(0),
                F.lit("SERVICE_WINDOW_SECONDS_NOT_POSITIVE"),
            )
            .when(
                F.col("expected_service_window_seconds").isNotNull()
                & (
                    F.col("service_window_seconds")
                    != F.col("expected_service_window_seconds")
                ),
                F.lit("SERVICE_WINDOW_SECONDS_DOES_NOT_RECONCILE_WITH_START_AND_END_SECONDS"),
            )
            .when(
                F.col("service_window_minutes").isNull(),
                F.lit("SERVICE_WINDOW_MINUTES_IS_NULL"),
            )
            .when(
                F.col("expected_service_window_minutes").isNotNull()
                & (
                    F.abs(
                        F.col("service_window_minutes")
                        - F.col("expected_service_window_minutes")
                    )
                    > F.lit(0.000001)
                ),
                F.lit("SERVICE_WINDOW_MINUTES_DOES_NOT_RECONCILE_WITH_SERVICE_WINDOW_SECONDS"),
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
            "trip_id",
            "start_time",
            "start_time_seconds",
            "start_time_minutes",
            "end_time",
            "end_time_seconds",
            "end_time_minutes",
            "service_window_seconds",
            "expected_service_window_seconds",
            "service_window_minutes",
            "expected_service_window_minutes",
            "headway_secs",
            "headway_minutes",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
