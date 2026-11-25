from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_frequencies"
TABLE_NAME = "silver.silver_gtfs_frequencies_invalid_headway_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent headway profile "
        "attributes in silver.silver_gtfs_frequencies."
    ),
)
def silver_gtfs_frequencies_invalid_headway_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_frequencies where headway attributes
    are invalid or do not reconcile with related derived fields.

    Purpose:
        Identify malformed headway definitions that can compromise downstream
        planned-service interpretation.

    Important:
        - headway_secs must be positive when present.
        - headway_minutes should reconcile with headway_secs.
        - headway_secs greater than service_window_seconds is surfaced here as
          a warning-level profile weakness rather than a strict structural error.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    enriched_df = (
        source_df.withColumn(
            "expected_headway_minutes",
            F.when(
                F.col("headway_secs").isNotNull(),
                F.col("headway_secs") / F.lit(60.0),
            ),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("headway_secs").isNull(),
                F.lit("HEADWAY_SECS_IS_NULL"),
            )
            .when(
                F.col("headway_secs") <= F.lit(0),
                F.lit("HEADWAY_SECS_NOT_POSITIVE"),
            )
            .when(
                F.col("headway_minutes").isNull(),
                F.lit("HEADWAY_MINUTES_IS_NULL"),
            )
            .when(
                F.col("expected_headway_minutes").isNotNull()
                & (
                    F.abs(
                        F.col("headway_minutes")
                        - F.col("expected_headway_minutes")
                    )
                    > F.lit(0.000001)
                ),
                F.lit("HEADWAY_MINUTES_DOES_NOT_RECONCILE_WITH_HEADWAY_SECS"),
            )
            .when(
                F.col("service_window_seconds").isNotNull()
                & F.col("headway_secs").isNotNull()
                & (F.col("headway_secs") > F.col("service_window_seconds")),
                F.lit("HEADWAY_SECS_GREATER_THAN_SERVICE_WINDOW_SECONDS"),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "audit_severity",
            F.when(
                F.col("audit_reason") == F.lit("HEADWAY_SECS_GREATER_THAN_SERVICE_WINDOW_SECONDS"),
                F.lit("WARNING"),
            ).otherwise(F.lit("ERROR")),
        )
    )

    audited_df = (
        enriched_df.filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("RECONCILIATION"))
        .withColumn("audit_rule", F.lit("INVALID_HEADWAY_PROFILE"))
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
            "service_window_minutes",
            "headway_secs",
            "headway_minutes",
            "expected_headway_minutes",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
