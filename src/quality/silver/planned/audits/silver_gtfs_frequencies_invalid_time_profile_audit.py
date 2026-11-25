from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_frequencies"
TABLE_NAME = "silver.silver_gtfs_frequencies_invalid_time_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent time profile "
        "attributes in silver.silver_gtfs_frequencies."
    ),
)
def silver_gtfs_frequencies_invalid_time_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_frequencies where start/end time fields
    are invalid, incomplete, or inconsistent with their derived seconds/minutes
    representations.

    Purpose:
        Identify malformed GTFS frequency windows and broken time derivations
        that can compromise downstream planned-service interpretation.

    Important:
        - GTFS allows extended-hour values such as 25:10:00.
        - This audit validates HH:MM:SS format using:
            ^\\d+:[0-5]\\d:[0-5]\\d$
        - This audit focuses on time-string and derived-field consistency.
        - Service-window and headway-specific logic should be handled by
          dedicated audits.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    valid_start_time_format_expr = (
        F.col("start_time").isNotNull()
        & F.col("start_time").rlike(r"^\d+:[0-5]\d:[0-5]\d$")
    )

    valid_end_time_format_expr = (
        F.col("end_time").isNotNull()
        & F.col("end_time").rlike(r"^\d+:[0-5]\d:[0-5]\d$")
    )

    start_time_parts = F.split(F.col("start_time"), ":")
    end_time_parts = F.split(F.col("end_time"), ":")

    enriched_df = (
        source_df.withColumn(
            "expected_start_time_seconds",
            F.when(
                valid_start_time_format_expr,
                start_time_parts.getItem(0).cast("int") * F.lit(3600)
                + start_time_parts.getItem(1).cast("int") * F.lit(60)
                + start_time_parts.getItem(2).cast("int"),
            ),
        )
        .withColumn(
            "expected_end_time_seconds",
            F.when(
                valid_end_time_format_expr,
                end_time_parts.getItem(0).cast("int") * F.lit(3600)
                + end_time_parts.getItem(1).cast("int") * F.lit(60)
                + end_time_parts.getItem(2).cast("int"),
            ),
        )
        .withColumn(
            "expected_start_time_minutes",
            F.when(
                F.col("expected_start_time_seconds").isNotNull(),
                F.col("expected_start_time_seconds") / F.lit(60.0),
            ),
        )
        .withColumn(
            "expected_end_time_minutes",
            F.when(
                F.col("expected_end_time_seconds").isNotNull(),
                F.col("expected_end_time_seconds") / F.lit(60.0),
            ),
        )
        .withColumn(
            "audit_reason",
            F.when(F.col("start_time").isNull(), F.lit("START_TIME_IS_NULL"))
            .when(F.trim(F.col("start_time")) == "", F.lit("START_TIME_IS_BLANK"))
            .when(~valid_start_time_format_expr, F.lit("START_TIME_INVALID_FORMAT"))
            .when(F.col("end_time").isNull(), F.lit("END_TIME_IS_NULL"))
            .when(F.trim(F.col("end_time")) == "", F.lit("END_TIME_IS_BLANK"))
            .when(~valid_end_time_format_expr, F.lit("END_TIME_INVALID_FORMAT"))
            .when(
                F.col("start_time_seconds").isNull(),
                F.lit("START_TIME_SECONDS_IS_NULL_WHILE_START_TIME_IS_PRESENT"),
            )
            .when(
                F.col("end_time_seconds").isNull(),
                F.lit("END_TIME_SECONDS_IS_NULL_WHILE_END_TIME_IS_PRESENT"),
            )
            .when(
                F.col("start_time_seconds") != F.col("expected_start_time_seconds"),
                F.lit("START_TIME_SECONDS_DOES_NOT_RECONCILE_WITH_START_TIME"),
            )
            .when(
                F.col("end_time_seconds") != F.col("expected_end_time_seconds"),
                F.lit("END_TIME_SECONDS_DOES_NOT_RECONCILE_WITH_END_TIME"),
            )
            .when(
                F.col("start_time_minutes").isNull(),
                F.lit("START_TIME_MINUTES_IS_NULL_WHILE_START_TIME_SECONDS_IS_PRESENT"),
            )
            .when(
                F.col("end_time_minutes").isNull(),
                F.lit("END_TIME_MINUTES_IS_NULL_WHILE_END_TIME_SECONDS_IS_PRESENT"),
            )
            .when(
                F.abs(
                    F.col("start_time_minutes") - F.col("expected_start_time_minutes")
                )
                > F.lit(0.000001),
                F.lit("START_TIME_MINUTES_DOES_NOT_RECONCILE_WITH_START_TIME_SECONDS"),
            )
            .when(
                F.abs(
                    F.col("end_time_minutes") - F.col("expected_end_time_minutes")
                )
                > F.lit(0.000001),
                F.lit("END_TIME_MINUTES_DOES_NOT_RECONCILE_WITH_END_TIME_SECONDS"),
            )
            .otherwise(F.lit(None)),
        )
    )

    audited_df = (
        enriched_df.filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("RECONCILIATION"))
        .withColumn("audit_rule", F.lit("INVALID_TIME_PROFILE"))
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
            "expected_start_time_seconds",
            "start_time_minutes",
            "expected_start_time_minutes",
            "end_time",
            "end_time_seconds",
            "expected_end_time_seconds",
            "end_time_minutes",
            "expected_end_time_minutes",
            "service_window_seconds",
            "service_window_minutes",
            "headway_secs",
            "headway_minutes",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
