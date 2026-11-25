from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_calendar"
TABLE_NAME = "silver.silver_gtfs_calendar_invalid_weekly_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent weekly profile "
        "attributes in silver.silver_gtfs_calendar."
    ),
)
def silver_gtfs_calendar_invalid_weekly_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_calendar where the weekly service profile
    is invalid or does not reconcile with derived weekday metrics.

    Purpose:
        Identify malformed weekly service profiles that can compromise
        downstream planned-service interpretation.

    Important:
        - This audit focuses on:
            * active_weekdays_count missing
            * active_weekdays_count outside the expected domain 0..5
            * active_weekdays_count inconsistent with monday_active..friday_active
            * rows with no active day at all in the weekly profile
        - The "no active day at all" case is treated as a warning-level
          profile weakness rather than a hard structural error.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    expected_active_weekdays_count_expr = (
        F.when(F.col("monday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("tuesday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("wednesday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("thursday_active") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("friday_active") == True, F.lit(1)).otherwise(F.lit(0))
    )

    has_any_active_day_expr = (
        F.coalesce(F.col("monday_active"), F.lit(False))
        | F.coalesce(F.col("tuesday_active"), F.lit(False))
        | F.coalesce(F.col("wednesday_active"), F.lit(False))
        | F.coalesce(F.col("thursday_active"), F.lit(False))
        | F.coalesce(F.col("friday_active"), F.lit(False))
        | F.coalesce(F.col("saturday_active"), F.lit(False))
        | F.coalesce(F.col("sunday_active"), F.lit(False))
    )

    enriched_df = (
        source_df.withColumn(
            "expected_active_weekdays_count",
            expected_active_weekdays_count_expr,
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("active_weekdays_count").isNull(),
                F.lit("ACTIVE_WEEKDAYS_COUNT_IS_NULL"),
            )
            .when(
                (F.col("active_weekdays_count") < F.lit(0))
                | (F.col("active_weekdays_count") > F.lit(5)),
                F.lit("ACTIVE_WEEKDAYS_COUNT_OUTSIDE_APPROVED_DOMAIN"),
            )
            .when(
                F.col("active_weekdays_count")
                != F.col("expected_active_weekdays_count"),
                F.lit(
                    "ACTIVE_WEEKDAYS_COUNT_DOES_NOT_RECONCILE_WITH_WEEKDAY_FLAGS"
                ),
            )
            .when(
                ~has_any_active_day_expr,
                F.lit("NO_ACTIVE_DAY_IN_WEEKLY_PROFILE"),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "audit_severity",
            F.when(
                F.col("audit_reason") == F.lit("NO_ACTIVE_DAY_IN_WEEKLY_PROFILE"),
                F.lit("WARNING"),
            ).otherwise(F.lit("ERROR")),
        )
    )

    audited_df = (
        enriched_df.filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("RECONCILIATION"))
        .withColumn("audit_rule", F.lit("INVALID_WEEKLY_PROFILE"))
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
            "active_weekdays_count",
            "expected_active_weekdays_count",
            "start_date",
            "end_date",
            "service_duration_days",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
