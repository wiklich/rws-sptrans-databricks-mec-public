from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_agency"
TABLE_NAME = "silver.silver_gtfs_agency_invalid_timezone_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent agency_timezone "
        "values in silver.silver_gtfs_agency."
    ),
)
def silver_gtfs_agency_invalid_timezone_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_agency where agency_timezone is invalid,
    missing, blank, or inconsistent across the feed.

    Purpose:
        Identify agency records with malformed timezone values or multiple
        timezone assignments that can compromise downstream metadata quality
        and GTFS semantic consistency.

    Important:
        - GTFS requires agency_timezone.
        - GTFS also expects all agencies in the same dataset to share the same
          agency_timezone.
        - This audit applies a pragmatic timezone-profile validation rule based on:
            * null / blank checks
            * simple IANA-like timezone pattern check
            * single-timezone consistency across the feed
        - More advanced timezone validation can be added later if needed.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    valid_timezone_expr = (
        F.col("agency_timezone").isNotNull()
        & (F.trim(F.col("agency_timezone")) != "")
        & F.col("agency_timezone").rlike(
            r"^[A-Za-z]+(?:[._+-][A-Za-z0-9]+)*(?:/[A-Za-z0-9]+(?:[._+-][A-Za-z0-9]+)*)+$"
        )
    )

    distinct_timezone_df = (
        source_df.filter(
            F.col("agency_timezone").isNotNull()
            & (F.trim(F.col("agency_timezone")) != "")
        )
        .select("agency_timezone")
        .distinct()
    )

    distinct_timezone_count = distinct_timezone_df.count()

    primary_timezone = (
        distinct_timezone_df.orderBy("agency_timezone").limit(1).collect()[0]["agency_timezone"]
        if distinct_timezone_count > 0
        else None
    )

    audited_df = (
        source_df.withColumn(
            "expected_primary_timezone",
            F.lit(primary_timezone),
        )
        .withColumn(
            "distinct_timezone_count",
            F.lit(distinct_timezone_count),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("agency_timezone").isNull(),
                F.lit("AGENCY_TIMEZONE_IS_NULL"),
            )
            .when(
                F.trim(F.col("agency_timezone")) == "",
                F.lit("AGENCY_TIMEZONE_IS_BLANK"),
            )
            .when(
                ~valid_timezone_expr,
                F.lit("AGENCY_TIMEZONE_INVALID_IANA_LIKE_FORMAT"),
            )
            .when(
                F.lit(distinct_timezone_count) > F.lit(1),
                F.lit("AGENCY_TIMEZONE_INCONSISTENT_ACROSS_FEED"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("BUSINESS_RULE"))
        .withColumn("audit_rule", F.lit("INVALID_TIMEZONE_PROFILE"))
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "expected_primary_timezone",
            "distinct_timezone_count",
            "agency_lang",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
