from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_agency"
TABLE_NAME = "silver.silver_gtfs_agency_invalid_url_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent agency_url values "
        "in silver.silver_gtfs_agency."
    ),
)
def silver_gtfs_agency_invalid_url_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_agency where agency_url is invalid,
    missing, blank, or not compliant with a basic HTTP/HTTPS URL profile.

    Purpose:
        Identify agency records with malformed URL values that can compromise
        downstream metadata quality and consumer-facing reference usage.

    Important:
        - GTFS requires agency_url.
        - This audit applies a pragmatic validation rule based on an
          HTTP/HTTPS URL pattern.
        - More advanced URL validation can be added later if needed.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("agency_url").isNull(), F.lit("AGENCY_URL_IS_NULL"))
            .when(F.trim(F.col("agency_url")) == "", F.lit("AGENCY_URL_IS_BLANK"))
            .when(
                ~F.col("agency_url").rlike(r"^https?://.+"),
                F.lit("AGENCY_URL_INVALID_HTTP_OR_HTTPS_FORMAT"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("INVALID_URL_PROFILE"))
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
            "agency_lang",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
