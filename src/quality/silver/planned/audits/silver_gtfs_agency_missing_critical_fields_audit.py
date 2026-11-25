from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_agency"
TABLE_NAME = "silver.silver_gtfs_agency_missing_critical_fields_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for missing critical fields in "
        "silver.silver_gtfs_agency. Identifies records where required "
        "identity, metadata, or lineage attributes are null or blank."
    ),
)
def silver_gtfs_agency_missing_critical_fields_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_agency that contain missing
    critical fields required for downstream analytical usage.

    Purpose:
        Identify agency records with missing required business or lineage
        attributes that can compromise agency reference, route linkage,
        and downstream planned analytical consumption.

    Important:
        - agency_id is intentionally not treated as a hard critical field here,
          because it may be optional in single-agency GTFS feeds.
        - agency_lang is also not treated as critical because it is optional
          in GTFS and more appropriate for profile-quality checks.
        - This audit focuses on the minimum set of attributes required for
          stable analytical consumption in this project.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("agency_name").isNull(), F.lit("AGENCY_NAME_IS_NULL"))
            .when(F.trim(F.col("agency_name")) == "", F.lit("AGENCY_NAME_IS_BLANK"))
            .when(F.col("agency_url").isNull(), F.lit("AGENCY_URL_IS_NULL"))
            .when(F.trim(F.col("agency_url")) == "", F.lit("AGENCY_URL_IS_BLANK"))
            .when(F.col("agency_timezone").isNull(), F.lit("AGENCY_TIMEZONE_IS_NULL"))
            .when(
                F.trim(F.col("agency_timezone")) == "",
                F.lit("AGENCY_TIMEZONE_IS_BLANK"),
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
