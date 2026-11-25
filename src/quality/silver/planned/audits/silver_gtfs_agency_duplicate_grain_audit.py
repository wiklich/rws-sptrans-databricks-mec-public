from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_agency"
TABLE_NAME = "silver.silver_gtfs_agency_duplicate_grain_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for duplicate records at the declared "
        "agency_id grain in silver.silver_gtfs_agency."
    ),
)
def silver_gtfs_agency_duplicate_grain_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_agency that violate the declared
    natural grain of the table.

    Declared grain:
        - One row per agency_id when agency_id is present

    Purpose:
        Identify cases where the same agency_id appears more than once,
        creating ambiguity for downstream agency reference, route linkage,
        and planned analytical consumption.

    Important:
        - agency_id may be optional in single-agency GTFS feeds.
        - This audit only evaluates rows where agency_id is populated.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.filter(F.col("agency_id").isNotNull())
        .groupBy("agency_id")
        .agg(F.count(F.lit(1)).alias("duplicate_row_count"))
        .filter(F.col("duplicate_row_count") > 1)
    )

    audited_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["agency_id"],
            how="inner",
        )
        .withColumn("audit_category", F.lit("GRAIN"))
        .withColumn(
            "audit_rule",
            F.lit("DUPLICATE_ROWS_AT_AGENCY_ID_GRAIN"),
        )
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn(
            "audit_reason",
            F.lit("MULTIPLE_ROWS_FOUND_FOR_SAME_AGENCY_ID"),
        )
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
            "duplicate_row_count",
            "audited_at",
        )
    )

    return audited_df
