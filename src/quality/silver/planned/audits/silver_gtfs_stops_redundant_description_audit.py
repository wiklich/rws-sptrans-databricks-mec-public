from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_stops"
TABLE_NAME = "silver.silver_gtfs_stops_redundant_description_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for redundant stop_desc values in "
        "silver.silver_gtfs_stops. Identifies records where stop_desc "
        "duplicates stop_name after basic normalization."
    ),
)
def silver_gtfs_stops_redundant_description_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stops where stop_desc redundantly
    duplicates stop_name.

    Purpose:
        Identify semantically redundant stop descriptions that do not add
        useful rider-facing information.

    Important:
        - This audit follows the semantic guidance documented in the schema:
          stop_desc should not duplicate stop_name.
        - The comparison is case-insensitive and trim-normalized.
        - This is treated as a quality warning rather than a structural error.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.filter(
            F.col("stop_desc").isNotNull()
            & (F.trim(F.col("stop_desc")) != "")
            & F.col("stop_name").isNotNull()
            & (F.trim(F.col("stop_name")) != "")
            & (
                F.lower(F.trim(F.col("stop_desc")))
                == F.lower(F.trim(F.col("stop_name")))
            )
        )
        .withColumn("audit_category", F.lit("PROFILE"))
        .withColumn("audit_rule", F.lit("REDUNDANT_STOP_DESCRIPTION"))
        .withColumn("audit_severity", F.lit("WARNING"))
        .withColumn(
            "audit_reason",
            F.lit("STOP_DESC_DUPLICATES_STOP_NAME_AFTER_NORMALIZATION"),
        )
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "stop_id",
            "stop_name",
            "stop_desc",
            "stop_lat",
            "stop_lon",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
