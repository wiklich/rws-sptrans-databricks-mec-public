from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_routes"
TABLE_NAME = "silver.silver_gtfs_routes_duplicate_grain_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for duplicate records at the declared "
        "route_id grain in silver.silver_gtfs_routes."
    ),
)
def silver_gtfs_routes_duplicate_grain_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_routes that violate the declared
    natural grain of the table.

    Declared grain:
        - One row per route_id

    Purpose:
        Identify cases where the same route_id appears more than once,
        creating ambiguity for downstream route reference, trip linkage,
        and planned analytical consumption.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy("route_id")
        .agg(F.count(F.lit(1)).alias("duplicate_row_count"))
        .filter(F.col("duplicate_row_count") > 1)
    )

    audited_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["route_id"],
            how="inner",
        )
        .withColumn("audit_category", F.lit("GRAIN"))
        .withColumn(
            "audit_rule",
            F.lit("DUPLICATE_ROWS_AT_ROUTE_ID_GRAIN"),
        )
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn(
            "audit_reason",
            F.lit("MULTIPLE_ROWS_FOUND_FOR_SAME_ROUTE_ID"),
        )
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_name_display",
            "route_type",
            "route_type_description",
            "route_color",
            "route_color_name",
            "route_text_color",
            "route_text_color_name",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "duplicate_row_count",
            "audited_at",
        )
    )

    return audited_df
