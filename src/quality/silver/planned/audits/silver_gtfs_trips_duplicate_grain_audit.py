from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_trips"
TABLE_NAME = "silver.silver_gtfs_trips_duplicate_grain_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for duplicate records at the declared "
        "trip_id grain in silver.silver_gtfs_trips."
    ),
)
def silver_gtfs_trips_duplicate_grain_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_trips that violate the declared
    natural grain of the table.

    Declared grain:
        - One row per trip_id

    Purpose:
        Identify cases where the same trip_id appears more than once,
        creating ambiguity for downstream trip reference, stop_times linkage,
        route/service linkage, and planned analytical usage.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy("trip_id")
        .agg(F.count(F.lit(1)).alias("duplicate_row_count"))
        .filter(F.col("duplicate_row_count") > 1)
    )

    audited_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["trip_id"],
            how="inner",
        )
        .withColumn("audit_category", F.lit("GRAIN"))
        .withColumn(
            "audit_rule",
            F.lit("DUPLICATE_ROWS_AT_TRIP_ID_GRAIN"),
        )
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn(
            "audit_reason",
            F.lit("MULTIPLE_ROWS_FOUND_FOR_SAME_TRIP_ID"),
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
            "service_id",
            "trip_id",
            "trip_headsign",
            "trip_short_name",
            "direction_id",
            "direction_description",
            "block_id",
            "shape_id",
            "wheelchair_accessible",
            "bikes_allowed",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "duplicate_row_count",
            "audited_at",
        )
    )

    return audited_df
