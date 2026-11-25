from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_stop_times"
TABLE_NAME = "silver.silver_gtfs_stop_times_duplicate_grain_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for duplicate records at the declared "
        "(trip_id, stop_sequence) grain in silver.silver_gtfs_stop_times."
    ),
)
def silver_gtfs_stop_times_duplicate_grain_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stop_times that violate the declared
    natural grain of the table.

    Declared grain:
        - One row per (trip_id, stop_sequence)

    Purpose:
        Identify cases where the same trip_id and stop_sequence appear
        more than once, which creates ambiguity for downstream stop order,
        timing interpretation, and trip reconstruction.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy("trip_id", "stop_sequence")
        .agg(F.count(F.lit(1)).alias("duplicate_row_count"))
        .filter(F.col("duplicate_row_count") > 1)
    )

    audited_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["trip_id", "stop_sequence"],
            how="inner",
        )
        .withColumn("audit_category", F.lit("GRAIN"))
        .withColumn(
            "audit_rule",
            F.lit("DUPLICATE_ROWS_AT_TRIP_ID_AND_STOP_SEQUENCE_GRAIN"),
        )
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn(
            "audit_reason",
            F.lit("MULTIPLE_ROWS_FOUND_FOR_SAME_TRIP_ID_AND_STOP_SEQUENCE"),
        )
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
            "arrival_time_seconds",
            "departure_time_seconds",
            "is_valid_time_order",
            "gtfs_load_date",
            "duplicate_row_count",
            "audited_at",
        )
    )

    return audited_df
