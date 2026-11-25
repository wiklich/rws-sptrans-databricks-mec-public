from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_stop_times"
TABLE_NAME = "silver.silver_gtfs_stop_times_negative_seconds_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for negative arrival_time_seconds or "
        "departure_time_seconds values in silver.silver_gtfs_stop_times."
    ),
)
def silver_gtfs_stop_times_negative_seconds_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stop_times where arrival_time_seconds
    or departure_time_seconds is negative.

    Purpose:
        Identify invalid seconds-based GTFS time representations that violate
        the expected non-negative semantics of arrival_time_seconds and
        departure_time_seconds.

    Important:
        - GTFS allows times greater than 24:00:00, so high values are valid.
        - This audit only flags negative values.
        - Null values are not treated as audit failures here.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(
                F.col("arrival_time_seconds").isNotNull()
                & (F.col("arrival_time_seconds") < F.lit(0)),
                F.lit("ARRIVAL_TIME_SECONDS_IS_NEGATIVE"),
            ).when(
                F.col("departure_time_seconds").isNotNull()
                & (F.col("departure_time_seconds") < F.lit(0)),
                F.lit("DEPARTURE_TIME_SECONDS_IS_NEGATIVE"),
            ).otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("NEGATIVE_TIME_SECONDS"))
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
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
            "arrival_time_seconds",
            "departure_time_seconds",
            "is_valid_time_order",
            "gtfs_load_date",
            "audited_at",
        )
    )

    return audited_df
