from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_stop_times"
TABLE_NAME = "silver.silver_gtfs_stop_times_invalid_time_order_flag_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for inconsistencies between is_valid_time_order "
        "and the seconds-based comparison of arrival_time_seconds and "
        "departure_time_seconds in silver.silver_gtfs_stop_times."
    ),
)
def silver_gtfs_stop_times_invalid_time_order_flag_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stop_times where is_valid_time_order
    does not reconcile with arrival_time_seconds and departure_time_seconds.

    Purpose:
        Identify cases where the derived flag is_valid_time_order is inconsistent
        with the declared rule:
            arrival_time_seconds <= departure_time_seconds

    Important:
        - This audit only evaluates rows where both seconds-based fields are present.
        - Rows with missing arrival_time_seconds or departure_time_seconds are not
          evaluated here and should be handled by profile/reconciliation audits.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.filter(
            F.col("arrival_time_seconds").isNotNull()
            & F.col("departure_time_seconds").isNotNull()
        )
        .withColumn(
            "expected_is_valid_time_order",
            F.col("arrival_time_seconds") <= F.col("departure_time_seconds"),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("is_valid_time_order").isNull(),
                F.lit("IS_VALID_TIME_ORDER_IS_NULL_WHILE_SECONDS_FIELDS_ARE_PRESENT"),
            )
            .when(
                (F.col("is_valid_time_order") == F.lit(True))
                & (F.col("expected_is_valid_time_order") == F.lit(False)),
                F.lit("IS_VALID_TIME_ORDER_TRUE_BUT_SECONDS_ORDER_IS_INVALID"),
            )
            .when(
                (F.col("is_valid_time_order") == F.lit(False))
                & (F.col("expected_is_valid_time_order") == F.lit(True)),
                F.lit("IS_VALID_TIME_ORDER_FALSE_BUT_SECONDS_ORDER_IS_VALID"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("RECONCILIATION"))
        .withColumn("audit_rule", F.lit("INVALID_TIME_ORDER_FLAG"))
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
            "expected_is_valid_time_order",
            "gtfs_load_date",
            "audited_at",
        )
    )

    return audited_df
