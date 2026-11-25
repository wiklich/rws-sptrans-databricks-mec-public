from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.silver_gtfs_stop_times"
TABLE_NAME = "silver.silver_gtfs_stop_times_non_monotonic_sequence_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for non-monotonic stop_sequence behavior in "
        "silver.silver_gtfs_stop_times. Identifies cases where stop_sequence "
        "does not remain strictly increasing within the same trip_id."
    ),
)
def silver_gtfs_stop_times_non_monotonic_sequence_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_stop_times where stop_sequence is not
    strictly increasing within the same trip_id.

    Purpose:
        Identify sequence behavior that can compromise downstream trip-stop
        reconstruction and stop ordering logic.

    Important:
        - This audit is stricter than sparse-sequence profiling.
        - Sparse jumps such as 10 -> 20 are allowed and should be handled by
          a profiling audit, not by this audit.
        - This audit focuses on repeated or decreasing stop_sequence values
          within the same trip_id.
        - Some overlap with duplicate-grain auditing is expected, but this
          table provides ordered-sequence context.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    sequence_window = Window.partitionBy("trip_id").orderBy("stop_sequence")

    audited_df = (
        source_df.withColumn(
            "previous_stop_sequence",
            F.lag("stop_sequence").over(sequence_window),
        )
        .withColumn(
            "sequence_difference_from_previous",
            F.when(
                F.col("previous_stop_sequence").isNotNull(),
                F.col("stop_sequence") - F.col("previous_stop_sequence"),
            ),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("previous_stop_sequence").isNotNull()
                & (F.col("stop_sequence") == F.col("previous_stop_sequence")),
                F.lit("STOP_SEQUENCE_REPEATED_WITHIN_TRIP"),
            ).when(
                F.col("previous_stop_sequence").isNotNull()
                & (F.col("stop_sequence") < F.col("previous_stop_sequence")),
                F.lit("STOP_SEQUENCE_DECREASES_WITHIN_TRIP"),
            ).otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("BUSINESS_RULE"))
        .withColumn("audit_rule", F.lit("NON_MONOTONIC_STOP_SEQUENCE"))
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
            "previous_stop_sequence",
            "stop_sequence",
            "sequence_difference_from_previous",
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
