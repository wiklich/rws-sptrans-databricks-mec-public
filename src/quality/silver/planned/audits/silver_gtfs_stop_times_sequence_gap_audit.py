from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SOURCE_TABLE = "main.silver.silver_gtfs_stop_times"
TABLE_NAME = "silver.silver_gtfs_stop_times_sequence_gap_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Profiling audit table for sparse stop_sequence progression in "
        "GTFS stop_times. This audit highlights non-consecutive jumps in "
        "stop_sequence within the same trip_id. "
        "Important: non-consecutive sequence values are not necessarily "
        "invalid under GTFS; this table is intended for profiling and review, "
        "not for strict validation failure."
    ),
)
def silver_gtfs_stop_times_sequence_gap_audit():
    """
    Create a profiling audit table for sparse stop_sequence progression
    in GTFS stop_times.

    Purpose:
        Identify cases where the next stop_sequence is greater than the
        current sequence plus one, which may indicate sparse sequencing.
        Under GTFS, stop_sequence must increase, but it does not need
        to be consecutive.

    Important:
        - This is a profiling / warning-oriented audit.
        - It should not be interpreted as a definitive GTFS validity failure.
        - Decreasing, repeated, or non-monotonic sequences should be handled
          by a dedicated structural audit such as
          silver_gtfs_stop_times_non_monotonic_sequence_audit.

    Returns:
        pyspark.sql.DataFrame:
            Rows where the next stop_sequence is greater than the current
            stop_sequence plus one for the same trip_id.
    """
    df = spark.read.table(SOURCE_TABLE)

    window_spec = Window.partitionBy("trip_id").orderBy("stop_sequence")

    audit_df = (
        df.withColumn(
            "next_stop_id",
            F.lead("stop_id").over(window_spec),
        )
        .withColumn(
            "next_stop_sequence",
            F.lead("stop_sequence").over(window_spec),
        )
        .withColumn(
            "expected_next_stop_sequence",
            F.col("stop_sequence") + F.lit(1),
        )
        .withColumn(
            "sequence_jump_size",
            F.when(
                F.col("next_stop_sequence").isNotNull(),
                F.col("next_stop_sequence") - F.col("stop_sequence"),
            ),
        )
        .withColumn(
            "has_sparse_sequence_pattern",
            F.when(
                F.col("next_stop_sequence").isNotNull()
                & (F.col("next_stop_sequence") > F.col("expected_next_stop_sequence")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .filter(F.col("has_sparse_sequence_pattern") == True)
        .withColumn("audit_category", F.lit("PROFILE"))
        .withColumn("audit_rule", F.lit("SPARSE_STOP_SEQUENCE_PATTERN"))
        .withColumn("audit_severity", F.lit("WARNING"))
        .withColumn(
            "audit_reason",
            F.lit(
                "NEXT_STOP_SEQUENCE_IS_GREATER_THAN_CURRENT_PLUS_ONE"
            ),
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
            "next_stop_id",
            "stop_sequence",
            "next_stop_sequence",
            "expected_next_stop_sequence",
            "sequence_jump_size",
            "arrival_time",
            "departure_time",
            "arrival_time_seconds",
            "departure_time_seconds",
            "is_valid_time_order",
            "audited_at",
        )
    )

    return audit_df
