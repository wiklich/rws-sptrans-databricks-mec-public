from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SOURCE_TABLE = "main.silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_sequence_gap_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Profiling audit table for sparse shape_pt_sequence progression in "
        "GTFS shapes. This audit highlights non-consecutive jumps in "
        "shape_pt_sequence within the same shape_id. "
        "Important: non-consecutive sequence values are not necessarily "
        "invalid under GTFS; this table is intended for profiling and review, "
        "not for strict validation failure."
    ),
)
def silver_gtfs_shapes_sequence_gap_audit():
    """
    Create a profiling audit table for sparse shape_pt_sequence progression
    in GTFS shapes.

    Purpose:
        Identify cases where the next shape_pt_sequence is greater than the
        current sequence plus one, which may indicate sparse sequencing.
        Under GTFS, shape_pt_sequence must increase, but it does not need
        to be consecutive.

    Important:
        - This is a profiling / warning-oriented audit.
        - It should not be interpreted as a definitive GTFS validity failure.
        - Decreasing, repeated, or non-monotonic sequences should be handled
          by a dedicated structural audit such as
          silver_gtfs_shapes_non_monotonic_sequence_audit.

    Returns:
        pyspark.sql.DataFrame:
            Rows where the next shape_pt_sequence is greater than the current
            shape_pt_sequence plus one for the same shape_id.
    """
    df = spark.read.table(SOURCE_TABLE)

    window_spec = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")

    audit_df = (
        df.withColumn(
            "next_shape_pt_sequence",
            F.lead("shape_pt_sequence").over(window_spec),
        )
        .withColumn(
            "expected_next_shape_pt_sequence",
            F.col("shape_pt_sequence") + F.lit(1),
        )
        .withColumn(
            "sequence_jump_size",
            F.when(
                F.col("next_shape_pt_sequence").isNotNull(),
                F.col("next_shape_pt_sequence") - F.col("shape_pt_sequence"),
            ),
        )
        .withColumn(
            "has_sparse_sequence_pattern",
            F.when(
                F.col("next_shape_pt_sequence").isNotNull()
                & (F.col("next_shape_pt_sequence") > F.col("expected_next_shape_pt_sequence")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .filter(F.col("has_sparse_sequence_pattern") == True)
        .withColumn("audit_category", F.lit("PROFILE"))
        .withColumn("audit_rule", F.lit("SPARSE_SEQUENCE_PATTERN"))
        .withColumn("audit_severity", F.lit("WARNING"))
        .withColumn(
            "audit_reason",
            F.lit(
                "NEXT_SHAPE_PT_SEQUENCE_IS_GREATER_THAN_CURRENT_PLUS_ONE"
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
            "shape_id",
            "shape_pt_sequence",
            "next_shape_pt_sequence",
            "expected_next_shape_pt_sequence",
            "sequence_jump_size",
            "shape_pt_lat",
            "shape_pt_lon",
            "shape_dist_traveled",
            "has_sparse_sequence_pattern",
            "audited_at",
        )
    )

    return audit_df