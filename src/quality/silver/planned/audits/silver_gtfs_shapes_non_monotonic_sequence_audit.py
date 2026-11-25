from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_non_monotonic_sequence_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for non-monotonic shape_pt_sequence behavior in "
        "silver.silver_gtfs_shapes. Identifies cases where shape_pt_sequence "
        "does not remain strictly increasing within the same shape_id."
    ),
)
def silver_gtfs_shapes_non_monotonic_sequence_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_shapes where shape_pt_sequence is not
    strictly increasing within the same shape_id.

    Purpose:
        Identify sequence behavior that can compromise ordered shape
        reconstruction downstream.

    Important:
        - This audit is stricter than sparse-sequence profiling.
        - Sparse jumps such as 10 -> 20 are allowed and should be handled by
          a profiling audit, not by this audit.
        - This audit focuses on cases where the ordered sequence is not
          strictly increasing, such as repeated or non-progressive values.
        - Some overlap with duplicate-grain auditing is expected, but this
          table provides ordered-sequence context.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    sequence_window = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")

    audited_df = (
        source_df.withColumn(
            "previous_shape_pt_sequence",
            F.lag("shape_pt_sequence").over(sequence_window),
        )
        .withColumn(
            "sequence_difference_from_previous",
            F.when(
                F.col("previous_shape_pt_sequence").isNotNull(),
                F.col("shape_pt_sequence") - F.col("previous_shape_pt_sequence"),
            ),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("previous_shape_pt_sequence").isNotNull()
                & (F.col("shape_pt_sequence") == F.col("previous_shape_pt_sequence")),
                F.lit("SHAPE_PT_SEQUENCE_REPEATED_WITHIN_SHAPE"),
            ).when(
                F.col("previous_shape_pt_sequence").isNotNull()
                & (F.col("shape_pt_sequence") < F.col("previous_shape_pt_sequence")),
                F.lit("SHAPE_PT_SEQUENCE_DECREASES_WITHIN_SHAPE"),
            ).otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("BUSINESS_RULE"))
        .withColumn("audit_rule", F.lit("NON_MONOTONIC_SHAPE_PT_SEQUENCE"))
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "shape_id",
            "previous_shape_pt_sequence",
            "shape_pt_sequence",
            "sequence_difference_from_previous",
            "shape_pt_lat",
            "shape_pt_lon",
            "shape_dist_traveled",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
