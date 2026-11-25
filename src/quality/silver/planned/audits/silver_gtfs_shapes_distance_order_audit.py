"""
Silver GTFS shapes distance order audit pipeline.

Purpose:
    Identify shapes with invalid shape_dist_traveled progression.

Audit rule:
    For each shape_id, shape_dist_traveled must not decrease when compared
    to the next shape_pt_sequence.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(
    name="silver.silver_gtfs_shapes_distance_order_audit",
    comment="Audit table with GTFS shapes records where shape_dist_traveled does not increase with shape_pt_sequence.",
)
def silver_gtfs_shapes_distance_order_audit():
    """
    Create an audit table for shape_dist_traveled order inconsistencies in GTFS shapes.

    Returns:
        pyspark.sql.DataFrame:
            Audit records where the next shape_dist_traveled is lower than the
            current value for the same shape_id.
    """
    df = spark.read.table("main.silver.silver_gtfs_shapes")

    window_spec = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")

    audit_df = (
        df
        .withColumn("next_shape_pt_sequence", F.lead("shape_pt_sequence").over(window_spec))
        .withColumn("next_shape_dist_traveled", F.lead("shape_dist_traveled").over(window_spec))
        .withColumn(
            "has_invalid_distance_order",
            F.when(
                F.col("next_shape_dist_traveled").isNotNull()
                & F.col("shape_dist_traveled").isNotNull()
                & (F.col("shape_dist_traveled") > F.col("next_shape_dist_traveled")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .filter(F.col("has_invalid_distance_order") == True)
        .select(
            "shape_id",
            "shape_pt_sequence",
            "next_shape_pt_sequence",
            "shape_pt_lat",
            "shape_pt_lon",
            "shape_dist_traveled",
            "next_shape_dist_traveled",
            "has_invalid_distance_order",
        )
    )

    return audit_df