from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_negative_distance_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for negative shape_dist_traveled values in "
        "silver.silver_gtfs_shapes."
    ),
)
def silver_gtfs_shapes_negative_distance_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_shapes where shape_dist_traveled is negative.

    Purpose:
        Identify records with invalid traveled-distance values that violate the
        expected non-negative semantics of shape_dist_traveled.

    Important:
        - shape_dist_traveled is optional in GTFS.
        - This audit only flags rows where the field is present and negative.
        - Null values are not treated as audit failures here.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.filter(
            F.col("shape_dist_traveled").isNotNull()
            & (F.col("shape_dist_traveled") < F.lit(0.0))
        )
        .withColumn("audit_category", F.lit("BUSINESS_RULE"))
        .withColumn("audit_rule", F.lit("NEGATIVE_SHAPE_DIST_TRAVELED"))
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn(
            "audit_reason",
            F.lit("SHAPE_DIST_TRAVELED_IS_NEGATIVE"),
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
