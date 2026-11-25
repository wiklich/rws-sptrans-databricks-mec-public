from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_invalid_coordinates_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or missing coordinates in "
        "silver.silver_gtfs_shapes."
    ),
)
def silver_gtfs_shapes_invalid_coordinates_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_shapes that contain invalid,
    missing, or globally out-of-domain coordinates.

    Purpose:
        Identify shape points whose latitude or longitude is null or falls
        outside the valid global geographic domain.

    Important:
        - This audit checks global geographic validity only.
        - It does not evaluate service-area or municipal bounding-box rules.
        - It is intended to protect downstream geometry construction and
          spatial analysis from malformed shape points.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("shape_pt_lat").isNull(), F.lit("SHAPE_PT_LAT_IS_NULL"))
            .when(F.col("shape_pt_lon").isNull(), F.lit("SHAPE_PT_LON_IS_NULL"))
            .when(
                (F.col("shape_pt_lat") < F.lit(-90.0))
                | (F.col("shape_pt_lat") > F.lit(90.0)),
                F.lit("SHAPE_PT_LAT_OUTSIDE_GLOBAL_DOMAIN"),
            )
            .when(
                (F.col("shape_pt_lon") < F.lit(-180.0))
                | (F.col("shape_pt_lon") > F.lit(180.0)),
                F.lit("SHAPE_PT_LON_OUTSIDE_GLOBAL_DOMAIN"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("INVALID_OR_MISSING_COORDINATES"))
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
