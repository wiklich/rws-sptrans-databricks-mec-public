from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_partial_distance_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Shape-level audit table for partially populated shape_dist_traveled "
        "profiles in silver.silver_gtfs_shapes. Identifies shape_id values "
        "where some rows have shape_dist_traveled populated and others do not."
    ),
)
def silver_gtfs_shapes_partial_distance_profile_audit() -> DataFrame:
    """
    Audit shape_id groups from silver.silver_gtfs_shapes that have partially
    populated shape_dist_traveled profiles.

    Purpose:
        Identify shapes where shape_dist_traveled is inconsistently populated
        across points within the same shape_id.

    Important:
        - shape_dist_traveled is optional in GTFS.
        - This audit does not treat missing values alone as invalid.
        - It flags only mixed-profile cases where the same shape_id contains
          both populated and null shape_dist_traveled values.
        - This is useful for downstream consumers that expect consistent
          distance profiling across all points of a shape.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    shape_profile_df = (
        source_df.groupBy("shape_id")
        .agg(
            F.count(F.lit(1)).alias("shape_point_count"),
            F.sum(
                F.when(
                    F.col("shape_dist_traveled").isNotNull(),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("non_null_distance_count"),
            F.sum(
                F.when(
                    F.col("shape_dist_traveled").isNull(),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("null_distance_count"),
            F.min("gtfs_load_date").alias("gtfs_load_date"),
            F.max("gtfs_loaded_at").alias("gtfs_loaded_at"),
        )
        .filter(
            (F.col("non_null_distance_count") > 0)
            & (F.col("null_distance_count") > 0)
        )
    )

    audited_df = (
        shape_profile_df.withColumn("audit_category", F.lit("PROFILE"))
        .withColumn(
            "audit_rule",
            F.lit("PARTIAL_SHAPE_DIST_TRAVELED_PROFILE"),
        )
        .withColumn("audit_severity", F.lit("WARNING"))
        .withColumn(
            "audit_reason",
            F.lit(
                "SHAPE_CONTAINS_MIXED_NULL_AND_NON_NULL_SHAPE_DIST_TRAVELED_VALUES"
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
            "shape_point_count",
            "non_null_distance_count",
            "null_distance_count",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "audited_at",
        )
    )

    return audited_df
