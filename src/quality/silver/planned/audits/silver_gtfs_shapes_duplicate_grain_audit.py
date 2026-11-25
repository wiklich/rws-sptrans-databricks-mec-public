from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_shapes"
TABLE_NAME = "silver.silver_gtfs_shapes_duplicate_grain_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for duplicate records at the declared "
        "(shape_id, shape_pt_sequence) grain in silver.silver_gtfs_shapes."
    ),
)
def silver_gtfs_shapes_duplicate_grain_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_shapes that violate the declared
    natural grain of the table.

    Declared grain:
        - One row per (shape_id, shape_pt_sequence)

    Purpose:
        Identify cases where the same shape_id and shape_pt_sequence appear
        more than once, which creates ambiguity for downstream geometry
        construction and ordered shape interpretation.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    duplicate_keys_df = (
        source_df.groupBy("shape_id", "shape_pt_sequence")
        .agg(F.count(F.lit(1)).alias("duplicate_row_count"))
        .filter(F.col("duplicate_row_count") > 1)
    )

    audited_df = (
        source_df.alias("src")
        .join(
            duplicate_keys_df.alias("dup"),
            on=["shape_id", "shape_pt_sequence"],
            how="inner",
        )
        .withColumn("audit_category", F.lit("GRAIN"))
        .withColumn(
            "audit_rule",
            F.lit("DUPLICATE_ROWS_AT_SHAPE_ID_AND_SEQUENCE_GRAIN"),
        )
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn(
            "audit_reason",
            F.lit("MULTIPLE_ROWS_FOUND_FOR_SAME_SHAPE_ID_AND_SHAPE_PT_SEQUENCE"),
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
            "duplicate_row_count",
            "audited_at",
        )
    )

    return audited_df
