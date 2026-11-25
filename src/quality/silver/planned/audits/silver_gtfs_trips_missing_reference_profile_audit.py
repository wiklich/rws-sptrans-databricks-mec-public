from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_trips"
TABLE_NAME = "silver.silver_gtfs_trips_missing_reference_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for weak or missing reference profile "
        "attributes in silver.silver_gtfs_trips."
    ),
)
def silver_gtfs_trips_missing_reference_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_trips that contain weak or incomplete
    reference-profile attributes.

    Purpose:
        Identify trips whose optional but important reference attributes are
        missing or blank, reducing analytical richness for downstream planned,
        specialized, and planned-vs-observed use cases.

    Important:
        - This audit is intentionally not focused on hard blockers such as
          route_id, service_id, trip_id, or lineage fields. Those belong to
          the missing-critical-fields audit.
        - This audit focuses on attributes that enrich trip interpretability
          and linkage quality, such as shape_id, headsign, short name, and
          block assignment.
        - Findings here should generally be interpreted as profile-quality
          weaknesses rather than strict structural failures.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("shape_id").isNull(), F.lit("SHAPE_ID_IS_NULL"))
            .when(F.trim(F.col("shape_id")) == "", F.lit("SHAPE_ID_IS_BLANK"))
            .when(F.col("trip_headsign").isNull(), F.lit("TRIP_HEADSIGN_IS_NULL"))
            .when(
                F.trim(F.col("trip_headsign")) == "",
                F.lit("TRIP_HEADSIGN_IS_BLANK"),
            )
            .when(F.col("trip_short_name").isNull(), F.lit("TRIP_SHORT_NAME_IS_NULL"))
            .when(
                F.trim(F.col("trip_short_name")) == "",
                F.lit("TRIP_SHORT_NAME_IS_BLANK"),
            )
            .when(F.col("block_id").isNull(), F.lit("BLOCK_ID_IS_NULL"))
            .when(F.trim(F.col("block_id")) == "", F.lit("BLOCK_ID_IS_BLANK"))
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("PROFILE"))
        .withColumn("audit_rule", F.lit("MISSING_REFERENCE_PROFILE"))
        .withColumn("audit_severity", F.lit("WARNING"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "route_id",
            "service_id",
            "trip_id",
            "trip_headsign",
            "trip_short_name",
            "direction_id",
            "direction_description",
            "block_id",
            "shape_id",
            "wheelchair_accessible",
            "bikes_allowed",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df
