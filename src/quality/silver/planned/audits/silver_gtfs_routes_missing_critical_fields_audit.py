from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_routes"
TABLE_NAME = "silver.silver_gtfs_routes_missing_critical_fields_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for missing critical fields in "
        "silver.silver_gtfs_routes. Identifies records where required "
        "identity, classification, or lineage attributes are null or blank."
    ),
)
def silver_gtfs_routes_missing_critical_fields_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_routes that contain missing
    critical fields required for downstream analytical usage.

    Purpose:
        Identify routes with missing identity, classification, or lineage
        attributes that can compromise planned reference, route grouping,
        and downstream joins.

    Important:
        - Some descriptive fields such as route_short_name and route_long_name
          may be optional in practice, depending on feed characteristics.
        - This audit focuses on fields considered critical in this project for
          route identification, type classification, and lineage tracking.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(F.col("route_id").isNull(), F.lit("ROUTE_ID_IS_NULL"))
            .when(F.trim(F.col("route_id")) == "", F.lit("ROUTE_ID_IS_BLANK"))
            .when(F.col("route_type").isNull(), F.lit("ROUTE_TYPE_IS_NULL"))
            .when(
                F.col("route_type_description").isNull(),
                F.lit("ROUTE_TYPE_DESCRIPTION_IS_NULL"),
            )
            .when(
                F.trim(F.col("route_type_description")) == "",
                F.lit("ROUTE_TYPE_DESCRIPTION_IS_BLANK"),
            )
            .when(F.col("gtfs_load_date").isNull(), F.lit("GTFS_LOAD_DATE_IS_NULL"))
            .when(F.col("gtfs_loaded_at").isNull(), F.lit("GTFS_LOADED_AT_IS_NULL"))
            .when(F.col("record_hash").isNull(), F.lit("RECORD_HASH_IS_NULL"))
            .when(F.trim(F.col("record_hash")) == "", F.lit("RECORD_HASH_IS_BLANK"))
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("STRUCTURAL"))
        .withColumn("audit_rule", F.lit("MISSING_CRITICAL_FIELDS"))
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_name_display",
            "route_type",
            "route_type_description",
            "route_color",
            "route_color_name",
            "route_text_color",
            "route_text_color_name",
            "gtfs_load_date",
            "gtfs_loaded_at",
            "record_hash",
            "audited_at",
        )
    )

    return audited_df

