from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_routes"
TABLE_NAME = "silver.silver_gtfs_routes_invalid_route_type_audit"

ALLOWED_ROUTE_TYPES = [0, 1, 2, 3, 4, 5, 6, 7, 11, 12]


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid route_type values in "
        "silver.silver_gtfs_routes."
    ),
)
def silver_gtfs_routes_invalid_route_type_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_routes where route_type falls outside
    the approved GTFS domain used by this project.

    Purpose:
        Identify routes with unsupported or unexpected route_type values that
        can compromise mode classification, descriptive enrichment, and
        downstream analytical consistency.

    Important:
        - This audit checks the allowed route_type domain defined for this project.
        - route_type values outside this domain are treated as audit failures.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    audited_df = (
        source_df.filter(
            F.col("route_type").isNull() | ~F.col("route_type").isin(ALLOWED_ROUTE_TYPES)
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("route_type").isNull(),
                F.lit("ROUTE_TYPE_IS_NULL"),
            ).otherwise(F.lit("ROUTE_TYPE_OUTSIDE_APPROVED_DOMAIN")),
        )
        .withColumn("audit_category", F.lit("BUSINESS_RULE"))
        .withColumn("audit_rule", F.lit("INVALID_ROUTE_TYPE"))
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
