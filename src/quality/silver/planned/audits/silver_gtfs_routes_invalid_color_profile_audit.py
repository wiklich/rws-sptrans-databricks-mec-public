from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_routes"
TABLE_NAME = "silver.silver_gtfs_routes_invalid_color_profile_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent color profile "
        "attributes in silver.silver_gtfs_routes."
    ),
)
def silver_gtfs_routes_invalid_color_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_routes where route_color and/or
    route_text_color do not follow the expected hexadecimal profile or where
    derived color names are missing when valid color values are present.

    Purpose:
        Identify invalid color formatting and incomplete color enrichment that
        can compromise visual consistency and downstream presentation logic.

    Important:
        - route_color and route_text_color are evaluated only when present.
        - This audit treats invalid hex formatting as ERROR.
        - Missing derived color names for valid hex values are also surfaced
          here as enrichment inconsistencies.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    valid_route_color_expr = (
        F.col("route_color").isNotNull()
        & F.col("route_color").rlike("^[0-9A-Fa-f]{6}$")
    )

    valid_route_text_color_expr = (
        F.col("route_text_color").isNotNull()
        & F.col("route_text_color").rlike("^[0-9A-Fa-f]{6}$")
    )

    audited_df = (
        source_df.withColumn(
            "audit_reason",
            F.when(
                F.col("route_color").isNotNull()
                & ~F.col("route_color").rlike("^[0-9A-Fa-f]{6}$"),
                F.lit("ROUTE_COLOR_INVALID_HEX_FORMAT"),
            )
            .when(
                F.col("route_text_color").isNotNull()
                & ~F.col("route_text_color").rlike("^[0-9A-Fa-f]{6}$"),
                F.lit("ROUTE_TEXT_COLOR_INVALID_HEX_FORMAT"),
            )
            .when(
                valid_route_color_expr
                & F.col("route_color_name").isNull(),
                F.lit("ROUTE_COLOR_NAME_MISSING_FOR_VALID_ROUTE_COLOR"),
            )
            .when(
                valid_route_color_expr
                & (F.trim(F.col("route_color_name")) == ""),
                F.lit("ROUTE_COLOR_NAME_BLANK_FOR_VALID_ROUTE_COLOR"),
            )
            .when(
                valid_route_text_color_expr
                & F.col("route_text_color_name").isNull(),
                F.lit("ROUTE_TEXT_COLOR_NAME_MISSING_FOR_VALID_ROUTE_TEXT_COLOR"),
            )
            .when(
                valid_route_text_color_expr
                & (F.trim(F.col("route_text_color_name")) == ""),
                F.lit("ROUTE_TEXT_COLOR_NAME_BLANK_FOR_VALID_ROUTE_TEXT_COLOR"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("METRIC"))
        .withColumn("audit_rule", F.lit("INVALID_COLOR_PROFILE"))
        .withColumn(
            "audit_severity",
            F.when(
                F.col("audit_reason").isin(
                    "ROUTE_COLOR_INVALID_HEX_FORMAT",
                    "ROUTE_TEXT_COLOR_INVALID_HEX_FORMAT",
                ),
                F.lit("ERROR"),
            ).otherwise(F.lit("WARNING")),
        )
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
