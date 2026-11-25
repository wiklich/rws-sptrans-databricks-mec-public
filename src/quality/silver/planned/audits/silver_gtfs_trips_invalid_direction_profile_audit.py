from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "silver.silver_gtfs_trips"
TABLE_NAME = "silver.silver_gtfs_trips_invalid_direction_profile_audit"

ALLOWED_DIRECTION_IDS = [0, 1]
DIRECTION_DESCRIPTION_MAPPING = {
    0: "outbound",
    1: "inbound",
}


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for invalid or inconsistent direction profile "
        "attributes in silver.silver_gtfs_trips."
    ),
)
def silver_gtfs_trips_invalid_direction_profile_audit() -> DataFrame:
    """
    Audit rows from silver.silver_gtfs_trips where direction_id and
    direction_description are invalid, inconsistent, or incomplete.

    Purpose:
        Identify trips with unsupported direction values or with
        direction_description that does not reconcile with direction_id.

    Important:
        - This audit checks the approved direction_id domain used in this project.
        - direction_description is expected to be derived deterministically from
          direction_id when direction_id is present and valid.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    mapping_expr = F.create_map(
        *[
            x
            for kv in DIRECTION_DESCRIPTION_MAPPING.items()
            for x in (F.lit(kv[0]), F.lit(kv[1]))
        ]
    )

    audited_df = (
        source_df.withColumn(
            "expected_direction_description",
            F.when(
                F.col("direction_id").isin(ALLOWED_DIRECTION_IDS),
                mapping_expr.getItem(F.col("direction_id")),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("direction_id").isNull(),
                F.lit("DIRECTION_ID_IS_NULL"),
            )
            .when(
                ~F.col("direction_id").isin(ALLOWED_DIRECTION_IDS),
                F.lit("DIRECTION_ID_OUTSIDE_APPROVED_DOMAIN"),
            )
            .when(
                F.col("direction_description").isNull(),
                F.lit("DIRECTION_DESCRIPTION_IS_NULL_FOR_VALID_DIRECTION_ID"),
            )
            .when(
                F.trim(F.col("direction_description")) == "",
                F.lit("DIRECTION_DESCRIPTION_IS_BLANK_FOR_VALID_DIRECTION_ID"),
            )
            .when(
                F.col("direction_description") != F.col("expected_direction_description"),
                F.lit("DIRECTION_DESCRIPTION_DOES_NOT_RECONCILE_WITH_DIRECTION_ID"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("BUSINESS_RULE"))
        .withColumn("audit_rule", F.lit("INVALID_DIRECTION_PROFILE"))
        .withColumn(
            "audit_severity",
            F.when(
                F.col("audit_reason").isin(
                    "DIRECTION_DESCRIPTION_IS_NULL_FOR_VALID_DIRECTION_ID",
                    "DIRECTION_DESCRIPTION_IS_BLANK_FOR_VALID_DIRECTION_ID",
                ),
                F.lit("WARNING"),
            ).otherwise(F.lit("ERROR")),
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
            "service_id",
            "trip_id",
            "trip_headsign",
            "trip_short_name",
            "direction_id",
            "direction_description",
            "expected_direction_description",
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
