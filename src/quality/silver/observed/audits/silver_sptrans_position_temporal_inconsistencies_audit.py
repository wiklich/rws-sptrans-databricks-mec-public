from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.silver_sptrans_positions"
TABLE_NAME = "silver.silver_sptrans_position_temporal_inconsistencies_audit"


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Row-level audit table for temporal inconsistencies in "
        "silver.silver_sptrans_positions. Identifies records with invalid "
        "time reconciliation, future timestamps, or non-monotonic temporal "
        "ordering within the same vehicle."
    ),
)
def silver_sptrans_position_temporal_inconsistencies_audit() -> DataFrame:
    """
    Audit rows from silver.silver_sptrans_positions that contain temporal
    inconsistencies relevant for downstream operational analysis.
    """
    source_df = spark.read.table(SOURCE_TABLE)

    vehicle_time_window = Window.partitionBy("vehicle_prefix").orderBy("captured_at_utc")

    enriched_df = (
        source_df.withColumn(
            "expected_captured_at_local",
            F.from_utc_timestamp(F.col("captured_at_utc"), "America/Sao_Paulo"),
        )
        .withColumn(
            "expected_captured_date_local",
            F.to_date(F.col("captured_at_local")),
        )
        .withColumn(
            "expected_captured_hour_local",
            F.hour(F.col("captured_at_local")),
        )
        .withColumn(
            "previous_captured_at_utc",
            F.lag("captured_at_utc").over(vehicle_time_window),
        )
        .withColumn(
            "audit_reason",
            F.when(
                F.col("captured_at_utc") > F.current_timestamp(),
                F.lit("CAPTURED_AT_UTC_IN_FUTURE"),
            )
            .when(
                F.col("captured_at_local") > F.current_timestamp(),
                F.lit("CAPTURED_AT_LOCAL_IN_FUTURE"),
            )
            .when(
                F.col("captured_at_local") != F.col("expected_captured_at_local"),
                F.lit("CAPTURED_AT_LOCAL_DOES_NOT_RECONCILE_WITH_CAPTURED_AT_UTC"),
            )
            .when(
                F.col("captured_date_local") != F.col("expected_captured_date_local"),
                F.lit("CAPTURED_DATE_LOCAL_DOES_NOT_RECONCILE_WITH_CAPTURED_AT_LOCAL"),
            )
            .when(
                F.col("captured_hour_local") != F.col("expected_captured_hour_local"),
                F.lit("CAPTURED_HOUR_LOCAL_DOES_NOT_RECONCILE_WITH_CAPTURED_AT_LOCAL"),
            )
            .when(
                F.col("previous_captured_at_utc").isNotNull()
                & (F.col("captured_at_utc") < F.col("previous_captured_at_utc")),
                F.lit("NON_MONOTONIC_CAPTURED_AT_UTC_WITHIN_VEHICLE"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("audit_reason").isNotNull())
        .withColumn("audit_category", F.lit("RECONCILIATION"))
        .withColumn("audit_rule", F.lit("TEMPORAL_INCONSISTENCIES"))
        .withColumn("audit_severity", F.lit("ERROR"))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("audited_at", F.current_timestamp())
        .select(
            "source_table",
            "audit_category",
            "audit_rule",
            "audit_severity",
            "audit_reason",
            "event_id",
            "line_id",
            "line_sign",
            "vehicle_prefix",
            "direction",
            "direction_description",
            "captured_at_utc",
            "captured_at_local",
            "captured_date_local",
            "captured_hour_local",
            "previous_captured_at_utc",
            "latitude",
            "longitude",
            "vehicles_count",
            "ingestion_ts",
            "ingestion_date",
            "audited_at",
        )
    )

    return enriched_df
