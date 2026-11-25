from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils.constants_utils import (
    BOUNDING_BOX_MIN_LAT,
    BOUNDING_BOX_MAX_LAT,
    BOUNDING_BOX_MIN_LON,
    BOUNDING_BOX_MAX_LON,
)

WATERMARK_DURATION = "10 minutes"


@dp.table(
    name="silver.silver_sptrans_positions",
    comment="Cleaned, validated, deduplicated SPTrans vehicle positions"
)
@dp.expect_or_drop("valid_line_id", "line_id IS NOT NULL AND line_id > 0")
@dp.expect_or_drop("valid_vehicle_prefix", "vehicle_prefix IS NOT NULL AND vehicle_prefix > 0")
@dp.expect_or_drop("valid_direction", "direction IN (1, 2)")
@dp.expect_or_drop("valid_vehicles_count", "vehicles_count IS NULL OR vehicles_count >= 0")
@dp.expect_or_drop("valid_latitude", "latitude BETWEEN -90 AND 90")
@dp.expect_or_drop("valid_longitude", "longitude BETWEEN -180 AND 180")
@dp.expect_or_drop(
    "within_sptrans_bounding_box",
    f"""
    latitude BETWEEN {BOUNDING_BOX_MIN_LAT} AND {BOUNDING_BOX_MAX_LAT}
    AND longitude BETWEEN {BOUNDING_BOX_MIN_LON} AND {BOUNDING_BOX_MAX_LON}
    """
)
@dp.expect_or_drop("valid_captured_at_utc", "captured_at_utc IS NOT NULL")
def silver_sptrans_positions():
    df = (
        spark.readStream.table("main.bronze.bronze_sptrans_positions")
        .withColumn("ingestion_ts", F.col("ingestion_ts").cast("timestamp"))
        .withColumn("captured_at_utc_raw", F.col("captured_at_utc"))
        .withColumn("captured_at_utc", F.to_timestamp("captured_at_utc"))
        .withWatermark("captured_at_utc", WATERMARK_DURATION)
    )

    cleaned_df = (
        df
        .withColumn(
            "reference_hour",
            F.when(F.length(F.trim(F.col("reference_hour"))) == 0, None)
            .otherwise(F.trim(F.col("reference_hour")))
        )
        .withColumn(
            "line_sign",
            F.when(F.length(F.trim(F.col("line_sign"))) == 0, None)
            .otherwise(F.trim(F.col("line_sign")))
        )
        .withColumn(
            "destination_label",
            F.when(F.length(F.trim(F.col("destination_label"))) == 0, None)
            .otherwise(F.trim(F.col("destination_label")))
        )
        .withColumn(
            "origin_label",
            F.when(F.length(F.trim(F.col("origin_label"))) == 0, None)
            .otherwise(F.trim(F.col("origin_label")))
        )
        .withColumn("line_id", F.col("line_id").cast("long"))
        .withColumn("direction", F.col("direction").cast("int"))
        .withColumn("vehicles_count", F.col("vehicles_count").cast("int"))
        .withColumn("vehicle_prefix", F.col("vehicle_prefix").cast("long"))
        .withColumn(
            "is_accessible",
            F.when(
                F.lower(F.col("is_accessible").cast("string")).isin("1", "true"),
                F.lit(True)
            ).when(
                F.lower(F.col("is_accessible").cast("string")).isin("0", "false"),
                F.lit(False)
            ).otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn(
            "captured_at_local",
            F.from_utc_timestamp(F.col("captured_at_utc"), "America/Sao_Paulo")
        )
        .withColumn("captured_date_local", F.to_date("captured_at_local"))
        .withColumn("captured_hour_local", F.hour("captured_at_local"))
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
        .withColumn(
            "direction_description",
            F.when(F.col("direction") == 1, F.lit("main_to_secondary"))
            .when(F.col("direction") == 2, F.lit("secondary_to_main"))
            .otherwise(F.lit(None))
        )
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("line_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("vehicle_prefix").cast("string"), F.lit("")),
                    F.coalesce(F.col("captured_at_utc").cast("string"), F.lit("")),
                    F.coalesce(F.col("longitude").cast("string"), F.lit("")),
                    F.coalesce(F.col("latitude").cast("string"), F.lit(""))
                ),
                256
            )
        )
        .select(
            "event_id",
            "reference_hour",
            "line_sign",
            "line_id",
            "direction",
            "direction_description",
            "destination_label",
            "origin_label",
            "vehicles_count",
            "vehicle_prefix",
            "is_accessible",
            "captured_at_utc_raw",
            "captured_at_utc",
            "captured_at_local",
            "captured_date_local",
            "captured_hour_local",
            "latitude",
            "longitude",
            "ingestion_ts",
            "ingestion_date",
            "source_file",
            "source_file_modification_time",
            "source_ingested_at",
            "ingestion_status",
            "total_elapsed_seconds",
            "auth_elapsed_seconds",
            "auth_http_status",
            "request_elapsed_seconds",
            "request_http_status",
            "landing_metadata",
            "auth_metadata",
            "request_metadata",
        )
    )

    return cleaned_df.dropDuplicates(["event_id"])