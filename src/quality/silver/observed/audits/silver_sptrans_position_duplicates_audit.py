from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver.silver_sptrans_position_duplicates_audit",
    comment="Audit table with duplicate SPTrans vehicle position events found in bronze layer"
)
def silver_sptrans_position_duplicates_audit():
    df = (
        spark.readStream.table("main.bronze.bronze_sptrans_positions")
        .withColumn("captured_at_utc_ts", F.to_timestamp("captured_at_utc"))
        .withColumn("line_id", F.col("line_id").cast("long"))
        .withColumn("vehicle_prefix", F.col("vehicle_prefix").cast("long"))
        .withColumn("direction", F.col("direction").cast("int"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withWatermark("captured_at_utc_ts", "10 minutes")
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("line_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("vehicle_prefix").cast("string"), F.lit("")),
                    F.coalesce(F.col("captured_at_utc_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("longitude").cast("string"), F.lit("")),
                    F.coalesce(F.col("latitude").cast("string"), F.lit(""))
                ),
                256
            )
        )
        .withColumn(
            "captured_at_local",
            F.from_utc_timestamp(F.col("captured_at_utc_ts"), "America/Sao_Paulo")
        )
        .withColumn("captured_date_local", F.to_date("captured_at_local"))
        .withColumn("captured_hour_local", F.hour("captured_at_local"))
    )

    duplicates_df = (
        df.groupBy(
            "event_id",
            "line_id",
            "vehicle_prefix",
            "direction",
            "captured_at_utc_ts",
            "captured_date_local",
            "captured_hour_local",
            "latitude",
            "longitude",
        )
        .agg(
            F.count("*").alias("duplicate_count"),
            F.min("ingestion_ts").alias("first_ingestion_ts"),
            F.max("ingestion_ts").alias("last_ingestion_ts"),
            F.min("source_file").alias("first_source_file"),
            F.max("source_file").alias("last_source_file"),
        )
        .filter(F.col("duplicate_count") > 1)
    )

    return duplicates_df