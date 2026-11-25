"""
Silver GTFS stop_times time order audit pipeline.

Purpose:
    Identify trips with invalid chronological order between consecutive stops
    in GTFS stop_times.

Audit rule:
    For each trip_id, arrival_time_seconds and departure_time_seconds must not
    decrease when compared to the next stop_sequence.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(
    name="silver.silver_gtfs_stop_times_time_order_audit",
    comment="Audit table with GTFS stop_times records where consecutive stop times are not in chronological order."
)
def silver_gtfs_stop_times_time_order_audit():
    """
    Create an audit table for chronological inconsistencies between consecutive
    stop_times records within the same trip_id.

    Returns:
        pyspark.sql.DataFrame:
            Audit records where the next arrival or departure time is lower than
            the current stop time for the same trip_id.
    """
    df = spark.read.table("main.silver.silver_gtfs_stop_times")

    window_spec = Window.partitionBy("trip_id").orderBy("stop_sequence")

    audit_df = (
        df
        .withColumn("next_stop_id", F.lead("stop_id").over(window_spec))
        .withColumn("next_stop_sequence", F.lead("stop_sequence").over(window_spec))
        .withColumn("next_arrival_time", F.lead("arrival_time").over(window_spec))
        .withColumn("next_departure_time", F.lead("departure_time").over(window_spec))
        .withColumn("next_arrival_time_seconds", F.lead("arrival_time_seconds").over(window_spec))
        .withColumn("next_departure_time_seconds", F.lead("departure_time_seconds").over(window_spec))
        .withColumn(
            "has_invalid_arrival_order",
            F.when(
                F.col("next_arrival_time_seconds").isNotNull()
                & F.col("arrival_time_seconds").isNotNull()
                & (F.col("arrival_time_seconds") > F.col("next_arrival_time_seconds")),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "has_invalid_departure_order",
            F.when(
                F.col("next_departure_time_seconds").isNotNull()
                & F.col("departure_time_seconds").isNotNull()
                & (F.col("departure_time_seconds") > F.col("next_departure_time_seconds")),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "has_invalid_time_order",
            F.col("has_invalid_arrival_order") | F.col("has_invalid_departure_order")
        )
        .filter(F.col("has_invalid_time_order") == True)
        .select(
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
            "arrival_time_seconds",
            "departure_time_seconds",
            "next_stop_id",
            "next_stop_sequence",
            "next_arrival_time",
            "next_departure_time",
            "next_arrival_time_seconds",
            "next_departure_time_seconds",
            "has_invalid_arrival_order",
            "has_invalid_departure_order",
            "has_invalid_time_order",
        )
    )

    return audit_df