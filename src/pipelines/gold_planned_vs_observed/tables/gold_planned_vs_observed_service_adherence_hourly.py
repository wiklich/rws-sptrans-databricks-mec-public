"""
Gold pipeline for proxy-based planned vs observed service adherence.

Purpose:
    Build a Gold analytical table that compares:
    - planned service frequency from GTFS frequencies
    - observed service frequency proxy from SPTrans vehicle positions

Business interpretation:
    This dataset is intentionally proxy-based.
    It measures service adherence through headway-oriented logic and
    MUST NOT be interpreted as official trip delay, stop delay,
    or exact trip punctuality.

Grain:
    One row per:
        service_date + line_sign + gtfs_direction_id + hour

Main logic:
    1. Determine active GTFS service_ids for observed dates using GTFS calendar.
    2. Expand GTFS frequency windows into hourly buckets.
    3. Produce planned hourly headway by line_sign + gtfs_direction_id.
    4. Read validated observed-to-GTFS direction mappings.
    5. Extract observed event proxies from SPTrans positions using
       vehicle reappearance after a minimum time gap.
    6. Aggregate observed event proxies to hourly line-direction grain.
    7. Compare planned and observed service adherence metrics.

Important limitations:
    - Observed service is proxy-based, not true trip departure-based.
    - This implementation uses GTFS calendar and does not include calendar_dates.
    - If your GTFS source column names differ, adjust the source reads.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.gold_planned_vs_observed.schemas.schema_gold_planned_vs_observed_service_adherence_hourly import (
    GOLD_PLANNED_VS_OBSERVED_SERVICE_ADHERENCE_HOURLY_SCHEMA,
)

TABLE_NAME = "gold.gold_planned_vs_observed_service_adherence_hourly"

OBSERVED_SOURCE = "main.silver.silver_sptrans_positions"
GTFS_ROUTES_SOURCE = "main.silver.silver_gtfs_routes"
GTFS_TRIPS_SOURCE = "main.silver.silver_gtfs_trips"
GTFS_FREQUENCIES_SOURCE = "main.silver.silver_gtfs_frequencies"
GTFS_CALENDAR_SOURCE = "main.silver.silver_gtfs_calendar"
DIRECTION_MAPPING_SOURCE = "main.gold.gold_direction_mapping_reference"

OBSERVED_EVENT_MIN_GAP_MINUTES = 15.0

ADHERENCE_TYPE = "PROXY_BASED_SERVICE_ADHERENCE"
ADHERENCE_NOTE = (
    "Proxy-based hourly service adherence using planned GTFS headway "
    "and observed vehicle activity; not trip delay."
)


def _gtfs_time_to_seconds(column_name: str):
    """
    Convert GTFS HH:MM:SS-like values to total seconds.

    Supports extended GTFS hour values such as 25:30:00.
    """
    parts = F.split(F.col(column_name), ":")

    return (
        parts.getItem(0).cast("int") * F.lit(3600)
        + parts.getItem(1).cast("int") * F.lit(60)
        + parts.getItem(2).cast("int")
    )


def _build_valid_service_dates() -> DataFrame:
    """
    Build valid GTFS service_ids for the observed local dates.

    This version uses GTFS calendar only.
    If your project also supports calendar_dates exceptions,
    extend this logic accordingly.
    """
    observed_dates = (
        spark.read.table(OBSERVED_SOURCE)
        .select(F.col("captured_date_local").alias("service_date"))
        .filter(F.col("service_date").isNotNull())
        .distinct()
    )

    calendar = (
        spark.read.table(GTFS_CALENDAR_SOURCE)
        .select(
            F.col("service_id"),
            F.col("start_date").cast("date").alias("start_date"),
            F.col("end_date").cast("date").alias("end_date"),
            F.col("monday_active").cast("boolean").alias("monday_active"),
            F.col("tuesday_active").cast("boolean").alias("tuesday_active"),
            F.col("wednesday_active").cast("boolean").alias("wednesday_active"),
            F.col("thursday_active").cast("boolean").alias("thursday_active"),
            F.col("friday_active").cast("boolean").alias("friday_active"),
            F.col("saturday_active").cast("boolean").alias("saturday_active"),
            F.col("sunday_active").cast("boolean").alias("sunday_active"),
        )
        .filter(
            F.col("service_id").isNotNull()
            & F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
        )
    )

    weekday_active = (
        F.when(F.dayofweek(F.col("obs.service_date")) == 1, F.col("cal.sunday_active"))
        .when(F.dayofweek(F.col("obs.service_date")) == 2, F.col("cal.monday_active"))
        .when(F.dayofweek(F.col("obs.service_date")) == 3, F.col("cal.tuesday_active"))
        .when(
            F.dayofweek(F.col("obs.service_date")) == 4,
            F.col("cal.wednesday_active"),
        )
        .when(
            F.dayofweek(F.col("obs.service_date")) == 5,
            F.col("cal.thursday_active"),
        )
        .when(F.dayofweek(F.col("obs.service_date")) == 6, F.col("cal.friday_active"))
        .when(
            F.dayofweek(F.col("obs.service_date")) == 7,
            F.col("cal.saturday_active"),
        )
        .otherwise(F.lit(False))
    )

    return (
        observed_dates.alias("obs")
        .join(
            calendar.alias("cal"),
            (
                F.col("obs.service_date").between(
                    F.col("cal.start_date"),
                    F.col("cal.end_date"),
                )
            )
            & weekday_active,
            "inner",
        )
        .select(
            F.col("obs.service_date").alias("service_date"),
            F.col("cal.service_id").alias("service_id"),
        )
        .distinct()
    )


def _build_planned_hourly() -> DataFrame:
    """
    Build the planned hourly headway dataset.

    Logic:
        - route_short_name is used as line_sign
        - GTFS frequencies are expanded across all covered hours
        - if multiple active frequency windows overlap the same hourly bucket,
          the minimum planned headway is used as the representative value
    """
    valid_service_dates = _build_valid_service_dates()

    routes = (
        spark.read.table(GTFS_ROUTES_SOURCE)
        .select(
            F.col("route_id"),
            F.col("route_short_name").alias("line_sign"),
        )
        .filter(F.col("route_id").isNotNull() & F.col("line_sign").isNotNull())
    )

    trips = (
        spark.read.table(GTFS_TRIPS_SOURCE)
        .select(
            F.col("trip_id"),
            F.col("route_id"),
            F.col("service_id"),
            F.col("direction_id").cast("int").alias("gtfs_direction_id"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("service_id").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
        )
    )

    frequencies = (
        spark.read.table(GTFS_FREQUENCIES_SOURCE)
        .select(
            F.col("trip_id"),
            F.col("start_time"),
            F.col("end_time"),
            F.col("headway_secs").cast("double").alias("headway_secs"),
        )
        .filter(
            F.col("trip_id").isNotNull()
            & F.col("start_time").isNotNull()
            & F.col("end_time").isNotNull()
            & F.col("headway_secs").isNotNull()
            & (F.col("headway_secs") > 0)
        )
    )

    planned_windows = (
        trips.join(routes, on="route_id", how="inner")
        .join(frequencies, on="trip_id", how="inner")
        .join(valid_service_dates, on="service_id", how="inner")
        .withColumn("start_seconds", _gtfs_time_to_seconds("start_time"))
        .withColumn("end_seconds", _gtfs_time_to_seconds("end_time"))
        .filter(F.col("end_seconds") > F.col("start_seconds"))
        .withColumn("planned_headway_minutes", F.col("headway_secs") / F.lit(60.0))
        .withColumn("service_midnight_ts", F.to_timestamp(F.col("service_date")))
        .withColumn(
            "start_ts",
            F.to_timestamp(
                F.from_unixtime(
                    F.unix_timestamp(F.col("service_midnight_ts"))
                    + F.col("start_seconds")
                )
            ),
        )
        .withColumn(
            "end_ts",
            F.to_timestamp(
                F.from_unixtime(
                    F.unix_timestamp(F.col("service_midnight_ts"))
                    + F.col("end_seconds")
                )
            ),
        )
        .withColumn(
            "hour_bucket_ts",
            F.explode(
                F.sequence(
                    F.date_trunc("hour", F.col("start_ts")),
                    F.date_trunc("hour", F.expr("end_ts - INTERVAL 1 SECOND")),
                    F.expr("INTERVAL 1 HOUR"),
                )
            ),
        )
        .select(
            F.to_date("hour_bucket_ts").alias("service_date"),
            F.col("line_sign"),
            F.col("gtfs_direction_id"),
            F.hour("hour_bucket_ts").cast("int").alias("hour"),
            F.col("planned_headway_minutes").cast("double"),
        )
    )

    return (
        planned_windows.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
        )
        .agg(
            F.min("planned_headway_minutes").alias("planned_headway_minutes"),
        )
        .withColumn("planned_service_present_flag", F.lit(True))
    )


def _build_valid_direction_mapping() -> DataFrame:
    """
    Read the production-safe observed-to-GTFS direction mapping reference.
    """
    return (
        spark.read.table(DIRECTION_MAPPING_SOURCE)
        .filter(
            (F.col("validated_flag") == F.lit(True))
            & (F.col("review_required_flag") == F.lit(False))
        )
        .select(
            F.col("line_sign"),
            F.col("observed_direction").cast("int").alias("observed_direction"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
        )
        .dropDuplicates(["line_sign", "observed_direction"])
    )


def _build_observed_hourly() -> DataFrame:
    """
    Build observed hourly service adherence proxy metrics.

    Proxy rule:
        A new observed event proxy is created for a vehicle when:
        - it is the first observed ping for that vehicle on the service date, or
        - the gap from the previous ping for that same vehicle, line, and direction
          is at least OBSERVED_EVENT_MIN_GAP_MINUTES

    Important:
        This is not a true departure event.
        It is an analytical proxy used for service adherence estimation.
    """
    direction_mapping = _build_valid_direction_mapping()

    observed_positions = (
        spark.read.table(OBSERVED_SOURCE)
        .select(
            F.col("captured_date_local").alias("service_date"),
            F.col("line_sign"),
            F.col("direction").cast("int").alias("observed_direction"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_id"),
            F.col("captured_at_local").alias("event_timestamp"),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & F.col("observed_direction").isNotNull()
            & F.col("vehicle_id").isNotNull()
            & F.col("event_timestamp").isNotNull()
        )
    )

    observed_mapped = observed_positions.join(
        direction_mapping,
        on=["line_sign", "observed_direction"],
        how="inner",
    )

    vehicle_window = Window.partitionBy(
        "service_date",
        "line_sign",
        "observed_direction",
        "vehicle_id",
    ).orderBy("event_timestamp")

    observed_event_candidates = (
        observed_mapped.withColumn(
            "previous_vehicle_timestamp",
            F.lag("event_timestamp").over(vehicle_window),
        )
        .withColumn(
            "minutes_since_previous_vehicle_ping",
            (
                F.col("event_timestamp").cast("long")
                - F.col("previous_vehicle_timestamp").cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "is_observed_event_proxy",
            F.when(F.col("previous_vehicle_timestamp").isNull(), F.lit(True))
            .when(
                F.col("minutes_since_previous_vehicle_ping")
                >= F.lit(OBSERVED_EVENT_MIN_GAP_MINUTES),
                F.lit(True),
            )
            .otherwise(F.lit(False)),
        )
        .filter(F.col("is_observed_event_proxy") == F.lit(True))
        .withColumn("hour", F.hour("event_timestamp").cast("int"))
    )

    headway_window = Window.partitionBy(
        "service_date",
        "line_sign",
        "gtfs_direction_id",
        "hour",
    ).orderBy("event_timestamp", "vehicle_id")

    observed_events_with_intervals = (
        observed_event_candidates.withColumn(
            "previous_event_timestamp",
            F.lag("event_timestamp").over(headway_window),
        )
        .withColumn(
            "observed_headway_minutes",
            (
                F.col("event_timestamp").cast("long")
                - F.col("previous_event_timestamp").cast("long")
            )
            / F.lit(60.0),
        )
    )

    return (
        observed_events_with_intervals.groupBy(
            "service_date",
            "line_sign",
            "gtfs_direction_id",
            "hour",
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("observed_event_count_proxy"),
            F.countDistinct("vehicle_id").cast("long").alias("observed_vehicle_count"),
            F.avg("observed_headway_minutes")
            .cast("double")
            .alias("observed_headway_avg_minutes"),
            F.expr("percentile_approx(observed_headway_minutes, 0.5)")
            .cast("double")
            .alias("observed_headway_p50_minutes"),
            F.expr("percentile_approx(observed_headway_minutes, 0.9)")
            .cast("double")
            .alias("observed_headway_p90_minutes"),
        )
        .withColumn("observed_service_present_flag", F.lit(True))
    )


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Gold analytical table for proxy-based planned versus observed service "
        "adherence at service_date + line_sign + gtfs_direction_id + hour grain. "
        "Compares GTFS planned headway against observed vehicle activity proxy. "
        "Not an official trip delay dataset."
    ),
    schema=GOLD_PLANNED_VS_OBSERVED_SERVICE_ADHERENCE_HOURLY_SCHEMA,
)
def gold_planned_vs_observed_service_adherence_hourly() -> DataFrame:
    """
    Create the final Gold table for proxy-based hourly service adherence.

    Final interpretation:
        - planned_headway_minutes comes from GTFS frequencies
        - observed_* metrics come from observed event proxy logic
        - missed_service_flag indicates a candidate missed planned service window
        - this table must not be interpreted as trip delay
    """
    planned_hourly = _build_planned_hourly()
    observed_hourly = _build_observed_hourly()

    final_df = (
        planned_hourly.join(
            observed_hourly,
            on=["service_date", "line_sign", "gtfs_direction_id", "hour"],
            how="full_outer",
        )
        .withColumn(
            "planned_service_present_flag",
            F.coalesce(F.col("planned_service_present_flag"), F.lit(False)),
        )
        .withColumn(
            "observed_service_present_flag",
            F.coalesce(F.col("observed_service_present_flag"), F.lit(False)),
        )
        .withColumn(
            "headway_deviation_minutes",
            F.when(
                F.col("planned_headway_minutes").isNotNull()
                & F.col("observed_headway_avg_minutes").isNotNull(),
                F.col("observed_headway_avg_minutes")
                - F.col("planned_headway_minutes"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "headway_deviation_pct",
            F.when(
                F.col("planned_headway_minutes").isNotNull()
                & (F.col("planned_headway_minutes") > 0)
                & F.col("observed_headway_avg_minutes").isNotNull(),
                (
                    F.col("observed_headway_avg_minutes")
                    - F.col("planned_headway_minutes")
                )
                / F.col("planned_headway_minutes"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "missed_service_flag",
            F.col("planned_service_present_flag")
            & (~F.col("observed_service_present_flag")),
        )
        .withColumn("adherence_type", F.lit(ADHERENCE_TYPE))
        .withColumn("adherence_note", F.lit(ADHERENCE_NOTE))
        .withColumn("record_created_at", F.current_timestamp())
        .filter(
            F.col("service_date").isNotNull()
            & F.col("line_sign").isNotNull()
            & F.col("gtfs_direction_id").isNotNull()
            & F.col("hour").isNotNull()
        )
        .select(
            F.col("service_date").cast("date").alias("service_date"),
            F.col("line_sign").cast("string").alias("line_sign"),
            F.col("gtfs_direction_id").cast("int").alias("gtfs_direction_id"),
            F.col("hour").cast("int").alias("hour"),
            F.col("planned_headway_minutes").cast("double").alias(
                "planned_headway_minutes"
            ),
            F.col("observed_event_count_proxy").cast("long").alias(
                "observed_event_count_proxy"
            ),
            F.col("observed_vehicle_count").cast("long").alias(
                "observed_vehicle_count"
            ),
            F.col("observed_headway_avg_minutes").cast("double").alias(
                "observed_headway_avg_minutes"
            ),
            F.col("observed_headway_p50_minutes").cast("double").alias(
                "observed_headway_p50_minutes"
            ),
            F.col("observed_headway_p90_minutes").cast("double").alias(
                "observed_headway_p90_minutes"
            ),
            F.col("headway_deviation_minutes").cast("double").alias(
                "headway_deviation_minutes"
            ),
            F.col("headway_deviation_pct").cast("double").alias(
                "headway_deviation_pct"
            ),
            F.col("planned_service_present_flag").cast("boolean").alias(
                "planned_service_present_flag"
            ),
            F.col("observed_service_present_flag").cast("boolean").alias(
                "observed_service_present_flag"
            ),
            F.col("missed_service_flag").cast("boolean").alias(
                "missed_service_flag"
            ),
            F.col("adherence_type").cast("string").alias("adherence_type"),
            F.col("adherence_note").cast("string").alias("adherence_note"),
            F.col("record_created_at").cast("timestamp").alias("record_created_at"),
        )
    )

    return final_df