"""
Schema for gold.gold_terminal_risk_daily.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOLD_TERMINAL_RISK_DAILY_SCHEMA = StructType(
    [
        StructField(
            "service_date",
            DateType(),
            False,
            metadata={
                "comment": "Local service date of the daily terminal risk aggregation."
            },
        ),
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Business-facing line identifier associated with the terminal risk row."
            },
        ),
        StructField(
            "route_id",
            StringType(),
            False,
            metadata={
                "comment": "Representative GTFS (General Transit Feed Specification) route identifier associated with the terminal reference."
            },
        ),
        StructField(
            "route_long_name",
            StringType(),
            True,
            metadata={
                "comment": "Representative GTFS route long name associated with the terminal reference."
            },
        ),
        StructField(
            "route_name_display",
            StringType(),
            True,
            metadata={
                "comment": "Display-friendly route name associated with the terminal reference."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "Canonical GTFS direction identifier associated with the terminal risk row."
            },
        ),
        StructField(
            "direction_description",
            StringType(),
            True,
            metadata={
                "comment": "Human-readable direction description associated with the terminal reference."
            },
        ),
        StructField(
            "trip_headsign_reference",
            StringType(),
            True,
            metadata={
                "comment": "Deterministic reference trip headsign associated with the selected representative terminal pair."
            },
        ),
        StructField(
            "terminal_role",
            StringType(),
            False,
            metadata={
                "comment": "Terminal role of the row. Approved values: ORIGIN or DESTINATION."
            },
        ),
        StructField(
            "stop_id",
            StringType(),
            False,
            metadata={
                "comment": "Representative GTFS stop identifier of the terminal."
            },
        ),
        StructField(
            "stop_name",
            StringType(),
            True,
            metadata={
                "comment": "Representative GTFS stop name of the terminal."
            },
        ),
        StructField(
            "latitude",
            DoubleType(),
            False,
            metadata={
                "comment": "Latitude of the representative terminal."
            },
        ),
        StructField(
            "longitude",
            DoubleType(),
            False,
            metadata={
                "comment": "Longitude of the representative terminal."
            },
        ),
        StructField(
            "representative_first_stop_id",
            StringType(),
            False,
            metadata={
                "comment": "First stop identifier of the representative endpoint pair used by the terminal reference."
            },
        ),
        StructField(
            "representative_last_stop_id",
            StringType(),
            False,
            metadata={
                "comment": "Last stop identifier of the representative endpoint pair used by the terminal reference."
            },
        ),
        StructField(
            "endpoint_pair_trip_count",
            LongType(),
            False,
            metadata={
                "comment": "Number of trips supporting the representative endpoint pair used by the terminal reference."
            },
        ),
        StructField(
            "endpoint_pair_rank",
            IntegerType(),
            False,
            metadata={
                "comment": "Ranking of the representative endpoint pair used by the terminal reference."
            },
        ),
        StructField(
            "planned_service_hours",
            LongType(),
            False,
            metadata={
                "comment": "Number of hourly windows where planned service existed for the route-direction on the service date."
            },
        ),
        StructField(
            "observed_service_hours",
            LongType(),
            False,
            metadata={
                "comment": "Number of hourly windows where observed service existed for the route-direction on the service date."
            },
        ),
        StructField(
            "missed_service_windows",
            LongType(),
            False,
            metadata={
                "comment": "Number of hourly windows classified as missed service for the route-direction on the service date."
            },
        ),
        StructField(
            "missed_service_rate",
            DoubleType(),
            True,
            metadata={
                "comment": "Daily missed service proportion relative to planned service hours."
            },
        ),
        StructField(
            "avg_planned_headway_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Daily average planned headway in minutes."
            },
        ),
        StructField(
            "avg_observed_headway_avg_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Daily average observed headway proxy in minutes."
            },
        ),
        StructField(
            "avg_headway_deviation_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Daily average headway deviation in minutes."
            },
        ),
        StructField(
            "avg_headway_deviation_pct",
            DoubleType(),
            True,
            metadata={
                "comment": "Daily average headway deviation percentage."
            },
        ),
        StructField(
            "terminal_risk_band",
            StringType(),
            False,
            metadata={
                "comment": "Presentation-ready daily terminal risk classification."
            },
        ),
        StructField(
            "gtfs_load_date",
            DateType(),
            False,
            metadata={
                "comment": "GTFS snapshot date inherited from the terminal reference."
            },
        ),
        StructField(
            "gtfs_loaded_at",
            TimestampType(),
            False,
            metadata={
                "comment": "GTFS ingestion timestamp inherited from the terminal reference."
            },
        ),
        StructField(
            "record_created_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp when the Gold terminal risk daily row was materialized."
            },
        ),
        StructField(
            "record_hash",
            StringType(),
            False,
            metadata={
                "comment": "Deterministic technical hash for lineage, change tracking, and daily terminal risk comparisons."
            },
        ),
    ]
)
