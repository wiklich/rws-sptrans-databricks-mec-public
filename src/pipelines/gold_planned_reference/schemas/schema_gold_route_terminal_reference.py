"""
Schema for gold.gold_route_terminal_reference.
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

GOLD_ROUTE_TERMINAL_REFERENCE_SCHEMA = StructType(
    [
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Business-facing line identifier of the representative route terminal reference."
            },
        ),
        StructField(
            "route_id",
            StringType(),
            False,
            metadata={
                "comment": "Representative GTFS route_id associated with the selected terminal pair."
            },
        ),
        StructField(
            "route_long_name",
            StringType(),
            True,
            metadata={
                "comment": "Representative GTFS route_long_name associated with the selected terminal pair."
            },
        ),
        StructField(
            "route_name_display",
            StringType(),
            True,
            metadata={
                "comment": "Display-friendly route name associated with the selected terminal pair."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "Canonical GTFS direction identifier of the representative terminal pair."
            },
        ),
        StructField(
            "direction_description",
            StringType(),
            True,
            metadata={
                "comment": "Human-readable GTFS direction description associated with the selected terminal pair."
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
                "comment": "GTFS stop_id of the representative terminal."
            },
        ),
        StructField(
            "stop_name",
            StringType(),
            True,
            metadata={
                "comment": "GTFS stop_name of the representative terminal."
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
                "comment": "First stop_id of the selected representative endpoint pair."
            },
        ),
        StructField(
            "representative_last_stop_id",
            StringType(),
            False,
            metadata={
                "comment": "Last stop_id of the selected representative endpoint pair."
            },
        ),
        StructField(
            "endpoint_pair_trip_count",
            LongType(),
            False,
            metadata={
                "comment": "Number of trips supporting the selected representative endpoint pair."
            },
        ),
        StructField(
            "endpoint_pair_rank",
            IntegerType(),
            False,
            metadata={
                "comment": "Ranking of the selected endpoint pair within the same line_sign and gtfs_direction_id."
            },
        ),
        StructField(
            "gtfs_load_date",
            DateType(),
            False,
            metadata={
                "comment": "GTFS snapshot date used to construct the representative route terminal reference."
            },
        ),
        StructField(
            "gtfs_loaded_at",
            TimestampType(),
            False,
            metadata={
                "comment": "GTFS ingestion timestamp associated with the selected representative route terminal reference."
            },
        ),
        StructField(
            "record_created_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp when the Gold route terminal record was materialized."
            },
        ),
        StructField(
            "record_hash",
            StringType(),
            False,
            metadata={
                "comment": "Deterministic technical hash for lineage, change tracking, and route terminal comparisons."
            },
        ),
    ]
)
