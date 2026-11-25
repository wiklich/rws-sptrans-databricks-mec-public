"""
Schema for gold.gold_route_geometry_reference.
"""

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOLD_ROUTE_GEOMETRY_REFERENCE_SCHEMA = StructType(
    [
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Business-facing line identifier of the representative GTFS route geometry."
            },
        ),
        StructField(
            "route_id",
            StringType(),
            False,
            metadata={
                "comment": "Representative GTFS route_id associated with the selected route geometry."
            },
        ),
        StructField(
            "route_long_name",
            StringType(),
            True,
            metadata={
                "comment": "Representative GTFS route_long_name associated with the selected route geometry."
            },
        ),
        StructField(
            "route_name_display",
            StringType(),
            True,
            metadata={
                "comment": "Display-friendly route name associated with the selected route geometry."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "Canonical GTFS direction identifier of the representative route geometry."
            },
        ),
        StructField(
            "direction_description",
            StringType(),
            True,
            metadata={
                "comment": "Human-readable GTFS direction description associated with the selected route geometry."
            },
        ),
        StructField(
            "trip_headsign_reference",
            StringType(),
            True,
            metadata={
                "comment": "Deterministic reference trip headsign associated with the selected representative shape."
            },
        ),
        StructField(
            "representative_shape_id",
            StringType(),
            False,
            metadata={
                "comment": "Selected GTFS shape_id used to construct the final representative route geometry."
            },
        ),
        StructField(
            "trip_count_using_shape",
            LongType(),
            False,
            metadata={
                "comment": "Number of trips using the selected representative shape."
            },
        ),
        StructField(
            "shape_usage_rank",
            IntegerType(),
            False,
            metadata={
                "comment": "Shape ranking of the selected representative shape within the same line_sign and direction."
            },
        ),
        StructField(
            "shape_point_count",
            LongType(),
            False,
            metadata={
                "comment": "Number of shape points available for the selected representative shape."
            },
        ),
        StructField(
            "geometry_point_count",
            LongType(),
            False,
            metadata={
                "comment": "Number of ordered points included in the final geometry_wkt representation."
            },
        ),
        StructField(
            "geometry_wkt",
            StringType(),
            False,
            metadata={
                "comment": "Map-ready representative route geometry encoded as WKT LINESTRING."
            },
        ),
        StructField(
            "gtfs_load_date",
            DateType(),
            False,
            metadata={
                "comment": "GTFS snapshot date used to construct the representative route geometry."
            },
        ),
        StructField(
            "gtfs_loaded_at",
            TimestampType(),
            False,
            metadata={
                "comment": "GTFS ingestion timestamp associated with the selected representative route geometry."
            },
        ),
        StructField(
            "record_created_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp when the Gold route geometry record was materialized."
            },
        ),
        StructField(
            "record_hash",
            StringType(),
            False,
            metadata={
                "comment": "Deterministic technical hash for lineage, change tracking, and route geometry comparisons."
            },
        ),
    ]
)
