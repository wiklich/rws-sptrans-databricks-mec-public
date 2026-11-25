"""
Schema for silver.silver_helper_route_shape_points_reference.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_HELPER_ROUTE_SHAPE_POINTS_REFERENCE_SCHEMA = StructType(
    [
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Business-facing line identifier derived from GTFS route_short_name."
            },
        ),
        StructField(
            "route_id",
            StringType(),
            False,
            metadata={
                "comment": "Representative GTFS route_id associated with the line_sign, direction, and shape."
            },
        ),
        StructField(
            "route_long_name",
            StringType(),
            True,
            metadata={
                "comment": "Representative GTFS route_long_name associated with the selected route reference."
            },
        ),
        StructField(
            "route_name_display",
            StringType(),
            True,
            metadata={
                "comment": "Display-friendly route name associated with the selected route reference."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "Canonical GTFS direction_id preserved from GTFS trips."
            },
        ),
        StructField(
            "direction_description",
            StringType(),
            True,
            metadata={
                "comment": "Representative human-readable direction description associated with the selected route reference."
            },
        ),
        StructField(
            "shape_id",
            StringType(),
            False,
            metadata={
                "comment": "GTFS shape identifier linked to the line_sign and direction."
            },
        ),
        StructField(
            "trip_headsign_reference",
            StringType(),
            True,
            metadata={
                "comment": "Deterministic reference trip headsign associated with the shape candidate."
            },
        ),
        StructField(
            "trip_count_using_shape",
            LongType(),
            False,
            metadata={
                "comment": "Number of distinct GTFS trips using the same shape for the line_sign and direction."
            },
        ),
        StructField(
            "shape_usage_rank",
            IntegerType(),
            False,
            metadata={
                "comment": "Ranking of the shape candidate within the same line_sign and direction based on trip usage."
            },
        ),
        StructField(
            "is_representative_shape_candidate",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates whether the shape is the top-ranked candidate for the line_sign and direction."
            },
        ),
        StructField(
            "shape_point_count",
            LongType(),
            False,
            metadata={
                "comment": "Number of shape points available for the specific shape within the selected GTFS snapshot."
            },
        ),
        StructField(
            "shape_pt_sequence",
            IntegerType(),
            False,
            metadata={
                "comment": "Sequence in which shape points connect to form the route path."
            },
        ),
        StructField(
            "shape_pt_lat",
            DoubleType(),
            False,
            metadata={
                "comment": "Latitude of the GTFS shape point."
            },
        ),
        StructField(
            "shape_pt_lon",
            DoubleType(),
            False,
            metadata={
                "comment": "Longitude of the GTFS shape point."
            },
        ),
        StructField(
            "shape_dist_traveled",
            DoubleType(),
            True,
            metadata={
                "comment": "Distance traveled along the shape from the first point to this shape point."
            },
        ),
        StructField(
            "gtfs_load_date",
            DateType(),
            False,
            metadata={
                "comment": "Latest common GTFS snapshot date used to build the helper table."
            },
        ),
        StructField(
            "gtfs_loaded_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp representing the GTFS snapshot ingestion time used for the helper output."
            },
        ),
        StructField(
            "record_hash",
            StringType(),
            False,
            metadata={
                "comment": "Deterministic technical hash for lineage, change tracking, and deduplication at the final helper grain."
            },
        ),
    ]
)
