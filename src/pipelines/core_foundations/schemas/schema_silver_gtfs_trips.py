from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_silver_gtfs_trips_schema() -> StructType:
    """
    Returns the schema for the silver GTFS trips table.

    This schema represents standardized GTFS trip information, including
    route linkage, service linkage, trip identifiers, optional headsign and
    short name fields, trip direction, shape linkage and audit metadata.

    Returns:
        StructType: The complete schema definition for the silver trips table.
    """
    return StructType(
        [
            StructField(
                "route_id",
                StringType(),
                True,
                {"comment": "Identifies a route."},
            ),
            StructField(
                "service_id",
                StringType(),
                True,
                {
                    "comment": (
                        "Identifies a set of dates when service is available "
                        "for one or more routes."
                    )
                },
            ),
            StructField(
                "trip_id",
                StringType(),
                True,
                {"comment": "Identifies a trip."},
            ),
            StructField(
                "trip_headsign",
                StringType(),
                True,
                {
                    "comment": (
                        "Text that appears on signage identifying the trip "
                        "destination to riders."
                    )
                },
            ),
            StructField(
                "trip_short_name",
                StringType(),
                True,
                {"comment": "Short text that identifies a trip."},
            ),
            StructField(
                "direction_id",
                IntegerType(),
                True,
                {
                    "comment": (
                        "Indicates the direction of travel for a trip. "
                        "Valid values: 0 = one direction, 1 = opposite direction."
                    )
                },
            ),
            StructField(
                "direction_description",
                StringType(),
                True,
                {
                    "comment": (
                        "Human-readable description derived from direction_id."
                    )
                },
            ),
            StructField(
                "block_id",
                StringType(),
                True,
                {
                    "comment": (
                        "Identifies the block to which the trip belongs, when available."
                    )
                },
            ),
            StructField(
                "shape_id",
                StringType(),
                True,
                {"comment": "Identifies a geospatial shape describing the vehicle travel path for a trip."},
            ),
            StructField(
                "wheelchair_accessible",
                StringType(),
                True,
                {
                    "comment": (
                        "Raw wheelchair accessibility value preserved from GTFS."
                    )
                },
            ),
            StructField(
                "bikes_allowed",
                StringType(),
                True,
                {
                    "comment": (
                        "Raw bikes allowed value preserved from GTFS."
                    )
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {"comment": "Logical GTFS load date extracted from landing metadata."},
            ),
            StructField(
                "gtfs_loaded_at",
                TimestampType(),
                True,
                {"comment": "Timestamp when the GTFS file was ingested into Bronze."},
            ),
            StructField(
                "record_hash",
                StringType(),
                True,
                {"comment": "Technical hash used for lineage and traceability."},
            ),
        ]
    )