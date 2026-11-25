from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_silver_gtfs_stops_schema() -> StructType:
    """
    Returns the schema for the silver GTFS stops table.

    This schema represents standardized GTFS stop information, including
    the stop identifier, rider-facing stop name, optional stop description,
    geographic coordinates and audit metadata.

    Returns:
        StructType: The complete schema definition for the silver stops table.

    Schema fields:
        - stop_id         : str       - Identifies a location
        - stop_name       : str       - Name of the location
        - stop_desc       : str       - Optional description of the location
        - stop_lat        : double    - Latitude of the location
        - stop_lon        : double    - Longitude of the location
        - gtfs_load_date  : date      - Logical GTFS reference/load date
        - gtfs_loaded_at  : timestamp - When the GTFS file was ingested into bronze
        - record_hash     : str       - Technical hash for lineage and traceability
    """
    return StructType(
        [
            StructField(
                "stop_id",
                StringType(),
                True,
                {
                    "comment": (
                        "Identifies a location. Multiple routes may use the same stop_id."
                    )
                },
            ),
            StructField(
                "stop_name",
                StringType(),
                True,
                {
                    "comment": (
                        "Name of the location. Should match the agency rider-facing "
                        "name for the location."
                    )
                },
            ),
            StructField(
                "stop_desc",
                StringType(),
                True,
                {
                    "comment": (
                        "Description of the location that provides useful, quality "
                        "information. Should not duplicate stop_name."
                    )
                },
            ),
            StructField(
                "stop_lat",
                DoubleType(),
                True,
                {
                    "comment": "Latitude of the location."
                },
            ),
            StructField(
                "stop_lon",
                DoubleType(),
                True,
                {
                    "comment": "Longitude of the location."
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {
                    "comment": "Logical GTFS load date extracted from landing metadata."
                },
            ),
            StructField(
                "gtfs_loaded_at",
                TimestampType(),
                True,
                {
                    "comment": "Timestamp when the GTFS file was ingested into Bronze."
                },
            ),
            StructField(
                "record_hash",
                StringType(),
                True,
                {
                    "comment": "Technical hash used for lineage and traceability."
                },
            ),
        ]
    )