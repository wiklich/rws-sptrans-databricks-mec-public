from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
)


def get_silver_gtfs_routes_schema() -> StructType:
    """
    Returns the schema for the silver GTFS routes table.

    This schema contains route-level information from the GTFS feed (routes.txt),
    enriched with display-friendly names, human-readable route type descriptions,
    color information (both hex and named), and ingestion/lineage metadata.

    Returns:
        StructType: The complete schema definition for the silver routes table.

    Schema fields:
        - route_id              : str   - Unique identifier of the route (GTFS routes.txt primary key)
        - agency_id             : str   - Identifier of the agency operating this route
        - route_short_name      : str   - Short name of the route (e.g. line number, code)
        - route_long_name       : str   - Full descriptive name of the route
        - route_name_display    : str   - Display-friendly name (combination or formatted version of short + long name)
        - route_type            : int   - GTFS route type code (0 = tram, 1 = subway, 2 = rail, 3 = bus, etc.)
        - route_type_description: str   - Human-readable description of the route_type value
        - route_color           : str   - Route color in hexadecimal format (RRGGBB)
        - route_color_name      : str   - Human-readable name of the color derived from route_color
        - route_text_color      : str   - Text color for route labels in hexadecimal format
        - route_text_color_name : str   - Human-readable name of the text color
        - gtfs_load_date        : date  - Logical GTFS reference/load date
        - gtfs_loaded_at        : timestamp - Timestamp when the GTFS file was ingested into bronze
        - record_hash           : str   - Technical hash for lineage, traceability and deduplication
    """
    return StructType([
        StructField(
            "route_id",
            StringType(),
            True,
            {"comment": "Unique identifier of the route (GTFS routes.txt primary key)"},
        ),
        StructField(
            "agency_id",
            StringType(),
            True,
            {"comment": "Identifier of the agency operating the route"},
        ),
        StructField(
            "route_short_name",
            StringType(),
            True,
            {"comment": "Short name of the route (e.g., line number)"},
        ),
        StructField(
            "route_long_name",
            StringType(),
            True,
            {"comment": "Full descriptive name of the route"},
        ),
        StructField(
            "route_name_display",
            StringType(),
            True,
            {"comment": "Display-friendly route name combining short and long names"},
        ),
        StructField(
            "route_type",
            IntegerType(),
            True,
            {"comment": "GTFS route type code"},
        ),
        StructField(
            "route_type_description",
            StringType(),
            True,
            {"comment": "Human-readable description of route_type"},
        ),
        StructField(
            "route_color",
            StringType(),
            True,
            {"comment": "Route color in hexadecimal format (RRGGBB)"},
        ),
        StructField(
            "route_color_name",
            StringType(),
            True,
            {"comment": "Human-readable color name derived from route_color"},
        ),
        StructField(
            "route_text_color",
            StringType(),
            True,
            {"comment": "Text color for route display in hexadecimal format"},
        ),
        StructField(
            "route_text_color_name",
            StringType(),
            True,
            {"comment": "Human-readable color name derived from route_text_color"},
        ),
        StructField(
            "gtfs_load_date",
            DateType(),
            True,
            {"comment": "Date when GTFS file was loaded"},
        ),
        StructField(
            "gtfs_loaded_at",
            TimestampType(),
            True,
            {"comment": "Timestamp when GTFS data was ingested"},
        ),
        StructField(
            "record_hash",
            StringType(),
            True,
            {"comment": "Hash used for deduplication and change tracking"},
        ),
    ])