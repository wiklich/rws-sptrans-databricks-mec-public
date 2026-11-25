from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
)


def get_gold_planned_lines_reference_schema() -> StructType:
    """
    Returns the schema for the Gold planned lines reference table.

    This schema represents the canonical planned-side line catalog used by the
    Planned vs Observed Service Comparison component.

    The table contains one row per normalized public line code derived from
    Silver GTFS routes and is intended to be the authoritative planned
    reference for downstream comparison against observed SPTrans line codes.

    Returns:
        StructType: The complete schema definition for the Gold planned lines
        reference table.
    """
    return StructType(
        [
            StructField(
                "line_code",
                StringType(),
                False,
                {
                    "comment": (
                        "Normalized planned line identifier used as the official "
                        "comparison key in the Planned vs Observed component. "
                        "Generated as UPPER(TRIM(planned_route_id))."
                    )
                },
            ),
            StructField(
                "planned_route_id",
                StringType(),
                False,
                {
                    "comment": (
                        "Original GTFS route identifier representing the planned-side "
                        "public transit line code."
                    )
                },
            ),
            StructField(
                "planned_route_short_name",
                StringType(),
                True,
                {
                    "comment": (
                        "GTFS route short name retained for descriptive enrichment "
                        "and business readability."
                    )
                },
            ),
            StructField(
                "planned_route_long_name",
                StringType(),
                True,
                {
                    "comment": (
                        "GTFS route long name retained for descriptive enrichment "
                        "and auditability."
                    )
                },
            ),
            StructField(
                "planned_route_name_display",
                StringType(),
                True,
                {
                    "comment": (
                        "Display-friendly route name derived in Silver for "
                        "downstream analytical and business usage."
                    )
                },
            ),
            StructField(
                "route_type",
                IntegerType(),
                True,
                {
                    "comment": (
                        "GTFS route type classification describing the transportation "
                        "mode associated with the planned route."
                    )
                },
            ),
            StructField(
                "route_type_description",
                StringType(),
                True,
                {
                    "comment": (
                        "Human-readable description of the GTFS route type."
                    )
                },
            ),
            StructField(
                "route_color",
                StringType(),
                True,
                {
                    "comment": (
                        "Route color in hexadecimal format inherited from Silver."
                    )
                },
            ),
            StructField(
                "route_color_name",
                StringType(),
                True,
                {
                    "comment": (
                        "Human-readable route color name derived from route_color."
                    )
                },
            ),
            StructField(
                "route_text_color",
                StringType(),
                True,
                {
                    "comment": (
                        "Text color for route labels in hexadecimal format."
                    )
                },
            ),
            StructField(
                "route_text_color_name",
                StringType(),
                True,
                {
                    "comment": (
                        "Human-readable text color name derived from route_text_color."
                    )
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {
                    "comment": (
                        "Logical GTFS load date inherited from Silver and used as "
                        "the planned reference date."
                    )
                },
            ),
            StructField(
                "gtfs_loaded_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the GTFS source record was ingested into Bronze "
                        "and preserved through Silver."
                    )
                },
            ),
            StructField(
                "record_hash",
                StringType(),
                True,
                {
                    "comment": (
                        "Technical hash preserved from upstream layers for lineage "
                        "and traceability."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold planned lines reference record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
