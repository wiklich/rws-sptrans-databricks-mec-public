from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    DateType,
    TimestampType,
)


def get_gold_planned_vs_observed_line_status_schema() -> StructType:
    """
    Returns the schema for the Gold planned vs observed line status table.

    This schema represents the canonical detailed comparison output used by the
    Planned vs Observed Service Comparison component.

    The table contains one row per analysis_date and normalized line code,
    classifying whether the line was planned and observed, planned but not
    observed, or observed but not present in the GTFS planned catalog.

    Returns:
        StructType: The complete schema definition for the Gold planned vs
        observed line status table.
    """
    return StructType(
        [
            StructField(
                "analysis_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Local calendar date representing the daily comparison grain."
                    )
                },
            ),
            StructField(
                "line_code",
                StringType(),
                False,
                {
                    "comment": (
                        "Normalized business key used to compare planned and observed "
                        "line catalogs."
                    )
                },
            ),
            StructField(
                "planned_route_id",
                StringType(),
                True,
                {
                    "comment": (
                        "Original GTFS route identifier when the line exists in the "
                        "planned catalog."
                    )
                },
            ),
            StructField(
                "observed_line_sign",
                StringType(),
                True,
                {
                    "comment": (
                        "Original observed public line identifier when the line exists "
                        "in the observed daily catalog."
                    )
                },
            ),
            StructField(
                "is_planned",
                BooleanType(),
                False,
                {
                    "comment": (
                        "Indicates whether the line exists in the GTFS planned catalog."
                    )
                },
            ),
            StructField(
                "is_observed",
                BooleanType(),
                False,
                {
                    "comment": (
                        "Indicates whether the line exists in the observed daily catalog "
                        "for the analysis date."
                    )
                },
            ),
            StructField(
                "match_status",
                StringType(),
                False,
                {
                    "comment": (
                        "Final comparison status for the line on the analysis date. "
                        "Allowed values: PLANNED_AND_OBSERVED, "
                        "PLANNED_NOT_OBSERVED, OBSERVED_NOT_IN_GTFS."
                    )
                },
            ),
            StructField(
                "planned_route_short_name",
                StringType(),
                True,
                {
                    "comment": (
                        "GTFS route short name retained for descriptive enrichment."
                    )
                },
            ),
            StructField(
                "planned_route_long_name",
                StringType(),
                True,
                {
                    "comment": (
                        "GTFS route long name retained for descriptive enrichment."
                    )
                },
            ),
            StructField(
                "planned_route_name_display",
                StringType(),
                True,
                {
                    "comment": (
                        "Display-friendly GTFS route name retained for downstream usage."
                    )
                },
            ),
            StructField(
                "route_type",
                IntegerType(),
                True,
                {
                    "comment": (
                        "GTFS route type classification associated with the planned line."
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
                        "Route color in hexadecimal format inherited from the planned "
                        "reference."
                    )
                },
            ),
            StructField(
                "route_color_name",
                StringType(),
                True,
                {
                    "comment": (
                        "Human-readable name of the route color."
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
                        "Human-readable name of the route text color."
                    )
                },
            ),
            StructField(
                "observed_events_count",
                LongType(),
                True,
                {
                    "comment": (
                        "Number of observed position events contributing to the line on "
                        "the analysis date when the line was observed."
                    )
                },
            ),
            StructField(
                "observed_vehicles_count",
                LongType(),
                True,
                {
                    "comment": (
                        "Number of distinct vehicles contributing to the line on the "
                        "analysis date when the line was observed."
                    )
                },
            ),
            StructField(
                "min_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Earliest local observed timestamp for the line on the "
                        "analysis date when the line was observed."
                    )
                },
            ),
            StructField(
                "max_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Latest local observed timestamp for the line on the "
                        "analysis date when the line was observed."
                    )
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {
                    "comment": (
                        "Logical GTFS load date preserved from the planned reference "
                        "as contextual metadata."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold planned vs observed line status record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
