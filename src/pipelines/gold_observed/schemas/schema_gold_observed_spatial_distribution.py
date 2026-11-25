from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)


def get_gold_observed_spatial_distribution_schema() -> StructType:
    """
    Returns the schema for the Gold observed spatial distribution table.

    This schema represents the canonical daily spatial distribution layer of
    the Observed Spatial Analysis component.

    The table contains one row per analysis_date and spatial_cell_id,
    summarizing observed vehicle position activity in a standardized spatial
    grid bucket.

    Returns:
        StructType: The complete schema definition for the Gold observed
        spatial distribution table.
    """
    return StructType(
        [
            StructField(
                "analysis_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Local calendar date representing the daily dimension "
                        "of the observed spatial distribution analysis."
                    )
                },
            ),
            StructField(
                "spatial_cell_id",
                StringType(),
                False,
                {
                    "comment": (
                        "Canonical spatial aggregation key derived from "
                        "latitude and longitude using the approved fixed-grid "
                        "encoding rule."
                    )
                },
            ),
            StructField(
                "spatial_resolution",
                StringType(),
                False,
                {
                    "comment": (
                        "Identifier of the spatial grid resolution used to "
                        "derive spatial_cell_id."
                    )
                },
            ),
            StructField(
                "cell_center_latitude",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Representative latitude of the fixed grid cell center "
                        "used for visualization and map display."
                    )
                },
            ),
            StructField(
                "cell_center_longitude",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Representative longitude of the fixed grid cell center "
                        "used for visualization and map display."
                    )
                },
            ),
            StructField(
                "observed_position_records_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of observed vehicle position records inside the "
                        "spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "distinct_observed_vehicles_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct observed vehicles present in the "
                        "spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "distinct_observed_lines_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct observed public line identifiers "
                        "present in the spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "min_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Earliest observed local timestamp inside the spatial "
                        "cell on the analysis date."
                    )
                },
            ),
            StructField(
                "max_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Latest observed local timestamp inside the spatial "
                        "cell on the analysis date."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold observed spatial distribution "
                        "record was materialized."
                    )
                },
            ),
        ]
    )
