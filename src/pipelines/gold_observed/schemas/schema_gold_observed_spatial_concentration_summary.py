from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    LongType,
    DoubleType,
    TimestampType,
)


def get_gold_observed_spatial_concentration_summary_schema() -> StructType:
    """
    Returns the schema for the Gold observed spatial concentration summary table.

    This schema represents the canonical daily summary layer of observed spatial
    concentration derived from the detailed spatial distribution Gold table.

    The table contains one row per analysis_date and summarizes how concentrated
    the observed vehicle positions are across active spatial cells.

    Returns:
        StructType: The complete schema definition for the Gold observed spatial
        concentration summary table.
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
                        "of the observed spatial concentration summary."
                    )
                },
            ),
            StructField(
                "active_spatial_cells_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of active spatial cells containing at least one "
                        "observed position record on the analysis date."
                    )
                },
            ),
            StructField(
                "total_observed_position_records_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Total number of observed position records across all "
                        "active spatial cells on the analysis date."
                    )
                },
            ),
            StructField(
                "avg_positions_per_active_cell",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Average number of observed position records per active "
                        "spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "max_positions_in_single_cell",
                LongType(),
                False,
                {
                    "comment": (
                        "Largest observed position-record volume found in a "
                        "single spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "max_distinct_observed_vehicles_in_single_cell",
                LongType(),
                False,
                {
                    "comment": (
                        "Largest number of distinct observed vehicles found in a "
                        "single spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "max_distinct_observed_lines_in_single_cell",
                LongType(),
                False,
                {
                    "comment": (
                        "Largest number of distinct observed lines found in a "
                        "single spatial cell on the analysis date."
                    )
                },
            ),
            StructField(
                "top_cell_position_share_ratio",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Share of all daily observed position records "
                        "concentrated in the most active spatial cell."
                    )
                },
            ),
            StructField(
                "top_5_cells_position_share_ratio",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Share of all daily observed position records "
                        "concentrated in the five most active spatial cells."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold observed spatial concentration "
                        "summary record was materialized."
                    )
                },
            ),
        ]
    )
