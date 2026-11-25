from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DateType,
    TimestampType,
)


def get_gold_observed_lines_daily_schema() -> StructType:
    """
    Returns the schema for the Gold observed lines daily table.

    This schema represents the canonical observed-side daily line catalog used by
    the Planned vs Observed Service Comparison component.

    The table contains one row per analysis_date and normalized observed line code,
    derived from Silver SPTrans positions, and is intended to be the authoritative
    observed daily reference for downstream comparison against GTFS planned lines.

    Returns:
        StructType: The complete schema definition for the Gold observed lines
        daily table.
    """
    return StructType(
        [
            StructField(
                "analysis_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Local calendar date representing the daily analysis grain "
                        "for the observed line catalog."
                    )
                },
            ),
            StructField(
                "line_code",
                StringType(),
                False,
                {
                    "comment": (
                        "Normalized observed line identifier used as the official "
                        "comparison key in the Planned vs Observed component. "
                        "Generated as UPPER(TRIM(observed_line_sign))."
                    )
                },
            ),
            StructField(
                "observed_line_sign",
                StringType(),
                False,
                {
                    "comment": (
                        "Original observed public line identifier from "
                        "silver_sptrans_positions."
                    )
                },
            ),
            StructField(
                "observed_events_count",
                LongType(),
                True,
                {
                    "comment": (
                        "Number of observed position events contributing to the "
                        "line on the analysis date."
                    )
                },
            ),
            StructField(
                "observed_vehicles_count",
                LongType(),
                True,
                {
                    "comment": (
                        "Number of distinct vehicles contributing to the "
                        "line on the analysis date."
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
                        "analysis date."
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
                        "analysis date."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold observed lines daily record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
