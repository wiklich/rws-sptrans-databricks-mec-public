from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DateType,
    TimestampType,
)


def get_gold_observed_intraday_hourly_activity_schema() -> StructType:
    """
    Returns the schema for the Gold observed intraday hourly activity table.

    This schema represents the canonical intraday observed activity layer of the
    Observed Temporal Activity Analysis component.

    The table contains one row per analysis_date and analysis_hour_local,
    summarizing observed operational activity from SPTrans vehicle position data
    within the captured observation window.

    Returns:
        StructType: The complete schema definition for the Gold observed
        intraday hourly activity table.
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
                        "of the intraday observed activity analysis."
                    )
                },
            ),
            StructField(
                "analysis_hour_local",
                IntegerType(),
                False,
                {
                    "comment": (
                        "Local hour of day representing the hourly grain of the "
                        "observed activity analysis."
                    )
                },
            ),
            StructField(
                "observed_position_records_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of observed position records captured within the "
                        "analysis_date and analysis_hour_local bucket."
                    )
                },
            ),
            StructField(
                "distinct_observed_vehicles_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct observed vehicles present within the "
                        "analysis_date and analysis_hour_local bucket."
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
                        "present within the analysis_date and analysis_hour_local bucket."
                    )
                },
            ),
            StructField(
                "distinct_minutes_with_data",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local minutes inside the hour for "
                        "which at least one observation exists."
                    )
                },
            ),
            StructField(
                "min_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Earliest observed local timestamp within the "
                        "analysis_date and analysis_hour_local bucket."
                    )
                },
            ),
            StructField(
                "max_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Latest observed local timestamp within the "
                        "analysis_date and analysis_hour_local bucket."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold observed intraday hourly "
                        "activity record was materialized."
                    )
                },
            ),
        ]
    )
