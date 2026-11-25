from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DateType,
    TimestampType,
)


def get_gold_observed_daily_activity_schema() -> StructType:
    """
    Returns the schema for the Gold observed daily activity table.

    This schema represents the canonical daily observed activity layer of the
    Observed Temporal Activity Analysis component.

    The table contains one row per analysis_date, summarizing observed
    operational activity from SPTrans vehicle position data at daily grain.

    Returns:
        StructType: The complete schema definition for the Gold observed
        daily activity table.
    """
    return StructType(
        [
            StructField(
                "analysis_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Local calendar date representing the daily grain of "
                        "the observed activity analysis."
                    )
                },
            ),
            StructField(
                "observed_position_records_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of observed position records captured within "
                        "the analysis_date bucket."
                    )
                },
            ),
            StructField(
                "distinct_observed_vehicles_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct observed vehicles present within "
                        "the analysis_date bucket."
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
                        "present within the analysis_date bucket."
                    )
                },
            ),
            StructField(
                "distinct_observed_hours_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local hours inside the analysis_date "
                        "for which at least one observation exists."
                    )
                },
            ),
            StructField(
                "distinct_observed_minutes_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local minutes inside the analysis_date "
                        "for which at least one observation exists."
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
                        "analysis_date bucket."
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
                        "analysis_date bucket."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold observed daily activity record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
