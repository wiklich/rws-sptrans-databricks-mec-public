from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    DateType,
    TimestampType,
)


def get_gold_vehicle_multi_day_continuity_schema() -> StructType:
    """
    Returns the schema for the Gold vehicle multi-day continuity table.

    This schema represents the canonical multi-day vehicle continuity layer of
    the Observed Update Frequency and Continuity Analysis component.

    The table contains one row per vehicle_prefix, summarizing how consistently
    each observed vehicle appears across multiple observed dates in the dataset.

    Returns:
        StructType: The complete schema definition for the Gold vehicle
        multi-day continuity table.
    """
    return StructType(
        [
            StructField(
                "vehicle_prefix",
                StringType(),
                False,
                {
                    "comment": (
                        "Vehicle-level analytical key used to represent one "
                        "observed vehicle in the multi-day continuity analysis."
                    )
                },
            ),
            StructField(
                "observed_dates_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local dates on which the vehicle "
                        "appears in the dataset."
                    )
                },
            ),
            StructField(
                "first_observed_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Earliest local date on which the vehicle is observed."
                    )
                },
            ),
            StructField(
                "last_observed_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Latest local date on which the vehicle is observed."
                    )
                },
            ),
            StructField(
                "observed_date_span_days",
                LongType(),
                False,
                {
                    "comment": (
                        "Total calendar-day span between first_observed_date "
                        "and last_observed_date, inclusive."
                    )
                },
            ),
            StructField(
                "missing_dates_within_span_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of calendar dates inside the vehicle's "
                        "observed span for which no observation exists."
                    )
                },
            ),
            StructField(
                "date_presence_ratio",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Proportion of dates with observations relative to the "
                        "full calendar span between first_observed_date and "
                        "last_observed_date."
                    )
                },
            ),
            StructField(
                "first_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Earliest local timestamp observed for the vehicle "
                        "across the full dataset."
                    )
                },
            ),
            StructField(
                "last_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Latest local timestamp observed for the vehicle "
                        "across the full dataset."
                    )
                },
            ),
            StructField(
                "total_observations_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Total number of observed position records for the "
                        "vehicle across all observed dates."
                    )
                },
            ),
            StructField(
                "avg_observations_per_observed_date",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Average number of observations for the vehicle on "
                        "dates where it was present."
                    )
                },
            ),
            StructField(
                "max_consecutive_observed_dates",
                LongType(),
                False,
                {
                    "comment": (
                        "Longest sequence of consecutive calendar dates on "
                        "which the vehicle was observed."
                    )
                },
            ),
            StructField(
                "max_consecutive_missing_dates_within_span",
                LongType(),
                False,
                {
                    "comment": (
                        "Longest sequence of consecutive missing calendar dates "
                        "inside the observed span of the vehicle."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold vehicle multi-day continuity "
                        "record was materialized."
                    )
                },
            ),
        ]
    )
