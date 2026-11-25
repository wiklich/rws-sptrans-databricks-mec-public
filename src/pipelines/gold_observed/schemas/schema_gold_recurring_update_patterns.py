from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
)


def get_gold_recurring_update_patterns_schema() -> StructType:
    """
    Returns the schema for the Gold recurring update patterns table.

    This schema represents the canonical recurring-pattern layer of the
    Observed Update Frequency and Continuity Analysis component.

    The table contains one row per recurring temporal bucket, using the
    recommended first-version grain of day_of_week + hour_of_day.

    Returns:
        StructType: The complete schema definition for the Gold recurring
        update patterns table.
    """
    return StructType(
        [
            StructField(
                "day_of_week",
                IntegerType(),
                False,
                {
                    "comment": (
                        "Local day-of-week bucket derived from captured_at_local "
                        "using Spark dayofweek semantics "
                        "(1 = Sunday, 2 = Monday, ..., 7 = Saturday)."
                    )
                },
            ),
            StructField(
                "hour_of_day",
                IntegerType(),
                False,
                {
                    "comment": (
                        "Local hour-of-day bucket derived from captured_at_local "
                        "using 24-hour clock semantics (0-23)."
                    )
                },
            ),
            StructField(
                "observations_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of observed records contributing to the recurring "
                        "temporal bucket."
                    )
                },
            ),
            StructField(
                "measured_intervals_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of valid measurable update intervals assigned to "
                        "the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "avg_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Average observed time in seconds between consecutive "
                        "updates contributing to the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "min_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Shortest observed time in seconds between consecutive "
                        "updates contributing to the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "max_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Longest observed time in seconds between consecutive "
                        "updates contributing to the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "stddev_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Population standard deviation of observed update "
                        "intervals in seconds for the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "distinct_vehicles_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct vehicles contributing to the "
                        "recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "distinct_dates_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local dates represented in the "
                        "recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "avg_intervals_per_vehicle",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Average number of measurable intervals contributed per "
                        "vehicle in the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "avg_intervals_per_date",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Average number of measurable intervals contributed per "
                        "observed date in the recurring temporal bucket."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold recurring update pattern record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
