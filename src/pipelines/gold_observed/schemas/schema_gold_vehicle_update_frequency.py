from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def get_gold_vehicle_update_frequency_schema() -> StructType:
    """
    Returns the schema for the Gold vehicle update frequency table.

    This schema represents the canonical vehicle-level update frequency layer
    of the Observed Update Frequency and Continuity Analysis component.

    The table contains one row per vehicle_prefix, summarizing how frequently
    each observed vehicle is updated in the dataset based on consecutive local
    timestamp differences.

    Returns:
        StructType: The complete schema definition for the Gold vehicle update
        frequency table.
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
                        "observed vehicle in the update frequency analysis."
                    )
                },
            ),
            StructField(
                "updates_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of observed position records available for the "
                        "vehicle within the captured observation window."
                    )
                },
            ),
            StructField(
                "measured_intervals_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of valid measurable update intervals derived "
                        "from consecutive local timestamps of the same vehicle."
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
                        "updates of the same vehicle."
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
                        "updates of the same vehicle."
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
                        "updates of the same vehicle."
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
                        "intervals in seconds for the vehicle."
                    )
                },
            ),
            StructField(
                "first_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Earliest observed local timestamp for the vehicle "
                        "within the captured observation window."
                    )
                },
            ),
            StructField(
                "last_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Latest observed local timestamp for the vehicle "
                        "within the captured observation window."
                    )
                },
            ),
            StructField(
                "observed_span_seconds",
                LongType(),
                True,
                {
                    "comment": (
                        "Total observed timespan in seconds between the first "
                        "and last local timestamps of the vehicle."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold vehicle update frequency "
                        "record was materialized."
                    )
                },
            ),
        ]
    )
