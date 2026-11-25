from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def get_gold_vehicle_observation_continuity_schema() -> StructType:
    """
    Returns the schema for the Gold vehicle observation continuity table.

    This schema represents the canonical vehicle-level observation continuity
    layer of the Observed Update Frequency and Continuity Analysis component.

    The table contains one row per vehicle_prefix, summarizing how continuously
    each observed vehicle appears in the dataset within the captured observation
    window.

    Returns:
        StructType: The complete schema definition for the Gold vehicle
        observation continuity table.
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
                        "observed vehicle in the observation continuity analysis."
                    )
                },
            ),
            StructField(
                "observations_count",
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
                "measured_gaps_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of valid measurable gaps derived from "
                        "consecutive local timestamps of the same vehicle."
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
                "distinct_minutes_with_data",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local minute buckets for which at "
                        "least one observation exists for the vehicle."
                    )
                },
            ),
            StructField(
                "distinct_hours_with_data",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct local hour buckets for which at "
                        "least one observation exists for the vehicle."
                    )
                },
            ),
            StructField(
                "avg_gap_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Average measurable time gap in seconds between "
                        "consecutive observations of the same vehicle."
                    )
                },
            ),
            StructField(
                "max_gap_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Largest measurable time gap in seconds between "
                        "consecutive observations of the same vehicle."
                    )
                },
            ),
            StructField(
                "large_gaps_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of measurable gaps whose duration exceeds the "
                        "configured large-gap threshold."
                    )
                },
            ),
            StructField(
                "continuity_support_ratio",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Approximate ratio between minutes with observed data "
                        "and the total minute span covered by the vehicle's "
                        "observed window."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold vehicle observation continuity "
                        "record was materialized."
                    )
                },
            ),
        ]
    )
