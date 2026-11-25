from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def get_gold_system_wide_update_behavior_schema() -> StructType:
    """
    Returns the schema for the Gold system-wide update behavior table.

    This schema represents the canonical system-wide update behavior layer of
    the Observed Update Frequency and Continuity Analysis component.

    The table contains one row per declared aggregation scope. For the first
    version, the recommended grain is one row for the full observed dataset
    window.

    Returns:
        StructType: The complete schema definition for the Gold system-wide
        update behavior table.
    """
    return StructType(
        [
            StructField(
                "aggregation_scope",
                StringType(),
                False,
                {
                    "comment": (
                        "Identifier of the declared system-wide aggregation "
                        "scope used by the table. In the first version, this "
                        "represents the full observed dataset window."
                    )
                },
            ),
            StructField(
                "vehicles_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct vehicles contributing to the "
                        "system-wide summary."
                    )
                },
            ),
            StructField(
                "total_observations_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Total number of observed position records "
                        "contributing to the system-wide summary."
                    )
                },
            ),
            StructField(
                "total_measured_intervals_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Total number of measurable update intervals "
                        "contributing to the system-wide summary."
                    )
                },
            ),
            StructField(
                "system_avg_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "System-wide weighted average observed update "
                        "interval in seconds, using measured interval count "
                        "as support weight."
                    )
                },
            ),
            StructField(
                "system_min_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Shortest observed vehicle-level update interval in "
                        "seconds contributing to the system-wide summary."
                    )
                },
            ),
            StructField(
                "system_max_update_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Largest observed vehicle-level update interval in "
                        "seconds contributing to the system-wide summary."
                    )
                },
            ),
            StructField(
                "system_avg_vehicle_stddev_interval_seconds",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Average vehicle-level interval variability in "
                        "seconds across the observed fleet."
                    )
                },
            ),
            StructField(
                "total_distinct_minutes_with_data",
                LongType(),
                False,
                {
                    "comment": (
                        "Aggregate minute-level support indicator derived "
                        "from lower-level observation continuity records."
                    )
                },
            ),
            StructField(
                "avg_continuity_support_ratio",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Average vehicle-level continuity support ratio "
                        "across the observed fleet."
                    )
                },
            ),
            StructField(
                "total_large_gaps_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Total number of large measured gaps contributing "
                        "to the system-wide continuity interpretation."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold system-wide update behavior "
                        "record was materialized."
                    )
                },
            ),
        ]
    )
