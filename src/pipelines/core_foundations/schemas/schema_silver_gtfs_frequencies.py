from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)


def get_silver_gtfs_frequencies_schema() -> StructType:
    """
    Returns the schema for the silver GTFS frequencies table.

    This schema represents standardized GTFS frequencies information,
    including trip linkage, GTFS time interval fields, derived second-based
    helper columns, headway interval in seconds and audit metadata.

    Returns:
        StructType: The complete schema definition for the silver frequencies table.
    """
    return StructType(
        [
            StructField(
                "trip_id",
                StringType(),
                True,
                {
                    "comment": "Identifies a trip to which the specified headway of service applies."
                },
            ),
            StructField(
                "start_time",
                StringType(),
                True,
                {
                    "comment": (
                        "Time at which the first vehicle departs from the first stop "
                        "of the trip with the specified headway, in GTFS HH:MM:SS format."
                    )
                },
            ),
            StructField(
                "start_time_seconds",
                IntegerType(),
                True,
                {
                    "comment": (
                        "Start time converted from HH:MM:SS format to total seconds "
                        "since 00:00:00. Supports GTFS extended hours (>24:00:00)."
                    )
                },
            ),
            StructField(
                "start_time_minutes",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Start time converted from seconds to minutes since 00:00:00. "
                        "Supports GTFS extended hours (>24:00:00)."
                    )
                },
            ),
            StructField(
                "end_time",
                StringType(),
                True,
                {
                    "comment": (
                        "Time at which service changes to a different headway or ceases "
                        "at the first stop in the trip, in GTFS HH:MM:SS format."
                    )
                },
            ),
            StructField(
                "end_time_seconds",
                IntegerType(),
                True,
                {
                    "comment": (
                        "End time converted from HH:MM:SS format to total seconds "
                        "since 00:00:00. Supports GTFS extended hours (>24:00:00)."
                    )
                },
            ),
            StructField(
                "end_time_minutes",
                DoubleType(),
                True,
                {
                    "comment": (
                        "End time converted from seconds to minutes since 00:00:00. "
                        "Supports GTFS extended hours (>24:00:00)."
                    )
                },
            ),
            StructField(
                "service_window_seconds",
                IntegerType(),
                True,
                {
                    "comment": (
                        "Duration of the service window in seconds, calculated as "
                        "end_time_seconds minus start_time_seconds."
                    )
                },
            ),
            StructField(
                "service_window_minutes",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Duration of the service window in minutes, calculated as "
                        "end_time_seconds minus start_time_seconds converted to minutes."
                    )
                },
            ),
            StructField(
                "headway_secs",
                IntegerType(),
                True,
                {
                    "comment": (
                        "Time, in seconds, between departures from the same stop for the trip "
                        "during the time interval specified by start_time and end_time."
                    )
                },
            ),
            StructField(
                "headway_minutes",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Headway interval converted from seconds to minutes for easier "
                        "human-readable analysis and visualization."
                    )
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {
                    "comment": "Logical GTFS load date extracted from landing metadata."
                },
            ),
            StructField(
                "gtfs_loaded_at",
                TimestampType(),
                True,
                {
                    "comment": "Timestamp when the GTFS file was ingested into Bronze."
                },
            ),
            StructField(
                "record_hash",
                StringType(),
                True,
                {
                    "comment": "Technical hash used for lineage and traceability."
                },
            ),
        ]
    )