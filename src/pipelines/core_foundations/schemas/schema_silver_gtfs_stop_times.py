from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    BooleanType,
    DateType,
)


def get_silver_gtfs_stop_times_schema() -> StructType:
    """
    Returns the schema for the silver layer GTFS stop_times table.

    This schema represents a cleaned and enriched version of the GTFS stop_times.txt file,
    including original time fields, time converted to seconds (to handle >24:00:00 times),
    a data quality flag for time consistency, and key linking fields.

    Returns:
        StructType: The complete Spark schema for the silver stop_times table.

    Schema fields:
        - trip_id                : str   - Unique identifier of the trip (foreign key to trips)
        - arrival_time           : str   - Arrival time at the stop in GTFS format (HH:MM:SS), may exceed 24:00:00
        - departure_time         : str   - Departure time from the stop in GTFS format (HH:MM:SS), may exceed 24:00:00
        - arrival_time_seconds   : int   - Arrival time converted to seconds since midnight (supports overnight trips)
        - departure_time_seconds : int   - Departure time converted to seconds since midnight (supports overnight trips)
        - is_valid_time_order    : bool  - True if arrival_time_seconds ≤ departure_time_seconds (data quality indicator)
        - stop_id                : str   - Unique identifier of the stop (foreign key to stops)
        - stop_sequence          : int   - Sequential order of the stop within the trip (must be increasing)
        - gtfs_load_date         : date  - Logical GTFS load date associated with the trip terminal snapshot

    Notes:
        - Time fields are kept as strings to preserve the exact GTFS format
        - *_seconds fields enable easier time arithmetic and filtering
        - is_valid_time_order helps identify potential data quality issues without rejecting records
    """
    return StructType(
        [
            StructField(
                "trip_id",
                StringType(),
                True,
                {"comment": "Identifies a trip."},
            ),
            StructField(
                "arrival_time",
                StringType(),
                True,
                {
                    "comment": (
                        "Arrival time at the stop for a specific trip in GTFS time format. "
                        "Times after midnight on the service day may be greater than 24:00:00."
                    )
                },
            ),
            StructField(
                "departure_time",
                StringType(),
                True,
                {
                    "comment": (
                        "Departure time from the stop for a specific trip in GTFS time format. "
                        "Times after midnight on the service day may be greater than 24:00:00."
                    )
                },
            ),
            StructField(
                "arrival_time_seconds",
                IntegerType(),
                True,
                metadata={
                    "comment": "Arrival time converted from HH:MM:SS format to total seconds since 00:00:00. Supports GTFS extended hours (>24:00:00)."
                },
            ),
            StructField(
                "departure_time_seconds",
                IntegerType(),
                True,
                metadata={
                    "comment": "Departure time converted from HH:MM:SS format to total seconds since 00:00:00. Supports GTFS extended hours (>24:00:00)."
                },
            ),
            StructField(
                "is_valid_time_order",
                BooleanType(),
                True,
                metadata={
                    "comment": "Indicates whether arrival_time_seconds is less than or equal to departure_time_seconds. Used for data quality validation; does not imply data rejection."
                },
            ),
            StructField(
                "stop_id",
                StringType(),
                True,
                {"comment": "Identifies the serviced stop."},
            ),
            StructField(
                "stop_sequence",
                IntegerType(),
                True,
                {
                    "comment": (
                        "Order of stops for a particular trip. "
                        "Values must increase along the trip but do not need to be consecutive."
                    )
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {"comment": "Logical GTFS load date associated with the trip terminal snapshot."},
            ),
        ]
    )