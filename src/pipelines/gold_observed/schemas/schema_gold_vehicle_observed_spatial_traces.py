from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def get_gold_vehicle_observed_spatial_traces_schema() -> StructType:
    """
    Returns the schema for the Gold vehicle observed spatial traces table.

    This schema represents the canonical observed spatial trace layer of the
    Observed Spatial Analysis component.

    The table contains one row per analysis_date, vehicle_prefix, and
    trace_sequence, preserving the ordered observed spatial points of each
    vehicle-day trace.

    Returns:
        StructType: The complete schema definition for the Gold vehicle
        observed spatial traces table.
    """
    return StructType(
        [
            StructField(
                "analysis_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Local calendar date representing the daily partition "
                        "and analytical context of the observed trace point."
                    )
                },
            ),
            StructField(
                "vehicle_prefix",
                StringType(),
                False,
                {
                    "comment": (
                        "Observed vehicle identifier used to group observed "
                        "positions into a vehicle-day trace."
                    )
                },
            ),
            StructField(
                "trace_sequence",
                LongType(),
                False,
                {
                    "comment": (
                        "Stable ordered position of the observed trace point "
                        "within the analysis_date + vehicle_prefix sequence."
                    )
                },
            ),
            StructField(
                "captured_at_local",
                TimestampType(),
                False,
                {
                    "comment": (
                        "Observed local timestamp of the trace point used for "
                        "trace sequencing."
                    )
                },
            ),
            StructField(
                "latitude",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Observed latitude coordinate of the vehicle trace point."
                    )
                },
            ),
            StructField(
                "longitude",
                DoubleType(),
                False,
                {
                    "comment": (
                        "Observed longitude coordinate of the vehicle trace point."
                    )
                },
            ),
            StructField(
                "line_sign",
                StringType(),
                True,
                {
                    "comment": (
                        "Observed public line identifier associated with the "
                        "trace point."
                    )
                },
            ),
            StructField(
                "direction",
                StringType(),
                True,
                {
                    "comment": (
                        "Observed direction context associated with the trace point."
                    )
                },
            ),
            StructField(
                "direction_description",
                StringType(),
                True,
                {
                    "comment": (
                        "Human-readable observed direction context associated "
                        "with the trace point."
                    )
                },
            ),
            StructField(
                "previous_captured_at_local",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Previous observed local timestamp for the same "
                        "analysis_date + vehicle_prefix trace."
                    )
                },
            ),
            StructField(
                "previous_latitude",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Latitude of the previous observed trace point for the "
                        "same analysis_date + vehicle_prefix trace."
                    )
                },
            ),
            StructField(
                "previous_longitude",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Longitude of the previous observed trace point for the "
                        "same analysis_date + vehicle_prefix trace."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold observed spatial trace record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
