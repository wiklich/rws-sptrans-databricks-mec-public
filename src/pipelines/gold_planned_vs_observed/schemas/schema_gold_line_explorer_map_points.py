"""
Schema for gold.gold_line_explorer_map_points.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOLD_LINE_EXPLORER_MAP_POINTS_SCHEMA = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            False,
            metadata={
                "comment": "Observed SPTrans position event identifier."
            },
        ),
        StructField(
            "service_date",
            DateType(),
            False,
            metadata={
                "comment": "Local service date derived from the observed position timestamp."
            },
        ),
        StructField(
            "captured_at_local",
            TimestampType(),
            False,
            metadata={
                "comment": "Local timestamp of the observed vehicle position."
            },
        ),
        StructField(
            "hour",
            IntegerType(),
            False,
            metadata={
                "comment": "Local hour derived from the observed vehicle position."
            },
        ),
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Business-facing line identifier preserved from observed telemetry."
            },
        ),
        StructField(
            "observed_direction",
            IntegerType(),
            False,
            metadata={
                "comment": "Observed raw direction value from SPTrans telemetry."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "Validated analytical GTFS direction identifier after direction mapping."
            },
        ),
        StructField(
            "direction_description",
            StringType(),
            True,
            metadata={
                "comment": "Human-readable direction description associated with the observed telemetry direction."
            },
        ),
        StructField(
            "vehicle_prefix",
            LongType(),
            False,
            metadata={
                "comment": "Observed SPTrans vehicle identifier used for dashboard point exploration."
            },
        ),
        StructField(
            "latitude",
            DoubleType(),
            False,
            metadata={
                "comment": "Latitude of the observed vehicle position."
            },
        ),
        StructField(
            "longitude",
            DoubleType(),
            False,
            metadata={
                "comment": "Longitude of the observed vehicle position."
            },
        ),
        StructField(
            "planned_headway_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Planned hourly headway proxy associated with the observed point after hourly adherence enrichment."
            },
        ),
        StructField(
            "observed_headway_avg_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Observed hourly average headway proxy associated with the observed point after hourly adherence enrichment."
            },
        ),
        StructField(
            "headway_deviation_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Hourly headway deviation in minutes associated with the observed point."
            },
        ),
        StructField(
            "headway_deviation_pct",
            DoubleType(),
            True,
            metadata={
                "comment": "Hourly headway deviation percentage associated with the observed point."
            },
        ),
        StructField(
            "missed_service_flag",
            BooleanType(),
            True,
            metadata={
                "comment": "Indicates whether the matched hourly adherence row is classified as missed service."
            },
        ),
        StructField(
            "adherence_band",
            StringType(),
            False,
            metadata={
                "comment": "Presentation-ready adherence classification for point map coloring."
            },
        ),
        StructField(
            "record_created_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp when the Gold line explorer map point record was materialized."
            },
        ),
        StructField(
            "record_hash",
            StringType(),
            False,
            metadata={
                "comment": "Deterministic technical hash for lineage, change tracking, and point-level comparisons."
            },
        ),
    ]
)
