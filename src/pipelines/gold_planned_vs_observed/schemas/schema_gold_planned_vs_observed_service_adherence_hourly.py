"""
Schema for gold.gold_planned_vs_observed_service_adherence_hourly.
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

GOLD_PLANNED_VS_OBSERVED_SERVICE_ADHERENCE_HOURLY_SCHEMA = StructType(
    [
        StructField(
            "service_date",
            DateType(),
            False,
            metadata={
                "comment": "Local service date used for the hourly planned versus observed service adherence comparison."
            },
        ),
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Observed SPTrans line sign used as the business identifier for the comparison."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "GTFS direction_id associated with the observed direction through the validated direction mapping reference."
            },
        ),
        StructField(
            "hour",
            IntegerType(),
            False,
            metadata={
                "comment": "Local hour bucket used for the planned versus observed comparison."
            },
        ),
        StructField(
            "planned_headway_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Expected interval in minutes between vehicles according to GTFS planned frequency for the line, direction, date, and hour."
            },
        ),
        StructField(
            "observed_event_count_proxy",
            LongType(),
            True,
            metadata={
                "comment": "Count of observed service event proxies detected within the hourly bucket. This is not an official departure count."
            },
        ),
        StructField(
            "observed_vehicle_count",
            LongType(),
            True,
            metadata={
                "comment": "Number of distinct observed vehicles contributing to the hourly observed service proxy."
            },
        ),
        StructField(
            "observed_headway_avg_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Average interval in minutes between consecutive observed service event proxies within the hourly bucket."
            },
        ),
        StructField(
            "observed_headway_p50_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Median interval in minutes between consecutive observed service event proxies within the hourly bucket."
            },
        ),
        StructField(
            "observed_headway_p90_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "90th percentile interval in minutes between consecutive observed service event proxies within the hourly bucket."
            },
        ),
        StructField(
            "headway_deviation_minutes",
            DoubleType(),
            True,
            metadata={
                "comment": "Difference in minutes between observed average headway proxy and planned headway."
            },
        ),
        StructField(
            "headway_deviation_pct",
            DoubleType(),
            True,
            metadata={
                "comment": "Percentage deviation between observed average headway proxy and planned headway."
            },
        ),
        StructField(
            "planned_service_present_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates whether GTFS planning expects service for the line, direction, date, and hour."
            },
        ),
        StructField(
            "observed_service_present_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates whether at least one observed service event proxy exists for the line, direction, date, and hour."
            },
        ),
        StructField(
            "missed_service_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates a candidate missed planned service window where planned service exists but no observed service proxy was detected."
            },
        ),
        StructField(
            "adherence_type",
            StringType(),
            False,
            metadata={
                "comment": "Classification of the row as a proxy-based planned versus observed service adherence record."
            },
        ),
        StructField(
            "adherence_note",
            StringType(),
            False,
            metadata={
                "comment": "Interpretation note clarifying that the dataset is proxy-based and must not be treated as official trip delay."
            },
        ),
        StructField(
            "record_created_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp indicating when the Gold row was materialized."
            },
        ),
    ]
)
