from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DateType,
    IntegerType,
    TimestampType,
)


def get_silver_gtfs_calendar_schema() -> StructType:
    """
    Returns the schema for the silver GTFS calendar table.

    This schema represents the enriched/calendar information from GTFS,
    including the original weekly pattern flags converted to boolean columns,
    service validity period, derived duration metrics and audit/metadata fields.

    Returns:
        StructType: The complete schema definition for the silver calendar table.

    Schema fields:
        - service_id              : str     - Identifies a set of dates when service is available
        - monday_active           : bool    - Boolean helper flag derived from monday
        - tuesday_active          : bool    - Boolean helper flag derived from tuesday
        - wednesday_active        : bool    - Boolean helper flag derived from wednesday
        - thursday_active         : bool    - Boolean helper flag derived from thursday
        - friday_active           : bool    - Boolean helper flag derived from friday
        - saturday_active         : bool    - Boolean helper flag derived from saturday
        - sunday_active           : bool    - Boolean helper flag derived from sunday
        - start_date              : date    - Start service day for the service interval
        - end_date                : date    - End service day (inclusive)
        - service_duration_days   : int     - Number of calendar days in the interval (inclusive)
        - active_weekdays_count   : int     - Count of weekdays marked as active (Mon–Fri pattern)
        - gtfs_load_date          : date    - Logical GTFS reference/load date
        - gtfs_loaded_at          : timestamp - When the GTFS file was ingested into bronze
        - record_hash             : str     - Technical hash for lineage and deduplication
    """
    return StructType(
        [
            StructField(
                "service_id",
                StringType(),
                True,
                {"comment": "Identifies a set of dates when service is available for one or more routes."},
            ),
            StructField(
                "monday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from monday."},
            ),
            StructField(
                "tuesday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from tuesday."},
            ),
            StructField(
                "wednesday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from wednesday."},
            ),
            StructField(
                "thursday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from thursday."},
            ),
            StructField(
                "friday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from friday."},
            ),
            StructField(
                "saturday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from saturday."},
            ),
            StructField(
                "sunday_active",
                BooleanType(),
                True,
                {"comment": "Boolean helper flag derived from sunday."},
            ),
            StructField(
                "start_date",
                DateType(),
                True,
                {"comment": "Start service day for the service interval."},
            ),
            StructField(
                "end_date",
                DateType(),
                True,
                {"comment": "End service day for the service interval. This date is included in the interval."},
            ),
            StructField(
                "service_duration_days",
                IntegerType(),
                True,
                {"comment": "Number of calendar days in the service interval, including start_date and end_date."},
            ),
            StructField(
                "active_weekdays_count",
                IntegerType(),
                True,
                {"comment": "Count of weekdays marked as active in the weekly service pattern."},
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {"comment": "Logical GTFS load date extracted from landing metadata."},
            ),
            StructField(
                "gtfs_loaded_at",
                TimestampType(),
                True,
                {"comment": "Timestamp when the GTFS file was ingested into Bronze."},
            ),
            StructField(
                "record_hash",
                StringType(),
                True,
                {"comment": "Technical hash used for lineage and traceability."},
            ),
        ]
    )