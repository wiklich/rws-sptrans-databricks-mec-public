from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
)


def get_silver_gtfs_agency_schema() -> StructType:
    """
    Returns the schema for the silver GTFS agency table.

    This schema represents agency-level information from the GTFS feed,
    containing identification, naming, website, timezone and language metadata,
    along with ingestion and lineage fields.

    Returns:
        StructType: The complete schema definition for the silver agency table.

    Schema fields:
        - agency_id         : str       - Identifies a transit brand / agency (may have multiple per dataset)
        - agency_name       : str       - Full name of the transit agency
        - agency_url        : str       - URL of the transit agency's website
        - agency_timezone   : str       - Timezone where the agency is located (IANA format recommended)
        - agency_lang       : str       - Primary language used by this agency (ISO 639-1 code recommended)
        - gtfs_load_date    : date      - Logical GTFS reference/load date
        - gtfs_loaded_at    : timestamp - When the GTFS file was ingested into bronze layer
        - record_hash       : str       - Technical hash for lineage, traceability and deduplication
    """
    return StructType(
        [
            StructField(
                "agency_id",
                StringType(),
                True,
                {
                    "comment": (
                        "Identifies a transit brand which is often synonymous with a transit agency. "
                        "A dataset may contain data from multiple agencies."
                    )
                },
            ),
            StructField(
                "agency_name",
                StringType(),
                True,
                {"comment": "Full name of the transit agency."},
            ),
            StructField(
                "agency_url",
                StringType(),
                True,
                {"comment": "URL of the transit agency."},
            ),
            StructField(
                "agency_timezone",
                StringType(),
                True,
                {"comment": "Timezone where the transit agency is located."},
            ),
            StructField(
                "agency_lang",
                StringType(),
                True,
                {
                    "comment": (
                        "Primary language used by this transit agency. "
                        "Helps GTFS consumers choose language-specific rules."
                    )
                },
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