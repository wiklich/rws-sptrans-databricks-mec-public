"""
Schema for gold.gold_direction_mapping_reference.

This Gold table provides a validated analytical mapping between:
- observed SPTrans direction values
- GTFS direction_id values

Grain:
    One row per:
        line_sign + observed_direction

Important:
    This table is an analytical direction reference.
    It is not an official operational truth table.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOLD_DIRECTION_MAPPING_REFERENCE_SCHEMA = StructType(
    [
        StructField(
            "line_sign",
            StringType(),
            False,
            metadata={
                "comment": "Observed SPTrans line sign used as the business identifier for the mapping."
            },
        ),
        StructField(
            "observed_direction",
            IntegerType(),
            False,
            metadata={
                "comment": "Observed direction value from SPTrans operational data."
            },
        ),
        StructField(
            "gtfs_direction_id",
            IntegerType(),
            False,
            metadata={
                "comment": "Recommended GTFS direction_id associated with the observed direction."
            },
        ),
        StructField(
            "evidence_type",
            StringType(),
            False,
            metadata={
                "comment": "Primary analytical basis used to support the mapping, such as spatial endpoint inference."
            },
        ),
        StructField(
            "evidence_score",
            DoubleType(),
            True,
            metadata={
                "comment": "Analytical score representing how strongly the observed endpoint behavior supports the selected GTFS direction candidate."
            },
        ),
        StructField(
            "mapping_confidence",
            StringType(),
            False,
            metadata={
                "comment": "Categorical interpretation of evidence quality. Expected values include high, moderate, low, or none."
            },
        ),
        StructField(
            "line_category",
            StringType(),
            True,
            metadata={
                "comment": "Optional descriptive category used to label the line or mapping scenario."
            },
        ),
        StructField(
            "dist_first_km",
            DoubleType(),
            True,
            metadata={
                "comment": "Distance in kilometers between the observed first-position pattern and the GTFS first-stop endpoint candidate."
            },
        ),
        StructField(
            "dist_last_km",
            DoubleType(),
            True,
            metadata={
                "comment": "Distance in kilometers between the observed last-position pattern and the GTFS last-stop endpoint candidate."
            },
        ),
        StructField(
            "vehicle_count",
            LongType(),
            False,
            metadata={
                "comment": "Number of distinct observed vehicles contributing to the spatial endpoint pattern for the row."
            },
        ),
        StructField(
            "low_sample_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates that the mapping is based on too few observed vehicles to be considered robust."
            },
        ),
        StructField(
            "direction_collision_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates a collision or consistency problem in the direction assignment logic."
            },
        ),
        StructField(
            "review_required_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates whether the row should be excluded from production-safe downstream analytical joins unless manually reviewed."
            },
        ),
        StructField(
            "validated_flag",
            BooleanType(),
            False,
            metadata={
                "comment": "Indicates whether the mapping is considered acceptable for downstream production joins."
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