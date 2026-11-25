from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_silver_gtfs_shapes_schema() -> StructType:
    """
    Returns the schema for the silver GTFS shapes table.

    This schema represents standardized GTFS shape information, including
    the shape identifier, geographic points, point sequence, traveled distance
    and audit metadata.

    Returns:
        StructType: The complete schema definition for the silver shapes table.

    Schema fields:
        - shape_id             : str       - Identifies a shape
        - shape_pt_lat         : double    - Latitude of a shape point
        - shape_pt_lon         : double    - Longitude of a shape point
        - shape_pt_sequence    : int       - Sequence of shape points along the shape
        - shape_dist_traveled  : double    - Distance traveled along the shape from the first point
        - gtfs_load_date       : date      - Logical GTFS reference/load date
        - gtfs_loaded_at       : timestamp - When the GTFS file was ingested into bronze
        - record_hash          : str       - Technical hash for lineage and traceability
    """
    return StructType(
        [
            StructField(
                "shape_id",
                StringType(),
                True,
                {
                    "comment": "Identifies a shape."
                },
            ),
            StructField(
                "shape_pt_lat",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Latitude of a shape point. Each record in shapes.txt represents "
                        "a shape point used to define the shape."
                    )
                },
            ),
            StructField(
                "shape_pt_lon",
                DoubleType(),
                True,
                {
                    "comment": "Longitude of a shape point."
                },
            ),
            StructField(
                "shape_pt_sequence",
                IntegerType(),
                True,
                {
                    "comment": (
                        "Sequence in which the shape points connect to form the shape. "
                        "Values must increase along the trip but do not need to be consecutive."
                    )
                },
            ),
            StructField(
                "shape_dist_traveled",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Actual distance traveled along the shape from the first shape point "
                        "to the point specified in this record. Values must increase along with "
                        "shape_pt_sequence."
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