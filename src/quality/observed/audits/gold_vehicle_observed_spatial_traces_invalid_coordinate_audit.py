"""
Gold vehicle observed spatial traces invalid coordinate trace audit pipeline.

Purpose:
    Identify trace rows with suspicious coordinates in
    gold_vehicle_observed_spatial_traces.

Audit focus:
    - latitude or longitude outside valid geographic range
    - missing coordinates
    - implausible trace-point values after filtering

Why this audit matters:
    This audit helps validate that the observed trace layer preserves only
    structurally plausible coordinate points for downstream interpretation
    and trace visualization.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOLD_SOURCE_TABLE = "main.gold.gold_vehicle_observed_spatial_traces"
OUTPUT_TABLE_NAME = "gold.gold_vehicle_observed_spatial_traces_invalid_coordinate_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying suspicious coordinate rows in "
        "gold_vehicle_observed_spatial_traces, including missing coordinates, "
        "out-of-range coordinates, NaN values, and implausible zero-point "
        "trace coordinates."
    ),
)
def gold_vehicle_observed_spatial_traces_invalid_coordinate_audit() -> DataFrame:
    """
    Materialize the invalid coordinate trace audit for
    gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit rows for suspicious coordinate trace points.
    """
    source_df = spark.read.table(GOLD_SOURCE_TABLE)
    return _build_gold_vehicle_observed_spatial_traces_invalid_coordinate_audit(
        source_df
    )


def _build_gold_vehicle_observed_spatial_traces_invalid_coordinate_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the invalid coordinate trace audit dataset.

    Processing steps:
        1. Read gold_vehicle_observed_spatial_traces.
        2. Flag missing coordinate values.
        3. Flag NaN coordinate values.
        4. Flag out-of-range geographic coordinates.
        5. Flag implausible zero-point coordinates.
        6. Keep only rows that match at least one invalid-coordinate condition.

    Args:
        source_df:
            Input DataFrame from gold.gold_vehicle_observed_spatial_traces.

    Returns:
        DataFrame:
            Audit DataFrame containing suspicious coordinate trace rows.
    """
    audit_df = (
        source_df.withColumn(
            "has_null_latitude",
            F.col("latitude").isNull(),
        )
        .withColumn(
            "has_null_longitude",
            F.col("longitude").isNull(),
        )
        .withColumn(
            "has_nan_latitude",
            F.isnan("latitude"),
        )
        .withColumn(
            "has_nan_longitude",
            F.isnan("longitude"),
        )
        .withColumn(
            "has_latitude_out_of_range",
            F.col("latitude").isNotNull()
            & ~F.isnan("latitude")
            & ~F.col("latitude").between(-90.0, 90.0),
        )
        .withColumn(
            "has_longitude_out_of_range",
            F.col("longitude").isNotNull()
            & ~F.isnan("longitude")
            & ~F.col("longitude").between(-180.0, 180.0),
        )
        .withColumn(
            "has_implausible_zero_point_coordinate",
            F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
            & ~F.isnan("latitude")
            & ~F.isnan("longitude")
            & (F.col("latitude") == F.lit(0.0))
            & (F.col("longitude") == F.lit(0.0)),
        )
        .withColumn(
            "has_invalid_coordinate_trace",
            F.col("has_null_latitude")
            | F.col("has_null_longitude")
            | F.col("has_nan_latitude")
            | F.col("has_nan_longitude")
            | F.col("has_latitude_out_of_range")
            | F.col("has_longitude_out_of_range")
            | F.col("has_implausible_zero_point_coordinate"),
        )
        .withColumn(
            "audit_reason",
            F.concat_ws(
                ", ",
                F.when(F.col("has_null_latitude"), F.lit("NULL_LATITUDE")),
                F.when(F.col("has_null_longitude"), F.lit("NULL_LONGITUDE")),
                F.when(F.col("has_nan_latitude"), F.lit("NAN_LATITUDE")),
                F.when(F.col("has_nan_longitude"), F.lit("NAN_LONGITUDE")),
                F.when(
                    F.col("has_latitude_out_of_range"),
                    F.lit("LATITUDE_OUT_OF_RANGE"),
                ),
                F.when(
                    F.col("has_longitude_out_of_range"),
                    F.lit("LONGITUDE_OUT_OF_RANGE"),
                ),
                F.when(
                    F.col("has_implausible_zero_point_coordinate"),
                    F.lit("IMPLAUSIBLE_ZERO_POINT_COORDINATE"),
                ),
            ),
        )
        .where(F.col("has_invalid_coordinate_trace") == True)
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "vehicle_prefix",
            "trace_sequence",
            "captured_at_local",
            "latitude",
            "longitude",
            "line_sign",
            "direction",
            "direction_description",
            "previous_captured_at_local",
            "previous_latitude",
            "previous_longitude",
            "has_null_latitude",
            "has_null_longitude",
            "has_nan_latitude",
            "has_nan_longitude",
            "has_latitude_out_of_range",
            "has_longitude_out_of_range",
            "has_implausible_zero_point_coordinate",
            "has_invalid_coordinate_trace",
            "audit_reason",
            "record_created_at",
        )
    )

    return audit_df
