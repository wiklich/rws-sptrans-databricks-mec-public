from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    DateType,
    TimestampType,
)


def get_gold_planned_vs_observed_daily_schema() -> StructType:
    """
    Returns the schema for the Gold planned vs observed daily summary table.

    This schema represents the canonical daily KPI summary output used by the
    Planned vs Observed Service Comparison component.

    The table contains one row per analysis_date and summarizes how the
    observed SPTrans line catalog compares with the GTFS planned line catalog.

    Returns:
        StructType: The complete schema definition for the Gold planned vs
        observed daily summary table.
    """
    return StructType(
        [
            StructField(
                "analysis_date",
                DateType(),
                False,
                {
                    "comment": (
                        "Local calendar date representing the daily summary grain "
                        "for the planned versus observed comparison."
                    )
                },
            ),
            StructField(
                "planned_lines_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct planned lines represented in the daily comparison."
                    )
                },
            ),
            StructField(
                "observed_lines_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of distinct observed lines represented in the daily comparison."
                    )
                },
            ),
            StructField(
                "planned_lines_observed_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of lines that were both planned and observed on the analysis date."
                    )
                },
            ),
            StructField(
                "planned_lines_not_observed_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of GTFS planned lines that were not observed on the analysis date."
                    )
                },
            ),
            StructField(
                "observed_lines_not_in_gtfs_count",
                LongType(),
                False,
                {
                    "comment": (
                        "Number of observed lines that did not exist in the GTFS planned catalog "
                        "on the analysis date."
                    )
                },
            ),
            StructField(
                "planned_lines_observed_pct",
                DoubleType(),
                True,
                {
                    "comment": (
                        "Percentage of planned lines that were observed on the analysis date."
                    )
                },
            ),
            StructField(
                "gtfs_load_date",
                DateType(),
                True,
                {
                    "comment": (
                        "Logical GTFS load date preserved from the detailed comparison output "
                        "as contextual metadata."
                    )
                },
            ),
            StructField(
                "record_created_at",
                TimestampType(),
                True,
                {
                    "comment": (
                        "Timestamp when the Gold planned vs observed daily summary record "
                        "was materialized."
                    )
                },
            ),
        ]
    )
