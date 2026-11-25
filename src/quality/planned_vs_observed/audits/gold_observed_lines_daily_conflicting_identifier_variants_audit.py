"""
Gold observed lines daily conflicting observed identifier variants audit pipeline.

Purpose:
    Identify cases where the same normalized daily observed business key is
    associated upstream with multiple raw line_sign variants.

Audit focus:
    - same normalized line_code generated from multiple formatting variants
      of line_sign
    - same analysis_date + line_code associated with multiple raw
      observed_line_sign values upstream

Why this audit matters:
    This audit is useful because gold_observed_lines_daily keeps only one
    representative observed identifier after aggregation. If multiple raw
    line_sign variants collapse into the same normalized daily key, the Gold
    table may hide that ambiguity by selecting only one value.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_SOURCE_TABLE = "main.silver.silver_sptrans_positions"
OUTPUT_TABLE_NAME = "gold.gold_observed_lines_daily_conflicting_identifier_variants_audit"


@dp.table(
    name=OUTPUT_TABLE_NAME,
    comment=(
        "Audit table identifying normalized daily observed line keys that are "
        "associated upstream with multiple raw line_sign variants in "
        "silver_sptrans_positions."
    ),
)
def gold_observed_lines_daily_conflicting_identifier_variants_audit() -> DataFrame:
    """
    Materialize the conflicting observed identifier variants audit for
    gold_observed_lines_daily.

    Returns:
        DataFrame:
            Audit rows for daily normalized observed keys associated with
            multiple raw line_sign variants upstream.
    """
    source_df = spark.read.table(SILVER_SOURCE_TABLE)
    return _build_gold_observed_lines_daily_conflicting_identifier_variants_audit(source_df)


def _normalize_line_code(column_name: str):
    """
    Normalize a public transit line identifier into the official comparison key.

    Normalization rule:
        UPPER(TRIM(value))

    Args:
        column_name:
            Name of the input column containing the raw observed line identifier.

    Returns:
        pyspark.sql.Column:
            Normalized line code expression.
    """
    return F.upper(F.trim(F.col(column_name)))


def _build_gold_observed_lines_daily_conflicting_identifier_variants_audit(
    source_df: DataFrame,
) -> DataFrame:
    """
    Build the conflicting observed identifier variants audit dataset.

    Processing steps:
        1. Read Silver SPTrans positions.
        2. Keep only records with valid captured_date_local and line_sign.
        3. Normalize line_sign into line_code.
        4. Aggregate by analysis_date and normalized line_code.
        5. Keep only keys backed by multiple raw line_sign variants.

    Args:
        source_df:
            Input DataFrame from silver.silver_sptrans_positions.

    Returns:
        DataFrame:
            Audit DataFrame containing daily normalized observed keys with
            conflicting raw identifier variants upstream.
    """
    prepared_df = (
        source_df.select(
            F.col("captured_date_local").cast("date").alias("analysis_date"),
            F.col("line_sign").cast("string").alias("observed_line_sign"),
            F.col("vehicle_prefix").cast("string").alias("vehicle_prefix"),
            F.col("captured_at_local").cast("timestamp").alias("captured_at_local"),
        )
        .where(F.col("analysis_date").isNotNull())
        .where(F.col("observed_line_sign").isNotNull())
        .where(F.trim(F.col("observed_line_sign")) != "")
        .withColumn("line_code", _normalize_line_code("observed_line_sign"))
        .withColumn("raw_line_sign_trimmed", F.trim(F.col("observed_line_sign")))
    )

    audit_df = (
        prepared_df.groupBy("analysis_date", "line_code")
        .agg(
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("observed_line_sign").alias("distinct_observed_line_sign_count"),
            F.countDistinct("raw_line_sign_trimmed").alias("distinct_trimmed_line_sign_count"),
            F.collect_set("observed_line_sign").alias("observed_line_signs"),
            F.collect_set("raw_line_sign_trimmed").alias("trimmed_line_signs"),
            F.countDistinct("vehicle_prefix").cast("long").alias("observed_vehicles_count"),
            F.min("captured_at_local").alias("min_captured_at_local"),
            F.max("captured_at_local").alias("max_captured_at_local"),
        )
        .withColumn(
            "has_multiple_observed_line_sign_variants",
            F.col("distinct_observed_line_sign_count") > F.lit(1),
        )
        .withColumn(
            "has_multiple_trimmed_line_sign_variants",
            F.col("distinct_trimmed_line_sign_count") > F.lit(1),
        )
        .where(
            (F.col("distinct_observed_line_sign_count") > 1)
            | (F.col("distinct_trimmed_line_sign_count") > 1)
        )
        .withColumn("record_created_at", F.current_timestamp())
        .select(
            "analysis_date",
            "line_code",
            "source_record_count",
            "distinct_observed_line_sign_count",
            "distinct_trimmed_line_sign_count",
            "observed_line_signs",
            "trimmed_line_signs",
            "observed_vehicles_count",
            "min_captured_at_local",
            "max_captured_at_local",
            "has_multiple_observed_line_sign_variants",
            "has_multiple_trimmed_line_sign_variants",
            "record_created_at",
        )
    )

    return audit_df
