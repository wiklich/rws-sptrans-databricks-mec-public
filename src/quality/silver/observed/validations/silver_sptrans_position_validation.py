"""
Validation pipeline for silver.silver_sptrans_positions.

Purpose:
    Provide a formal pass/fail validation output that confirms whether
    silver.silver_sptrans_positions is acceptable for downstream observed,
    spatial, and planned-vs-observed use cases.

Validation categories:
    A. Structural validation
    B. Grain validation
    C. Business-rule validation
    D. Metric validation
    E. Reconciliation validation

Important:
    - Validation is different from auditing.
    - Audit tables expose duplicates, anomalies, edge cases, and suspicious patterns.
    - This validation table provides an explicit readiness view.
    - ERROR rows should be treated as blockers for downstream analytical usage.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.constants_utils import (
    BOUNDING_BOX_MIN_LAT,
    BOUNDING_BOX_MAX_LAT,
    BOUNDING_BOX_MIN_LON,
    BOUNDING_BOX_MAX_LON,
)

SOURCE_TABLE = "silver.silver_sptrans_positions"
TABLE_NAME = "silver.silver_sptrans_position_validation"

ALLOWED_DIRECTIONS = [1, 2]


@dp.table(
    name=TABLE_NAME,
    comment=(
        "Formal validation output for silver.silver_sptrans_positions. "
        "Provides pass/fail results across structural, grain, business-rule, "
        "metric, and reconciliation validations."
    ),
)
def silver_sptrans_position_validation() -> DataFrame:
    """
    Create a formal validation table for silver.silver_sptrans_positions.
    """
    source_df = spark.read.table(SOURCE_TABLE)
    results = []

    columns = set(source_df.columns)

    required_columns = [
        "event_id",
        "line_id",
        "vehicle_prefix",
        "direction",
        "direction_description",
        "captured_at_utc",
        "captured_at_local",
        "captured_date_local",
        "captured_hour_local",
        "latitude",
        "longitude",
        "ingestion_ts",
        "ingestion_date",
    ]

    required_not_null_columns = [
        "event_id",
        "line_id",
        "vehicle_prefix",
        "direction",
        "direction_description",
        "captured_at_utc",
        "captured_at_local",
        "captured_date_local",
        "captured_hour_local",
        "latitude",
        "longitude",
        "ingestion_ts",
        "ingestion_date",
    ]

    missing_columns = sorted(set(required_columns) - columns)

    _append_result(
        results=results,
        validation_category="STRUCTURAL",
        validation_rule="REQUIRED_COLUMNS_PRESENT",
        severity="ERROR",
        validation_passed=len(missing_columns) == 0,
        failed_row_count=len(missing_columns),
        validation_message=(
            "All required columns are present."
            if len(missing_columns) == 0
            else f"Missing required columns: {', '.join(missing_columns)}."
        ),
    )

    for column_name in required_not_null_columns:
        if column_name in columns:
            null_count = source_df.filter(F.col(column_name).isNull()).count()

            _append_result(
                results=results,
                validation_category="STRUCTURAL",
                validation_rule=f"{column_name.upper()}_NOT_NULL",
                severity="ERROR",
                validation_passed=null_count == 0,
                failed_row_count=null_count,
                validation_message=(
                    f"Column {column_name} has no null values."
                    if null_count == 0
                    else f"Column {column_name} contains null values."
                ),
            )

    if "event_id" in columns:
        blank_event_id_count = source_df.filter(
            F.trim(F.col("event_id")) == ""
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="EVENT_ID_NOT_BLANK",
            severity="ERROR",
            validation_passed=blank_event_id_count == 0,
            failed_row_count=blank_event_id_count,
            validation_message=(
                "All event_id values are populated and non-blank."
                if blank_event_id_count == 0
                else "Some event_id values are blank."
            ),
        )

    if "line_sign" in columns:
        blank_line_sign_count = source_df.filter(
            F.col("line_sign").isNotNull() & (F.trim(F.col("line_sign")) == "")
        ).count()

        _append_result(
            results=results,
            validation_category="STRUCTURAL",
            validation_rule="LINE_SIGN_NOT_BLANK_WHEN_PRESENT",
            severity="WARNING",
            validation_passed=blank_line_sign_count == 0,
            failed_row_count=blank_line_sign_count,
            validation_message=(
                "All non-null line_sign values are non-blank."
                if blank_line_sign_count == 0
                else "Some non-null line_sign values are blank."
            ),
        )

    duplicate_event_id_count = (
        source_df.groupBy("event_id")
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_EVENT_ID_GRAIN",
        severity="ERROR",
        validation_passed=duplicate_event_id_count == 0,
        failed_row_count=duplicate_event_id_count,
        validation_message=(
            "No duplicate rows exist at the declared event_id grain."
            if duplicate_event_id_count == 0
            else "Duplicate rows exist at the declared event_id grain."
        ),
    )

    natural_key_duplicate_count = (
        source_df.groupBy(
            "line_id",
            "vehicle_prefix",
            "captured_at_utc",
            "latitude",
            "longitude",
        )
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )

    _append_result(
        results=results,
        validation_category="GRAIN",
        validation_rule="NO_DUPLICATE_ROWS_AT_NATURAL_EVENT_GRAIN",
        severity="ERROR",
        validation_passed=natural_key_duplicate_count == 0,
        failed_row_count=natural_key_duplicate_count,
        validation_message=(
            "No duplicate rows exist at the natural SPTrans position grain."
            if natural_key_duplicate_count == 0
            else "Duplicate rows exist at the natural SPTrans position grain."
        ),
    )

    invalid_direction_count = source_df.filter(
        ~F.col("direction").isin(ALLOWED_DIRECTIONS)
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="DIRECTION_BELONGS_TO_APPROVED_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_direction_count == 0,
        failed_row_count=invalid_direction_count,
        validation_message=(
            "All direction values belong to the approved domain."
            if invalid_direction_count == 0
            else "Some direction values are outside the approved domain."
        ),
    )

    direction_description_mismatch_count = source_df.filter(
        (
            (F.col("direction") == 1)
            & (F.col("direction_description") != F.lit("main_to_secondary"))
        )
        | (
            (F.col("direction") == 2)
            & (F.col("direction_description") != F.lit("secondary_to_main"))
        )
    ).count()

    _append_result(
        results=results,
        validation_category="BUSINESS_RULE",
        validation_rule="DIRECTION_DESCRIPTION_RECONCILES_WITH_DIRECTION",
        severity="ERROR",
        validation_passed=direction_description_mismatch_count == 0,
        failed_row_count=direction_description_mismatch_count,
        validation_message=(
            "direction_description is consistent with direction."
            if direction_description_mismatch_count == 0
            else "Some direction_description values are inconsistent with direction."
        ),
    )

    negative_vehicles_count = (
        source_df.filter(
            F.col("vehicles_count").isNotNull() & (F.col("vehicles_count") < 0)
        ).count()
        if "vehicles_count" in columns
        else 0
    )

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="VEHICLES_COUNT_NON_NEGATIVE_WHEN_PRESENT",
        severity="ERROR",
        validation_passed=negative_vehicles_count == 0,
        failed_row_count=negative_vehicles_count,
        validation_message=(
            "All non-null vehicles_count values are non-negative."
            if negative_vehicles_count == 0
            else "Some non-null vehicles_count values are negative."
        ),
    )

    invalid_captured_hour_count = source_df.filter(
        (F.col("captured_hour_local") < 0) | (F.col("captured_hour_local") > 23)
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="CAPTURED_HOUR_LOCAL_BETWEEN_0_AND_23",
        severity="ERROR",
        validation_passed=invalid_captured_hour_count == 0,
        failed_row_count=invalid_captured_hour_count,
        validation_message=(
            "All captured_hour_local values are between 0 and 23."
            if invalid_captured_hour_count == 0
            else "Some captured_hour_local values are outside the range 0 to 23."
        ),
    )

    invalid_latitude_count = source_df.filter(
        (F.col("latitude") < F.lit(-90.0)) | (F.col("latitude") > F.lit(90.0))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="LATITUDE_WITHIN_GLOBAL_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_latitude_count == 0,
        failed_row_count=invalid_latitude_count,
        validation_message=(
            "All latitude values are within the global geographic domain."
            if invalid_latitude_count == 0
            else "Some latitude values are outside the global geographic domain."
        ),
    )

    invalid_longitude_count = source_df.filter(
        (F.col("longitude") < F.lit(-180.0)) | (F.col("longitude") > F.lit(180.0))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="LONGITUDE_WITHIN_GLOBAL_DOMAIN",
        severity="ERROR",
        validation_passed=invalid_longitude_count == 0,
        failed_row_count=invalid_longitude_count,
        validation_message=(
            "All longitude values are within the global geographic domain."
            if invalid_longitude_count == 0
            else "Some longitude values are outside the global geographic domain."
        ),
    )

    out_of_bounding_box_count = source_df.filter(
        (F.col("latitude") < F.lit(BOUNDING_BOX_MIN_LAT))
        | (F.col("latitude") > F.lit(BOUNDING_BOX_MAX_LAT))
        | (F.col("longitude") < F.lit(BOUNDING_BOX_MIN_LON))
        | (F.col("longitude") > F.lit(BOUNDING_BOX_MAX_LON))
    ).count()

    _append_result(
        results=results,
        validation_category="METRIC",
        validation_rule="POSITIONS_WITHIN_SPTRANS_BOUNDING_BOX",
        severity="ERROR",
        validation_passed=out_of_bounding_box_count == 0,
        failed_row_count=out_of_bounding_box_count,
        validation_message=(
            "All positions are within the approved SPTrans bounding box."
            if out_of_bounding_box_count == 0
            else "Some positions are outside the approved SPTrans bounding box."
        ),
    )

    captured_at_local_reconciliation_failures = source_df.filter(
        F.col("captured_at_local")
        != F.from_utc_timestamp(F.col("captured_at_utc"), "America/Sao_Paulo")
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="CAPTURED_AT_LOCAL_RECONCILES_WITH_CAPTURED_AT_UTC",
        severity="ERROR",
        validation_passed=captured_at_local_reconciliation_failures == 0,
        failed_row_count=captured_at_local_reconciliation_failures,
        validation_message=(
            "captured_at_local reconciles with captured_at_utc."
            if captured_at_local_reconciliation_failures == 0
            else "Some captured_at_local values do not reconcile with captured_at_utc."
        ),
    )

    captured_date_reconciliation_failures = source_df.filter(
        F.col("captured_date_local") != F.to_date(F.col("captured_at_local"))
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="CAPTURED_DATE_LOCAL_RECONCILES_WITH_CAPTURED_AT_LOCAL",
        severity="ERROR",
        validation_passed=captured_date_reconciliation_failures == 0,
        failed_row_count=captured_date_reconciliation_failures,
        validation_message=(
            "captured_date_local reconciles with captured_at_local."
            if captured_date_reconciliation_failures == 0
            else "Some captured_date_local values do not reconcile with captured_at_local."
        ),
    )

    captured_hour_reconciliation_failures = source_df.filter(
        F.col("captured_hour_local") != F.hour(F.col("captured_at_local"))
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="CAPTURED_HOUR_LOCAL_RECONCILES_WITH_CAPTURED_AT_LOCAL",
        severity="ERROR",
        validation_passed=captured_hour_reconciliation_failures == 0,
        failed_row_count=captured_hour_reconciliation_failures,
        validation_message=(
            "captured_hour_local reconciles with captured_at_local."
            if captured_hour_reconciliation_failures == 0
            else "Some captured_hour_local values do not reconcile with captured_at_local."
        ),
    )

    ingestion_date_reconciliation_failures = source_df.filter(
        F.col("ingestion_date") != F.to_date(F.col("ingestion_ts"))
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="INGESTION_DATE_RECONCILES_WITH_INGESTION_TS",
        severity="ERROR",
        validation_passed=ingestion_date_reconciliation_failures == 0,
        failed_row_count=ingestion_date_reconciliation_failures,
        validation_message=(
            "ingestion_date reconciles with ingestion_ts."
            if ingestion_date_reconciliation_failures == 0
            else "Some ingestion_date values do not reconcile with ingestion_ts."
        ),
    )

    expected_event_id_df = source_df.withColumn(
        "expected_event_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("line_id").cast("string"), F.lit("")),
                F.coalesce(F.col("vehicle_prefix").cast("string"), F.lit("")),
                F.coalesce(F.col("captured_at_utc").cast("string"), F.lit("")),
                F.coalesce(F.col("longitude").cast("string"), F.lit("")),
                F.coalesce(F.col("latitude").cast("string"), F.lit("")),
            ),
            256,
        ),
    )

    event_id_reconciliation_failures = expected_event_id_df.filter(
        F.col("event_id") != F.col("expected_event_id")
    ).count()

    _append_result(
        results=results,
        validation_category="RECONCILIATION",
        validation_rule="EVENT_ID_RECONCILES_WITH_DECLARED_HASH_LOGIC",
        severity="ERROR",
        validation_passed=event_id_reconciliation_failures == 0,
        failed_row_count=event_id_reconciliation_failures,
        validation_message=(
            "event_id reconciles with the declared hash generation logic."
            if event_id_reconciliation_failures == 0
            else "Some event_id values do not reconcile with the declared hash generation logic."
        ),
    )

    validation_df = (
        spark.createDataFrame(results)
        .withColumn("validated_at", F.current_timestamp())
        .select(
            "source_table",
            "validation_category",
            "validation_rule",
            "severity",
            "validation_passed",
            "failed_row_count",
            "validation_message",
            "validated_at",
        )
    )

    return validation_df


def _append_result(
    results: list,
    validation_category: str,
    validation_rule: str,
    severity: str,
    validation_passed: bool,
    failed_row_count: int,
    validation_message: str,
) -> None:
    results.append(
        {
            "source_table": SOURCE_TABLE,
            "validation_category": validation_category,
            "validation_rule": validation_rule,
            "severity": severity,
            "validation_passed": validation_passed,
            "failed_row_count": failed_row_count,
            "validation_message": validation_message,
        }
    )
