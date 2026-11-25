from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_nullable_boolean_from_int(
    df: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """
    Converts an integer column (0/1) into a nullable boolean column.

    Rules:
        1 -> True
        0 -> False
        any other value -> NULL

    Args:
        df (DataFrame): Input DataFrame.
        source_column (str): Source column name (integer-like).
        target_column (str): Target column name (boolean).

    Returns:
        DataFrame: DataFrame with the new boolean column.
    """
    return df.withColumn(
        target_column,
        F.when(F.col(source_column) == 1, F.lit(True))
         .when(F.col(source_column) == 0, F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )


def with_nullable_boolean_from_int_columns(
    df: DataFrame,
    column_mapping: dict[str, str],
) -> DataFrame:
    """
    Applies nullable boolean conversion (0/1 -> True/False/NULL)
    for multiple columns.

    Args:
        df (DataFrame): Input DataFrame.
        column_mapping (dict[str, str]):
            Mapping of source_column -> target_column.

    Returns:
        DataFrame: DataFrame with all boolean columns added.
    """
    for source_column, target_column in column_mapping.items():
        df = with_nullable_boolean_from_int(
            df,
            source_column,
            target_column,
        )

    return df