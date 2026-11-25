from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_trimmed_nullable_string(
    df: DataFrame,
    column_name: str,
    output_column: str | None = None,
) -> DataFrame:
    """
    Trim a string column and convert empty strings to null.

    Args:
        df: Input Spark DataFrame.
        column_name: Source column name.
        output_column: Output column name. If not provided, the source column
            will be overwritten.

    Returns:
        DataFrame: DataFrame with the cleaned string column.
    """
    target_column = output_column or column_name

    return df.withColumn(
        target_column,
        F.when(
            F.length(F.trim(F.col(column_name))) == 0,
            F.lit(None).cast("string"),
        ).otherwise(F.trim(F.col(column_name))),
    )


def with_trimmed_nullable_strings(
    df: DataFrame,
    column_names: list[str],
) -> DataFrame:
    """
    Apply trim and empty-string-to-null normalization to multiple string columns.

    Args:
        df: Input Spark DataFrame.
        column_names: List of column names to normalize.

    Returns:
        DataFrame: DataFrame with normalized string columns.
    """
    result_df = df

    for column_name in column_names:
        result_df = with_trimmed_nullable_string(result_df, column_name)

    return result_df

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_uppercase_string(
    df: DataFrame,
    column_name: str,
    output_column: str | None = None,
) -> DataFrame:
    """
    Convert a string column to uppercase.

    Args:
        df: Input Spark DataFrame.
        column_name: Source column name.
        output_column: Output column name. If not provided, the source column
            will be overwritten.

    Returns:
        DataFrame: DataFrame with the uppercased string column.

    Notes:
        - Null values remain null.
        - This function is intended to be applied after trimming and
          empty-string-to-null normalization.
    """
    target_column = output_column or column_name

    return df.withColumn(
        target_column,
        F.when(
            F.col(column_name).isNull(),
            F.lit(None).cast("string"),
        ).otherwise(F.upper(F.col(column_name))),
    )

from pyspark.sql import DataFrame


def with_uppercase_strings(
    df: DataFrame,
    column_names: list[str],
) -> DataFrame:
    """
    Convert multiple string columns to uppercase.

    Args:
        df: Input Spark DataFrame.
        column_names: List of column names to normalize to uppercase.

    Returns:
        DataFrame: DataFrame with the uppercased string columns.
    """
    result_df = df

    for column_name in column_names:
        result_df = with_uppercase_string(result_df, column_name)

    return result_df
