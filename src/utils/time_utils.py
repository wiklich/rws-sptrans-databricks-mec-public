from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_time_to_seconds(
    df: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """
    Converts a GTFS time string (HH:MM:SS) into total seconds.

    Supports hours greater than 24 (GTFS standard).

    Example:
        00:10:00 -> 600
        14:30:00 -> 52200
        25:35:00 -> 92100

    Args:
        df (DataFrame): Input DataFrame.
        source_column (str): Column containing time string.
        target_column (str): Column to store seconds.

    Returns:
        DataFrame: DataFrame with new column in seconds.
    """
    return df.withColumn(
        target_column,
        F.when(
            F.col(source_column).isNotNull(),
            F.split(F.col(source_column), ":")[0].cast("int") * 3600
            + F.split(F.col(source_column), ":")[1].cast("int") * 60
            + F.split(F.col(source_column), ":")[2].cast("int"),
        ).otherwise(F.lit(None).cast("int")),
    )


def with_time_to_seconds_columns(
    df: DataFrame,
    column_mapping: dict[str, str],
) -> DataFrame:
    """
    Applies GTFS time to seconds conversion for multiple columns.

    Args:
        df (DataFrame): Input DataFrame.
        column_mapping (dict[str, str]):
            Mapping of source_column -> target_column.

    Returns:
        DataFrame: DataFrame with new columns added.
    """
    for source_column, target_column in column_mapping.items():
        df = with_time_to_seconds(df, source_column, target_column)

    return df


def with_seconds_to_minutes_columns(df, mapping: dict[str, str]):
    """
    Convert columns from seconds to minutes.

    Args:
        df (DataFrame): Input DataFrame
        mapping (dict): Mapping of source_column -> target_column

    Returns:
        DataFrame: DataFrame with new minute-based columns
    """
    for source_col, target_col in mapping.items():
        df = df.withColumn(
            target_col,
            F.when(
                F.col(source_col).isNotNull(),
                F.col(source_col) / F.lit(60.0),
            ).otherwise(F.lit(None).cast("double")),
        )

    return df
