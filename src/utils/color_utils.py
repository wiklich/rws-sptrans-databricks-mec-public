"""
Utility functions for color enrichment.

This module provides reusable helpers to translate hexadecimal color codes
into human-readable color names in Spark DataFrames.

Design notes:
    - Input color codes are expected without the leading '#'
    - Matching is case-insensitive
    - Unknown or unmapped color codes return null
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


COLOR_MAP = {
    "F0F8FF": "ALICE BLUE",
    "FAEBD7": "ANTIQUE WHITE",
    "00FFFF": "AQUA",
    "7FFFD4": "AQUAMARINE",
    "F0FFFF": "AZURE",
    "F5F5DC": "BEIGE",
    "FFE4C4": "BISQUE",
    "000000": "BLACK",
    "FFEBCD": "BLANCHEDALMOND",
    "0000FF": "BLUE",
    "8A2BE2": "BLUEVIOLET",
    "A52A2A": "BROWN",
    "DEB887": "BURLYWOOD",
    "5F9EA0": "CADET BLUE",
    "7FFF00": "CHARTREUSE",
    "D2691E": "CHOCOLATE",
    "FF7F50": "CORAL",
    "6495ED": "CORNFLOWER BLUE",
    "FFF8DC": "CORNSILK",
    "DC143C": "CRIMSON",
    "008B8B": "DARK CYAN",
    "00008B": "DARK BLUE",
    "B8860B": "DARK GOLDEN ROD",
    "006400": "DARK GREEN",
    "A9A9A9": "DARK GREY",
    "BDB76B": "DARK KHAKI",
    "8B008B": "DARK MAGENTA",
    "556B2F": "DARK OLIVEGR EEN",
    "FF8C00": "DARK ORANGE",
    "9932CC": "DARK ORCHID",
    "8B0000": "DARK RED",
    "E9967A": "DARK SALMON",
    "8FBC8F": "DARK SEA GREEN",
    "483D8B": "DARK SLATE BLUE",
    "2F4F4F": "DARK SLATE GREY",
    "00CED1": "DARK TURQUOISE",
    "9400D3": "DARK VIOLET",
    "FF1493": "DEEP PINK",
    "00BFFF": "DEEP SKY BLUE",
    "696969": "DIM GREY",
    "1E90FF": "DODGER BLUE",
    "B22222": "FIREBRICK",
    "FFFAF0": "FLORAL WHITE",
    "228B22": "FOREST GREEN",
    "FF00FF": "FUCHSIA",
    "DCDCDC": "GAINSBORO",
    "F8F8FF": "GHOST WHITE",
    "FFD700": "GOLD",
    "DAA520": "GOLDEN ROD",
    "008000": "GREEN",
    "ADFF2F": "GREEN YELLOW",
    "808080": "GREY",
    "F0FFF0": "HONEY DEW",
    "FF69B4": "HOT PINK",
    "CD5C5C": "INDIAN RED",
    "4B0082": "INDIGO",
    "FFFFF0": "IVORY",
    "F0E68C": "KHAKI",
    "E6E6FA": "LAVENDER",
    "FFF0F5": "LAVENDER BLUSH",
    "7CFC00": "LAWN GREEN",
    "FFFACD": "LEMON CHIFFON",
    "ADD8E6": "LIGHT BLUE",
    "F08080": "LIGHT CORAL",
    "E0FFFF": "LIGHT CYAN",
    "FAFAD2": "LIGHT GOLDEN ROD YELLOW",
    "90EE90": "LIGHT GREEN",
    "D3D3D3": "LIGHT GREY",
    "FFB6C1": "LIGHT PINK",
    "FFA07A": "LIGHT SALMON",
    "20B2AA": "LIGHT SEA GREEN",
    "87CEFA": "LIGHT SKY BLUE",
    "778899": "LIGHT SLATE GREY",
    "B0C4DE": "LIGHT STEEL BLUE",
    "FFFFE0": "LIGHT YELLOW",
    "00FF00": "LIME",
    "32CD32": "LIME GREEN",
    "FAF0E6": "LINEN",
    "800000": "MAROON",
    "66CDAA": "MEDIUM AQUAMARINE",
    "0000CD": "MEDIUM BLUE",
    "BA55D3": "MEDIUM ORCHID",
    "9370DB": "MEDIUM PURPLE",
    "3CB371": "MEDIUM SEA GREEN",
    "7B68EE": "MEDIUM SLATE BLUE",
    "00FA9A": "MEDIUM SPRING GREEN",
    "48D1CC": "MEDIUM TURQUOISE",
    "C71585": "MEDIUM VIOLET RED",
    "191970": "MIDNIGHT BLUE",
    "F5FFFA": "MINTCREAM",
    "FFE4E1": "MISTYROSE",
    "FFE4B5": "MOCCASIN",
    "FFDEAD": "NAVAJO WHITE",
    "000080": "NAVY",
    "FDF5E6": "OLD LACE",
    "808000": "OLIVE",
    "6B8E23": "OLIVE DRAB",
    "FFA500": "ORANGE",
    "FF4500": "ORANGE RED",
    "DA70D6": "ORCHID",
    "EEE8AA": "PALE GOLDEN ROD",
    "98FB98": "PALE GREEN",
    "AFEEEE": "PALE TURQUOISE",
    "DB7093": "PALE VIOLET RED",
    "FFEFD5": "PAPAYAWHIP",
    "FFDAB9": "PEACHPUFF",
    "CD853F": "PERU",
    "FFC0CB": "PINK",
    "DDA0DD": "PLUM",
    "B0E0E6": "POWDER BLUE",
    "800080": "PURPLE",
    "663399": "REBECCA PURPLE",
    "FF0000": "RED",
    "BC8F8F": "ROSYBROWN",
    "4169E1": "ROYAL BLUE",
    "8B4513": "SADDLE BROWN",
    "FA8072": "SALMON",
    "F4A460": "SANDY BROWN",
    "2E8B57": "SEA GREEN",
    "FFF5EE": "SEASHELL",
    "A0522D": "SIENNA",
    "C0C0C0": "SILVER",
    "87CEEB": "SKY BLUE",
    "6A5ACD": "SLATE BLUE",
    "708090": "SLATE GREY",
    "FFFAFA": "SNOW",
    "00FF7F": "SPRING GREEN",
    "4682B4": "STEEL BLUE",
    "D2B48C": "TAN",
    "008080": "TEAL",
    "D8BFD8": "THISTLE",
    "FF6347": "TOMATO",
    "40E0D0": "TURQUOISE",
    "EE82EE": "VIOLET",
    "F5DEB3": "WHEAT",
    "FFFFFF": "WHITE",
    "F5F5F5": "WHITESMOKE",
    "FFFF00": "YELLOW",
    "9ACD32": "YELLOW GREEN",
    "509E2F": "LIMEADE",
    "002F6C": "DARK ULTRAMARINE",
    "FFD100": "TANGERINE YELLOW",
    "DA291C": "SCARLET RED",
    "006341": "BOTTLE GREEN",
    "0082BA": "OCEAN BLUE",
    "FF671F": "VIVID ORANGE",
    "782F40": "WINE RED",
    "CA016B": "MAGENTA",
    "97A098": "GREYISH GREEN",
    "01A9A7": "LIGHT TEAL",
    "049FC3": "TEAL BLUE",
    "F68368": "SALMON CORAL",
    "133C8D": "NAVY DARK BLUE",
    "00B352": "MEDIUM GREEN",
    "0455A1": "COBALT BLUE",
    "007E5E": "DARK CYAN-GREEN",
    "EE372F": "RED ORANGE",
    "9B3894": "DEEP PURPLE",
}


def with_color_description(
    df: DataFrame,
    color_column: str,
    output_column: str,
) -> DataFrame:
    """
    Add a color description column based on a hexadecimal color code column.

    Args:
        df: Input Spark DataFrame.
        color_column: Name of the column containing the hexadecimal color code.
            The value is expected without the leading '#'.
        output_column: Name of the output column that will store the color name.

    Returns:
        DataFrame:
            A new DataFrame with the additional color description column.

    Behavior:
        - Input values are normalized to uppercase before lookup.
        - If the input value is null, the output will be null.
        - If the input value is not found in the mapping dictionary, the output
          will be null.
    """
    normalized_col = F.upper(F.col(color_column))

    mapping_expr = F.create_map(
        *[F.lit(item) for key_value in COLOR_MAP.items() for item in key_value]
    )

    return df.withColumn(
        output_column,
        F.when(
            F.col(color_column).isNull(),
            F.lit(None).cast("string"),
        ).otherwise(mapping_expr[normalized_col]),
    )