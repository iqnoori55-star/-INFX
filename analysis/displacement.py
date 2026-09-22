import pandas as pd
import numpy as np


# =============================================================
# DISPLACEMENT ENGINE
# =============================================================
#
# Purpose:
# Detect high-quality displacement candles.
#
# A valid displacement should generally have:
#
#   1. Meaningful candle range relative to ATR
#   2. Strong body relative to total range
#   3. Strong close location inside the candle
#   4. Clear bullish / bearish direction
#
# The engine also calculates:
#
#   - displacement strength
#   - displacement score
#   - ATR ratio
#   - body ratio
#   - close location
#   - wick ratios
#
# Designed to work with:
#
#   Order Blocks
#   FVG
#   Liquidity Sweeps
#   BOS / CHOCH
#   Confluence Engine
#
# No future candles are used.
# =============================================================


def detect_displacement(
    df: pd.DataFrame,
    atr_period: int = 14,
    body_ratio_threshold: float = 0.60,
    atr_multiplier_threshold: float = 1.20,
    close_location_threshold: float = 0.70,
    strong_atr_ratio: float = 1.80,
    very_strong_atr_ratio: float = 2.20,
) -> pd.DataFrame:

    # =========================================================
    # VALIDATION
    # =========================================================

    required_columns = {
        "time",
        "open",
        "high",
        "low",
        "close",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing columns: {sorted(missing)}"
        )

    data = df.copy().reset_index(drop=True)

    # =========================================================
    # NORMALIZE DATA
    # =========================================================

    data["time"] = pd.to_datetime(
        data["time"],
        errors="coerce",
    )

    for column in [
        "open",
        "high",
        "low",
        "close",
    ]:

        data[column] = pd.to_numeric(
            data[column],
            errors="coerce",
        )

    data = (
        data
        .dropna(
            subset=[
                "time",
                "open",
                "high",
                "low",
                "close",
            ]
        )
        .sort_values("time")
        .reset_index(drop=True)
    )

    if data.empty:
        return _empty_result()

    # =========================================================
    # BASIC CANDLE METRICS
    # =========================================================

    data["candle_range"] = (
        data["high"]
        - data["low"]
    )

    data["candle_body"] = (
        data["close"]
        - data["open"]
    ).abs()

    # =========================================================
    # BODY / RANGE RATIO
    # =========================================================

    data["body_range_ratio"] = 0.0

    valid_range = (
        data["candle_range"] > 0
    )

    data.loc[
        valid_range,
        "body_range_ratio",
    ] = (
        data.loc[
            valid_range,
            "candle_body",
        ]
        /
        data.loc[
            valid_range,
            "candle_range",
        ]
    )

    # =========================================================
    # DIRECTION
    # =========================================================

    data["displacement_direction"] = None

    bullish = (
        data["close"]
        > data["open"]
    )

    bearish = (
        data["close"]
        < data["open"]
    )

    data.loc[
        bullish,
        "displacement_direction",
    ] = "bullish"

    data.loc[
        bearish,
        "displacement_direction",
    ] = "bearish"

    # =========================================================
    # WICKS
    # =========================================================

    data["upper_wick"] = (
        data["high"]
        -
        data[
            ["open", "close"]
        ].max(axis=1)
    ).clip(lower=0.0)

    data["lower_wick"] = (
        data[
            ["open", "close"]
        ].min(axis=1)
        -
        data["low"]
    ).clip(lower=0.0)

    data["upper_wick_ratio"] = 0.0

    data["lower_wick_ratio"] = 0.0

    data.loc[
        valid_range,
        "upper_wick_ratio",
    ] = (
        data.loc[
            valid_range,
            "upper_wick",
        ]
        /
        data.loc[
            valid_range,
            "candle_range",
        ]
    )

    data.loc[
        valid_range,
        "lower_wick_ratio",
    ] = (
        data.loc[
            valid_range,
            "lower_wick",
        ]
        /
        data.loc[
            valid_range,
            "candle_range",
        ]
    )

    # =========================================================
    # CLOSE LOCATION VALUE
    # =========================================================
    #
    # 1.0 = close at high
    # 0.0 = close at low
    #
    # Bullish displacement:
    # close should be near the high.
    #
    # Bearish displacement:
    # close should be near the low.
    # =========================================================

    data["close_location"] = 0.5

    data.loc[
        valid_range,
        "close_location",
    ] = (
        (
            data.loc[
                valid_range,
                "close",
            ]
            -
            data.loc[
                valid_range,
                "low",
            ]
        )
        /
        data.loc[
            valid_range,
            "candle_range",
        ]
    ).clip(
        lower=0.0,
        upper=1.0,
    )

    # =========================================================
    # TRUE RANGE
    # =========================================================

    previous_close = (
        data["close"]
        .shift(1)
    )

    tr1 = (
        data["high"]
        - data["low"]
    )

    tr2 = (
        data["high"]
        - previous_close
    ).abs()

    tr3 = (
        data["low"]
        - previous_close
    ).abs()

    data["true_range"] = pd.concat(
        [
            tr1,
            tr2,
            tr3,
        ],
        axis=1,
    ).max(axis=1)

    # =========================================================
    # ATR - WILDER STYLE
    # =========================================================
    #
    # More stable than simple rolling mean.
    #
    # No future data.
    # =========================================================

    data["atr"] = (
        data["true_range"]
        .ewm(
            alpha=1.0 / atr_period,
            adjust=False,
            min_periods=atr_period,
        )
        .mean()
    )

    # =========================================================
    # RANGE / ATR
    # =========================================================

    data["range_atr_ratio"] = 0.0

    valid_atr = (
        data["atr"].notna()
        &
        (data["atr"] > 0)
    )

    data.loc[
        valid_atr,
        "range_atr_ratio",
    ] = (
        data.loc[
            valid_atr,
            "candle_range",
        ]
        /
        data.loc[
            valid_atr,
            "atr",
        ]
    )

    # =========================================================
    # CLOSE LOCATION QUALITY
    # =========================================================

    data["close_quality"] = 0.0

    # Bullish:
    # closer to high = better
    bullish_mask = (
        data["displacement_direction"]
        == "bullish"
    )

    data.loc[
        bullish_mask,
        "close_quality",
    ] = (
        data.loc[
            bullish_mask,
            "close_location",
        ]
        * 100.0
    )

    # Bearish:
    # closer to low = better
    bearish_mask = (
        data["displacement_direction"]
        == "bearish"
    )

    data.loc[
        bearish_mask,
        "close_quality",
    ] = (
        (
            1.0
            -
            data.loc[
                bearish_mask,
                "close_location",
            ]
        )
        * 100.0
    )

    # =========================================================
    # DIRECTIONAL CLOSE CONDITION
    # =========================================================
    #
    # Bullish displacement:
    # close >= 70% of candle range
    #
    # Bearish displacement:
    # close <= 30% of candle range
    # =========================================================

    bullish_close_condition = (
        bullish_mask
        &
        (
            data["close_location"]
            >= close_location_threshold
        )
    )

    bearish_close_condition = (
        bearish_mask
        &
        (
            data["close_location"]
            <= (
                1.0
                -
                close_location_threshold
            )
        )
    )

    close_condition = (
        bullish_close_condition
        |
        bearish_close_condition
    )

    # =========================================================
    # BODY CONDITION
    # =========================================================

    body_condition = (
        data["body_range_ratio"]
        >= body_ratio_threshold
    )

    # =========================================================
    # ATR CONDITION
    # =========================================================

    atr_condition = (
        data["range_atr_ratio"]
        >= atr_multiplier_threshold
    )

    # =========================================================
    # FINAL DISPLACEMENT CONDITION
    # =========================================================
    #
    # All three are required:
    #
    #   strong body
    #   large relative range
    #   strong directional close
    # =========================================================

    displacement_condition = (
        body_condition
        &
        atr_condition
        &
        close_condition
        &
        data[
            "displacement_direction"
        ].notna()
    )

    data["is_displacement"] = (
        displacement_condition
    )

    # =========================================================
    # FILTER
    # =========================================================

    result = data[
        data["is_displacement"]
    ].copy()

    if result.empty:
        return _empty_result()

    # =========================================================
    # DISPLACEMENT STRENGTH
    # =========================================================

    result["displacement_strength"] = (
        "weak"
    )

    medium_condition = (
        result["range_atr_ratio"]
        >= 1.50
    )

    strong_condition = (
        result["range_atr_ratio"]
        >= strong_atr_ratio
    )

    very_strong_condition = (
        result["range_atr_ratio"]
        >= very_strong_atr_ratio
    )

    result.loc[
        medium_condition,
        "displacement_strength",
    ] = "medium"

    result.loc[
        strong_condition,
        "displacement_strength",
    ] = "strong"

    result.loc[
        very_strong_condition,
        "displacement_strength",
    ] = "very_strong"

    # =========================================================
    # DISPLACEMENT SCORE
    # =========================================================
    #
    # Components:
    #
    # Body quality      = 35%
    # ATR expansion     = 35%
    # Close quality     = 20%
    # Wick efficiency   = 10%
    #
    # Maximum = 100
    # =========================================================

    body_score = (
        result[
            "body_range_ratio"
        ]
        .clip(
            lower=0.0,
            upper=1.0,
        )
        * 35.0
    )

    atr_score = (
        (
            result[
                "range_atr_ratio"
            ]
            / 3.0
        )
        .clip(
            lower=0.0,
            upper=1.0,
        )
        * 35.0
    )

    close_score = (
        result[
            "close_quality"
        ]
        .clip(
            lower=0.0,
            upper=100.0,
        )
        / 100.0
        * 20.0
    )

    # =========================================================
    # WICK EFFICIENCY
    # =========================================================
    #
    # Bullish:
    # lower wick should not dominate.
    #
    # Bearish:
    # upper wick should not dominate.
    # =========================================================

    wick_efficiency = np.zeros(
        len(result),
        dtype=float,
    )

    result_direction = (
        result[
            "displacement_direction"
        ]
        .to_numpy()
    )

    upper_wick_ratio = (
        result[
            "upper_wick_ratio"
        ]
        .to_numpy()
    )

    lower_wick_ratio = (
        result[
            "lower_wick_ratio"
        ]
        .to_numpy()
    )

    for i in range(
        len(result)
    ):

        direction = (
            result_direction[i]
        )

        if direction == "bullish":

            efficiency = (
                1.0
                -
                lower_wick_ratio[i]
            )

        elif direction == "bearish":

            efficiency = (
                1.0
                -
                upper_wick_ratio[i]
            )

        else:

            efficiency = 0.0

        wick_efficiency[i] = (
            np.clip(
                efficiency,
                0.0,
                1.0,
            )
        )

    wick_score = (
        pd.Series(
            wick_efficiency,
            index=result.index,
        )
        * 10.0
    )

    result["displacement_score"] = (
        body_score
        + atr_score
        + close_score
        + wick_score
    ).clip(
        lower=0.0,
        upper=100.0,
    )

    # =========================================================
    # QUALITY LABEL
    # =========================================================

    result["displacement_quality"] = (
        "C"
    )

    result.loc[
        result[
            "displacement_score"
        ] >= 60,
        "displacement_quality",
    ] = "B"

    result.loc[
        result[
            "displacement_score"
        ] >= 75,
        "displacement_quality",
    ] = "A"

    result.loc[
        (
            result[
                "displacement_score"
            ] >= 90
        )
        &
        (
            result[
                "range_atr_ratio"
            ] >= 2.0
        ),
        "displacement_quality",
    ] = "A+"

    # =========================================================
    # OUTPUT
    # =========================================================

    result = result[
        [
            "time",
            "open",
            "high",
            "low",
            "close",

            "candle_range",
            "candle_body",

            "body_range_ratio",

            "upper_wick",
            "lower_wick",

            "upper_wick_ratio",
            "lower_wick_ratio",

            "close_location",
            "close_quality",

            "true_range",
            "atr",
            "range_atr_ratio",

            "displacement_direction",
            "is_displacement",

            "displacement_strength",
            "displacement_score",
            "displacement_quality",
        ]
    ].copy()

    result = (
        result
        .sort_values(
            "time",
            ascending=True,
        )
        .reset_index(drop=True)
    )

    return result


# =============================================================
# EMPTY RESULT
# =============================================================

def _empty_result() -> pd.DataFrame:

    return pd.DataFrame(
        columns=[
            "time",
            "open",
            "high",
            "low",
            "close",

            "candle_range",
            "candle_body",

            "body_range_ratio",

            "upper_wick",
            "lower_wick",

            "upper_wick_ratio",
            "lower_wick_ratio",

            "close_location",
            "close_quality",

            "true_range",
            "atr",
            "range_atr_ratio",

            "displacement_direction",
            "is_displacement",

            "displacement_strength",
            "displacement_score",
            "displacement_quality",
        ]
    )