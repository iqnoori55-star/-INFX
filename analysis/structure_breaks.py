
import pandas as pd
import numpy as np


# =========================================================
# CONFIGURATION
# =========================================================

EQUAL_TOLERANCE = 0.0


# =========================================================
# HELPERS
# =========================================================

def _validate_input(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and normalize market dataframe.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            "df must be a pandas DataFrame."
        )

    required = {
        "time",
        "open",
        "high",
        "low",
        "close",
        "swing_high",
        "swing_low",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            "Structure break input missing columns: "
            f"{sorted(missing)}"
        )

    result = df.copy().reset_index(drop=True)

    result["time"] = pd.to_datetime(
        result["time"],
        errors="coerce",
    )

    for column in [
        "open",
        "high",
        "low",
        "close",
    ]:

        result[column] = pd.to_numeric(
            result[column],
            errors="coerce",
        )

    result["swing_high"] = (
        result["swing_high"]
        .fillna(False)
        .astype(bool)
    )

    result["swing_low"] = (
        result["swing_low"]
        .fillna(False)
        .astype(bool)
    )

    result = result.dropna(
        subset=[
            "time",
            "open",
            "high",
            "low",
            "close",
        ]
    ).reset_index(drop=True)

    return result


def _is_equal(
    a: float,
    b: float,
) -> bool:

    if EQUAL_TOLERANCE <= 0:
        return a == b

    return (
        abs(a - b)
        <= EQUAL_TOLERANCE
    )


# =========================================================
# STRUCTURE BREAK DETECTION
# =========================================================

def detect_structure_breaks(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Structure Break Engine - Final

    Detects BOS and CHOCH from confirmed swing levels.

    CORE RULES
    ----------
    1. Only confirmed swings are structural levels.
    2. Bullish break requires candle CLOSE above a swing high.
    3. Bearish break requires candle CLOSE below a swing low.
    4. Wick penetration does not count.
    5. A broken level is consumed and cannot generate another
       break event.
    6. BOS continues the current structural bias.
    7. CHOCH changes the structural bias.
    8. The first valid directional break is BOS.
    9. If both sides are broken on one candle, the closest
       broken level to the close determines the event.
    10. Only actual BOS / CHOCH rows are returned.

    Returns
    -------
    pd.DataFrame
        Only structure-break events.
    """

    data = _validate_input(df)

    # =====================================================
    # EMPTY RESULT TEMPLATE
    # =====================================================

    empty_columns = [
        "time",
        "break_type",
        "break_direction",
        "break_level",
        "break_time",
        "broken_swing",
        "previous_bias",
        "break_distance",
        "break_strength_pct",
        "swing_time",
        "swing_index",
    ]

    if data.empty:
        return pd.DataFrame(
            columns=empty_columns
        )

    # =====================================================
    # STRUCTURAL LEVELS
    # =====================================================

    swing_high_levels = []
    swing_low_levels = []

    # =====================================================
    # MARKET STATE
    # =====================================================

    market_bias = None

    events = []

    # =====================================================
    # PROCESS CANDLES
    # =====================================================

    for i in range(len(data)):

        current_time = data.iloc[i]["time"]

        current_close = float(
            data.iloc[i]["close"]
        )

        # =================================================
        # REGISTER NEW CONFIRMED SWING HIGH
        # =================================================

        if bool(
            data.iloc[i]["swing_high"]
        ):

            swing_high_levels.append(
                {
                    "price": float(
                        data.iloc[i]["high"]
                    ),
                    "time": current_time,
                    "index": i,
                    "broken": False,
                }
            )

        # =================================================
        # REGISTER NEW CONFIRMED SWING LOW
        # =================================================

        if bool(
            data.iloc[i]["swing_low"]
        ):

            swing_low_levels.append(
                {
                    "price": float(
                        data.iloc[i]["low"]
                    ),
                    "time": current_time,
                    "index": i,
                    "broken": False,
                }
            )

        # =================================================
        # FIND UNBROKEN BULLISH LEVELS
        # =================================================

        bullish_candidates = []

        for level in swing_high_levels:

            if level["broken"]:
                continue

            # Cannot break the swing on the same candle.
            if i <= level["index"]:
                continue

            if (
                current_close
                >
                level["price"]
            ):

                bullish_candidates.append(
                    level
                )

        # =================================================
        # FIND UNBROKEN BEARISH LEVELS
        # =================================================

        bearish_candidates = []

        for level in swing_low_levels:

            if level["broken"]:
                continue

            # Cannot break the swing on the same candle.
            if i <= level["index"]:
                continue

            if (
                current_close
                <
                level["price"]
            ):

                bearish_candidates.append(
                    level
                )

        # =================================================
        # SELECT BULLISH LEVEL
        # =================================================

        bullish_level = None

        if bullish_candidates:

            # Most recent confirmed swing high.
            bullish_level = max(
                bullish_candidates,
                key=lambda x: x["index"],
            )

        # =================================================
        # SELECT BEARISH LEVEL
        # =================================================

        bearish_level = None

        if bearish_candidates:

            # Most recent confirmed swing low.
            bearish_level = max(
                bearish_candidates,
                key=lambda x: x["index"],
            )

        # =================================================
        # NO BREAK
        # =================================================

        if (
            bullish_level is None
            and
            bearish_level is None
        ):
            continue

        # =================================================
        # SELECT DIRECTION
        # =================================================

        selected_direction = None
        selected_level = None

        # -------------------------------------------------
        # Both directions broken by one candle.
        # -------------------------------------------------

        if (
            bullish_level is not None
            and
            bearish_level is not None
        ):

            bullish_distance = (
                current_close
                - bullish_level["price"]
            )

            bearish_distance = (
                bearish_level["price"]
                - current_close
            )

            if (
                bullish_distance
                <=
                bearish_distance
            ):

                selected_direction = "bullish"
                selected_level = bullish_level

            else:

                selected_direction = "bearish"
                selected_level = bearish_level

        # -------------------------------------------------
        # Bullish only
        # -------------------------------------------------

        elif bullish_level is not None:

            selected_direction = "bullish"
            selected_level = bullish_level

        # -------------------------------------------------
        # Bearish only
        # -------------------------------------------------

        else:

            selected_direction = "bearish"
            selected_level = bearish_level

        # =================================================
        # PREVIOUS BIAS
        # =================================================

        previous_bias = market_bias

        # =================================================
        # BOS / CHOCH
        # =================================================

        if market_bias is None:

            break_type = "BOS"

        elif (
            market_bias
            ==
            selected_direction
        ):

            break_type = "BOS"

        else:

            break_type = "CHOCH"

        # =================================================
        # LEVEL
        # =================================================

        break_level = float(
            selected_level["price"]
        )

        # =================================================
        # BREAK DISTANCE
        # =================================================

        if selected_direction == "bullish":

            break_distance = (
                current_close
                - break_level
            )

        else:

            break_distance = (
                break_level
                - current_close
            )

        # =================================================
        # BREAK STRENGTH
        # =================================================

        if (
            break_level != 0
            and
            np.isfinite(break_level)
        ):

            break_strength_pct = (
                break_distance
                /
                abs(break_level)
                *
                100.0
            )

        else:

            break_strength_pct = 0.0

        # =================================================
        # MARK BROKEN LEVELS
        # =================================================
        #
        # Every confirmed structural level already crossed
        # by the close becomes consumed.
        #
        # This prevents repeated BOS events from old levels.
        # =================================================

        if selected_direction == "bullish":

            for level in swing_high_levels:

                if level["broken"]:
                    continue

                if i <= level["index"]:
                    continue

                if (
                    current_close
                    >
                    level["price"]
                ):

                    level["broken"] = True

        else:

            for level in swing_low_levels:

                if level["broken"]:
                    continue

                if i <= level["index"]:
                    continue

                if (
                    current_close
                    <
                    level["price"]
                ):

                    level["broken"] = True

        # =================================================
        # UPDATE MARKET BIAS
        # =================================================

        market_bias = selected_direction

        # =================================================
        # STORE EVENT
        # =================================================

        events.append(
            {
                "time": current_time,

                "break_type":
                    break_type,

                "break_direction":
                    selected_direction,

                "break_level":
                    break_level,

                "break_time":
                    current_time,

                "broken_swing":
                    (
                        "swing_high"
                        if selected_direction == "bullish"
                        else "swing_low"
                    ),

                "previous_bias":
                    previous_bias,

                "break_distance":
                    float(
                        break_distance
                    ),

                "break_strength_pct":
                    float(
                        break_strength_pct
                    ),

                "swing_time":
                    selected_level["time"],

                "swing_index":
                    selected_level["index"],
            }
        )

    # =====================================================
    # RETURN ONLY EVENTS
    # =====================================================

    if not events:

        return pd.DataFrame(
            columns=empty_columns
        )

    return (
        pd.DataFrame(events)
        .reset_index(drop=True)
    )


# =========================================================
# END
# =========================================================
