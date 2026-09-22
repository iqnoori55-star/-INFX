
import pandas as pd
import numpy as np


# =========================================================
# CONFIGURATION
# =========================================================

LEFT_BARS = 3
RIGHT_BARS = 3

# Equal swing tolerance.
# 0.0 means strict comparison.
EQUAL_TOLERANCE = 0.0


# =========================================================
# INTERNAL HELPERS
# =========================================================

def _validate_market_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and normalize OHLC market data.

    Returns:
        Cleaned complete market dataframe.
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
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            "Market data missing columns: "
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


def _prices_equal(
    price_a: float,
    price_b: float,
    tolerance: float = EQUAL_TOLERANCE,
) -> bool:
    """
    Determine whether two prices should be treated as equal.
    """

    if tolerance <= 0:
        return price_a == price_b

    return abs(price_a - price_b) <= tolerance


# =========================================================
# 1. SWING DETECTION
# =========================================================

def detect_swings(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Detect confirmed swing highs and swing lows.

    Swing High:
        High[i] is greater than all highs in the
        LEFT_BARS candles before it and RIGHT_BARS
        candles after it.

    Swing Low:
        Low[i] is lower than all lows in the
        LEFT_BARS candles before it and RIGHT_BARS
        candles after it.

    IMPORTANT
    ---------
    A swing is only confirmed after RIGHT_BARS candles.

    The function returns the COMPLETE market dataframe.

    Added columns:
        swing_high
        swing_low
    """

    result = _validate_market_data(df)

    # -----------------------------------------------------
    # Initialize
    # -----------------------------------------------------

    result["swing_high"] = False
    result["swing_low"] = False

    if len(result) < (
        LEFT_BARS + RIGHT_BARS + 1
    ):
        return result

    # =====================================================
    # Detect confirmed swings
    # =====================================================

    for i in range(
        LEFT_BARS,
        len(result) - RIGHT_BARS,
    ):

        current_high = float(
            result.iloc[i]["high"]
        )

        current_low = float(
            result.iloc[i]["low"]
        )

        previous_highs = result.iloc[
            i - LEFT_BARS:i
        ]["high"]

        next_highs = result.iloc[
            i + 1:i + RIGHT_BARS + 1
        ]["high"]

        previous_lows = result.iloc[
            i - LEFT_BARS:i
        ]["low"]

        next_lows = result.iloc[
            i + 1:i + RIGHT_BARS + 1
        ]["low"]

        # -------------------------------------------------
        # Swing High
        # -------------------------------------------------

        if (
            current_high > previous_highs.max()
            and
            current_high > next_highs.max()
        ):
            result.loc[
                result.index[i],
                "swing_high"
            ] = True

        # -------------------------------------------------
        # Swing Low
        # -------------------------------------------------

        if (
            current_low < previous_lows.min()
            and
            current_low < next_lows.min()
        ):
            result.loc[
                result.index[i],
                "swing_low"
            ] = True

    return result


# =========================================================
# 2. MARKET STRUCTURE CLASSIFICATION
# =========================================================

def classify_market_structure(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Classify confirmed swings as:

        HH = Higher High
        LH = Lower High
        HL = Higher Low
        LL = Lower Low

    IMPORTANT
    ---------
    Highs are compared ONLY against previous swing highs.

    Lows are compared ONLY against previous swing lows.

    This prevents a high from being compared against a low
    or vice versa.

    Returns the COMPLETE market dataframe.

    Columns:
        market_structure
        structure
    """

    result = _validate_market_data(df)

    # -----------------------------------------------------
    # Ensure swing columns exist
    # -----------------------------------------------------

    if (
        "swing_high" not in result.columns
        or
        "swing_low" not in result.columns
    ):
        result = detect_swings(result)

    # -----------------------------------------------------
    # Normalize swing flags
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # Structure columns
    # -----------------------------------------------------

    result["market_structure"] = None
    result["structure"] = None

    previous_swing_high = None
    previous_swing_low = None

    # =====================================================
    # Scan all candles
    # =====================================================

    for i in range(len(result)):

        index = result.index[i]

        # =================================================
        # SWING HIGH
        # =================================================

        if bool(
            result.iloc[i]["swing_high"]
        ):

            current_high = float(
                result.iloc[i]["high"]
            )

            label = None

            if previous_swing_high is not None:

                if (
                    current_high
                    >
                    previous_swing_high
                    and
                    not _prices_equal(
                        current_high,
                        previous_swing_high,
                    )
                ):
                    label = "HH"

                elif (
                    current_high
                    <
                    previous_swing_high
                    and
                    not _prices_equal(
                        current_high,
                        previous_swing_high,
                    )
                ):
                    label = "LH"

            if label is not None:

                result.loc[
                    index,
                    "market_structure"
                ] = label

                result.loc[
                    index,
                    "structure"
                ] = label

            previous_swing_high = current_high

        # =================================================
        # SWING LOW
        # =================================================

        if bool(
            result.iloc[i]["swing_low"]
        ):

            current_low = float(
                result.iloc[i]["low"]
            )

            label = None

            if previous_swing_low is not None:

                if (
                    current_low
                    >
                    previous_swing_low
                    and
                    not _prices_equal(
                        current_low,
                        previous_swing_low,
                    )
                ):
                    label = "HL"

                elif (
                    current_low
                    <
                    previous_swing_low
                    and
                    not _prices_equal(
                        current_low,
                        previous_swing_low,
                    )
                ):
                    label = "LL"

            if label is not None:

                result.loc[
                    index,
                    "market_structure"
                ] = label

                result.loc[
                    index,
                    "structure"
                ] = label

            previous_swing_low = current_low

    return result


# =========================================================
# 3. BOS / CHOCH DETECTION
# =========================================================

def detect_structure_breaks(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Detect confirmed BOS / CHOCH events.

    CORE LOGIC
    ----------

    Bullish structural break:
        candle CLOSE > active confirmed swing high

    Bearish structural break:
        candle CLOSE < active confirmed swing low

    Wick alone does NOT create BOS/CHOCH.

    STATE
    -----
    Every confirmed swing becomes a structural level.

    Once a level is broken, it is permanently marked as
    broken and cannot generate another event.

    If one candle jumps across multiple old levels, the
    nearest/latest structural level is used for the actual
    event and older crossed levels are also marked broken.

    BIAS
    ----
    No initial bias.

    First valid bullish break:
        BOS

    First valid bearish break:
        BOS

    Bullish break while bias == bearish:
        CHOCH

    Bearish break while bias == bullish:
        CHOCH

    Returns ONLY actual break-event rows.
    """

    result = _validate_market_data(df)

    # -----------------------------------------------------
    # Ensure swings exist
    # -----------------------------------------------------

    if (
        "swing_high" not in result.columns
        or
        "swing_low" not in result.columns
    ):
        result = detect_swings(result)

    # -----------------------------------------------------
    # Normalize swing flags
    # -----------------------------------------------------

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

    # =====================================================
    # EVENT STORAGE
    # =====================================================

    events = []

    # =====================================================
    # STRUCTURAL LEVEL STORAGE
    # =====================================================

    swing_high_levels = []
    swing_low_levels = []

    # =====================================================
    # STATE
    # =====================================================

    bias = None

    # =====================================================
    # SCAN MARKET
    # =====================================================

    for i in range(len(result)):

        current_time = result.iloc[i]["time"]

        high = float(
            result.iloc[i]["high"]
        )

        low = float(
            result.iloc[i]["low"]
        )

        close = float(
            result.iloc[i]["close"]
        )

        # =================================================
        # REGISTER CONFIRMED SWING HIGH
        # =================================================

        if bool(
            result.iloc[i]["swing_high"]
        ):

            swing_high_levels.append(
                {
                    "price": high,
                    "index": i,
                    "time": current_time,
                    "broken": False,
                }
            )

        # =================================================
        # REGISTER CONFIRMED SWING LOW
        # =================================================

        if bool(
            result.iloc[i]["swing_low"]
        ):

            swing_low_levels.append(
                {
                    "price": low,
                    "index": i,
                    "time": current_time,
                    "broken": False,
                }
            )

        # =================================================
        # BULLISH BREAK CANDIDATES
        # =================================================

        bullish_candidates = []

        for level in swing_high_levels:

            if level["broken"]:
                continue

            # A swing cannot break on its own candle.
            if i <= level["index"]:
                continue

            if close > level["price"]:

                bullish_candidates.append(level)

        # =================================================
        # BEARISH BREAK CANDIDATES
        # =================================================

        bearish_candidates = []

        for level in swing_low_levels:

            if level["broken"]:
                continue

            # A swing cannot break on its own candle.
            if i <= level["index"]:
                continue

            if close < level["price"]:

                bearish_candidates.append(level)

        # =================================================
        # DETERMINE BREAK DIRECTION
        # =================================================

        bullish_level = None
        bearish_level = None

        if bullish_candidates:

            # Most recent confirmed swing high.
            bullish_level = max(
                bullish_candidates,
                key=lambda x: x["index"],
            )

        if bearish_candidates:

            # Most recent confirmed swing low.
            bearish_level = max(
                bearish_candidates,
                key=lambda x: x["index"],
            )

        # =================================================
        # BOTH SIDES BROKEN ON SAME CANDLE
        # =================================================
        #
        # This can happen during a very large candle.
        #
        # We choose the side whose structural level is
        # closer to the candle close.
        #
        # This avoids generating two contradictory events
        # on the same candle.
        # =================================================

        selected_direction = None
        selected_level = None

        if (
            bullish_level is not None
            and
            bearish_level is not None
        ):

            bullish_distance = (
                close
                - bullish_level["price"]
            )

            bearish_distance = (
                bearish_level["price"]
                - close
            )

            if bullish_distance <= bearish_distance:

                selected_direction = "bullish"
                selected_level = bullish_level

            else:

                selected_direction = "bearish"
                selected_level = bearish_level

        elif bullish_level is not None:

            selected_direction = "bullish"
            selected_level = bullish_level

        elif bearish_level is not None:

            selected_direction = "bearish"
            selected_level = bearish_level

        # =================================================
        # NO BREAK
        # =================================================

        if selected_level is None:
            continue

        # =================================================
        # DETERMINE BREAK TYPE
        # =================================================

        previous_bias = bias

        if selected_direction == "bullish":

            if bias == "bearish":
                break_type = "CHOCH"
            else:
                break_type = "BOS"

        else:

            if bias == "bullish":
                break_type = "CHOCH"
            else:
                break_type = "BOS"

        # =================================================
        # BREAK LEVEL
        # =================================================

        break_level = float(
            selected_level["price"]
        )

        # =================================================
        # BREAK DISTANCE
        # =================================================

        if selected_direction == "bullish":

            break_distance = (
                close
                - break_level
            )

        else:

            break_distance = (
                break_level
                - close
            )

        if break_level != 0:

            break_strength_pct = (
                break_distance
                / abs(break_level)
                * 100.0
            )

        else:

            break_strength_pct = 0.0

        # =================================================
        # MARK ALL OLD CROSSED LEVELS AS BROKEN
        # =================================================
        #
        # Important:
        #
        # Suppose price closes above:
        #
        # High A = 4600
        # High B = 4605
        # High C = 4610
        #
        # and close = 4615.
        #
        # We emit one structural event for the latest
        # relevant level, but all levels already crossed
        # by the close become broken so they cannot create
        # fake BOS events on future candles.
        # =================================================

        if selected_direction == "bullish":

            for level in swing_high_levels:

                if level["broken"]:
                    continue

                if i <= level["index"]:
                    continue

                if close > level["price"]:

                    level["broken"] = True

        else:

            for level in swing_low_levels:

                if level["broken"]:
                    continue

                if i <= level["index"]:
                    continue

                if close < level["price"]:

                    level["broken"] = True

        # =================================================
        # UPDATE BIAS
        # =================================================

        bias = selected_direction

        # =================================================
        # STORE EVENT
        # =================================================

        events.append(
            {
                "time": current_time,

                "break_type": break_type,

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
                    float(break_distance),

                "break_strength_pct":
                    float(break_strength_pct),

                # Extra diagnostic information.
                "swing_time":
                    selected_level["time"],

                "swing_index":
                    selected_level["index"],
            }
        )

    # =====================================================
    # RETURN EVENTS ONLY
    # =====================================================

    if not events:

        return pd.DataFrame(
            columns=[
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
        )

    return pd.DataFrame(
        events
    ).reset_index(
        drop=True
    )


# =========================================================
# 4. COMPLETE MARKET STRUCTURE ANALYSIS
# =========================================================

def analyze_market_structure(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Complete market-structure pipeline.

    Steps:

        1. Detect confirmed swings
        2. Classify HH / HL / LH / LL
        3. Detect BOS / CHOCH
        4. Merge break events back into complete dataframe

    Returns ALL candles.

    Columns added:

        swing_high
        swing_low

        market_structure
        structure

        break_type
        break_direction
        break_level
        break_time
        broken_swing
        previous_bias
        break_distance
        break_strength_pct
    """

    # =====================================================
    # STEP 1
    # =====================================================

    result = detect_swings(
        df
    )

    # =====================================================
    # STEP 2
    # =====================================================

    result = classify_market_structure(
        result
    )

    # =====================================================
    # STEP 3
    # =====================================================

    breaks = detect_structure_breaks(
        result
    )

    # =====================================================
    # INITIALIZE BREAK COLUMNS
    # =====================================================

    break_columns = [
        "break_type",
        "break_direction",
        "break_level",
        "break_time",
        "broken_swing",
        "previous_bias",
        "break_distance",
        "break_strength_pct",
    ]

    for column in break_columns:
        result[column] = None

    # =====================================================
    # MERGE EVENTS BY CANDLE TIME
    # =====================================================

    if (
        not breaks.empty
        and
        "time" in breaks.columns
    ):

        for _, event in breaks.iterrows():

            event_time = event["time"]

            matches = (
                result["time"]
                ==
                event_time
            )

            if not matches.any():
                continue

            target_index = result.index[
                matches
            ][0]

            for column in break_columns:

                if column in event.index:

                    result.loc[
                        target_index,
                        column
                    ] = event[column]

    # =====================================================
    # RETURN COMPLETE DATAFRAME
    # =====================================================

    return result.reset_index(
        drop=True
    )


# =========================================================
# 5. OPTIONAL EVENT SUMMARY
# =========================================================

def get_structure_breaks(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Convenience wrapper.

    Returns only BOS / CHOCH events from a complete
    market-structure analysis dataframe.

    If the input already contains break information,
    those events are extracted directly.
    Otherwise the structure pipeline is executed.
    """

    required_event_columns = {
        "break_type",
        "break_direction",
        "break_level",
    }

    if required_event_columns.issubset(
        df.columns
    ):

        result = df[
            df["break_type"].notna()
        ].copy()

        return result.reset_index(
            drop=True
        )

    return detect_structure_breaks(
        df
    )


# =========================================================
# END OF MARKET STRUCTURE ENGINE
# =========================================================