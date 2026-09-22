import pandas as pd
import numpy as np


# =============================================================
# ORDER BLOCK ENGINE
# =============================================================
#
# Trading logic:
#
# Bullish OB:
#   Last meaningful bearish candle/base before bullish
#   displacement and preferably bullish BOS/CHOCH.
#
# Bearish OB:
#   Last meaningful bullish candle/base before bearish
#   displacement and preferably bearish BOS/CHOCH.
#
# Priority:
#
#   1. Opposing candle
#   2. Strong displacement
#   3. Structure confirmation
#   4. Efficient price departure
#   5. Fresh / unmitigated zone
#
# This module does NOT require every OB to have structure.
# Confluence Engine decides the final qualification.
#
# =============================================================


def detect_order_blocks(
    df: pd.DataFrame,
    displacement_df: pd.DataFrame | None = None,
    structure_breaks_df: pd.DataFrame | None = None,
    lookback: int = 150,
    max_confirmation_bars: int = 8,
    min_expansion_atr: float = 1.20,
    min_body_ratio: float = 0.35,
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
            f"Missing required columns: {sorted(missing)}"
        )

    data = df.copy().reset_index(drop=True)

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

    data = data.dropna(
        subset=[
            "time",
            "open",
            "high",
            "low",
            "close",
        ]
    ).reset_index(drop=True)

    if data.empty:
        return _empty_result()

    data = (
        data
        .sort_values("time")
        .reset_index(drop=True)
    )

    # =========================================================
    # CANDLE METRICS
    # =========================================================

    data["range"] = (
        data["high"]
        - data["low"]
    )

    data["body"] = (
        data["close"]
        - data["open"]
    ).abs()

    data["body_ratio"] = np.where(
        data["range"] > 0,
        data["body"] / data["range"],
        0.0,
    )

    data["bullish"] = (
        data["close"] > data["open"]
    )

    data["bearish"] = (
        data["close"] < data["open"]
    )

    # =========================================================
    # ATR
    # =========================================================

    previous_close = data["close"].shift(1)

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

    data["atr"] = (
        data["true_range"]
        .rolling(
            window=14,
            min_periods=5,
        )
        .mean()
    )

    # =========================================================
    # INPUT NORMALIZATION
    # =========================================================

    displacement_data = _safe_copy(
        displacement_df
    )

    structure_data = _safe_copy(
        structure_breaks_df
    )

    _normalize_time(
        displacement_data
    )

    _normalize_time(
        structure_data
    )

    # =========================================================
    # PREPARE DISPLACEMENT DATA
    # =========================================================

    displacement_by_time = {}

    if not displacement_data.empty:

        if "time" in displacement_data.columns:

            for _, row in displacement_data.iterrows():

                timestamp = _to_timestamp(
                    row.get("time")
                )

                if timestamp is None:
                    continue

                direction = _normalize_direction(
                    row.get(
                        "displacement_direction"
                    )
                )

                if direction not in (
                    "bullish",
                    "bearish",
                ):
                    continue

                if "is_displacement" in row.index:

                    if not _is_true(
                        row.get(
                            "is_displacement"
                        )
                    ):
                        continue

                displacement_by_time[
                    timestamp
                ] = row

    # =========================================================
    # PREPARE STRUCTURE DATA
    # =========================================================

    structure_by_time = {}

    if not structure_data.empty:

        if "time" in structure_data.columns:

            for _, row in structure_data.iterrows():

                timestamp = _to_timestamp(
                    row.get("time")
                )

                if timestamp is None:
                    continue

                direction = _normalize_direction(
                    row.get(
                        "break_direction"
                    )
                )

                if direction not in (
                    "bullish",
                    "bearish",
                ):
                    continue

                break_type = str(
                    row.get(
                        "break_type",
                        "",
                    )
                ).strip().upper()

                if break_type not in (
                    "BOS",
                    "CHOCH",
                ):
                    continue

                structure_by_time[
                    timestamp
                ] = row

    # =========================================================
    # MARKET INDEX
    # =========================================================

    market_times = (
        data["time"]
        .values
    )

    # =========================================================
    # SEARCH RANGE
    # =========================================================

    start_index = max(
        1,
        len(data) - lookback,
    )

    candidates = []

    # =========================================================
    # DETECT OB CANDIDATES
    # =========================================================

    for i in range(
        start_index,
        len(data) - 1,
    ):

        candle = data.iloc[i]

        candle_time = _to_timestamp(
            candle["time"]
        )

        if candle_time is None:
            continue

        atr = _safe_float(
            candle["atr"]
        )

        if atr is None or atr <= 0:
            continue

        # =====================================================
        # BULLISH ORDER BLOCK
        # =====================================================
        #
        # Last bearish candle before bullish expansion.
        #
        # =====================================================

        if candle["bearish"]:

            bullish_signal = _find_bullish_departure(
                data=data,
                start_index=i,
                max_bars=max_confirmation_bars,
                atr=atr,
                min_expansion_atr=min_expansion_atr,
                displacement_by_time=displacement_by_time,
                structure_by_time=structure_by_time,
                market_times=market_times,
            )

            if bullish_signal is not None:

                candidates.append(
                    _build_order_block(
                        candle=candle,
                        direction="bullish",
                        signal=bullish_signal,
                    )
                )

        # =====================================================
        # BEARISH ORDER BLOCK
        # =====================================================

        if candle["bullish"]:

            bearish_signal = _find_bearish_departure(
                data=data,
                start_index=i,
                max_bars=max_confirmation_bars,
                atr=atr,
                min_expansion_atr=min_expansion_atr,
                displacement_by_time=displacement_by_time,
                structure_by_time=structure_by_time,
                market_times=market_times,
            )

            if bearish_signal is not None:

                candidates.append(
                    _build_order_block(
                        candle=candle,
                        direction="bearish",
                        signal=bearish_signal,
                    )
                )

    # =========================================================
    # RESULT
    # =========================================================

    result = pd.DataFrame(
        candidates
    )

    if result.empty:
        return _empty_result()

    # =========================================================
    # REMOVE DUPLICATE OBs
    # =========================================================

    result = (
        result
        .sort_values(
            [
                "time",
                "score",
            ],
            ascending=[
                True,
                False,
            ],
        )
        .drop_duplicates(
            subset=[
                "time",
                "ob_type",
            ],
            keep="first",
        )
        .reset_index(drop=True)
    )

    # =========================================================
    # REMOVE NESTED / NEAR IDENTICAL OBs
    # =========================================================

    result = _remove_redundant_ob(
        result
    )

    if result.empty:
        return _empty_result()

    # =========================================================
    # STATUS / MITIGATION
    # =========================================================

    statuses = []
    touches = []
    mitigated_flags = []

    for _, ob in result.iterrows():

        (
            status,
            touch_count,
            was_mitigated,
        ) = _evaluate_ob_status(
            ob=ob,
            data=data,
        )

        statuses.append(
            status
        )

        touches.append(
            touch_count
        )

        mitigated_flags.append(
            was_mitigated
        )

    result["status"] = statuses

    result["touches"] = touches

    result["mitigated"] = (
        mitigated_flags
    )

    # =========================================================
    # STATUS QUALITY ADJUSTMENT
    # =========================================================

    result["score"] = (
        result["score"]
        .astype(float)
    )

    for index, row in result.iterrows():

        score = float(
            row["score"]
        )

        status = str(
            row["status"]
        ).lower()

        if status == "fresh":

            score += 8.0

        elif status == "tested":

            score += 2.0

        elif status == "mitigated":

            score -= 25.0

        result.at[
            index,
            "score"
        ] = np.clip(
            score,
            0.0,
            100.0,
        )

    # =========================================================
    # FINAL ORDER
    # =========================================================

    result = (
        result
        .sort_values(
            [
                "time",
                "score",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )

    return result


# =============================================================
# BULLISH DEPARTURE
# =============================================================

def _find_bullish_departure(
    data,
    start_index,
    max_bars,
    atr,
    min_expansion_atr,
    displacement_by_time,
    structure_by_time,
    market_times,
):

    end_index = min(
        start_index + max_bars,
        len(data) - 1,
    )

    best = None

    for j in range(
        start_index + 1,
        end_index + 1,
    ):

        candle = data.iloc[j]

        move_high = (
            data.iloc[
                start_index + 1:
                j + 1
            ]["high"].max()
        )

        expansion = (
            move_high
            - data.iloc[
                start_index
            ]["low"]
        ) / atr

        if expansion < min_expansion_atr:
            continue

        candle_time = _to_timestamp(
            candle["time"]
        )

        if candle_time is None:
            continue

        displacement_match = False
        displacement_row = None

        # -----------------------------------------------------
        # Displacement confirmation
        # -----------------------------------------------------

        for k in range(
            start_index + 1,
            j + 1,
        ):

            t = _to_timestamp(
                data.iloc[k]["time"]
            )

            if t not in displacement_by_time:
                continue

            d = displacement_by_time[t]

            if (
                _normalize_direction(
                    d.get(
                        "displacement_direction"
                    )
                )
                == "bullish"
            ):

                displacement_match = True
                displacement_row = d
                break

        # -----------------------------------------------------
        # Structure confirmation
        # -----------------------------------------------------

        structure_match = False
        structure_row = None

        for k in range(
            start_index + 1,
            j + 1,
        ):

            t = _to_timestamp(
                data.iloc[k]["time"]
            )

            if t not in structure_by_time:
                continue

            s = structure_by_time[t]

            if (
                _normalize_direction(
                    s.get(
                        "break_direction"
                    )
                )
                == "bullish"
            ):

                structure_match = True
                structure_row = s
                break

        # -----------------------------------------------------
        # Price departure
        # -----------------------------------------------------

        closes_above_ob = (
            data.iloc[
                start_index + 1:
                j + 1
            ]["close"]
            > data.iloc[
                start_index
            ]["high"]
        ).any()

        if not closes_above_ob:
            continue

        # -----------------------------------------------------
        # Prefer displacement / structure
        # -----------------------------------------------------

        if (
            not displacement_match
            and not structure_match
        ):
            continue

        score = _departure_score(
            expansion=expansion,
            candle=data.iloc[
                start_index
            ],
            displacement=displacement_match,
            structure=structure_match,
            structure_row=structure_row,
        )

        candidate = {
            "signal_index": j,
            "expansion_atr": expansion,
            "displacement_confirmation":
                displacement_match,
            "structure_confirmation":
                structure_match,
            "displacement_row":
                displacement_row,
            "structure_row":
                structure_row,
            "score":
                score,
        }

        if (
            best is None
            or candidate["score"]
            > best["score"]
        ):
            best = candidate

    return best


# =============================================================
# BEARISH DEPARTURE
# =============================================================

def _find_bearish_departure(
    data,
    start_index,
    max_bars,
    atr,
    min_expansion_atr,
    displacement_by_time,
    structure_by_time,
    market_times,
):

    end_index = min(
        start_index + max_bars,
        len(data) - 1,
    )

    best = None

    for j in range(
        start_index + 1,
        end_index + 1,
    ):

        candle = data.iloc[j]

        move_low = (
            data.iloc[
                start_index + 1:
                j + 1
            ]["low"].min()
        )

        expansion = (
            data.iloc[
                start_index
            ]["high"]
            - move_low
        ) / atr

        if expansion < min_expansion_atr:
            continue

        displacement_match = False
        displacement_row = None

        # -----------------------------------------------------
        # Displacement
        # -----------------------------------------------------

        for k in range(
            start_index + 1,
            j + 1,
        ):

            t = _to_timestamp(
                data.iloc[k]["time"]
            )

            if t not in displacement_by_time:
                continue

            d = displacement_by_time[t]

            if (
                _normalize_direction(
                    d.get(
                        "displacement_direction"
                    )
                )
                == "bearish"
            ):

                displacement_match = True
                displacement_row = d
                break

        # -----------------------------------------------------
        # Structure
        # -----------------------------------------------------

        structure_match = False
        structure_row = None

        for k in range(
            start_index + 1,
            j + 1,
        ):

            t = _to_timestamp(
                data.iloc[k]["time"]
            )

            if t not in structure_by_time:
                continue

            s = structure_by_time[t]

            if (
                _normalize_direction(
                    s.get(
                        "break_direction"
                    )
                )
                == "bearish"
            ):

                structure_match = True
                structure_row = s
                break

        # -----------------------------------------------------
        # Price departure
        # -----------------------------------------------------

        closes_below_ob = (
            data.iloc[
                start_index + 1:
                j + 1
            ]["close"]
            < data.iloc[
                start_index
            ]["low"]
        ).any()

        if not closes_below_ob:
            continue

        if (
            not displacement_match
            and not structure_match
        ):
            continue

        score = _departure_score(
            expansion=expansion,
            candle=data.iloc[
                start_index
            ],
            displacement=displacement_match,
            structure=structure_match,
            structure_row=structure_row,
        )

        candidate = {
            "signal_index": j,
            "expansion_atr": expansion,
            "displacement_confirmation":
                displacement_match,
            "structure_confirmation":
                structure_match,
            "displacement_row":
                displacement_row,
            "structure_row":
                structure_row,
            "score":
                score,
        }

        if (
            best is None
            or candidate["score"]
            > best["score"]
        ):
            best = candidate

    return best


# =============================================================
# SCORE
# =============================================================

def _departure_score(
    expansion,
    candle,
    displacement,
    structure,
    structure_row,
):

    score = 35.0

    # ---------------------------------------------------------
    # Candle quality
    # ---------------------------------------------------------

    body_ratio = _safe_float(
        candle["body_ratio"],
        default=0.0,
    )

    if body_ratio >= 0.70:

        score += 12.0

    elif body_ratio >= 0.55:

        score += 9.0

    elif body_ratio >= 0.35:

        score += 5.0

    # ---------------------------------------------------------
    # Expansion
    # ---------------------------------------------------------

    if expansion >= 2.50:

        score += 25.0

    elif expansion >= 2.00:

        score += 20.0

    elif expansion >= 1.50:

        score += 15.0

    elif expansion >= 1.20:

        score += 10.0

    # ---------------------------------------------------------
    # Displacement
    # ---------------------------------------------------------

    if displacement:
        score += 15.0

    # ---------------------------------------------------------
    # Structure
    # ---------------------------------------------------------

    if structure:

        score += 10.0

        if structure_row is not None:

            break_type = str(
                structure_row.get(
                    "break_type",
                    "",
                )
            ).strip().upper()

            if break_type == "CHOCH":
                score += 5.0

    return float(
        min(
            score,
            100.0,
        )
    )


# =============================================================
# BUILD OB
# =============================================================

def _build_order_block(
    candle,
    direction,
    signal,
):

    ob_high = _safe_float(
        candle["high"]
    )

    ob_low = _safe_float(
        candle["low"]
    )

    signal_time = None

    if signal.get(
        "signal_index"
    ) is not None:

        signal_time = None

    displacement_time = None

    displacement_row = signal.get(
        "displacement_row"
    )

    if displacement_row is not None:

        displacement_time = _to_timestamp(
            displacement_row.get(
                "time"
            )
        )

    structure_time = None

    structure_row = signal.get(
        "structure_row"
    )

    if structure_row is not None:

        structure_time = _to_timestamp(
            structure_row.get(
                "time"
            )
        )

    structure_type = None

    if structure_row is not None:

        structure_type = str(
            structure_row.get(
                "break_type",
                "",
            )
        ).strip().upper()

    return {
        "time": candle["time"],

        "ob_type": direction,

        "ob_high": float(
            ob_high
        ),

        "ob_low": float(
            ob_low
        ),

        "ob_size": float(
            ob_high - ob_low
        ),

        "body_ratio": float(
            candle["body_ratio"]
        ),

        "expansion_atr": float(
            signal["expansion_atr"]
        ),

        "displacement_confirmation":
            bool(
                signal[
                    "displacement_confirmation"
                ]
            ),

        "displacement_time":
            displacement_time,

        "structure_confirmation":
            bool(
                signal[
                    "structure_confirmation"
                ]
            ),

        "structure_break_time":
            structure_time,

        "structure_type":
            structure_type,

        "score": float(
            signal["score"]
        ),
    }


# =============================================================
# STATUS / MITIGATION
# =============================================================

def _evaluate_ob_status(
    ob,
    data,
):

    ob_time = _to_timestamp(
        ob.get("time")
    )

    if ob_time is None:
        return (
            "mitigated",
            0,
            True,
        )

    ob_high = _safe_float(
        ob.get("ob_high")
    )

    ob_low = _safe_float(
        ob.get("ob_low")
    )

    if (
        ob_high is None
        or ob_low is None
        or ob_high <= ob_low
    ):
        return (
            "mitigated",
            0,
            True,
        )

    future = data[
        data["time"] > ob_time
    ].copy()

    touch_count = 0

    mitigated = False

    # =========================================================
    # IMPORTANT
    #
    # A wick into the OB is a TEST.
    #
    # A CLOSE through the opposite edge is MITIGATION /
    # INVALIDATION.
    # =========================================================

    for _, candle in future.iterrows():

        overlaps = (
            candle["low"] <= ob_high
            and
            candle["high"] >= ob_low
        )

        if overlaps:
            touch_count += 1

        if ob["ob_type"] == "bullish":

            if candle["close"] < ob_low:

                mitigated = True
                break

        else:

            if candle["close"] > ob_high:

                mitigated = True
                break

    if mitigated:

        return (
            "mitigated",
            touch_count,
            True,
        )

    if touch_count == 0:

        return (
            "fresh",
            0,
            False,
        )

    return (
        "tested",
        touch_count,
        False,
    )


# =============================================================
# REDUNDANT OB FILTER
# =============================================================

def _remove_redundant_ob(
    result,
):

    if result.empty:
        return result

    working = (
        result
        .sort_values(
            [
                "score",
                "time",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )

    kept = []

    for _, candidate in working.iterrows():

        duplicate = False

        c_high = _safe_float(
            candidate.get(
                "ob_high"
            )
        )

        c_low = _safe_float(
            candidate.get(
                "ob_low"
            )
        )

        c_direction = _normalize_direction(
            candidate.get(
                "ob_type"
            )
        )

        if (
            c_high is None
            or c_low is None
            or c_high <= c_low
        ):
            continue

        for existing in kept:

            e_direction = _normalize_direction(
                existing.get(
                    "ob_type"
                )
            )

            if (
                e_direction
                != c_direction
            ):
                continue

            e_high = _safe_float(
                existing.get(
                    "ob_high"
                )
            )

            e_low = _safe_float(
                existing.get(
                    "ob_low"
                )
            )

            if (
                e_high is None
                or e_low is None
            ):
                continue

            overlap_high = min(
                c_high,
                e_high,
            )

            overlap_low = max(
                c_low,
                e_low,
            )

            overlap = (
                overlap_high
                - overlap_low
            )

            if overlap <= 0:
                continue

            c_size = (
                c_high
                - c_low
            )

            e_size = (
                e_high
                - e_low
            )

            smaller = min(
                c_size,
                e_size,
            )

            if smaller <= 0:
                continue

            overlap_ratio = (
                overlap
                / smaller
            )

            # Strong overlap means the two OBs represent
            # essentially the same price area.
            if overlap_ratio >= 0.70:

                duplicate = True
                break

        if not duplicate:

            kept.append(
                candidate
            )

    if not kept:

        return pd.DataFrame(
            columns=result.columns
        )

    return (
        pd.DataFrame(
            kept
        )
        .sort_values(
            "time",
            ascending=False,
        )
        .reset_index(drop=True)
    )


# =============================================================
# HELPERS
# =============================================================

def _safe_copy(
    frame,
):

    if frame is None:
        return pd.DataFrame()

    if not isinstance(
        frame,
        pd.DataFrame,
    ):
        return pd.DataFrame()

    return frame.copy()


def _normalize_time(
    frame,
):

    if frame.empty:
        return frame

    if "time" in frame.columns:

        frame["time"] = pd.to_datetime(
            frame["time"],
            errors="coerce",
        )

    return frame


def _to_timestamp(
    value,
):

    if value is None:
        return None

    try:

        timestamp = pd.Timestamp(
            value
        )

        if pd.isna(timestamp):
            return None

        return timestamp

    except Exception:

        return None


def _safe_float(
    value,
    default=None,
):

    try:

        number = float(value)

        if np.isfinite(number):
            return number

    except (
        TypeError,
        ValueError,
    ):

        pass

    return default


def _normalize_direction(
    value,
):

    if value is None:
        return ""

    text = str(
        value
    ).strip().lower()

    aliases = {

        "buy":
            "bullish",

        "long":
            "bullish",

        "bull":
            "bullish",

        "bullish":
            "bullish",

        "sell":
            "bearish",

        "short":
            "bearish",

        "bear":
            "bearish",

        "bearish":
            "bearish",
    }

    return aliases.get(
        text,
        text,
    )


def _is_true(
    value,
):

    if isinstance(
        value,
        (
            bool,
            np.bool_,
        ),
    ):

        return bool(value)

    if value is None:
        return False

    if isinstance(
        value,
        (
            int,
            float,
            np.integer,
            np.floating,
        ),
    ):

        try:

            if not np.isfinite(
                float(value)
            ):
                return False

            return (
                float(value)
                != 0.0
            )

        except Exception:

            return False

    text = str(
        value
    ).strip().lower()

    return text in {
        "true",
        "1",
        "yes",
        "y",
        "confirmed",
        "valid",
    }


# =============================================================
# EMPTY RESULT
# =============================================================

def _empty_result():

    return pd.DataFrame(
        columns=[
            "time",
            "ob_type",
            "ob_high",
            "ob_low",
            "ob_size",
            "body_ratio",
            "expansion_atr",
            "displacement_confirmation",
            "displacement_time",
            "structure_confirmation",
            "structure_break_time",
            "structure_type",
            "score",
            "status",
            "touches",
            "mitigated",
        ]
    )