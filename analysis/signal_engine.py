
import pandas as pd
import numpy as np


# =========================================================
# SIGNAL ENGINE
# =========================================================
#
# Converts Confluence Zones into executable trading setups.
#
# IMPORTANT:
#
# A Confluence Zone is NOT considered available at OB time.
#
# The zone becomes tradable only after all confirmations that
# were used to create it have actually occurred.
#
# Therefore:
#
#     zone_ready_time =
#         max(
#             ob_time,
#             fvg_time,
#             sweep_time,
#             displacement_time,
#             structure_break_time
#         )
#
# Only confirmations that are actually enabled in the zone
# are included.
#
# This prevents look-ahead bias during:
#
#     Live
#     Backtest
#     Walk-forward
#
# =========================================================


DEFAULT_MIN_SCORE = 55.0
DEFAULT_MIN_CONFIRMATIONS = 2
DEFAULT_MAX_DISTANCE_ATR = 3.0
DEFAULT_ATR_PERIOD = 14

# =========================================================
# TREND REGIME FILTER
# =========================================================
#
# The Signal Engine is used on M5 XAUUSD. A high-confluence zone can
# still be a counter-trend setup, so confluence alone is not sufficient
# for an executable signal. This gate uses only CLOSED data:
#
#   M5  -> local momentum
#   M15 -> primary scalp trend
#   H1  -> higher-timeframe regime
#
# A BUY requires M15 bullish and neither H1 nor M5 to be bearish.
# A SELL requires M15 bearish and neither H1 nor M5 to be bullish.
#
# This is a validation gate, not a ranking bonus. It is deliberately
# conservative and contains no future/intrabar information.
# =========================================================

TREND_FAST_EMA = 20
TREND_SLOW_EMA = 50
TREND_SLOPE_LOOKBACK = 3


def _trend_state(frame: pd.DataFrame) -> str:
    if frame is None or frame.empty or len(frame) < TREND_SLOW_EMA + TREND_SLOPE_LOOKBACK:
        return "NEUTRAL"

    close = pd.to_numeric(frame["close"], errors="coerce")
    fast = close.ewm(span=TREND_FAST_EMA, adjust=False, min_periods=TREND_FAST_EMA).mean()
    slow = close.ewm(span=TREND_SLOW_EMA, adjust=False, min_periods=TREND_SLOW_EMA).mean()

    if fast.isna().iloc[-1] or slow.isna().iloc[-1]:
        return "NEUTRAL"

    last_close = float(close.iloc[-1])
    last_fast = float(fast.iloc[-1])
    last_slow = float(slow.iloc[-1])
    slope_fast = float(fast.iloc[-1] - fast.iloc[-TREND_SLOPE_LOOKBACK])

    if last_close > last_fast > last_slow and slope_fast > 0:
        return "BULLISH"
    if last_close < last_fast < last_slow and slope_fast < 0:
        return "BEARISH"

    return "NEUTRAL"


def _build_trend_regime(market_data: pd.DataFrame):
    """Return M5/M15/H1 closed-candle trend states without look-ahead."""
    frame = market_data[["time", "open", "high", "low", "close"]].copy()
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["time", "close"]).sort_values("time").reset_index(drop=True)

    m5_state = _trend_state(frame)

    indexed = frame.set_index("time")
    m15 = indexed["close"].resample("15min", label="right", closed="right").last().dropna().to_frame()
    h1 = indexed["close"].resample("1h", label="right", closed="right").last().dropna().to_frame()

    m15_state = _trend_state(m15)
    h1_state = _trend_state(h1)

    return {
        "m5": m5_state,
        "m15": m15_state,
        "h1": h1_state,
    }


def _trend_filter_passes(signal_direction: str, regime: dict) -> bool:
    direction = str(signal_direction or "").upper()

    m5 = regime.get("m5", "NEUTRAL")
    m15 = regime.get("m15", "NEUTRAL")
    h1 = regime.get("h1", "NEUTRAL")

    if direction == "BUY":
        return (
            m15 == "BULLISH"
            and m5 != "BEARISH"
            and h1 != "BEARISH"
        )

    if direction == "SELL":
        return (
            m15 == "BEARISH"
            and m5 != "BULLISH"
            and h1 != "BULLISH"
        )

    return False


# =========================================================
# PRIORITY / RANKING SETTINGS
# =========================================================
#
# Ranking does NOT decide whether a signal is valid.
#
# It only decides which valid signal should be considered
# first by the caller / backtest / manual trader.
#
# Higher score = higher priority.
#
# =========================================================

RANK_WEIGHT_CONFLUENCE = 0.50
RANK_WEIGHT_CONFIRMATIONS = 0.20
RANK_WEIGHT_PROXIMITY = 0.15
RANK_WEIGHT_FRESHNESS = 0.10
RANK_WEIGHT_QUALITY = 0.05


# =========================================================
# PUBLIC FUNCTION
# =========================================================

def detect_signals(
    df: pd.DataFrame,
    confluence_zones: pd.DataFrame,
    min_score: float = DEFAULT_MIN_SCORE,
    min_confirmations: int = DEFAULT_MIN_CONFIRMATIONS,
    max_distance_atr: float = DEFAULT_MAX_DISTANCE_ATR,
    as_of_time=None,
) -> pd.DataFrame:

    # =====================================================
    # VALIDATE MARKET DATA
    # =====================================================

    required_market_columns = {
        "time",
        "open",
        "high",
        "low",
        "close",
    }

    missing_market = (
        required_market_columns
        - set(df.columns)
    )

    if missing_market:
        raise ValueError(
            "Market data missing columns: "
            f"{sorted(missing_market)}"
        )

    # =====================================================
    # EMPTY CONFLUENCE
    # =====================================================

    if confluence_zones is None:
        return _empty_signal_result()

    if confluence_zones.empty:
        return _empty_signal_result()

    # =====================================================
    # REQUIRED CONFLUENCE COLUMNS
    # =====================================================

    required_confluence_columns = {
        "ob_time",
        "direction",
        "zone_high",
        "zone_low",
        "confluence_score",
        "quality",
    }

    missing_confluence = (
        required_confluence_columns
        - set(confluence_zones.columns)
    )

    if missing_confluence:
        raise ValueError(
            "Confluence data missing columns: "
            f"{sorted(missing_confluence)}"
        )

    # =====================================================
    # PREPARE MARKET DATA
    # =====================================================

    data = (
        df.copy()
        .reset_index(drop=True)
    )

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
        return _empty_signal_result()

    # =====================================================
    # DETERMINE AS-OF TIME
    # =====================================================

    if as_of_time is None:

        current_time = pd.Timestamp(
            data.iloc[-1]["time"]
        )

    else:

        try:

            current_time = pd.Timestamp(
                as_of_time
            )

        except Exception:

            current_time = pd.Timestamp(
                data.iloc[-1]["time"]
            )

    # =====================================================
    # REMOVE FUTURE MARKET DATA
    # =====================================================

    historical_data = data[
        data["time"] <= current_time
    ].copy()

    if historical_data.empty:
        return _empty_signal_result()

    historical_data = (
        historical_data
        .reset_index(drop=True)
    )

    # =====================================================
    # MULTI-TIMEFRAME TREND REGIME
    # =====================================================
    # Only data at or before the current closed candle is used.
    trend_regime = _build_trend_regime(historical_data)

    # =====================================================
    # CURRENT CANDLE
    # =====================================================

    current_row = (
        historical_data
        .iloc[-1]
    )

    current_price = float(
        current_row["close"]
    )

    current_time = pd.Timestamp(
        current_row["time"]
    )

    # =====================================================
    # ATR
    # =====================================================

    previous_close = (
        historical_data["close"]
        .shift(1)
    )

    tr1 = (
        historical_data["high"]
        - historical_data["low"]
    )

    tr2 = (
        historical_data["high"]
        - previous_close
    ).abs()

    tr3 = (
        historical_data["low"]
        - previous_close
    ).abs()

    historical_data["true_range"] = (
        pd.concat(
            [
                tr1,
                tr2,
                tr3,
            ],
            axis=1,
        )
        .max(axis=1)
    )

    historical_data["atr"] = (
        historical_data["true_range"]
        .rolling(
            DEFAULT_ATR_PERIOD,
            min_periods=5,
        )
        .mean()
    )

    current_atr = (
        historical_data
        .iloc[-1]["atr"]
    )

    # =====================================================
    # ATR FALLBACK
    # =====================================================

    if (
        pd.isna(current_atr)
        or current_atr <= 0
    ):

        current_atr = (
            historical_data[
                "true_range"
            ]
            .tail(DEFAULT_ATR_PERIOD)
            .mean()
        )

    if (
        pd.isna(current_atr)
        or current_atr <= 0
    ):

        current_atr = (
            (
                historical_data["high"]
                - historical_data["low"]
            )
            .tail(DEFAULT_ATR_PERIOD)
            .mean()
        )

    if pd.isna(current_atr):
        current_atr = None

    # =====================================================
    # PREPARE ZONES
    # =====================================================

    zones = (
        confluence_zones.copy()
        .reset_index(drop=True)
    )

    zones["ob_time"] = pd.to_datetime(
        zones["ob_time"],
        errors="coerce",
    )

    # =====================================================
    # NORMALIZE CONFIRMATION TIMES
    # =====================================================

    _normalize_zone_time(
        zones,
        "fvg_time",
    )

    _normalize_zone_time(
        zones,
        "sweep_time",
    )

    _normalize_zone_time(
        zones,
        "displacement_time",
    )

    _normalize_zone_time(
        zones,
        "structure_break_time",
    )

    # =====================================================
    # CALCULATE REAL ZONE READY TIME
    # =====================================================

    zones["zone_ready_time"] = zones.apply(
        _calculate_zone_ready_time,
        axis=1,
    )

    # =====================================================
    # ONLY FULLY FORMED ZONES
    # =====================================================

    zones = zones[
        zones["zone_ready_time"].notna()
        & (
            zones["zone_ready_time"]
            <= current_time
        )
    ].copy()

    if zones.empty:
        return _empty_signal_result()

    # =====================================================
    # SORT OLDEST -> NEWEST
    # =====================================================

    zones = (
        zones
        .sort_values(
            "zone_ready_time"
        )
        .reset_index(drop=True)
    )

    # =====================================================
    # SIGNAL STORAGE
    # =====================================================

    signals = []

    # =====================================================
    # PROCESS ZONES
    # =====================================================

    for _, zone in zones.iterrows():

        # =================================================
        # ZONE READY TIME
        # =================================================

        zone_ready_time = zone.get(
            "zone_ready_time",
            None,
        )

        if pd.isna(zone_ready_time):
            continue

        zone_ready_time = pd.Timestamp(
            zone_ready_time
        )

        if zone_ready_time > current_time:
            continue

        # =================================================
        # ORIGINAL OB TIME
        # =================================================

        zone_time = zone.get(
            "ob_time",
            None,
        )

        if pd.isna(zone_time):
            continue

        zone_time = pd.Timestamp(
            zone_time
        )

        # =================================================
        # DIRECTION
        # =================================================

        direction = str(
            zone.get(
                "direction",
                "",
            )
        ).strip().lower()

        if direction not in (
            "bullish",
            "bearish",
        ):
            continue

        # =================================================
        # ZONE PRICE
        # =================================================

        zone_high = _safe_float(
            zone.get(
                "zone_high"
            )
        )

        zone_low = _safe_float(
            zone.get(
                "zone_low"
            )
        )

        if (
            zone_high is None
            or zone_low is None
            or zone_high <= zone_low
        ):
            continue

        # =================================================
        # CONFLUENCE SCORE
        # =================================================

        confluence_score = _safe_float(
            zone.get(
                "confluence_score",
                0.0,
            )
        )

        if confluence_score is None:
            continue

        if confluence_score < float(
            min_score
        ):
            continue

        # =================================================
        # CONFIRMATION COUNT
        # =================================================

        confirmation_count = _safe_int(
            zone.get(
                "confirmation_count",
                0,
            ),
            default=0,
        )

        if confirmation_count < int(
            min_confirmations
        ):
            continue

        # =================================================
        # CONFIRMATION FLAGS
        # =================================================

        fvg_confirmation = _to_bool(
            zone.get(
                "fvg_confirmation",
                False,
            )
        )

        liquidity_confirmation = _to_bool(
            zone.get(
                "liquidity_confirmation",
                False,
            )
        )

        displacement_confirmation = _to_bool(
            zone.get(
                "displacement_confirmation",
                False,
            )
        )

        structure_confirmation = _to_bool(
            zone.get(
                "structure_confirmation",
                False,
            )
        )

        # =================================================
        # ORDER BLOCK STATUS
        # =================================================

        ob_status = str(
            zone.get(
                "ob_status",
                "",
            )
        ).strip().lower()

        if ob_status == "mitigated":
            continue

        # =================================================
        # CURRENT PRICE VS ZONE
        # =================================================

        if (
            zone_low
            <= current_price
            <= zone_high
        ):

            price_inside_zone = True
            distance = 0.0
            setup_location = "INSIDE_ZONE"

        elif current_price > zone_high:

            price_inside_zone = False

            distance = (
                current_price
                - zone_high
            )

            setup_location = "BELOW_PRICE"

        else:

            price_inside_zone = False

            distance = (
                zone_low
                - current_price
            )

            setup_location = "ABOVE_PRICE"

        # =================================================
        # DISTANCE IN ATR
        # =================================================

        if (
            current_atr is not None
            and current_atr > 0
        ):

            distance_atr = (
                distance
                / float(current_atr)
            )

        else:

            distance_atr = distance

        if not np.isfinite(
            distance_atr
        ):
            continue

        if (
            distance_atr
            > float(max_distance_atr)
        ):
            continue

        # =================================================
        # SIGNAL DIRECTION
        # =================================================

        if direction == "bullish":

            signal_direction = "BUY"

            if setup_location == "ABOVE_PRICE":
                continue

        else:

            signal_direction = "SELL"

            if setup_location == "BELOW_PRICE":
                continue

        # =================================================
        # MULTI-TIMEFRAME TREND GATE
        # =================================================
        # A setup can have excellent confluence and still be counter-trend.
        # Do not emit it as an executable signal unless the closed M5/M15/H1
        # regime agrees with its direction.
        if not _trend_filter_passes(
            signal_direction,
            trend_regime,
        ):
            continue

        # =================================================
        # STRUCTURE TYPE
        # =================================================

        structure_type = zone.get(
            "structure_type",
            None,
        )

        if pd.isna(
            structure_type
        ):

            structure_type = None

        elif structure_type is not None:

            structure_type = str(
                structure_type
            ).strip().upper()

        # =================================================
        # SIGNAL STRENGTH
        # =================================================

        if (
            confluence_score >= 90.0
            and confirmation_count >= 4
        ):

            signal_strength = "VERY_STRONG"

        elif (
            confluence_score >= 80.0
            and confirmation_count >= 3
        ):

            signal_strength = "STRONG"

        elif (
            confluence_score >= 70.0
            and confirmation_count >= 3
        ):

            signal_strength = "NORMAL"

        elif (
            confluence_score >= 60.0
            and confirmation_count >= 2
        ):

            signal_strength = "QUALIFIED"

        else:

            signal_strength = "VALID"

        # =================================================
        # SETUP STATUS
        # =================================================

        if price_inside_zone:

            setup_status = "ACTIVE"

        else:

            setup_status = "WAITING_RETEST"

        # =================================================
        # ENTRY REFERENCE
        # =================================================

        entry_reference = (
            zone_high
            + zone_low
        ) / 2.0

        # =================================================
        # QUALITY
        # =================================================

        quality = str(
            zone.get(
                "quality",
                "LOW",
            )
        ).strip().upper()

        # =================================================
        # EVENT TIMES
        # =================================================

        fvg_time = _safe_timestamp(
            zone.get("fvg_time")
        )

        sweep_time = _safe_timestamp(
            zone.get("sweep_time")
        )

        displacement_time = _safe_timestamp(
            zone.get(
                "displacement_time"
            )
        )

        structure_break_time = _safe_timestamp(
            zone.get(
                "structure_break_time"
            )
        )

        # =================================================
        # AGE / FRESHNESS
        # =================================================

        age_candles = _calculate_age_candles(
            zone_ready_time=zone_ready_time,
            current_time=current_time,
            market_data=historical_data,
        )

        freshness_score = _freshness_score(
            age_candles
        )

        # =================================================
        # PROXIMITY SCORE
        # =================================================

        proximity_score = _proximity_score(
            distance_atr
        )

        # =================================================
        # CONFIRMATION SCORE
        # =================================================

        confirmation_score = _confirmation_score(
            confirmation_count
        )

        # =================================================
        # QUALITY SCORE
        # =================================================

        quality_score = _quality_score(
            quality
        )

        # =================================================
        # STATUS BONUS
        # =================================================

        status_bonus = _status_bonus(
            setup_status
        )

        # =================================================
        # FINAL PRIORITY SCORE
        # =================================================
        #
        # This ranking does not invalidate a signal.
        #
        # It only determines which valid setup comes first.
        #
        # =================================================

        normalized_confluence = np.clip(
            confluence_score,
            0.0,
            100.0,
        )

        priority_score = (
            normalized_confluence
            * RANK_WEIGHT_CONFLUENCE
            +
            confirmation_score
            * RANK_WEIGHT_CONFIRMATIONS
            +
            proximity_score
            * RANK_WEIGHT_PROXIMITY
            +
            freshness_score
            * RANK_WEIGHT_FRESHNESS
            +
            quality_score
            * RANK_WEIGHT_QUALITY
            +
            status_bonus
        )

        priority_score = float(
            np.clip(
                priority_score,
                0.0,
                100.0,
            )
        )

        # =================================================
        # APPEND SIGNAL
        # =================================================

        signals.append(
            {

                # -----------------------------------------
                # TIME
                # -----------------------------------------

                "signal_time":
                    current_time,

                "zone_time":
                    zone_time,

                "zone_ready_time":
                    zone_ready_time,

                # -----------------------------------------
                # DIRECTION
                # -----------------------------------------

                "direction":
                    direction,

                "signal":
                    signal_direction,

                # -----------------------------------------
                # TREND REGIME
                # -----------------------------------------

                "trend_m5":
                    trend_regime.get("m5", "NEUTRAL"),

                "trend_m15":
                    trend_regime.get("m15", "NEUTRAL"),

                "trend_h1":
                    trend_regime.get("h1", "NEUTRAL"),

                "trend_filter":
                    "ALIGNED",

                # -----------------------------------------
                # SETUP
                # -----------------------------------------

                "setup_status":
                    setup_status,

                "setup_location":
                    setup_location,

                "signal_strength":
                    signal_strength,

                "quality":
                    quality,

                # -----------------------------------------
                # CONFLUENCE
                # -----------------------------------------

                "confluence_score":
                    confluence_score,

                "confirmation_count":
                    confirmation_count,

                "fvg_confirmation":
                    fvg_confirmation,

                "liquidity_confirmation":
                    liquidity_confirmation,

                "displacement_confirmation":
                    displacement_confirmation,

                "structure_confirmation":
                    structure_confirmation,

                "structure_type":
                    structure_type,

                # -----------------------------------------
                # EVENT TIMES
                # -----------------------------------------

                "fvg_time":
                    fvg_time,

                "sweep_time":
                    sweep_time,

                "displacement_time":
                    displacement_time,

                "structure_break_time":
                    structure_break_time,

                # -----------------------------------------
                # ORDER BLOCK
                # -----------------------------------------

                "ob_status":
                    ob_status,

                # -----------------------------------------
                # ZONE
                # -----------------------------------------

                "zone_high":
                    zone_high,

                "zone_low":
                    zone_low,

                "zone_size":
                    zone_high - zone_low,

                # -----------------------------------------
                # ENTRY
                # -----------------------------------------

                "entry_reference":
                    entry_reference,

                # -----------------------------------------
                # MARKET
                # -----------------------------------------

                "current_price":
                    current_price,

                "distance_from_price":
                    distance,

                "distance_from_price_atr":
                    distance_atr,

                "price_inside_zone":
                    price_inside_zone,

                # -----------------------------------------
                # RANKING
                # -----------------------------------------

                "age_candles":
                    age_candles,

                "freshness_score":
                    freshness_score,

                "proximity_score":
                    proximity_score,

                "confirmation_score":
                    confirmation_score,

                "quality_score":
                    quality_score,

                "status_bonus":
                    status_bonus,

                "priority_score":
                    priority_score,
            }
        )

    # =====================================================
    # NO SIGNALS
    # =====================================================

    if not signals:
        return _empty_signal_result()

    # =====================================================
    # CREATE RESULT
    # =====================================================

    result = pd.DataFrame(
        signals
    )

    if result.empty:
        return _empty_signal_result()

    # =====================================================
    # REMOVE DUPLICATE ZONE SIGNALS
    # =====================================================

    result = (
        result
        .sort_values(
            [
                "priority_score",
                "confluence_score",
                "confirmation_count",
                "zone_ready_time",
                "zone_time",
            ],
            ascending=[
                False,
                False,
                False,
                False,
                False,
            ],
            na_position="last",
        )
        .drop_duplicates(
            subset=[
                "zone_time",
                "direction",
            ],
            keep="first",
        )
        .reset_index(drop=True)
    )

    # =====================================================
    # FINAL PRIORITY SORT
    # =====================================================

    result = (
        result
        .sort_values(
            [
                "priority_score",
                "confluence_score",
                "confirmation_count",
                "proximity_score",
                "freshness_score",
                "zone_ready_time",
                "zone_time",
            ],
            ascending=[
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    return result


# =========================================================
# ZONE READY TIME
# =========================================================

def _calculate_zone_ready_time(
    row,
):
    """
    Determine the first time at which the complete
    confluence zone actually exists.

    Only confirmed components are included.
    """

    times = []

    # -----------------------------------------------------
    # OB IS ALWAYS REQUIRED
    # -----------------------------------------------------

    ob_time = _safe_timestamp(
        row.get("ob_time")
    )

    if ob_time is not None:
        times.append(ob_time)

    # -----------------------------------------------------
    # FVG
    # -----------------------------------------------------

    if _to_bool(
        row.get(
            "fvg_confirmation",
            False,
        )
    ):

        fvg_time = _safe_timestamp(
            row.get("fvg_time")
        )

        if fvg_time is not None:
            times.append(fvg_time)

    # -----------------------------------------------------
    # LIQUIDITY
    # -----------------------------------------------------

    if _to_bool(
        row.get(
            "liquidity_confirmation",
            False,
        )
    ):

        sweep_time = _safe_timestamp(
            row.get("sweep_time")
        )

        if sweep_time is not None:
            times.append(sweep_time)

    # -----------------------------------------------------
    # DISPLACEMENT
    # -----------------------------------------------------

    if _to_bool(
        row.get(
            "displacement_confirmation",
            False,
        )
    ):

        displacement_time = _safe_timestamp(
            row.get(
                "displacement_time"
            )
        )

        if displacement_time is not None:
            times.append(
                displacement_time
            )

    # -----------------------------------------------------
    # STRUCTURE
    # -----------------------------------------------------

    if _to_bool(
        row.get(
            "structure_confirmation",
            False,
        )
    ):

        structure_time = _safe_timestamp(
            row.get(
                "structure_break_time"
            )
        )

        if structure_time is not None:
            times.append(
                structure_time
            )

    # -----------------------------------------------------
    # Missing timestamp protection
    # -----------------------------------------------------

    required_confirmation_times = [
        (
            "fvg_confirmation",
            "fvg_time",
        ),
        (
            "liquidity_confirmation",
            "sweep_time",
        ),
        (
            "displacement_confirmation",
            "displacement_time",
        ),
        (
            "structure_confirmation",
            "structure_break_time",
        ),
    ]

    for flag_column, time_column in (
        required_confirmation_times
    ):

        if _to_bool(
            row.get(
                flag_column,
                False,
            )
        ):

            if (
                _safe_timestamp(
                    row.get(time_column)
                )
                is None
            ):

                return pd.NaT

    if not times:
        return pd.NaT

    return max(times)


# =========================================================
# AGE / FRESHNESS
# =========================================================

def _calculate_age_candles(
    zone_ready_time,
    current_time,
    market_data,
):

    try:

        if (
            zone_ready_time is None
            or
            current_time is None
            or
            market_data.empty
        ):
            return 999

        zone_ready_time = pd.Timestamp(
            zone_ready_time
        )

        current_time = pd.Timestamp(
            current_time
        )

        if zone_ready_time > current_time:
            return 0

        mask = (
            market_data["time"]
            >= zone_ready_time
        ) & (
            market_data["time"]
            <= current_time
        )

        count = int(
            mask.sum()
        )

        if count <= 0:
            return 0

        return max(
            0,
            count - 1,
        )

    except Exception:

        return 999


def _freshness_score(
    age_candles,
):

    try:

        age = float(
            age_candles
        )

    except Exception:

        age = 999.0

    if age <= 0:
        return 100.0

    if age <= 2:
        return 95.0

    if age <= 5:
        return 90.0

    if age <= 10:
        return 82.0

    if age <= 20:
        return 70.0

    if age <= 30:
        return 58.0

    if age <= 40:
        return 45.0

    if age <= 60:
        return 30.0

    return 15.0


# =========================================================
# PROXIMITY SCORE
# =========================================================

def _proximity_score(
    distance_atr,
):

    try:

        distance = float(
            distance_atr
        )

    except Exception:

        distance = 999.0

    if distance <= 0:
        return 100.0

    if distance <= 0.25:
        return 98.0

    if distance <= 0.50:
        return 95.0

    if distance <= 1.00:
        return 90.0

    if distance <= 1.50:
        return 82.0

    if distance <= 2.00:
        return 72.0

    if distance <= 2.50:
        return 60.0

    if distance <= 3.00:
        return 45.0

    return 20.0


# =========================================================
# CONFIRMATION SCORE
# =========================================================

def _confirmation_score(
    confirmation_count,
):

    try:

        count = int(
            confirmation_count
        )

    except Exception:

        count = 0

    if count >= 4:
        return 100.0

    if count == 3:
        return 85.0

    if count == 2:
        return 70.0

    if count == 1:
        return 40.0

    return 0.0


# =========================================================
# QUALITY SCORE
# =========================================================

def _quality_score(
    quality,
):

    text = str(
        quality
    ).strip().upper()

    mapping = {
        "A+": 100.0,
        "A": 90.0,
        "B": 78.0,
        "C": 65.0,
        "LOW": 40.0,
    }

    return mapping.get(
        text,
        50.0,
    )


# =========================================================
# STATUS BONUS
# =========================================================

def _status_bonus(
    setup_status,
):

    text = str(
        setup_status
    ).strip().upper()

    if text == "ACTIVE":
        return 5.0

    if text == "WAITING_RETEST":
        return 1.0

    return 0.0


# =========================================================
# TIME NORMALIZATION
# =========================================================

def _normalize_zone_time(
    zones,
    column,
):

    if column not in zones.columns:
        zones[column] = pd.NaT
        return

    zones[column] = pd.to_datetime(
        zones[column],
        errors="coerce",
    )


# =========================================================
# SAFE TIMESTAMP
# =========================================================

def _safe_timestamp(
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


# =========================================================
# SAFE FLOAT
# =========================================================

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


# =========================================================
# SAFE INT
# =========================================================

def _safe_int(
    value,
    default=0,
):

    try:

        number = int(
            float(value)
        )

        return number

    except (
        TypeError,
        ValueError,
    ):

        return default


# =========================================================
# BOOLEAN CONVERSION
# =========================================================

def _to_bool(
    value,
) -> bool:

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

    try:

        if pd.isna(value):
            return False

    except Exception:
        pass

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

            return bool(value)

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
        "on",
        "confirmed",
        "valid",
        "active",
    }


# =========================================================
# EMPTY SIGNAL RESULT
# =========================================================

def _empty_signal_result():

    return pd.DataFrame(
        columns=[

            # ---------------------------------------------
            # Time
            # ---------------------------------------------

            "signal_time",
            "zone_time",
            "zone_ready_time",

            # ---------------------------------------------
            # Direction
            # ---------------------------------------------

            "direction",
            "signal",

            # ---------------------------------------------
            # Trend Regime
            # ---------------------------------------------

            "trend_m5",
            "trend_m15",
            "trend_h1",
            "trend_filter",

            # ---------------------------------------------
            # Setup
            # ---------------------------------------------

            "setup_status",
            "setup_location",
            "signal_strength",
            "quality",

            # ---------------------------------------------
            # Confluence
            # ---------------------------------------------

            "confluence_score",
            "confirmation_count",

            "fvg_confirmation",
            "liquidity_confirmation",
            "displacement_confirmation",
            "structure_confirmation",

            "structure_type",

            # ---------------------------------------------
            # Event Times
            # ---------------------------------------------

            "fvg_time",
            "sweep_time",
            "displacement_time",
            "structure_break_time",

            # ---------------------------------------------
            # Order Block
            # ---------------------------------------------

            "ob_status",

            # ---------------------------------------------
            # Zone
            # ---------------------------------------------

            "zone_high",
            "zone_low",
            "zone_size",

            # ---------------------------------------------
            # Entry
            # ---------------------------------------------

            "entry_reference",

            # ---------------------------------------------
            # Market
            # ---------------------------------------------

            "current_price",
            "distance_from_price",
            "distance_from_price_atr",
            "price_inside_zone",

            # ---------------------------------------------
            # Ranking
            # ---------------------------------------------

            "age_candles",
            "freshness_score",
            "proximity_score",
            "confirmation_score",
            "quality_score",
            "status_bonus",
            "priority_score",
        ]
    )
