
import pandas as pd
import numpy as np


# =========================================================
# RISK MANAGEMENT ENGINE
# =========================================================
#
# Calculates:
#   Entry
#   Stop Loss
#   Take Profit 1
#   Take Profit 2
#   Take Profit 3
#   Risk
#   Reward
#   Risk / Reward
#   Lot Size
#
# IMPORTANT:
#
# The engine can now receive:
#
#     execution_price
#
# When provided, that price becomes the ACTUAL ENTRY PRICE.
#
# This is important for walk-forward backtesting:
#
#     Signal candle
#           ↓
#     Next candle OPEN
#           ↓
#     execution_price
#           ↓
#     SL / TP / RR calculated from actual entry
#
# This prevents the previous mismatch where:
#
# Risk Engine calculated levels from one entry
# and the Backtest Engine later changed the entry.
#
# =========================================================


def calculate_trade_risk(
    df: pd.DataFrame,
    setups: pd.DataFrame,
    account_balance: float = 100.0,
    risk_percent: float = 1.0,
    min_rr: float = 2.0,
    max_risk_percent: float = 20.0,
    sl_atr_multiplier: float = 1.0,
    tp1_rr: float = 1.5,
    tp2_rr: float = 2.0,
    tp3_rr: float = 3.0,
    lot_per_100_dollars: float = 0.01,
    min_lot: float = 0.01,
    max_lot: float = 100.0,
    execution_price=None,
) -> pd.DataFrame:

    # =====================================================
    # REQUIRED MARKET DATA
    # =====================================================

    required_market_columns = {
        "time",
        "open",
        "high",
        "low",
        "close",
    }

    missing = (
        required_market_columns
        - set(df.columns)
    )

    if missing:

        raise ValueError(
            "Market data missing columns: "
            f"{sorted(missing)}"
        )


    # =====================================================
    # PREPARE MARKET DATA
    # =====================================================

    data = (
        df
        .copy()
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
        .reset_index(drop=True)
    )


    if data.empty:

        return _empty_risk_result()


    # =====================================================
    # PREPARE SETUPS
    # =====================================================

    if setups is None or setups.empty:

        return _empty_risk_result()


    setup_data = (
        setups
        .copy()
        .reset_index(drop=True)
    )


    required_setup_columns = {
        "signal_time",
        "zone_time",
        "direction",
        "signal",
        "zone_high",
        "zone_low",
    }


    missing_setup = (
        required_setup_columns
        - set(setup_data.columns)
    )


    if missing_setup:

        raise ValueError(
            "Signal setup data missing columns: "
            f"{sorted(missing_setup)}"
        )


    # =====================================================
    # ACCOUNT / RISK VALIDATION
    # =====================================================

    try:

        account_balance = float(
            account_balance
        )

    except (
        TypeError,
        ValueError,
    ):

        account_balance = 0.0


    try:

        risk_percent = float(
            risk_percent
        )

    except (
        TypeError,
        ValueError,
    ):

        risk_percent = 1.0


    try:

        min_rr = float(
            min_rr
        )

    except (
        TypeError,
        ValueError,
    ):

        min_rr = 2.0


    try:

        max_risk_percent = float(
            max_risk_percent
        )

    except (
        TypeError,
        ValueError,
    ):

        max_risk_percent = 20.0


    try:

        sl_atr_multiplier = float(
            sl_atr_multiplier
        )

    except (
        TypeError,
        ValueError,
    ):

        sl_atr_multiplier = 1.0


    try:

        tp1_rr = float(
            tp1_rr
        )

    except (
        TypeError,
        ValueError,
    ):

        tp1_rr = 1.5


    try:

        tp2_rr = float(
            tp2_rr
        )

    except (
        TypeError,
        ValueError,
    ):

        tp2_rr = 2.0


    try:

        tp3_rr = float(
            tp3_rr
        )

    except (
        TypeError,
        ValueError,
    ):

        tp3_rr = 3.0


    try:

        lot_per_100_dollars = float(
            lot_per_100_dollars
        )

    except (
        TypeError,
        ValueError,
    ):

        lot_per_100_dollars = 0.01


    try:

        min_lot = float(
            min_lot
        )

    except (
        TypeError,
        ValueError,
    ):

        min_lot = 0.01


    try:

        max_lot = float(
            max_lot
        )

    except (
        TypeError,
        ValueError,
    ):

        max_lot = 100.0


    # =====================================================
    # SAFETY LIMITS
    # =====================================================

    risk_percent = max(
        0.0,
        min(
            risk_percent,
            max_risk_percent,
        ),
    )


    min_rr = max(
        0.1,
        min_rr,
    )


    sl_atr_multiplier = max(
        0.1,
        sl_atr_multiplier,
    )


    tp1_rr = max(
        0.1,
        tp1_rr,
    )


    tp2_rr = max(
        tp1_rr,
        tp2_rr,
    )


    tp3_rr = max(
        tp2_rr,
        tp3_rr,
    )


    # =====================================================
    # ATR
    # =====================================================

    previous_close = (
        data["close"]
        .shift(1)
    )


    tr1 = (
        data["high"]
        -
        data["low"]
    )


    tr2 = (
        data["high"]
        -
        previous_close
    ).abs()


    tr3 = (
        data["low"]
        -
        previous_close
    ).abs()


    data["true_range"] = (
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


    data["atr"] = (
        data["true_range"]
        .rolling(
            14,
            min_periods=5,
        )
        .mean()
    )


    # =====================================================
    # CURRENT MARKET PRICE
    #
    # Used only when execution_price is NOT supplied.
    # =====================================================

    current_price = _safe_float(
        data.iloc[-1]["close"],
        default=0.0,
    )


    current_atr = data.iloc[-1]["atr"]


    if (
        pd.isna(current_atr)
        or
        current_atr <= 0
    ):

        current_atr = (
            data["true_range"]
            .tail(14)
            .mean()
        )


    if (
        pd.isna(current_atr)
        or
        current_atr <= 0
    ):

        current_atr = (
            (
                data["high"]
                -
                data["low"]
            )
            .tail(14)
            .mean()
        )


    # =====================================================
    # RISK AMOUNT
    # =====================================================

    risk_amount = (
        account_balance
        *
        risk_percent
        /
        100.0
    )


    # =====================================================
    # EXECUTION PRICE VALIDATION
    #
    # The same execution price is applied to every setup
    # in this function call.
    #
    # Backtest sends:
    #
    #     next_candle_open
    #
    # =====================================================

    forced_execution_price = _safe_float(
        execution_price,
        default=None,
    )


    # =====================================================
    # PROCESS SETUPS
    # =====================================================

    trades = []


    for _, setup in setup_data.iterrows():

        # =================================================
        # BASIC VALUES
        # =================================================

        direction = str(
            setup.get(
                "direction",
                "",
            )
        ).lower().strip()


        signal = str(
            setup.get(
                "signal",
                "",
            )
        ).upper().strip()


        if direction not in [
            "bullish",
            "bearish",
        ]:

            continue


        if signal not in [
            "BUY",
            "SELL",
        ]:

            continue


        # =================================================
        # ZONE
        # =================================================

        try:

            zone_high = float(
                setup["zone_high"]
            )


            zone_low = float(
                setup["zone_low"]
            )


        except (
            TypeError,
            ValueError,
        ):

            continue


        if (
            not np.isfinite(zone_high)
            or
            not np.isfinite(zone_low)
            or
            zone_high <= zone_low
        ):

            continue


        # =================================================
        # SIGNAL / SETUP TIME
        # =================================================

        setup_time = setup.get(
            "signal_time",
            None,
        )


        normalized_setup_time = _normalize_timestamp(
            setup_time
        )


        # =================================================
        # ATR AT SIGNAL TIME
        # =================================================

        setup_atr = current_atr


        if pd.notna(
            normalized_setup_time
        ):

            try:

                nearest_index = (
                    data["time"]
                    .searchsorted(
                        normalized_setup_time,
                        side="left",
                    )
                )


                nearest_index = min(
                    max(
                        nearest_index,
                        0,
                    ),
                    len(data) - 1,
                )


                historical_atr = (
                    data.iloc[
                        nearest_index
                    ]["atr"]
                )


                if (
                    pd.notna(
                        historical_atr
                    )
                    and
                    historical_atr > 0
                ):

                    setup_atr = float(
                        historical_atr
                    )


            except Exception:

                setup_atr = current_atr


        if (
            pd.isna(setup_atr)
            or
            setup_atr <= 0
        ):

            setup_atr = (
                zone_high
                -
                zone_low
            )


        # =================================================
        # ENTRY PRICE
        # =================================================
        #
        # PRIORITY:
        #
        # 1. Explicit execution_price
        # 2. Active setup current price
        # 3. entry_reference
        # 4. zone midpoint
        #
        # =================================================

        if (
            forced_execution_price
            is not None
            and
            forced_execution_price > 0
        ):

            entry = (
                forced_execution_price
            )

            entry_source = (
                "EXECUTION_PRICE"
            )


        else:

            entry_reference = setup.get(
                "entry_reference",
                np.nan,
            )


            try:

                if pd.isna(
                    entry_reference
                ):

                    entry = (
                        zone_high
                        +
                        zone_low
                    ) / 2.0

                else:

                    entry = float(
                        entry_reference
                    )

            except (
                TypeError,
                ValueError,
            ):

                entry = (
                    zone_high
                    +
                    zone_low
                ) / 2.0


            setup_status = str(
                setup.get(
                    "setup_status",
                    "",
                )
            ).upper().strip()


            price_inside_zone = _to_bool(
                setup.get(
                    "price_inside_zone",
                    False,
                )
            )


            if (
                setup_status == "ACTIVE"
                and
                price_inside_zone
            ):

                entry = current_price

                entry_source = (
                    "CURRENT_PRICE"
                )

            else:

                entry_source = (
                    "ENTRY_REFERENCE"
                )


        # =================================================
        # ENTRY VALIDATION
        # =================================================

        if (
            not np.isfinite(entry)
            or
            entry <= 0
        ):

            continue


        # =================================================
        # STOP LOSS
        # =================================================

        sl_buffer = (
            setup_atr
            *
            sl_atr_multiplier
        )


        if direction == "bullish":

            stop_loss = (
                zone_low
                -
                sl_buffer
            )

        else:

            stop_loss = (
                zone_high
                +
                sl_buffer
            )


        # =================================================
        # RISK DISTANCE
        # =================================================

        if direction == "bullish":

            price_risk = (
                entry
                -
                stop_loss
            )

        else:

            price_risk = (
                stop_loss
                -
                entry
            )


        if (
            not np.isfinite(
                price_risk
            )
            or
            price_risk <= 0
        ):

            continue


        # =================================================
        # TAKE PROFITS
        # =================================================

        tp1_distance = (
            price_risk
            *
            tp1_rr
        )


        tp2_distance = (
            price_risk
            *
            tp2_rr
        )


        tp3_distance = (
            price_risk
            *
            tp3_rr
        )


        if direction == "bullish":

            take_profit_1 = (
                entry
                +
                tp1_distance
            )


            take_profit_2 = (
                entry
                +
                tp2_distance
            )


            take_profit_3 = (
                entry
                +
                tp3_distance
            )


        else:

            take_profit_1 = (
                entry
                -
                tp1_distance
            )


            take_profit_2 = (
                entry
                -
                tp2_distance
            )


            take_profit_3 = (
                entry
                -
                tp3_distance
            )


        # =================================================
        # RR
        # =================================================

        rr_tp1 = (
            abs(
                take_profit_1
                -
                entry
            )
            /
            price_risk
        )


        rr_tp2 = (
            abs(
                take_profit_2
                -
                entry
            )
            /
            price_risk
        )


        rr_tp3 = (
            abs(
                take_profit_3
                -
                entry
            )
            /
            price_risk
        )


        # =================================================
        # MINIMUM RR
        # =================================================

        risk_reward_valid = (
            rr_tp2
            >=
            min_rr
        )


        # =================================================
        # LOT SIZE
        # =================================================

        if (
            account_balance > 0
            and
            lot_per_100_dollars > 0
        ):

            lot_size = (
                account_balance
                /
                100.0
            ) * lot_per_100_dollars

        else:

            lot_size = 0.0


        lot_size = max(
            min_lot,
            lot_size,
        )


        lot_size = min(
            max_lot,
            lot_size,
        )


        # =================================================
        # MONETARY RISK
        # =================================================

        estimated_risk = (
            risk_amount
        )


        # =================================================
        # POTENTIAL REWARD
        # =================================================

        reward_tp1 = (
            estimated_risk
            *
            rr_tp1
        )


        reward_tp2 = (
            estimated_risk
            *
            rr_tp2
        )


        reward_tp3 = (
            estimated_risk
            *
            rr_tp3
        )


        # =================================================
        # METADATA
        # =================================================

        quality = setup.get(
            "quality",
            None,
        )


        confluence_score = setup.get(
            "confluence_score",
            np.nan,
        )


        confirmation_count = setup.get(
            "confirmation_count",
            0,
        )


        zone_ready_time = setup.get(
            "zone_ready_time",
            None,
        )


        # =================================================
        # TRADE STATUS
        # =================================================

        if not risk_reward_valid:

            trade_status = (
                "REJECTED_RR"
            )

        else:

            trade_status = (
                "READY"
            )


        # =================================================
        # SAVE
        # =================================================

        trades.append(
            {

                # -----------------------------------------
                # Setup
                # -----------------------------------------

                "signal_time":
                    setup.get(
                        "signal_time",
                        None,
                    ),

                "zone_time":
                    setup.get(
                        "zone_time",
                        None,
                    ),

                "zone_ready_time":
                    zone_ready_time,

                "direction":
                    direction,

                "signal":
                    signal,

                "setup_status":
                    setup.get(
                        "setup_status",
                        "",
                    ),

                "trade_status":
                    trade_status,

                "setup_location":
                    setup.get(
                        "setup_location",
                        None,
                    ),

                "signal_strength":
                    setup.get(
                        "signal_strength",
                        None,
                    ),

                "quality":
                    quality,

                # -----------------------------------------
                # Confluence
                # -----------------------------------------

                "confluence_score":
                    confluence_score,

                "confirmation_count":
                    confirmation_count,

                "fvg_confirmation":
                    setup.get(
                        "fvg_confirmation",
                        False,
                    ),

                "liquidity_confirmation":
                    setup.get(
                        "liquidity_confirmation",
                        False,
                    ),

                "displacement_confirmation":
                    setup.get(
                        "displacement_confirmation",
                        False,
                    ),

                "structure_confirmation":
                    setup.get(
                        "structure_confirmation",
                        False,
                    ),

                "structure_type":
                    setup.get(
                        "structure_type",
                        None,
                    ),

                # -----------------------------------------
                # Zone
                # -----------------------------------------

                "zone_high":
                    zone_high,

                "zone_low":
                    zone_low,

                "entry_reference":
                    setup.get(
                        "entry_reference",
                        np.nan,
                    ),

                # -----------------------------------------
                # Entry
                # -----------------------------------------

                "entry":
                    float(entry),

                "entry_source":
                    entry_source,

                # -----------------------------------------
                # Levels
                # -----------------------------------------

                "stop_loss":
                    float(
                        stop_loss
                    ),

                "risk_distance":
                    float(
                        price_risk
                    ),

                "take_profit_1":
                    float(
                        take_profit_1
                    ),

                "take_profit_2":
                    float(
                        take_profit_2
                    ),

                "take_profit_3":
                    float(
                        take_profit_3
                    ),

                # -----------------------------------------
                # RR
                # -----------------------------------------

                "rr_tp1":
                    float(
                        rr_tp1
                    ),

                "rr_tp2":
                    float(
                        rr_tp2
                    ),

                "rr_tp3":
                    float(
                        rr_tp3
                    ),

                "risk_reward_valid":
                    bool(
                        risk_reward_valid
                    ),

                # -----------------------------------------
                # Account
                # -----------------------------------------

                "account_balance":
                    float(
                        account_balance
                    ),

                "risk_percent":
                    float(
                        risk_percent
                    ),

                "risk_amount":
                    float(
                        estimated_risk
                    ),

                "reward_tp1":
                    float(
                        reward_tp1
                    ),

                "reward_tp2":
                    float(
                        reward_tp2
                    ),

                "reward_tp3":
                    float(
                        reward_tp3
                    ),

                # -----------------------------------------
                # Lot
                # -----------------------------------------

                "lot_size":
                    float(
                        lot_size
                    ),

                # -----------------------------------------
                # Market
                # -----------------------------------------

                "current_price":
                    float(
                        current_price
                    ),

                "execution_price":
                    (
                        float(
                            forced_execution_price
                        )
                        if forced_execution_price
                        is not None
                        else np.nan
                    ),

                "atr":
                    float(
                        setup_atr
                    ),
            }
        )


    # =====================================================
    # RESULT
    # =====================================================

    result = pd.DataFrame(
        trades
    )


    if result.empty:

        return _empty_risk_result()


    # =====================================================
    # SORT
    # =====================================================

    result = (
        result
        .sort_values(
            [
                "trade_status",
                "confluence_score",
                "signal_time",
            ],
            ascending=[
                True,
                False,
                False,
            ],
            na_position="last",
        )
        .reset_index(drop=True)
    )


    return result


# =========================================================
# TIMESTAMP NORMALIZATION
# =========================================================

def _normalize_timestamp(
    value,
):

    if value is None:

        return pd.NaT


    try:

        timestamp = pd.Timestamp(
            value
        )


        if pd.isna(timestamp):

            return pd.NaT


        if timestamp.tz is not None:

            timestamp = (
                timestamp
                .tz_localize(None)
            )


        return timestamp


    except Exception:

        return pd.NaT


# =========================================================
# BOOLEAN
# =========================================================

def _to_bool(
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

            return bool(
                float(value)
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
        "active",
        "confirmed",
        "valid",
    }


# =========================================================
# SAFE FLOAT
# =========================================================

def _safe_float(
    value,
    default=None,
):

    try:

        number = float(
            value
        )


        if np.isfinite(
            number
        ):

            return number


    except (
        TypeError,
        ValueError,
    ):

        pass


    return default


# =========================================================
# EMPTY RESULT
# =========================================================

def _empty_risk_result():

    return pd.DataFrame(
        columns=[

            "signal_time",

            "zone_time",

            "zone_ready_time",

            "direction",

            "signal",

            "setup_status",

            "trade_status",

            "setup_location",

            "signal_strength",

            "quality",

            "confluence_score",

            "confirmation_count",

            "fvg_confirmation",

            "liquidity_confirmation",

            "displacement_confirmation",

            "structure_confirmation",

            "structure_type",

            "zone_high",

            "zone_low",

            "entry_reference",

            "entry",

            "entry_source",

            "stop_loss",

            "risk_distance",

            "take_profit_1",

            "take_profit_2",

            "take_profit_3",

            "rr_tp1",

            "rr_tp2",

            "rr_tp3",

            "risk_reward_valid",

            "account_balance",

            "risk_percent",

            "risk_amount",

            "reward_tp1",

            "reward_tp2",

            "reward_tp3",

            "lot_size",

            "current_price",

            "execution_price",

            "atr",
        ]
    )
