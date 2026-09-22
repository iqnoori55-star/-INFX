
import pandas as pd
import numpy as np


# =============================================================
# CONFLUENCE ENGINE
# =============================================================
#
# SMC / ICT CONFLUENCE ENGINE
#
# IMPORTANT:
#
# This version preserves the original matching logic that
# previously produced valid confluence zones.
#
# Only safe temporal protection has been added:
#
#     zone_ready_time
#
# zone_ready_time is the moment when the OB and all confirmations
# actually used by the zone are available.
#
# We intentionally DO NOT change the original event matching
# rules because those rules were already producing confluence
# zones correctly in previous tests.
#
# Original matching:
#
# Liquidity      -> BEFORE / AT OB
# Displacement   -> AFTER / AT OB
# Structure      -> AFTER / AT OB
# FVG            -> AFTER / AT OB
#
# This file does NOT modify structure_breaks logic.
# =============================================================


def detect_confluence_zones(
    df: pd.DataFrame,
    order_blocks: pd.DataFrame | None = None,
    fvgs: pd.DataFrame | None = None,
    liquidity_sweeps: pd.DataFrame | None = None,
    displacement: pd.DataFrame | None = None,
    structure_breaks: pd.DataFrame | None = None,
    confirmation_window: int = 40,
    max_fvg_distance_atr: float = 2.5,
    max_sweep_distance_atr: float = 4.0,
    min_confluence_score: float = 55.0,
    min_confirmations: int = 2,
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

        return _empty_confluence_result()


    # =========================================================
    # CURRENT SNAPSHOT TIME
    # =========================================================

    current_time = _to_timestamp(
        data.iloc[-1]["time"]
    )


    if current_time is None:

        return _empty_confluence_result()


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
    # INPUT DATA
    # =========================================================

    ob_data = _safe_copy(
        order_blocks
    )

    fvg_data = _safe_copy(
        fvgs
    )

    sweep_data = _safe_copy(
        liquidity_sweeps
    )

    displacement_data = _safe_copy(
        displacement
    )

    structure_data = _safe_copy(
        structure_breaks
    )


    # =========================================================
    # NORMALIZE EVENT TIMES
    # =========================================================

    _normalize_time(
        ob_data,
        [
            "time",
            "ob_time",
        ],
    )


    _normalize_time(
        fvg_data,
        [
            "time",
            "fvg_time",
        ],
    )


    _normalize_time(
        sweep_data,
        [
            "time",
            "sweep_time",
        ],
    )


    _normalize_time(
        displacement_data,
        [
            "time",
            "displacement_time",
        ],
    )


    _normalize_time(
        structure_data,
        [
            "time",
            "break_time",
            "structure_break_time",
        ],
    )


    # =========================================================
    # FUTURE PROTECTION
    #
    # Keep only events that already exist inside the current
    # historical snapshot.
    #
    # This does NOT change the matching logic.
    # =========================================================

    ob_data = _remove_future_events(
        ob_data,
        current_time,
    )

    fvg_data = _remove_future_events(
        fvg_data,
        current_time,
    )

    sweep_data = _remove_future_events(
        sweep_data,
        current_time,
    )

    displacement_data = _remove_future_events(
        displacement_data,
        current_time,
    )

    structure_data = _remove_future_events(
        structure_data,
        current_time,
    )


    # =========================================================
    # ORDER BLOCK VALIDATION
    # =========================================================

    if ob_data.empty:

        return _empty_confluence_result()


    required_ob_columns = {
        "time",
        "ob_type",
        "ob_high",
        "ob_low",
    }


    missing_ob = (
        required_ob_columns
        - set(ob_data.columns)
    )


    if missing_ob:

        raise ValueError(
            "Order Block data missing columns: "
            f"{sorted(missing_ob)}"
        )


    market_times = data["time"].values


    zones = []


    # =========================================================
    # DIAGNOSTIC
    # =========================================================

    diagnostic = {

        "ob_candidates": 0,

        "invalid_ob": 0,

        "future_ob": 0,

        "mitigated_ob": 0,

        "no_displacement": 0,

        "no_structure": 0,

        "no_fvg": 0,

        "no_liquidity": 0,

        "less_than_min_confirmations": 0,

        "rejected_by_score": 0,

        "accepted": 0,

    }


    # =========================================================
    # PROCESS ORDER BLOCKS
    # =========================================================

    for _, ob in ob_data.iterrows():

        diagnostic[
            "ob_candidates"
        ] += 1


        # =====================================================
        # OB TIME
        # =====================================================

        ob_time = _to_timestamp(
            ob.get("time")
        )


        if ob_time is None:

            diagnostic[
                "invalid_ob"
            ] += 1

            continue


        if ob_time > current_time:

            diagnostic[
                "future_ob"
            ] += 1

            continue


        # =====================================================
        # DIRECTION
        # =====================================================

        direction = _normalize_direction(
            ob.get("ob_type")
        )


        if direction not in [
            "bullish",
            "bearish",
        ]:

            diagnostic[
                "invalid_ob"
            ] += 1

            continue


        # =====================================================
        # OB PRICE
        # =====================================================

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

            diagnostic[
                "invalid_ob"
            ] += 1

            continue


        # =====================================================
        # OB INDEX
        # =====================================================

        ob_index = _time_to_index(
            market_times,
            ob_time,
        )


        if ob_index is None:

            diagnostic[
                "invalid_ob"
            ] += 1

            continue


        # =====================================================
        # OB STATUS
        # =====================================================

        ob_status = str(
            ob.get(
                "status",
                "",
            )
        ).strip().lower()


        explicit_mitigated = (
            ob_status == "mitigated"
            or
            _is_true(
                ob.get(
                    "mitigated",
                    False,
                )
            )
        )


        if explicit_mitigated:

            diagnostic[
                "mitigated_ob"
            ] += 1

            continue


        # =====================================================
        # OB SCORE
        # =====================================================

        raw_ob_score = _safe_float(
            ob.get(
                "score",
                0.0,
            ),
            default=0.0,
        )


        score = _ob_quality_score(
            raw_ob_score
        )


        if ob_status == "fresh":

            score += 8.0

        elif ob_status == "tested":

            score += 3.0


        # =====================================================
        # MATCHED EVENTS
        # =====================================================

        matched_displacement = None

        matched_structure = None

        matched_fvg = None

        matched_sweep = None


        displacement_confirmation = False

        structure_confirmation = False

        fvg_confirmation = False

        liquidity_confirmation = False


        structure_type = None

        fvg_strength = None


        # =====================================================
        # 1. DISPLACEMENT
        #
        # ORIGINAL LOGIC:
        # Displacement must occur after OB.
        # =====================================================

        (
            matched_displacement,
            displacement_confirmation,
        ) = _find_displacement(
            ob_time=ob_time,
            ob_index=ob_index,
            direction=direction,
            displacement_data=displacement_data,
            market_times=market_times,
            confirmation_window=confirmation_window,
        )


        if displacement_confirmation:

            displacement_strength = _normalize_strength(
                matched_displacement.get(
                    "displacement_strength",
                    "",
                )
            )


            if displacement_strength == "strong":

                score += 18.0

            elif displacement_strength == "medium":

                score += 15.0

            else:

                score += 12.0


        else:

            diagnostic[
                "no_displacement"
            ] += 1


        # =====================================================
        # 2. STRUCTURE
        #
        # ORIGINAL LOGIC:
        # BOS / CHOCH must occur after OB.
        # =====================================================

        (
            matched_structure,
            structure_confirmation,
            structure_type,
        ) = _find_structure(
            ob_time=ob_time,
            ob_index=ob_index,
            direction=direction,
            structure_data=structure_data,
            market_times=market_times,
            confirmation_window=confirmation_window,
        )


        if structure_confirmation:

            if structure_type == "CHOCH":

                score += 22.0

            elif structure_type == "BOS":

                score += 18.0

            else:

                score += 12.0


        else:

            diagnostic[
                "no_structure"
            ] += 1


        # =====================================================
        # 3. LIQUIDITY SWEEP
        #
        # ORIGINAL LOGIC:
        # Sweep must occur before or at OB and same direction.
        # =====================================================

        (
            matched_sweep,
            liquidity_confirmation,
        ) = _find_liquidity_sweep(
            ob_time=ob_time,
            ob_index=ob_index,
            direction=direction,
            ob_high=ob_high,
            ob_low=ob_low,
            sweep_data=sweep_data,
            data=data,
            market_times=market_times,
            confirmation_window=confirmation_window,
            max_distance_atr=max_sweep_distance_atr,
        )


        if liquidity_confirmation:

            sweep_strength = _safe_float(
                matched_sweep.get(
                    "sweep_strength_pct",
                    0.0,
                ),
                default=0.0,
            )


            score += _liquidity_score(
                sweep_strength
            )


        else:

            diagnostic[
                "no_liquidity"
            ] += 1


        # =====================================================
        # 4. FVG
        #
        # ORIGINAL LOGIC:
        # FVG must occur after OB.
        # =====================================================

        (
            matched_fvg,
            fvg_confirmation,
        ) = _find_fvg(
            ob_time=ob_time,
            ob_index=ob_index,
            direction=direction,
            ob_high=ob_high,
            ob_low=ob_low,
            fvg_data=fvg_data,
            data=data,
            market_times=market_times,
            confirmation_window=confirmation_window,
            max_distance_atr=max_fvg_distance_atr,
        )


        if fvg_confirmation:

            fvg_strength = _fvg_strength_from_data(
                matched_fvg
            )


            score += _fvg_score(
                fvg_strength,
                matched_fvg,
                ob_high,
                ob_low,
            )


        else:

            diagnostic[
                "no_fvg"
            ] += 1


        # =====================================================
        # CONFIRMATION COUNT
        # =====================================================

        confirmation_count = sum(
            [
                int(
                    displacement_confirmation
                ),
                int(
                    structure_confirmation
                ),
                int(
                    fvg_confirmation
                ),
                int(
                    liquidity_confirmation
                ),
            ]
        )


        # =====================================================
        # MINIMUM CONFIRMATIONS
        # =====================================================

        if (
            confirmation_count
            <
            min_confirmations
        ):

            diagnostic[
                "less_than_min_confirmations"
            ] += 1

            continue


        # =====================================================
        # CONFLUENCE BONUSES
        # =====================================================

        if (
            displacement_confirmation
            and
            structure_confirmation
        ):

            score += 5.0


        if (
            liquidity_confirmation
            and
            displacement_confirmation
        ):

            score += 6.0


        if (
            structure_confirmation
            and
            fvg_confirmation
        ):

            score += 4.0


        if (
            liquidity_confirmation
            and
            fvg_confirmation
        ):

            score += 5.0


        if confirmation_count >= 3:

            score += 6.0


        if confirmation_count >= 4:

            score += 8.0


        if (
            structure_type == "CHOCH"
            and
            displacement_confirmation
        ):

            score += 5.0


        # =====================================================
        # WEAK OB PENALTY
        # =====================================================

        if raw_ob_score < 50:

            score -= 5.0


        # =====================================================
        # FINAL SCORE
        # =====================================================

        final_score = float(
            np.clip(
                score,
                0.0,
                100.0,
            )
        )


        if final_score < min_confluence_score:

            diagnostic[
                "rejected_by_score"
            ] += 1

            continue


        # =====================================================
        # BUILD ZONE
        # =====================================================

        zone_high = float(
            ob_high
        )

        zone_low = float(
            ob_low
        )


        # =====================================================
        # EXPAND WITH FVG
        # =====================================================

        if matched_fvg is not None:

            fvg_high = _safe_float(
                matched_fvg.get(
                    "fvg_high"
                )
            )


            fvg_low = _safe_float(
                matched_fvg.get(
                    "fvg_low"
                )
            )


            if (
                fvg_high is not None
                and
                fvg_low is not None
                and
                fvg_high > fvg_low
            ):

                if _zones_are_close(
                    ob_low,
                    ob_high,
                    fvg_low,
                    fvg_high,
                    data,
                    ob_index,
                    max_fvg_distance_atr,
                ):

                    zone_high = max(
                        zone_high,
                        fvg_high,
                    )

                    zone_low = min(
                        zone_low,
                        fvg_low,
                    )


        zone_size = (
            zone_high
            -
            zone_low
        )


        if zone_size <= 0:

            continue


        # =====================================================
        # EVENT TIMES
        # =====================================================

        displacement_time = _extract_time(
            matched_displacement,
            [
                "time",
                "displacement_time",
            ],
        )


        structure_break_time = _extract_time(
            matched_structure,
            [
                "time",
                "break_time",
                "structure_break_time",
            ],
        )


        sweep_time = _extract_time(
            matched_sweep,
            [
                "time",
                "sweep_time",
            ],
        )


        fvg_time = _extract_time(
            matched_fvg,
            [
                "time",
                "fvg_time",
            ],
        )


        # =====================================================
        # ZONE READY TIME
        #
        # This is the ONLY major temporal addition.
        #
        # It does NOT change the matching rules.
        # =====================================================

        event_times = [
            ob_time
        ]


        if (
            displacement_confirmation
            and
            displacement_time is not None
        ):

            event_times.append(
                displacement_time
            )


        if (
            structure_confirmation
            and
            structure_break_time is not None
        ):

            event_times.append(
                structure_break_time
            )


        if (
            fvg_confirmation
            and
            fvg_time is not None
        ):

            event_times.append(
                fvg_time
            )


        if (
            liquidity_confirmation
            and
            sweep_time is not None
        ):

            event_times.append(
                sweep_time
            )


        zone_ready_time = max(
            event_times
        )


        # -----------------------------------------------------
        # Defensive future protection
        # -----------------------------------------------------

        if zone_ready_time > current_time:

            continue


        # =====================================================
        # BUILD RECORD
        # =====================================================

        zones.append(
            {

                "ob_time":
                    ob_time,

                "zone_ready_time":
                    zone_ready_time,

                "direction":
                    direction,

                "zone_high":
                    float(
                        zone_high
                    ),

                "zone_low":
                    float(
                        zone_low
                    ),

                "zone_size":
                    float(
                        zone_size
                    ),

                "ob_high":
                    float(
                        ob_high
                    ),

                "ob_low":
                    float(
                        ob_low
                    ),

                "ob_score":
                    float(
                        raw_ob_score
                    ),

                "ob_status":
                    ob_status,


                # -------------------------------------------------
                # FVG
                # -------------------------------------------------

                "fvg_confirmation":
                    bool(
                        fvg_confirmation
                    ),

                "fvg_time":
                    fvg_time,

                "fvg_high":
                    (
                        _safe_float(
                            matched_fvg.get(
                                "fvg_high"
                            )
                        )
                        if matched_fvg is not None
                        else None
                    ),

                "fvg_low":
                    (
                        _safe_float(
                            matched_fvg.get(
                                "fvg_low"
                            )
                        )
                        if matched_fvg is not None
                        else None
                    ),

                "fvg_strength":
                    fvg_strength,


                # -------------------------------------------------
                # LIQUIDITY
                # -------------------------------------------------

                "liquidity_confirmation":
                    bool(
                        liquidity_confirmation
                    ),

                "liquidity_type":
                    (
                        matched_sweep.get(
                            "liquidity_type"
                        )
                        if matched_sweep is not None
                        else None
                    ),

                "liquidity_level":
                    (
                        _safe_float(
                            matched_sweep.get(
                                "liquidity_level"
                            )
                        )
                        if matched_sweep is not None
                        else None
                    ),

                "sweep_time":
                    sweep_time,

                "sweep_direction":
                    (
                        matched_sweep.get(
                            "sweep_direction"
                        )
                        if matched_sweep is not None
                        else None
                    ),

                "sweep_strength_pct":
                    (
                        _safe_float(
                            matched_sweep.get(
                                "sweep_strength_pct"
                            ),
                            default=0.0,
                        )
                        if matched_sweep is not None
                        else None
                    ),


                # -------------------------------------------------
                # DISPLACEMENT
                # -------------------------------------------------

                "displacement_confirmation":
                    bool(
                        displacement_confirmation
                    ),

                "displacement_time":
                    displacement_time,

                "displacement_strength":
                    (
                        _normalize_strength(
                            matched_displacement.get(
                                "displacement_strength",
                                "",
                            )
                        )
                        if matched_displacement is not None
                        else None
                    ),


                # -------------------------------------------------
                # STRUCTURE
                # -------------------------------------------------

                "structure_confirmation":
                    bool(
                        structure_confirmation
                    ),

                "structure_type":
                    structure_type,

                "structure_break_time":
                    structure_break_time,


                # -------------------------------------------------
                # FINAL
                # -------------------------------------------------

                "confirmation_count":
                    int(
                        confirmation_count
                    ),

                "confluence_score":
                    float(
                        final_score
                    ),

                "quality":
                    _quality_from_score(
                        final_score,
                        confirmation_count,
                        structure_type,
                    ),
            }
        )


        diagnostic[
            "accepted"
        ] += 1


    # =========================================================
    # RESULT
    # =========================================================

    result = pd.DataFrame(
        zones
    )


    # =========================================================
    # DIAGNOSTIC
    # =========================================================

    print("")
    print("------------------------------------")
    print("CONFLUENCE DIAGNOSTIC")
    print("------------------------------------")

    print(
        f"Snapshot time: "
        f"{current_time}"
    )

    print(
        f"OB candidates: "
        f"{diagnostic['ob_candidates']}"
    )

    print(
        f"Invalid OB: "
        f"{diagnostic['invalid_ob']}"
    )

    print(
        f"Future OB: "
        f"{diagnostic['future_ob']}"
    )

    print(
        f"Mitigated OB: "
        f"{diagnostic['mitigated_ob']}"
    )

    print(
        f"No displacement: "
        f"{diagnostic['no_displacement']}"
    )

    print(
        f"No structure: "
        f"{diagnostic['no_structure']}"
    )

    print(
        f"No FVG: "
        f"{diagnostic['no_fvg']}"
    )

    print(
        f"No liquidity: "
        f"{diagnostic['no_liquidity']}"
    )

    print(
        f"Less than "
        f"{min_confirmations} confirmations: "
        f"{diagnostic['less_than_min_confirmations']}"
    )

    print(
        f"Rejected by score: "
        f"{diagnostic['rejected_by_score']}"
    )

    print(
        f"Accepted zones: "
        f"{diagnostic['accepted']}"
    )

    print("------------------------------------")
    print("")


    if result.empty:

        return _empty_confluence_result()


    # =========================================================
    # SAME OB DEDUPLICATION
    # =========================================================

    result = (
        result
        .sort_values(
            [
                "confluence_score",
                "confirmation_count",
                "ob_time",
            ],
            ascending=[
                False,
                False,
                False,
            ],
        )
        .drop_duplicates(
            subset=[
                "ob_time",
                "direction",
            ],
            keep="first",
        )
        .reset_index(drop=True)
    )


    # =========================================================
    # OVERLAP FILTER
    # =========================================================

    result = _remove_overlapping_zones(
        result,
        overlap_threshold=0.60,
        max_center_distance_atr=0.50,
        data=data,
    )


    if result.empty:

        return _empty_confluence_result()


    # =========================================================
    # CURRENT PRICE
    # =========================================================

    current_price = _safe_float(
        data.iloc[-1]["close"],
        default=0.0,
    )


    current_atr = _safe_float(
        data.iloc[-1]["atr"]
    )


    distance_values = []

    distance_atr_values = []

    price_inside_values = []


    for _, zone in result.iterrows():

        zone_high = float(
            zone["zone_high"]
        )

        zone_low = float(
            zone["zone_low"]
        )


        if current_price > zone_high:

            distance = (
                current_price
                - zone_high
            )

        elif current_price < zone_low:

            distance = (
                zone_low
                - current_price
            )

        else:

            distance = 0.0


        distance_values.append(
            float(distance)
        )


        if (
            current_atr is not None
            and
            current_atr > 0
        ):

            distance_atr = (
                distance
                / current_atr
            )

        else:

            distance_atr = distance


        distance_atr_values.append(
            float(distance_atr)
        )


        price_inside_values.append(
            bool(
                zone_low
                <= current_price
                <= zone_high
            )
        )


    result[
        "distance_from_price"
    ] = distance_values


    result[
        "distance_from_price_atr"
    ] = distance_atr_values


    result[
        "price_inside_zone"
    ] = price_inside_values


    # =========================================================
    # TRADE RELEVANCE
    # =========================================================

    result[
        "trade_relevance"
    ] = result.apply(
        lambda row:
        _trade_relevance(
            row,
            current_price,
            current_atr,
        ),
        axis=1,
    )


    # =========================================================
    # RANKING
    # =========================================================

    result[
        "ranking_score"
    ] = (
        result[
            "confluence_score"
        ]
        * 0.80
        +
        result[
            "trade_relevance"
        ]
        * 0.20
    )


    # =========================================================
    # QUALITY
    # =========================================================

    result[
        "quality"
    ] = result.apply(
        lambda row:
        _quality_from_score(
            row[
                "confluence_score"
            ],
            row[
                "confirmation_count"
            ],
            row[
                "structure_type"
            ],
        ),
        axis=1,
    )


    # =========================================================
    # FINAL SORT
    # =========================================================

    result = (
        result
        .sort_values(
            [
                "ranking_score",
                "confluence_score",
                "confirmation_count",
                "ob_time",
            ],
            ascending=[
                False,
                False,
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )


    return result


# =============================================================
# REMOVE FUTURE EVENTS
# =============================================================

def _remove_future_events(
    frame,
    current_time,
):

    if frame.empty:

        return frame


    if "time" not in frame.columns:

        return frame


    frame = frame.copy()


    frame["time"] = pd.to_datetime(
        frame["time"],
        errors="coerce",
    )


    frame = frame[
        frame["time"].notna()
    ]


    frame = frame[
        frame["time"]
        <= current_time
    ]


    return (
        frame
        .reset_index(drop=True)
    )


# =============================================================
# BASIC HELPERS
# =============================================================

def _safe_copy(frame):

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
    candidates,
):

    if frame.empty:

        return frame


    if "time" in frame.columns:

        frame["time"] = pd.to_datetime(
            frame["time"],
            errors="coerce",
        )

        return frame


    for column in candidates:

        if column in frame.columns:

            frame["time"] = pd.to_datetime(
                frame[column],
                errors="coerce",
            )

            return frame


    return frame


def _to_timestamp(
    value
):

    if value is None:

        return None


    try:

        ts = pd.Timestamp(
            value
        )


        if pd.isna(ts):

            return None


        return ts

    except Exception:

        return None


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


def _normalize_direction(
    value
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


def _normalize_strength(
    value
):

    if value is None:

        return ""


    return str(
        value
    ).strip().lower()


def _time_to_index(
    market_times,
    timestamp,
):

    try:

        timestamp = np.datetime64(
            timestamp
        )


        index = np.searchsorted(
            market_times,
            timestamp,
            side="left",
        )


        if (
            index < 0
            or
            index >= len(
                market_times
            )
        ):

            return None


        return int(
            index
        )


    except Exception:

        return None


# =============================================================
# OB SCORE
# =============================================================

def _ob_quality_score(
    score
):

    score = _safe_float(
        score,
        default=0.0,
    )


    if score >= 90:

        return 25.0


    if score >= 80:

        return 22.0


    if score >= 70:

        return 19.0


    if score >= 60:

        return 16.0


    if score >= 50:

        return 12.0


    return 8.0


# =============================================================
# FVG STRENGTH
# =============================================================

def _fvg_strength_from_data(
    fvg
):

    if fvg is None:

        return ""


    explicit = _normalize_strength(
        fvg.get(
            "fvg_strength",
            "",
        )
    )


    if explicit in [
        "strong",
        "medium",
        "weak",
    ]:

        return explicit


    size = _safe_float(
        fvg.get(
            "fvg_size"
        ),
        default=0.0,
    )


    if size <= 0:

        return "weak"


    return "auto"


# =============================================================
# FVG SCORE
# =============================================================

def _fvg_score(
    strength,
    fvg,
    ob_high,
    ob_low,
):

    points = 0.0


    if strength == "strong":

        points += 15.0


    elif strength == "medium":

        points += 12.0


    elif strength == "weak":

        points += 8.0


    else:

        fvg_size = _safe_float(
            fvg.get(
                "fvg_size"
            ),
            default=0.0,
        )


        ob_size = (
            float(ob_high)
            -
            float(ob_low)
        )


        if (
            fvg_size > 0
            and
            ob_size > 0
        ):

            ratio = (
                fvg_size
                /
                ob_size
            )


            if ratio >= 0.75:

                points += 15.0

            elif ratio >= 0.40:

                points += 12.0

            elif ratio >= 0.15:

                points += 9.0

            else:

                points += 6.0


        else:

            points += 6.0


    fvg_high = _safe_float(
        fvg.get(
            "fvg_high"
        )
    )


    fvg_low = _safe_float(
        fvg.get(
            "fvg_low"
        )
    )


    if (
        fvg_high is not None
        and
        fvg_low is not None
    ):

        overlap = _price_overlap_ratio(
            ob_low,
            ob_high,
            fvg_low,
            fvg_high,
        )


        if overlap >= 0.75:

            points += 5.0


        elif overlap > 0:

            points += 3.0


    return points


# =============================================================
# LIQUIDITY SCORE
# =============================================================

def _liquidity_score(
    strength
):

    strength = _safe_float(
        strength,
        default=0.0,
    )


    if strength >= 2.0:

        return 18.0


    if strength >= 1.0:

        return 16.0


    if strength >= 0.5:

        return 14.0


    return 12.0


# =============================================================
# DISPLACEMENT
# ORIGINAL MATCHING PRESERVED
# =============================================================

def _find_displacement(
    ob_time,
    ob_index,
    direction,
    displacement_data,
    market_times,
    confirmation_window,
):

    if displacement_data.empty:

        return None, False


    if "time" not in displacement_data.columns:

        return None, False


    best = None

    best_key = None


    for _, row in displacement_data.iterrows():

        timestamp = _to_timestamp(
            row.get(
                "time"
            )
        )


        if timestamp is None:

            continue


        # -----------------------------------------------------
        # ORIGINAL RULE:
        # Displacement after OB.
        # -----------------------------------------------------

        if timestamp < ob_time:

            continue


        row_direction = _normalize_direction(
            row.get(
                "displacement_direction"
            )
        )


        if row_direction != direction:

            continue


        if "is_displacement" in row.index:

            if not _is_true(
                row.get(
                    "is_displacement"
                )
            ):

                continue


        index = _time_to_index(
            market_times,
            timestamp,
        )


        if index is None:

            continue


        distance = (
            index
            -
            ob_index
        )


        if distance < 0:

            continue


        if distance > confirmation_window:

            continue


        strength = _normalize_strength(
            row.get(
                "displacement_strength",
                "",
            )
        )


        strength_rank = {

            "strong": 0,

            "medium": 1,

            "weak": 2,

        }.get(
            strength,
            3,
        )


        key = (
            strength_rank,
            distance,
        )


        if (
            best_key is None
            or
            key < best_key
        ):

            best_key = key

            best = row


    return (
        best,
        best is not None,
    )


# =============================================================
# STRUCTURE
# ORIGINAL MATCHING PRESERVED
# =============================================================

def _find_structure(
    ob_time,
    ob_index,
    direction,
    structure_data,
    market_times,
    confirmation_window,
):

    if structure_data.empty:

        return None, False, None


    if "time" not in structure_data.columns:

        return None, False, None


    best = None

    best_key = None

    best_type = None


    for _, row in structure_data.iterrows():

        timestamp = _to_timestamp(
            row.get(
                "time"
            )
        )


        if timestamp is None:

            continue


        # -----------------------------------------------------
        # ORIGINAL RULE:
        # Structure after OB.
        # -----------------------------------------------------

        if timestamp < ob_time:

            continue


        row_direction = _normalize_direction(
            row.get(
                "break_direction"
            )
        )


        if row_direction != direction:

            continue


        break_type = str(
            row.get(
                "break_type",
                "",
            )
        ).strip().upper()


        if break_type not in [
            "BOS",
            "CHOCH",
        ]:

            continue


        index = _time_to_index(
            market_times,
            timestamp,
        )


        if index is None:

            continue


        distance = (
            index
            -
            ob_index
        )


        if distance < 0:

            continue


        if distance > confirmation_window:

            continue


        type_rank = (
            0
            if break_type == "CHOCH"
            else 1
        )


        key = (
            type_rank,
            distance,
        )


        if (
            best_key is None
            or
            key < best_key
        ):

            best_key = key

            best = row

            best_type = break_type


    if best is None:

        return (
            None,
            False,
            None,
        )


    return (
        best,
        True,
        best_type,
    )


# =============================================================
# FVG
# ORIGINAL MATCHING PRESERVED
# =============================================================

def _find_fvg(
    ob_time,
    ob_index,
    direction,
    ob_high,
    ob_low,
    fvg_data,
    data,
    market_times,
    confirmation_window,
    max_distance_atr,
):

    if fvg_data.empty:

        return None, False


    required = {
        "time",
        "fvg_type",
        "fvg_high",
        "fvg_low",
    }


    if not required.issubset(
        fvg_data.columns
    ):

        return None, False


    best = None

    best_key = None


    for _, fvg in fvg_data.iterrows():

        fvg_time = _to_timestamp(
            fvg.get(
                "time"
            )
        )


        if fvg_time is None:

            continue


        # -----------------------------------------------------
        # ORIGINAL RULE:
        # FVG after OB.
        # -----------------------------------------------------

        if fvg_time < ob_time:

            continue


        fvg_direction = _normalize_direction(
            fvg.get(
                "fvg_type"
            )
        )


        if fvg_direction != direction:

            continue


        fvg_high = _safe_float(
            fvg.get(
                "fvg_high"
            )
        )


        fvg_low = _safe_float(
            fvg.get(
                "fvg_low"
            )
        )


        if (
            fvg_high is None
            or
            fvg_low is None
            or
            fvg_high <= fvg_low
        ):

            continue


        fvg_index = _time_to_index(
            market_times,
            fvg_time
        )


        if fvg_index is None:

            continue


        candle_distance = (
            fvg_index
            -
            ob_index
        )


        if candle_distance < 0:

            continue


        if candle_distance > confirmation_window:

            continue


        overlap_ratio = _price_overlap_ratio(
            ob_low,
            ob_high,
            fvg_low,
            fvg_high,
        )


        if overlap_ratio > 0:

            price_distance = 0.0


        elif fvg_low > ob_high:

            price_distance = (
                fvg_low
                -
                ob_high
            )


        else:

            price_distance = (
                ob_low
                -
                fvg_high
            )


        atr_index = min(
            max(
                fvg_index,
                0,
            ),
            len(data) - 1,
        )


        atr = _safe_float(
            data.iloc[
                atr_index
            ].get(
                "atr"
            )
        )


        if (
            atr is not None
            and
            atr > 0
        ):

            distance_atr = (
                price_distance
                /
                atr
            )


        else:

            distance_atr = price_distance


        if (
            distance_atr
            >
            max_distance_atr
        ):

            continue


        fvg_size = _safe_float(
            fvg.get(
                "fvg_size"
            ),
            default=0.0,
        )


        key = (

            0
            if overlap_ratio > 0
            else 1,

            -float(
                overlap_ratio
            ),

            float(
                distance_atr
            ),

            -float(
                fvg_size
            ),

            int(
                candle_distance
            ),
        )


        if (
            best_key is None
            or
            key < best_key
        ):

            best_key = key

            best = fvg


    return (
        best,
        best is not None,
    )


# =============================================================
# LIQUIDITY
# ORIGINAL MATCHING PRESERVED
# =============================================================

def _find_liquidity_sweep(
    ob_time,
    ob_index,
    direction,
    ob_high,
    ob_low,
    sweep_data,
    data,
    market_times,
    confirmation_window,
    max_distance_atr,
):

    if sweep_data.empty:

        return None, False


    if "time" not in sweep_data.columns:

        return None, False


    best = None

    best_key = None


    for _, sweep in sweep_data.iterrows():

        sweep_time = _to_timestamp(
            sweep.get(
                "time"
            )
        )


        if sweep_time is None:

            continue


        # -----------------------------------------------------
        # ORIGINAL RULE:
        # Sweep before / at OB.
        # -----------------------------------------------------

        if sweep_time > ob_time:

            continue


        sweep_direction = _normalize_direction(
            sweep.get(
                "sweep_direction"
            )
        )


        if sweep_direction != direction:

            continue


        sweep_index = _time_to_index(
            market_times,
            sweep_time,
        )


        if sweep_index is None:

            continue


        candle_distance = (
            ob_index
            -
            sweep_index
        )


        if candle_distance < 0:

            continue


        if candle_distance > confirmation_window:

            continue


        level = _safe_float(
            sweep.get(
                "liquidity_level"
            )
        )


        if level is None:

            continue


        if (
            ob_low
            <= level
            <= ob_high
        ):

            distance = 0.0


        elif level > ob_high:

            distance = (
                level
                -
                ob_high
            )


        else:

            distance = (
                ob_low
                -
                level
            )


        atr_index = min(
            max(
                sweep_index,
                0,
            ),
            len(data) - 1,
        )


        atr = _safe_float(
            data.iloc[
                atr_index
            ].get(
                "atr"
            )
        )


        if (
            atr is not None
            and
            atr > 0
        ):

            distance_atr = (
                distance
                /
                atr
            )


        else:

            distance_atr = distance


        if (
            distance_atr
            >
            max_distance_atr
        ):

            continue


        strength = _safe_float(
            sweep.get(
                "sweep_strength_pct"
            ),
            default=0.0,
        )


        key = (

            float(
                distance_atr
            ),

            -float(
                strength
            ),

            int(
                candle_distance
            ),
        )


        if (
            best_key is None
            or
            key < best_key
        ):

            best_key = key

            best = sweep


    return (
        best,
        best is not None,
    )


# =============================================================
# PRICE OVERLAP
# =============================================================

def _price_overlap_ratio(
    low1,
    high1,
    low2,
    high2,
):

    overlap_low = max(
        low1,
        low2,
    )


    overlap_high = min(
        high1,
        high2,
    )


    overlap = (
        overlap_high
        -
        overlap_low
    )


    if overlap <= 0:

        return 0.0


    size1 = (
        high1
        -
        low1
    )


    size2 = (
        high2
        -
        low2
    )


    smaller = min(
        size1,
        size2,
    )


    if smaller <= 0:

        return 0.0


    return float(
        overlap
        /
        smaller
    )


# =============================================================
# ZONE DISTANCE
# =============================================================

def _zones_are_close(
    ob_low,
    ob_high,
    fvg_low,
    fvg_high,
    data,
    index,
    max_distance_atr,
):

    overlap = _price_overlap_ratio(
        ob_low,
        ob_high,
        fvg_low,
        fvg_high,
    )


    if overlap > 0:

        return True


    if fvg_low > ob_high:

        distance = (
            fvg_low
            -
            ob_high
        )

    else:

        distance = (
            ob_low
            -
            fvg_high
        )


    atr = _safe_float(
        data.iloc[
            min(
                max(
                    index,
                    0,
                ),
                len(data) - 1,
            )
        ].get(
            "atr"
        )
    )


    if (
        atr is None
        or
        atr <= 0
    ):

        return True


    return (
        distance
        /
        atr
        <=
        max_distance_atr
    )


# =============================================================
# OVERLAP FILTER
# =============================================================

def _remove_overlapping_zones(
    result,
    overlap_threshold=0.60,
    max_center_distance_atr=0.50,
    data=None,
):

    if result.empty:

        return result


    working = (
        result
        .sort_values(
            [
                "confluence_score",
                "confirmation_count",
                "ob_time",
            ],
            ascending=[
                False,
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )


    kept = []


    current_atr = None


    if (
        data is not None
        and
        not data.empty
        and
        "atr" in data.columns
    ):

        current_atr = _safe_float(
            data.iloc[-1]["atr"]
        )


    for _, candidate in working.iterrows():

        candidate_high = _safe_float(
            candidate.get(
                "zone_high"
            )
        )


        candidate_low = _safe_float(
            candidate.get(
                "zone_low"
            )
        )


        if (
            candidate_high is None
            or
            candidate_low is None
            or
            candidate_high <= candidate_low
        ):

            continue


        candidate_direction = _normalize_direction(
            candidate.get(
                "direction"
            )
        )


        candidate_size = (
            candidate_high
            -
            candidate_low
        )


        candidate_center = (
            candidate_high
            +
            candidate_low
        ) / 2.0


        duplicate = False


        for existing in kept:

            existing_direction = _normalize_direction(
                existing.get(
                    "direction"
                )
            )


            if (
                candidate_direction
                !=
                existing_direction
            ):

                continue


            existing_high = _safe_float(
                existing.get(
                    "zone_high"
                )
            )


            existing_low = _safe_float(
                existing.get(
                    "zone_low"
                )
            )


            if (
                existing_high is None
                or
                existing_low is None
            ):

                continue


            existing_size = (
                existing_high
                -
                existing_low
            )


            existing_center = (
                existing_high
                +
                existing_low
            ) / 2.0


            # -------------------------------------------------
            # OVERLAP
            # -------------------------------------------------

            overlap_high = min(
                candidate_high,
                existing_high,
            )


            overlap_low = max(
                candidate_low,
                existing_low,
            )


            overlap = (
                overlap_high
                -
                overlap_low
            )


            if overlap > 0:

                smaller_zone = min(
                    candidate_size,
                    existing_size,
                )


                if smaller_zone > 0:

                    ratio = (
                        overlap
                        /
                        smaller_zone
                    )


                    if (
                        ratio
                        >=
                        overlap_threshold
                    ):

                        duplicate = True

                        break


            # -------------------------------------------------
            # VERY CLOSE ZONES
            # -------------------------------------------------

            if (
                not duplicate
                and
                current_atr is not None
                and
                current_atr > 0
            ):

                center_distance = abs(
                    candidate_center
                    -
                    existing_center
                )


                center_atr = (
                    center_distance
                    /
                    current_atr
                )


                if (
                    center_atr
                    <=
                    max_center_distance_atr
                ):

                    edge_gap = min(
                        abs(
                            candidate_low
                            -
                            existing_high
                        ),
                        abs(
                            existing_low
                            -
                            candidate_high
                        ),
                    )


                    if (
                        edge_gap
                        /
                        current_atr
                        <=
                        0.25
                    ):

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
        .reset_index(drop=True)
    )


# =============================================================
# TRADE RELEVANCE
# =============================================================

def _trade_relevance(
    row,
    current_price,
    current_atr,
):

    distance_atr = _safe_float(
        row.get(
            "distance_from_price_atr"
        ),
        default=999.0,
    )


    inside = bool(
        row.get(
            "price_inside_zone",
            False,
        )
    )


    if inside:

        return 100.0


    if distance_atr <= 0.50:

        return 95.0


    if distance_atr <= 1.00:

        return 90.0


    if distance_atr <= 2.00:

        return 80.0


    if distance_atr <= 3.00:

        return 65.0


    if distance_atr <= 5.00:

        return 45.0


    if distance_atr <= 10.00:

        return 25.0


    return 10.0


# =============================================================
# QUALITY
# =============================================================

def _quality_from_score(
    score,
    confirmation_count,
    structure_type,
):

    score = float(
        score
    )


    if (
        score >= 90
        and
        confirmation_count >= 3
    ):

        return "A+"


    if (
        score >= 80
        and
        confirmation_count >= 3
    ):

        return "A"


    if (
        score >= 70
        and
        confirmation_count >= 2
    ):

        return "B"


    if (
        score >= 60
        and
        confirmation_count >= 2
    ):

        return "C"


    return "LOW"


# =============================================================
# BOOLEAN
# =============================================================

def _is_true(
    value
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

            number = float(
                value
            )


            if not np.isfinite(
                number
            ):

                return False


            return number != 0.0


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
        "active",
    }


# =============================================================
# EXTRACT TIME
# =============================================================

def _extract_time(
    series,
    preferred=(),
):

    if series is None:

        return None


    if not hasattr(
        series,
        "index",
    ):

        return None


    for key in preferred:

        if key not in series.index:

            continue


        value = _to_timestamp(
            series.get(key)
        )


        if value is not None:

            return value


    return None


# =============================================================
# EMPTY RESULT
# =============================================================

def _empty_confluence_result():

    return pd.DataFrame(
        columns=[

            "ob_time",

            "zone_ready_time",

            "direction",

            "zone_high",

            "zone_low",

            "zone_size",

            "ob_high",

            "ob_low",

            "ob_score",

            "ob_status",

            "fvg_confirmation",

            "fvg_time",

            "fvg_high",

            "fvg_low",

            "fvg_strength",

            "liquidity_confirmation",

            "liquidity_type",

            "liquidity_level",

            "sweep_time",

            "sweep_direction",

            "sweep_strength_pct",

            "displacement_confirmation",

            "displacement_time",

            "displacement_strength",

            "structure_confirmation",

            "structure_type",

            "structure_break_time",

            "confirmation_count",

            "confluence_score",

            "quality",

            "distance_from_price",

            "distance_from_price_atr",

            "price_inside_zone",

            "trade_relevance",

            "ranking_score",
        ]
    )
