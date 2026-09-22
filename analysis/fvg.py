
import pandas as pd
import numpy as np


def detect_fvg(
    df: pd.DataFrame,
    min_gap: float = 0.0,
    atr_period: int = 14,
    min_gap_atr: float = 0.05,
    max_mitigation_ratio: float = 0.80,
) -> pd.DataFrame:

    required = [
        "time",
        "open",
        "high",
        "low",
        "close",
    ]

    missing = [
        c for c in required
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing columns: {missing}"
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
        .reset_index(drop=True)
    )

    if len(data) < 3:
        return _empty_result()

    # =========================================================
    # TRUE RANGE / ATR
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
            window=atr_period,
            min_periods=5,
        )
        .mean()
    )

    results = []

    # =========================================================
    # FVG DETECTION
    # =========================================================

    for i in range(2, len(data)):

        first = data.iloc[i - 2]
        middle = data.iloc[i - 1]
        third = data.iloc[i]

        first_high = float(first["high"])
        first_low = float(first["low"])

        middle_open = float(middle["open"])
        middle_close = float(middle["close"])
        middle_high = float(middle["high"])
        middle_low = float(middle["low"])

        third_high = float(third["high"])
        third_low = float(third["low"])

        # =====================================================
        # MIDDLE CANDLE BODY / RANGE
        # =====================================================

        middle_range = (
            middle_high
            - middle_low
        )

        middle_body = abs(
            middle_close
            - middle_open
        )

        if middle_range > 0:

            middle_body_ratio = (
                middle_body
                / middle_range
            )

        else:

            middle_body_ratio = 0.0

        # =====================================================
        # BULLISH FVG
        #
        # Candle 3 low > Candle 1 high
        #
        # Zone:
        # Candle 1 high -> Candle 3 low
        # =====================================================

        bullish_gap = (
            third_low
            - first_high
        )

        bullish_atr = data.iloc[i]["atr"]

        bullish_valid = (
            bullish_gap > min_gap
        )

        if (
            pd.notna(bullish_atr)
            and bullish_atr > 0
        ):

            bullish_valid = (
                bullish_valid
                and
                (
                    bullish_gap
                    >= bullish_atr
                    * min_gap_atr
                )
            )

        if bullish_valid:

            # -------------------------------------------------
            # FVG QUALITY
            # -------------------------------------------------

            gap_atr = 0.0

            if (
                pd.notna(bullish_atr)
                and bullish_atr > 0
            ):

                gap_atr = (
                    bullish_gap
                    / bullish_atr
                )

            score = 50.0

            # Strong middle candle
            if middle_body_ratio >= 0.60:
                score += 15.0

            # Large gap relative to ATR
            if gap_atr >= 0.10:
                score += 10.0

            if gap_atr >= 0.20:
                score += 10.0

            if gap_atr >= 0.30:
                score += 10.0

            score = min(
                score,
                100.0,
            )

            results.append(
                {
                    "fvg_time": third["time"],
                    "fvg_type": "bullish",

                    "fvg_high": float(
                        third_low
                    ),

                    "fvg_low": float(
                        first_high
                    ),

                    "fvg_size": float(
                        bullish_gap
                    ),

                    "fvg_mid": float(
                        (
                            third_low
                            + first_high
                        )
                        / 2.0
                    ),

                    "fvg_size_atr": float(
                        gap_atr
                    ),

                    "middle_body_ratio": float(
                        middle_body_ratio
                    ),

                    "fvg_score": float(
                        score
                    ),

                    "first_candle_time":
                        first["time"],

                    "middle_candle_time":
                        middle["time"],

                    "third_candle_time":
                        third["time"],
                }
            )

        # =====================================================
        # BEARISH FVG
        #
        # Candle 3 high < Candle 1 low
        #
        # Zone:
        # Candle 3 high -> Candle 1 low
        # =====================================================

        bearish_gap = (
            first_low
            - third_high
        )

        bearish_atr = data.iloc[i]["atr"]

        bearish_valid = (
            bearish_gap > min_gap
        )

        if (
            pd.notna(bearish_atr)
            and bearish_atr > 0
        ):

            bearish_valid = (
                bearish_valid
                and
                (
                    bearish_gap
                    >= bearish_atr
                    * min_gap_atr
                )
            )

        if bearish_valid:

            # -------------------------------------------------
            # FVG QUALITY
            # -------------------------------------------------

            gap_atr = 0.0

            if (
                pd.notna(bearish_atr)
                and bearish_atr > 0
            ):

                gap_atr = (
                    bearish_gap
                    / bearish_atr
                )

            score = 50.0

            if middle_body_ratio >= 0.60:
                score += 15.0

            if gap_atr >= 0.10:
                score += 10.0

            if gap_atr >= 0.20:
                score += 10.0

            if gap_atr >= 0.30:
                score += 10.0

            score = min(
                score,
                100.0,
            )

            results.append(
                {
                    "fvg_time": third["time"],
                    "fvg_type": "bearish",

                    "fvg_high": float(
                        first_low
                    ),

                    "fvg_low": float(
                        third_high
                    ),

                    "fvg_size": float(
                        bearish_gap
                    ),

                    "fvg_mid": float(
                        (
                            first_low
                            + third_high
                        )
                        / 2.0
                    ),

                    "fvg_size_atr": float(
                        gap_atr
                    ),

                    "middle_body_ratio": float(
                        middle_body_ratio
                    ),

                    "fvg_score": float(
                        score
                    ),

                    "first_candle_time":
                        first["time"],

                    "middle_candle_time":
                        middle["time"],

                    "third_candle_time":
                        third["time"],
                }
            )

    if not results:
        return _empty_result()

    result = (
        pd.DataFrame(results)
        .sort_values(
            "fvg_time",
            ascending=True,
        )
        .reset_index(drop=True)
    )

    # =========================================================
    # FVG MITIGATION
    #
    # Bullish:
    # close below FVG low = fully invalidated
    #
    # Bearish:
    # close above FVG high = fully invalidated
    #
    # We also count touches.
    # =========================================================

    statuses = []
    touches = []
    mitigated = []
    mitigation_ratio = []

    for _, fvg in result.iterrows():

        fvg_time = pd.Timestamp(
            fvg["fvg_time"]
        )

        future = data[
            pd.to_datetime(
                data["time"]
            ) > fvg_time
        ]

        touch_count = 0
        was_mitigated = False
        max_fill = 0.0

        zone_high = float(
            fvg["fvg_high"]
        )

        zone_low = float(
            fvg["fvg_low"]
        )

        zone_size = (
            zone_high
            - zone_low
        )

        if zone_size <= 0:

            statuses.append(
                "invalid"
            )

            touches.append(0)
            mitigated.append(True)
            mitigation_ratio.append(1.0)

            continue

        for _, candle in future.iterrows():

            candle_high = float(
                candle["high"]
            )

            candle_low = float(
                candle["low"]
            )

            candle_close = float(
                candle["close"]
            )

            # -------------------------------------------------
            # TOUCH
            # -------------------------------------------------

            overlaps = (
                candle_low <= zone_high
                and
                candle_high >= zone_low
            )

            if overlaps:
                touch_count += 1

            # -------------------------------------------------
            # BULLISH FVG
            # -------------------------------------------------

            if fvg["fvg_type"] == "bullish":

                # How deep price entered the zone
                if candle_low < zone_high:

                    fill = (
                        zone_high
                        - max(
                            candle_low,
                            zone_low,
                        )
                    )

                    fill_ratio = (
                        fill
                        / zone_size
                    )

                    max_fill = max(
                        max_fill,
                        fill_ratio,
                    )

                # Full invalidation
                if candle_close < zone_low:

                    was_mitigated = True
                    max_fill = 1.0
                    break

            # -------------------------------------------------
            # BEARISH FVG
            # -------------------------------------------------

            else:

                if candle_high > zone_low:

                    fill = (
                        min(
                            candle_high,
                            zone_high,
                        )
                        - zone_low
                    )

                    fill_ratio = (
                        fill
                        / zone_size
                    )

                    max_fill = max(
                        max_fill,
                        fill_ratio,
                    )

                # Full invalidation
                if candle_close > zone_high:

                    was_mitigated = True
                    max_fill = 1.0
                    break

        if was_mitigated:

            status = "mitigated"

        elif touch_count == 0:

            status = "fresh"

        elif max_fill >= max_mitigation_ratio:

            status = "deep_tested"

        else:

            status = "tested"

        statuses.append(status)
        touches.append(touch_count)
        mitigated.append(was_mitigated)
        mitigation_ratio.append(
            min(
                max_fill,
                1.0,
            )
        )

    result["status"] = statuses
    result["touches"] = touches
    result["mitigated"] = mitigated
    result["mitigation_ratio"] = (
        mitigation_ratio
    )

    # =========================================================
    # FINAL QUALITY
    # =========================================================

    result["is_valid"] = (
        (~result["mitigated"])
        &
        (result["status"] != "invalid")
    )

    # Fresh FVG receives a small quality bonus.
    result.loc[
        result["status"] == "fresh",
        "fvg_score",
    ] += 5.0

    result.loc[
        result["status"] == "deep_tested",
        "fvg_score",
    ] -= 10.0

    result["fvg_score"] = (
        result["fvg_score"]
        .clip(
            lower=0.0,
            upper=100.0,
        )
    )

    return result[
        [
            "fvg_time",
            "fvg_type",
            "fvg_high",
            "fvg_low",
            "fvg_size",
            "fvg_mid",
            "fvg_size_atr",
            "middle_body_ratio",
            "fvg_score",
            "first_candle_time",
            "middle_candle_time",
            "third_candle_time",
            "status",
            "touches",
            "mitigation_ratio",
            "mitigated",
            "is_valid",
        ]
    ].copy()


def _empty_result() -> pd.DataFrame:

    return pd.DataFrame(
        columns=[
            "fvg_time",
            "fvg_type",
            "fvg_high",
            "fvg_low",
            "fvg_size",
            "fvg_mid",
            "fvg_size_atr",
            "middle_body_ratio",
            "fvg_score",
            "first_candle_time",
            "middle_candle_time",
            "third_candle_time",
            "status",
            "touches",
            "mitigation_ratio",
            "mitigated",
            "is_valid",
        ]
    )
