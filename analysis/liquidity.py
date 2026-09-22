import pandas as pd


def detect_liquidity_sweeps(df: pd.DataFrame) -> pd.DataFrame:
    required = {"time", "high", "low", "close", "swing_high", "swing_low"}

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    sweeps = []

    latest_high = None
    latest_low = None

    for i in range(len(df)):
        row = df.iloc[i]

        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        time = row["time"]

        if bool(row["swing_high"]):
            latest_high = {
                "price": high,
                "time": time,
                "index": i,
            }

        if bool(row["swing_low"]):
            latest_low = {
                "price": low,
                "time": time,
                "index": i,
            }

        if latest_high is not None:
            level = latest_high["price"]

            if i > latest_high["index"] and high > level and close < level:
                wick_distance = high - level
                close_distance = level - close

                sweeps.append({
                    "sweep_time": time,
                    "liquidity_time": latest_high["time"],
                    "liquidity_type": "buy_side",
                    "sweep_direction": "bearish",
                    "liquidity_level": level,
                    "sweep_high": high,
                    "sweep_low": low,
                    "sweep_close": close,
                    "wick_distance": wick_distance,
                    "close_distance": close_distance,
                    "sweep_strength_pct": (
                        wick_distance / level * 100
                        if level != 0
                        else 0.0
                    ),
                })

                latest_high = None

        if latest_low is not None:
            level = latest_low["price"]

            if i > latest_low["index"] and low < level and close > level:
                wick_distance = level - low
                close_distance = close - level

                sweeps.append({
                    "sweep_time": time,
                    "liquidity_time": latest_low["time"],
                    "liquidity_type": "sell_side",
                    "sweep_direction": "bullish",
                    "liquidity_level": level,
                    "sweep_high": high,
                    "sweep_low": low,
                    "sweep_close": close,
                    "wick_distance": wick_distance,
                    "close_distance": close_distance,
                    "sweep_strength_pct": (
                        wick_distance / level * 100
                        if level != 0
                        else 0.0
                    ),
                })

                latest_low = None

    return pd.DataFrame(sweeps)