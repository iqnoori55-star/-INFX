# ============================================================
# INFX V11
# LIVE ANALYSIS DASHBOARD
# ============================================================
#
# Architecture:
#
#     INFX
#          |
#          +---- TradingView LIVE CHART
#          |
#          +---- LIVE ANALYSIS BRAIN
#                     |
#                     +---- Market Structure
#                     +---- Structure Breaks
#                     +---- Liquidity
#                     +---- Displacement
#                     +---- Order Blocks
#                     +---- FVG
#                     +---- Confluence
#                     +---- Signal Engine
#                     +---- Risk Management
#
# IMPORTANT:
# This application is ANALYSIS ONLY.
#
# ============================================================

import json
import sqlite3
import threading
import socket
import webbrowser
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

mt5 = None
import pandas as pd
import numpy as np
from websocket import create_connection

from analysis.market_structure import (
    detect_swings,
    classify_market_structure,
)

from analysis.structure_breaks import (
    detect_structure_breaks,
)

from analysis.liquidity import (
    detect_liquidity_sweeps,
)

from analysis.displacement import (
    detect_displacement,
)

from analysis.order_blocks import (
    detect_order_blocks,
)

from analysis.fvg import (
    detect_fvg,
)

from analysis.confluence_engine import (
    detect_confluence_zones,
)

from analysis.signal_engine import (
    detect_signals,
)

from analysis.risk_management import (
    calculate_trade_risk,
)


# ============================================================
# CONFIGURATION
# ============================================================

HOST = "127.0.0.1"
PORT = 8877
DEFAULT_SYMBOL = "XAUUSD"
DEFAULT_TIMEFRAME = 5
DEFAULT_TIMEFRAME_NAME = "M5"

BARS = 500

RISK_PERCENT = 3.0
MIN_RR = 2.0
# Live M5 scalping guard: reject setups whose structural stop is more than
# this many ATRs away from the actual MARKET execution price.
MAX_STOP_DISTANCE_ATR = 3.0

DB_FILE = "trading_journal.db"

BRAIN_REFRESH_SECONDS = 15.0
LIVE_REFRESH_SECONDS = 1.0

# ============================================================
# TIMEFRAME MAP
# ============================================================

TIMEFRAMES = {
    "M1": 1, "M2": 2, "M3": 3, "M4": 4,
    "M5": 5, "M10": 10, "M15": 15, "M30": 30,
    "H1": 60, "H4": 240, "D1": 1440,
}

TIMEFRAME_NAMES = {
    value: key
    for key, value in TIMEFRAMES.items()
}

# TradingView / dashboard fallback symbols.
DEFAULT_SYMBOL_CHOICES = [
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "NZDUSD",
]

DEFAULT_TIMEFRAME_CHOICES = list(TIMEFRAMES.keys())


# ============================================================
# GLOBAL STATE
# ============================================================

mt5_lock = threading.RLock()
state_lock = threading.RLock()

CURRENT_SYMBOL = DEFAULT_SYMBOL
CURRENT_TIMEFRAME = DEFAULT_TIMEFRAME
CURRENT_TIMEFRAME_NAME = DEFAULT_TIMEFRAME_NAME

_cached_state = None
_cached_state_time = 0.0

_server = None


# ============================================================
# UTILITY
# ============================================================

def safe_float(value, default=None):
    """
    Convert a value to float safely.
    """

    try:
        if value is None:
            return default

        if pd.isna(value):
            return default

        return float(value)

    except Exception:
        return default


def safe_int(value, default=None):
    """
    Convert a value to int safely.
    """

    try:
        if value is None:
            return default

        if pd.isna(value):
            return default

        return int(value)

    except Exception:
        return default


def safe_len(value):
    """
    Safely return the length of DataFrame/list-like objects.
    """

    if value is None:
        return 0

    try:
        return len(value)

    except Exception:
        return 0


def dataframe_records(value):
    """
    Convert DataFrame into JSON-safe records.
    """

    if value is None:
        return []

    if not isinstance(value, pd.DataFrame):
        return []

    if value.empty:
        return []

    result = []

    for record in value.to_dict(orient="records"):
        cleaned = {}

        for key, item in record.items():
            if isinstance(item, (np.integer,)):
                cleaned[key] = int(item)

            elif isinstance(item, (np.floating,)):
                if np.isnan(item):
                    cleaned[key] = None
                else:
                    cleaned[key] = float(item)

            elif isinstance(item, (np.bool_,)):
                cleaned[key] = bool(item)

            elif isinstance(item, pd.Timestamp):
                cleaned[key] = item.isoformat()

            elif pd.isna(item):
                cleaned[key] = None

            else:
                cleaned[key] = item

        result.append(cleaned)

    return result


def json_response(data):
    """
    Serialize data safely for HTTP.
    """

    return json.dumps(
        data,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


# ============================================================
# ============================================================
# TRADINGVIEW-ONLY MARKET CONFIGURATION
# ============================================================

def ensure_mt5():
    # Kept only as a legacy compatibility name.
    return False


def shutdown_mt5():
    return None


def find_best_symbol(requested_symbol):
    return str(requested_symbol).strip().upper() or DEFAULT_SYMBOL


def get_available_symbols():
    return list(DEFAULT_SYMBOL_CHOICES)


# MARKET CONFIGURATION
# ============================================================

def get_current_config():
    """
    Return current symbol/timeframe configuration.
    """

    with state_lock:

        return {
            "symbol": CURRENT_SYMBOL,
            "timeframe": CURRENT_TIMEFRAME_NAME,
            "timeframe_value": CURRENT_TIMEFRAME,
        }


def set_market_config(symbol=None, timeframe_name=None):
    """Set TradingView symbol/timeframe. Never contacts MT5."""
    global CURRENT_SYMBOL
    global CURRENT_TIMEFRAME
    global CURRENT_TIMEFRAME_NAME
    global _cached_state
    global _cached_state_time

    with state_lock:
        if symbol is not None:
            requested = str(symbol).strip().upper()
            if requested:
                CURRENT_SYMBOL = requested

        if timeframe_name is not None:
            tf_name = str(timeframe_name).upper()
            if tf_name not in TIMEFRAMES:
                raise ValueError("Unsupported timeframe: " + tf_name)
            CURRENT_TIMEFRAME_NAME = tf_name
            CURRENT_TIMEFRAME = TIMEFRAMES[tf_name]

        _cached_state = None
        _cached_state_time = 0.0
        return get_current_config()


# ============================================================
# ============================================================
# ============================================================
# MARKET DATA — TRADINGVIEW LIVE WEBSOCKET
# ============================================================
#
# The visible chart and the Brain use the same TradingView symbol
# namespace (OANDA for the default FX/metal instruments).
#
# No MetaTrader account, terminal, broker login, or MT5 data is used.
# The TradingView widget is cross-origin, so the Brain obtains the
# same TradingView market candles through TradingView's public
# chart WebSocket session.
# ============================================================

TV_SYMBOL_MAP = {
    "XAUUSD": "OANDA:XAUUSD",
    "XAGUSD": "OANDA:XAGUSD",
    "EURUSD": "OANDA:EURUSD",
    "GBPUSD": "OANDA:GBPUSD",
    "USDJPY": "OANDA:USDJPY",
    "USDCHF": "OANDA:USDCHF",
    "USDCAD": "OANDA:USDCAD",
    "AUDUSD": "OANDA:AUDUSD",
    "NZDUSD": "OANDA:NZDUSD",
}


def _tv_symbol(symbol):
    value = str(symbol or "").strip().upper()
    return TV_SYMBOL_MAP.get(value, value)


def _tv_interval(timeframe_name):
    mapping = {
        "M1": "1",
        "M2": "2",
        "M3": "3",
        "M4": "4",
        "M5": "5",
        "M10": "10",
        "M15": "15",
        "M30": "30",
        "H1": "60",
        "H4": "240",
        "D1": "1D",
    }
    return mapping.get(
        str(timeframe_name or "").upper(),
        "5",
    )


def _tv_session(prefix):
    return (
        prefix
        + "".join(
            np.random.choice(
                list("abcdefghijklmnopqrstuvwxyz0123456789"),
                size=12,
            )
        )
    )


def _tv_message(method, params):
    payload = json.dumps(
        {
            "m": method,
            "p": params,
        },
        separators=(",", ":"),
    )
    return (
        "~m~"
        + str(len(payload))
        + "~m~"
        + payload
    )


def _tv_messages(raw):
    messages = []
    position = 0

    while position < len(raw):
        marker = raw.find("~m~", position)
        if marker == -1:
            break

        length_start = marker + 3
        length_end = raw.find("~m~", length_start)

        if length_end == -1:
            break

        try:
            size = int(
                raw[length_start:length_end]
            )
        except ValueError:
            position = length_end + 3
            continue

        payload_start = length_end + 3
        payload_end = payload_start + size

        if payload_end > len(raw):
            break

        messages.append(
            raw[payload_start:payload_end]
        )
        position = payload_end

    return messages


def _tv_parse_series(message):
    try:
        obj = json.loads(message)
    except (TypeError, ValueError):
        return []

    if obj.get("m") != "timescale_update":
        return []

    params = obj.get("p") or []
    if len(params) < 2:
        return []

    payload = params[1] or {}

    series = payload.get("sds_1")
    if not isinstance(series, dict):
        return []

    raw_rows = series.get("s") or []
    rows = []

    for item in raw_rows:
        if not isinstance(item, dict):
            continue

        values = item.get("v")
        if not isinstance(values, (list, tuple)):
            continue

        if len(values) < 5:
            continue

        try:
            timestamp = float(values[0])
            open_price = float(values[1])
            high_price = float(values[2])
            low_price = float(values[3])
            close_price = float(values[4])
            volume = (
                float(values[5])
                if len(values) > 5
                and values[5] is not None
                else 0.0
            )
        except (TypeError, ValueError):
            continue

        if not all(
            np.isfinite(value)
            for value in (
                timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
            )
        ):
            continue

        rows.append(
            {
                "time": datetime.fromtimestamp(
                    timestamp
                ),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "tick_volume": volume,
                "volume": volume,
            }
        )

    return rows


def get_market_data(symbol=None, timeframe=None, bars=None):
    symbol = str(
        symbol or CURRENT_SYMBOL
    ).strip().upper()

    timeframe_name = str(
        timeframe or CURRENT_TIMEFRAME_NAME
    ).strip().upper()

    bars = int(
        bars or BARS
    )

    tv_symbol = _tv_symbol(symbol)
    interval = _tv_interval(
        timeframe_name
    )

    chart_session = _tv_session("cs_")
    quote_session = _tv_session("qs_")

    ws = None
    rows = []
    deadline = time.time() + 20.0

    try:
        ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket",
            origin="https://data.tradingview.com",
            timeout=5,
        )

        ws.send(
            _tv_message(
                "set_auth_token",
                ["unauthorized_user_token"],
            )
        )

        ws.send(
            _tv_message(
                "chart_create_session",
                [
                    chart_session,
                    "",
                ],
            )
        )

        ws.send(
            _tv_message(
                "quote_create_session",
                [
                    quote_session,
                ],
            )
        )

        # TradingView's public chart websocket is more reliable when the
        # resolved symbol is also registered in the quote session. This is
        # especially important for OANDA FX/metal symbols such as XAUUSD.
        ws.send(
            _tv_message(
                "quote_set_fields",
                [
                    quote_session,
                    "lp",
                    "lp_time",
                    "ch",
                    "chp",
                    "volume",
                    "currency_code",
                    "exchange",
                    "description",
                    "type",
                ],
            )
        )

        ws.send(
            _tv_message(
                "quote_add_symbols",
                [
                    quote_session,
                    tv_symbol,
                    {"flags": ["force_permission"]},
                ],
            )
        )

        ws.send(
            _tv_message(
                "quote_fast_symbols",
                [
                    quote_session,
                    tv_symbol,
                ],
            )
        )

        ws.send(
            _tv_message(
                "switch_timezone",
                [
                    chart_session,
                    "Etc/UTC",
                ],
            )
        )

        symbol_config = json.dumps(
            {
                "symbol": tv_symbol,
                "adjustment": "splits",
                "session": "regular",
            },
            separators=(",", ":"),
        )

        ws.send(
            _tv_message(
                "resolve_symbol",
                [
                    chart_session,
                    "sds_sym_1",
                    "=" + symbol_config,
                ],
            )
        )

        ws.send(
            _tv_message(
                "create_series",
                [
                    chart_session,
                    "sds_1",
                    "s1",
                    "sds_sym_1",
                    interval,
                    bars,
                ],
            )
        )

        while time.time() < deadline:
            try:
                raw = ws.recv()
            except Exception:
                break

            if raw is None:
                break

            raw = str(raw)

            if raw.startswith("~protocol_error~"):
                raise RuntimeError(
                    "TradingView WebSocket protocol error."
                )

            for message in _tv_messages(raw):
                if message.startswith("~m~"):
                    continue

                if message.startswith("~protocol_error~"):
                    raise RuntimeError(
                        "TradingView WebSocket protocol error."
                    )

                if "~m~~h~" in message:
                    try:
                        ws.send(message)
                    except Exception:
                        pass
                    continue

                parsed_rows = _tv_parse_series(
                    message
                )

                if parsed_rows:
                    rows.extend(parsed_rows)

                    if len(rows) >= bars:
                        break

                try:
                    obj = json.loads(message)
                    method = obj.get("m")

                    if method in {
                        "series_error",
                        "symbol_error",
                    }:
                        params = obj.get("p") or []
                        detail = (
                            str(params[-1])
                            if params
                            else "TradingView rejected the symbol."
                        )
                        raise RuntimeError(
                            "TradingView data error: "
                            + detail
                        )

                except RuntimeError:
                    raise
                except (TypeError, ValueError):
                    pass

            if rows:
                break

    except Exception as exc:
        raise RuntimeError(
            "TradingView LIVE data unavailable for "
            + symbol
            + " "
            + timeframe_name
            + ": "
            + str(exc)
        ) from exc

    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    if not rows:
        raise RuntimeError(
            "TradingView returned no candles for "
            + tv_symbol
            + " "
            + interval
        )

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .tail(bars)
        .reset_index(drop=True)
    )


# ============================================================
# LIVE MARKET CACHE
# ============================================================
# TradingView LIVE is polled frequently by the browser. Each /api/live
# request may open a short-lived websocket, so a transient websocket failure
# must not make the dashboard blank. Keep the last successful snapshot and
# serve it as stale data until a new snapshot arrives.
_live_market_cache = {}
_live_market_cache_lock = threading.Lock()


def get_live_market():
    global _live_market_cache

    try:
        df = get_market_data(
            symbol=CURRENT_SYMBOL,
            timeframe=CURRENT_TIMEFRAME_NAME,
            bars=3,
        )

        latest = df.iloc[-1]
        snapshot = {
            "ok": True,
            "source": "TradingView LIVE",
            "symbol": CURRENT_SYMBOL,
            "timeframe": CURRENT_TIMEFRAME_NAME,
            "price": safe_float(latest["close"]),
            "time": str(latest["time"]),
            "candles": dataframe_records(df),
            "stale": False,
            "error": None,
        }

        cache_key = (str(CURRENT_SYMBOL).upper(), str(CURRENT_TIMEFRAME_NAME).upper())
        with _live_market_cache_lock:
            _live_market_cache[cache_key] = dict(snapshot)

        return snapshot

    except Exception as exc:
        # Do not blank the dashboard because one websocket poll failed.
        # Return the last known-good snapshot for THIS market/timeframe only.
        cache_key = (str(CURRENT_SYMBOL).upper(), str(CURRENT_TIMEFRAME_NAME).upper())
        with _live_market_cache_lock:
            cached = dict(_live_market_cache.get(cache_key, {})) if cache_key in _live_market_cache else None

        if cached and cached.get("price") is not None:
            cached["stale"] = True
            cached["error"] = str(exc)
            cached["source"] = "TradingView LIVE (LAST KNOWN GOOD)"
            return cached

        return {
            "ok": False,
            "source": "TradingView LIVE",
            "analysis_only": True,
            "stale": True,
            "error": str(exc),
        }


def run_brain(df, execution_price=None):
    """
    Run the complete analysis pipeline on TradingView candles only.

    Only CLOSED candles are analyzed: the last candle returned by
    TradingView may still be forming, so it is excluded.
    """

    if df is None:
        raise ValueError("Market data is empty.")

    if len(df) < 100:
        raise ValueError("Not enough candles for analysis.")

    # --------------------------------------------------------
    # CLOSED CANDLES ONLY
    # --------------------------------------------------------
    if len(df) > 1:
        closed = (
            df.iloc[:-1]
            .copy()
            .reset_index(drop=True)
        )
    else:
        closed = (
            df.copy()
            .reset_index(drop=True)
        )

    # --------------------------------------------------------
    # MARKET STRUCTURE
    # --------------------------------------------------------
    closed = detect_swings(closed)
    closed = classify_market_structure(closed)

    # --------------------------------------------------------
    # STRUCTURE BREAKS
    # IMPORTANT: this engine accepts ONE dataframe argument.
    # --------------------------------------------------------
    structure_breaks = detect_structure_breaks(closed)

    # --------------------------------------------------------
    # LIQUIDITY
    # IMPORTANT: this engine accepts ONE dataframe argument.
    # --------------------------------------------------------
    liquidity_sweeps = detect_liquidity_sweeps(closed)

    # --------------------------------------------------------
    # DISPLACEMENT
    # --------------------------------------------------------
    displacement = detect_displacement(closed)

    # --------------------------------------------------------
    # ORDER BLOCKS
    # --------------------------------------------------------
    order_blocks = detect_order_blocks(
        df=closed,
        displacement_df=displacement,
        structure_breaks_df=structure_breaks,
    )

    # --------------------------------------------------------
    # FVG
    # --------------------------------------------------------
    fvgs = detect_fvg(closed)

    # --------------------------------------------------------
    # CONFLUENCE
    # --------------------------------------------------------
    confluence_zones = detect_confluence_zones(
        df=closed,
        order_blocks=order_blocks,
        fvgs=fvgs,
        liquidity_sweeps=liquidity_sweeps,
        displacement=displacement,
        structure_breaks=structure_breaks,
    )

    # --------------------------------------------------------
    # SIGNAL ENGINE
    # --------------------------------------------------------
    latest_time = closed.iloc[-1]["time"]

    try:
        signals = detect_signals(
            df=closed,
            confluence_zones=confluence_zones,
            as_of_time=latest_time,
        )
    except TypeError:
        # Compatibility with the existing signal engine if its
        # installed version does not expose as_of_time.
        signals = detect_signals(
            df=closed,
            confluence_zones=confluence_zones,
        )

    # Keep only signals belonging to the latest closed candle.
    if (
        signals is not None
        and not signals.empty
        and "signal_time" in signals.columns
    ):
        signals = signals.copy()
        signals["signal_time"] = pd.to_datetime(
            signals["signal_time"],
            errors="coerce",
        )
        signals = signals[
            signals["signal_time"] == pd.Timestamp(latest_time)
        ].copy()

    if signals is None:
        signals = pd.DataFrame()

    # --------------------------------------------------------
    # RISK / ENTRY / SL / TP
    # Analysis-only fixed balance. No broker/account connection.
    #
    # IMPORTANT: for the live dashboard the execution price is captured
    # from the same TradingView dataframe that supplied the Brain. This
    # prevents the old mismatch where MARKET ENTRY was only painted on top
    # of SL/TP calculated from a different setup entry.
    # --------------------------------------------------------
    risk_result = pd.DataFrame()

    if not signals.empty:
        try:
            risk_result = calculate_trade_risk(
                df=closed,
                setups=signals,
                account_balance=100.0,
                risk_percent=RISK_PERCENT,
                min_rr=MIN_RR,
                max_stop_distance_atr=MAX_STOP_DISTANCE_ATR,
                execution_price=execution_price,
            )
        except TypeError:
            # Compatibility fallback for an older risk engine.
            risk_result = calculate_trade_risk(
                df=closed,
                setups=signals,
                account_balance=100.0,
                risk_percent=RISK_PERCENT,
                min_rr=MIN_RR,
                execution_price=execution_price,
            )

    # --------------------------------------------------------
    # RISK GATE
    # Only risk-valid setups become executable signals. Rejected setups are
    # retained separately for diagnostics instead of being silently shown
    # as trades with unusable levels.
    # --------------------------------------------------------
    rejected_signals = pd.DataFrame()

    if not signals.empty and not risk_result.empty:
        valid_rows = risk_result[
            risk_result.get("trade_risk_valid", pd.Series(False, index=risk_result.index)).fillna(False).astype(bool)
            if "trade_risk_valid" in risk_result.columns
            else risk_result.get("risk_reward_valid", pd.Series(False, index=risk_result.index)).fillna(False).astype(bool)
        ].copy()

        rejected_rows = risk_result[
            ~risk_result.index.isin(valid_rows.index)
        ].copy()

        def _same_setup(signal_row, risk_row):
            if str(signal_row.get("direction", "")).lower() != str(risk_row.get("direction", "")).lower():
                return False
            if str(signal_row.get("signal", "")).upper() != str(risk_row.get("signal", "")).upper():
                return False

            st = pd.to_datetime(signal_row.get("signal_time"), errors="coerce")
            rt = pd.to_datetime(risk_row.get("signal_time"), errors="coerce")
            sz = pd.to_datetime(signal_row.get("zone_time"), errors="coerce")
            rz = pd.to_datetime(risk_row.get("zone_time"), errors="coerce")

            # When both identities are present, BOTH must match. This is
            # essential because several confluence zones can legitimately
            # produce BUY/SELL setups on the same closed candle.
            if pd.notna(st) and pd.notna(rt):
                if st != rt:
                    return False
                if pd.notna(sz) and pd.notna(rz):
                    return sz == rz
                return True

            if pd.notna(sz) and pd.notna(rz):
                return sz == rz

            return False

        valid_risk_records = valid_rows.to_dict(orient="records")
        rejected_risk_records = rejected_rows.to_dict(orient="records")

        valid_signal_records = []
        rejected_signal_records = []
        for _, signal_row in signals.iterrows():
            match_valid = next((r for r in valid_risk_records if _same_setup(signal_row, r)), None)
            if match_valid is not None:
                record = signal_row.to_dict()
                record["trade_risk_valid"] = True
                record["risk_status"] = match_valid.get("trade_status", "READY")
                valid_signal_records.append(record)
            else:
                match_rejected = next((r for r in rejected_risk_records if _same_setup(signal_row, r)), None)
                record = signal_row.to_dict()
                record["trade_risk_valid"] = False
                record["risk_status"] = (match_rejected or {}).get("trade_status", "REJECTED_RISK")
                rejected_signal_records.append(record)

        signals = pd.DataFrame(valid_signal_records) if valid_signal_records else pd.DataFrame(columns=signals.columns.tolist() + ["trade_risk_valid", "risk_status"])
        rejected_signals = pd.DataFrame(rejected_signal_records) if rejected_signal_records else pd.DataFrame()

        # Keep only the risk rows belonging to executable signals.
        if not signals.empty:
            keep_indices = []
            for idx, risk_row in risk_result.iterrows():
                if any(_same_setup(sig, risk_row) for _, sig in signals.iterrows()):
                    keep_indices.append(idx)
            risk_result = risk_result.loc[keep_indices].reset_index(drop=True)
        else:
            risk_result = pd.DataFrame(columns=risk_result.columns)

    # --------------------------------------------------------
    # IMMUTABLE MARKET EXECUTION SNAPSHOT
    # --------------------------------------------------------
    # Every executable signal carries the exact same Entry/SL/TP snapshot
    # that is stored in Risk Engine output. No later display layer is allowed
    # to substitute a newer market price.
    if not signals.empty and not risk_result.empty:
        risk_records = risk_result.to_dict(orient="records")

        def _match_snapshot(signal_row, risk_row):
            if str(signal_row.get("direction", "")).lower() != str(risk_row.get("direction", "")).lower():
                return False
            st = pd.to_datetime(signal_row.get("signal_time"), errors="coerce")
            rt = pd.to_datetime(risk_row.get("signal_time"), errors="coerce")
            sz = pd.to_datetime(signal_row.get("zone_time"), errors="coerce")
            rz = pd.to_datetime(risk_row.get("zone_time"), errors="coerce")
            if pd.notna(st) and pd.notna(rt) and st != rt:
                return False
            if pd.notna(sz) and pd.notna(rz) and sz != rz:
                return False
            return (pd.notna(st) and pd.notna(rt)) or (pd.notna(sz) and pd.notna(rz))

        enriched_signals = []
        for _, signal_row in signals.iterrows():
            record = signal_row.to_dict()
            match = next((r for r in risk_records if _match_snapshot(record, r)), None)
            if match is not None:
                entry_value = safe_float(match.get("entry"))
                record["execution_type"] = "MARKET"
                record["entry"] = entry_value
                record["market_entry"] = entry_value
                record["stop_loss"] = safe_float(match.get("stop_loss"))
                record["take_profit_1"] = safe_float(match.get("take_profit_1"))
                record["take_profit_2"] = safe_float(match.get("take_profit_2"))
                record["take_profit_3"] = safe_float(match.get("take_profit_3"))
                record["rr_tp2"] = safe_float(match.get("rr_tp2"))
                record["stop_distance_atr"] = safe_float(match.get("stop_distance_atr"))
            enriched_signals.append(record)

        signals = pd.DataFrame(enriched_signals)
        for row in risk_result.to_dict(orient="records"):
            row["execution_type"] = "MARKET"
            row["market_entry"] = safe_float(row.get("entry"))
        # Rebuild DataFrame so the metadata is JSON-visible in the state.
        risk_result = pd.DataFrame(risk_result.to_dict(orient="records"))
        if not risk_result.empty:
            risk_result["execution_type"] = "MARKET"
            risk_result["market_entry"] = risk_result["entry"].apply(safe_float)

    # --------------------------------------------------------
    # LATEST CLOSED CANDLE
    # --------------------------------------------------------
    latest = closed.iloc[-1]

    latest_data = {
        "time": str(latest.get("time")),
        "open": safe_float(latest.get("open")),
        "high": safe_float(latest.get("high")),
        "low": safe_float(latest.get("low")),
        "close": safe_float(latest.get("close")),
        "volume": safe_float(latest.get("tick_volume")),
    }

    # --------------------------------------------------------
    # RETURN
    # --------------------------------------------------------
    return {
        "latest": latest_data,

        # Recent closed candles are returned only for the visual
        # signal-level overlay. They are the same TradingView candles
        # used by the Brain; no broker/account data is involved.
        "chart_candles": dataframe_records(
            closed.tail(150),
        ),

        "swings": dataframe_records(
            closed,
        ),

        "structure": dataframe_records(
            closed,
        ),

        "structure_breaks": dataframe_records(
            structure_breaks,
        ),

        "liquidity": dataframe_records(
            liquidity_sweeps,
        ),

        "displacement": dataframe_records(
            displacement,
        ),

        "order_blocks": dataframe_records(
            order_blocks,
        ),

        "fvg": dataframe_records(
            fvgs,
        ),

        "confluence": dataframe_records(
            confluence_zones,
        ),

        "signals": dataframe_records(
            signals,
        ),

        "rejected_signals": dataframe_records(
            rejected_signals,
        ),

        "risk": dataframe_records(
            risk_result,
        ),

        "counts": {
            "candles": len(closed),
            "swings": safe_len(
                closed[
                    (
                        closed["swing_high"].fillna(False)
                        | closed["swing_low"].fillna(False)
                    )
                ]
            ) if (
                "swing_high" in closed.columns
                and "swing_low" in closed.columns
            ) else safe_len(closed),
            "structure_breaks": safe_len(
                structure_breaks
            ),
            "liquidity": safe_len(
                liquidity_sweeps
            ),
            "displacement": safe_len(
                displacement
            ),
            "order_blocks": safe_len(
                order_blocks
            ),
            "fvg": safe_len(
                fvgs
            ),
            "confluence": safe_len(
                confluence_zones
            ),
            "signals": safe_len(
                signals
            ),
            "rejected_signals": safe_len(
                rejected_signals
            ),
        },
    }


# ============================================================
# ACCOUNT/BROKER DATA — DISABLED
#
# TradingView-only analysis. No account/broker data is read.
#
# JOURNAL DATABASE
# ============================================================

# ============================================================
# ACCOUNT/BROKER DATA — DISABLED

# JOURNAL DATABASE
# ============================================================

def init_journal():
    """
    Create local journal database.
    """

    connection = sqlite3.connect(
        DB_FILE
    )

    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            symbol TEXT,
            timeframe TEXT,
            direction TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            result TEXT,
            note TEXT
        )
        """
    )

    connection.commit()
    connection.close()


# ============================================================
# EVENT MEMORY V11
# ============================================================
# Persistent analysis-event memory. This layer records what the V9 brain
# already produced; it does not alter the V9 detection engines or signal
# calculations.

EVENT_MEMORY_LIMIT = 200


def init_event_memory():
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS event_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            event_type TEXT NOT NULL,
            direction TEXT,
            event_time TEXT,
            status TEXT,
            payload TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_memory_market_time "
        "ON event_memory(symbol, timeframe, event_time)"
    )
    connection.commit()
    connection.close()


def _event_key(event_type, event, symbol, timeframe):
    """Stable persistent identity; signal market price is never identity."""
    raw = dict(event or {})
    event_time = raw.get("signal_time", raw.get("time", raw.get("created_at", "")))
    zone_time = raw.get("zone_time", "")
    ready_time = raw.get("zone_ready_time", "")
    direction = str(raw.get("direction", "") or "").upper()
    signal_name = str(raw.get("signal", "") or "").upper()
    identity = raw.get("id") or raw.get("key") or ""

    if str(event_type).lower() == "signal":
        return "|".join([
            str(symbol).upper(), str(timeframe).upper(), "signal",
            str(zone_time), str(ready_time), direction, signal_name
        ])

    if identity:
        return "|".join([
            str(symbol).upper(), str(timeframe).upper(), str(event_type),
            str(event_time), direction, str(identity)
        ])

    return "|".join([
        str(symbol).upper(), str(timeframe).upper(), str(event_type),
        str(event_time), direction
    ])


def remember_event(event_type, event, symbol, timeframe, status=None):
    if not isinstance(event, dict):
        return

    payload = json.dumps(
        event,
        ensure_ascii=False,
        default=str,
    )
    event_time = event.get(
        "signal_time",
        event.get("time", event.get("created_at")),
    )
    direction = str(
        event.get("signal", event.get("direction", ""))
        or ""
    ).upper()
    key = _event_key(
        event_type, event, symbol, timeframe
    )

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    if str(event_type).lower() == "signal":
        # Immutable execution snapshot: an existing signal's Entry/SL/TP
        # must never be overwritten by a later live-price refresh.
        cursor.execute(
            """
            INSERT INTO event_memory (
                event_key, created_at, symbol, timeframe, event_type,
                direction, event_time, status, payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                status=COALESCE(excluded.status, event_memory.status)
            """,
            (
                key, datetime.now().isoformat(timespec="seconds"),
                str(symbol).upper(), str(timeframe).upper(), str(event_type),
                direction, None if event_time is None else str(event_time),
                None if status is None else str(status), payload,
            ),
        )
    else:
        cursor.execute(
            """
            INSERT INTO event_memory (
                event_key, created_at, symbol, timeframe, event_type,
                direction, event_time, status, payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                status=excluded.status,
                payload=excluded.payload
            """,
            (
                key, datetime.now().isoformat(timespec="seconds"),
                str(symbol).upper(), str(timeframe).upper(), str(event_type),
                direction, None if event_time is None else str(event_time),
                None if status is None else str(status), payload,
            ),
        )
    connection.commit()
    connection.close()


def get_event_memory(symbol=None, timeframe=None, limit=50):
    connection = sqlite3.connect(DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    clauses = []
    params = []
    if symbol:
        clauses.append("symbol = ?")
        params.append(str(symbol).upper())
    if timeframe:
        clauses.append("timeframe = ?")
        params.append(str(timeframe).upper())

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(int(limit))
    cursor.execute(
        f"""
        SELECT id, event_key, created_at, symbol, timeframe, event_type,
               direction, event_time, status, payload
        FROM event_memory
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        params,
    )
    rows = cursor.fetchall()
    connection.close()

    result = []
    for row in rows:
        item = dict(row)
        try:
            item["payload"] = json.loads(item["payload"])
        except Exception:
            pass
        result.append(item)
    return result


def remember_brain_events(brain, symbol, timeframe):
    if not isinstance(brain, dict):
        return

    event_sources = {
        "signal": brain.get("signals", []),
        "confluence": brain.get("confluence", []),
        "structure_break": brain.get("structure_breaks", []),
        "liquidity": brain.get("liquidity", []),
        "displacement": brain.get("displacement", []),
        "order_block": brain.get("order_blocks", []),
        "fvg": brain.get("fvg", []),
    }

    # Signal events are persisted with their matching Risk Engine levels so
    # the dashboard can reconstruct an old signal after a browser refresh.
    risk_rows = brain.get("risk", [])
    if isinstance(risk_rows, pd.DataFrame):
        risk_rows = dataframe_records(risk_rows)
    if not isinstance(risk_rows, list):
        risk_rows = []

    for event_type, events in event_sources.items():
        if isinstance(events, pd.DataFrame):
            events = dataframe_records(events)
        if not isinstance(events, list):
            continue

        for event in events[-EVENT_MEMORY_LIMIT:]:
            if not isinstance(event, dict):
                continue

            payload_event = dict(event)

            if event_type == "signal":
                # Versioned immutable trade snapshot. Old browser/server
                # snapshots are intentionally not reused by V11.
                payload_event["snapshot_version"] = 2
                signal_time = str(event.get("signal_time", ""))
                zone_time = str(event.get("zone_time", ""))
                direction = str(event.get("direction", "")).lower()

                def _risk_match(row):
                    if not isinstance(row, dict):
                        return False
                    r_signal = str(row.get("signal_time", row.get("time", "")))
                    r_zone = str(row.get("zone_time", ""))
                    r_dir = str(row.get("direction", "")).lower()
                    return (
                        direction == r_dir
                        and ((signal_time and signal_time == r_signal)
                             or (zone_time and zone_time == r_zone))
                    )

                matched_risk = next((r for r in risk_rows if _risk_match(r)), None)
                if matched_risk:
                    payload_event["risk"] = matched_risk

            status = payload_event.get("status")
            remember_event(
                event_type, payload_event, symbol, timeframe, status=status
            )


def get_journal(limit=50):
    """
    Read latest journal records.
    """

    connection = sqlite3.connect(
        DB_FILE
    )

    connection.row_factory = sqlite3.Row

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM journal
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    )

    rows = cursor.fetchall()

    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def add_journal_entry(data):
    """
    Add journal record.
    """

    connection = sqlite3.connect(
        DB_FILE
    )

    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO journal (
            created_at,
            symbol,
            timeframe,
            direction,
            entry,
            sl,
            tp1,
            tp2,
            tp3,
            result,
            note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().isoformat(
                timespec="seconds"
            ),
            data.get(
                "symbol",
                CURRENT_SYMBOL,
            ),
            data.get(
                "timeframe",
                CURRENT_TIMEFRAME_NAME,
            ),
            data.get(
                "direction",
                "",
            ),
            safe_float(
                data.get("entry")
            ),
            safe_float(
                data.get("sl")
            ),
            safe_float(
                data.get("tp1")
            ),
            safe_float(
                data.get("tp2")
            ),
            safe_float(
                data.get("tp3")
            ),
            data.get(
                "result",
                "",
            ),
            data.get(
                "note",
                "",
            ),
        ),
    )

    connection.commit()

    connection.close()


# ============================================================
# BRAIN STATE
# ============================================================

def build_state(force=False):
    """
    Build/cached Brain state.

    Brain does NOT need to run every second.
    """

    global _cached_state
    global _cached_state_time

    now = time.time()

    with state_lock:

        if (
            not force
            and _cached_state is not None
            and (
                now - _cached_state_time
                < BRAIN_REFRESH_SECONDS
            )
        ):

            return _cached_state

        config = get_current_config()

        try:
            df = get_market_data(
                symbol=config["symbol"],
                timeframe=config["timeframe"],
                bars=BARS,
            )
        except Exception as exc:
            # A temporary TradingView websocket failure must never make the
            # browser pretend that an old signal is a new live signal. If a
            # previous state exists, return it explicitly as STALE so the UI
            # can keep history while clearly marking the data feed problem.
            if _cached_state is not None:
                stale = dict(_cached_state)
                stale["data_status"] = "STALE"
                stale["data_error"] = str(exc)
                stale["updated_at"] = datetime.now().isoformat(timespec="seconds")
                return stale
            raise

        # The last TradingView row is the current/forming market candle.
        # Capture its price from the SAME dataframe before running the Brain,
        # so chart, execution price and risk levels share one market snapshot.
        live_price = safe_float(df.iloc[-1].get("close")) if len(df) else None
        live_time = str(df.iloc[-1].get("time")) if len(df) else None

        brain = run_brain(
            df,
            execution_price=live_price,
        )

        # Event Memory is strictly downstream of V9 analysis.
        # It records the already-computed events and never feeds new
        # information into the Signal Engine, preserving V9 behavior.
        try:
            remember_brain_events(
                brain,
                config["symbol"],
                config["timeframe"],
            )
        except Exception as exc:
            print("[WARNING] Event Memory:", exc)

        event_memory = get_event_memory(
            symbol=config["symbol"],
            timeframe=config["timeframe"],
            limit=50,
        )

        live = {
            "ok": live_price is not None,
            "source": "TradingView LIVE SNAPSHOT",
            "symbol": config["symbol"],
            "timeframe": config["timeframe"],
            "price": live_price,
            "time": live_time,
            "candles": dataframe_records(df.tail(3)),
        }

        state = {
            "ok": True,
            "updated_at": datetime.now().isoformat(
                timespec="seconds"
            ),
            "config": config,
            "brain": brain,
            "event_memory": event_memory,
            "account": {
                "ok": False,
                "analysis_only": True,
                "message": "No broker/account connection is used."
            },
            "live": live,
            "data_status": "LIVE",
            "data_error": None,
        }

        _cached_state = state
        _cached_state_time = now

        return state


# ============================================================
# CHATBOT
# ============================================================

def chatbot_answer(message):
    """
    Simple local analysis assistant.

    This is not an external AI API.
    """

    text = str(
        message or ""
    ).strip().lower()

    try:
        state = build_state(
            force=False
        )

    except Exception as exc:

        return (
            "Brain is currently unavailable: "
            + str(exc)
        )

    config = state["config"]
    brain = state["brain"]
    live = state.get("live", {})

    symbol = config["symbol"]
    timeframe = config["timeframe"]

    price = None

    if live.get("ok"):
        price = live.get("price")

    latest = brain.get(
        "latest",
        {},
    )

    closed_price = latest.get(
        "close"
    )

    signals = brain.get(
        "signals",
        [],
    )

    if "price" in text or "قیمت" in text:

        if price is not None:
            return (
                f"{symbol} live price: "
                f"{price}"
            )

        return (
            f"Last closed price: "
            f"{closed_price}"
        )

    if (
        "signal" in text
        or "سیگنال" in text
    ):

        if not signals:
            return (
                f"No active signal on "
                f"{symbol} {timeframe}."
            )

        return (
            f"{len(signals)} signal(s) "
            f"detected on "
            f"{symbol} {timeframe}."
        )

    if (
        "status" in text
        or "وضعیت" in text
    ):

        counts = brain.get(
            "counts",
            {},
        )

        return (
            f"{symbol} {timeframe} | "
            f"Live: {price} | "
            f"Signals: "
            f"{counts.get('signals', 0)} | "
            f"Confluence: "
            f"{counts.get('confluence', 0)}"
        )

    return (
        "I can analyze the current "
        f"{symbol} {timeframe} market. "
        "Ask about price, signal, or status."
    )


# ============================================================
# HTML
# ============================================================

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>INFX</title>

<style>

* {
    box-sizing: border-box;
}

html,
body {
    margin: 0;
    padding: 0;
    background: #080b12;
    color: #e8edf7;
    font-family:
        Inter,
        Arial,
        Helvetica,
        sans-serif;
}

body {
    min-height: 100vh;
}

.header {
    height: 70px;
    padding: 0 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #0c1019;
    border-bottom: 1px solid #1d2635;
    position: sticky;
    top: 0;
    z-index: 20;
}

.brand-logo {
    display: block;
    width: auto;
    height: 42px;
    max-width: 180px;
    object-fit: contain;
}

.logo {
    font-size: 21px;
    font-weight: 800;
    letter-spacing: 1.5px;
}

.logo span {
    color: #4ade80;
}

.header-right {
    display: flex;
    align-items: center;
    gap: 10px;
}

.status-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: #4ade80;
    box-shadow: 0 0 12px #4ade80;
}

.status-text {
    font-size: 12px;
    color: #9ca9bd;
}

.container {
    width: 100%;
    max-width: 1800px;
    margin: 0 auto;
    padding: 18px;
}

.toolbar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px;
    margin-bottom: 16px;
}

.control {
    background: #101622;
    border: 1px solid #263144;
    color: #e8edf7;
    border-radius: 8px;
    padding: 10px 12px;
    min-width: 150px;
    outline: none;
}

.control:focus {
    border-color: #4ade80;
}

.button {
    border: 0;
    border-radius: 8px;
    padding: 10px 16px;
    background: #4ade80;
    color: #07110a;
    font-weight: 700;
    cursor: pointer;
}

.button:hover {
    filter: brightness(1.08);
}

.button.secondary {
    background: #1b2434;
    color: #e8edf7;
}

.grid {
    display: grid;
    grid-template-columns:
        minmax(0, 1fr)
        360px;
    gap: 16px;
}

.card {
    background: #0d131e;
    border: 1px solid #1d2737;
    border-radius: 12px;
    overflow: hidden;
}

.card-header {
    min-height: 50px;
    padding: 12px 15px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #1d2737;
}

.card-title {
    font-size: 13px;
    font-weight: 800;
    letter-spacing: .7px;
}

.chart-card {
    min-width: 0;
}

.chart-wrapper {
    position: relative;
    width: 100%;
    height: 620px;
    background: #080b12;
}

#tvChart {
    width: 100%;
    height: 100%;
    display: block;
    border: 0;
}

/*
 * Signal overlay:
 * The TradingView iframe is cross-origin, so browser JavaScript cannot
 * draw native TradingView objects inside that iframe. These transparent
 * HTML lines are therefore positioned over the visible chart using the
 * same TradingView candle data received by the Brain.
 */
.signal-overlay {
    position: absolute;
    inset: 42px 0 42px 0;
    z-index: 3;
    pointer-events: none;
    display: none;
}

.signal-overlay.show {
    display: block;
}

.signal-level {
    position: absolute;
    left: 0;
    right: 0;
    height: 0;
    border-top: 1px dashed;
}

.signal-level-label {
    position: absolute;
    right: 8px;
    top: -12px;
    padding: 3px 6px;
    border-radius: 4px;
    background: rgba(8, 11, 18, .92);
    border: 1px solid currentColor;
    font-size: 10px;
    font-weight: 800;
    white-space: nowrap;
}

.signal-level.entry {
    color: #60a5fa;
}

.signal-level.sl {
    color: #fb7185;
}

.signal-level.tp {
    color: #4ade80;
}

.signal-meta {
    margin-top: 10px;
    padding: 8px 10px;
    border-radius: 7px;
    background: #0b1019;
    border: 1px solid #1d2737;
    font-size: 11px;
    color: #aeb9ca;
}

.signal-score {
    color: #fbbf24;
    font-weight: 900;
}

.signal-strength {
    font-weight: 800;
    margin-left: 8px;
}

.chart-loading {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #080b12;
    color: #8794a9;
    z-index: 2;
    pointer-events: none;
}

.chart-loading.hidden {
    display: none;
}

.chart-error {
    position: absolute;
    left: 20px;
    right: 20px;
    bottom: 20px;
    padding: 12px;
    border-radius: 8px;
    background: rgba(16, 22, 34, .96);
    border: 1px solid #354157;
    color: #aeb9ca;
    font-size: 12px;
    display: none;
    z-index: 4;
}

.chart-error.show {
    display: block;
}

.stats {
    display: grid;
    grid-template-columns:
        repeat(4, minmax(120px, 1fr));
    gap: 10px;
    margin-top: 16px;
}

.stat {
    background: #0d131e;
    border: 1px solid #1d2737;
    border-radius: 10px;
    padding: 13px;
}

.stat-label {
    color: #748197;
    font-size: 11px;
    margin-bottom: 7px;
}

.stat-value {
    font-size: 17px;
    font-weight: 800;
}

.live-price {
    color: #4ade80;
}

.signal {
    padding: 14px;
    margin: 12px;
    border-radius: 10px;
    background: #101722;
    border: 1px solid #283348;
}

.signal.buy {
    border-color: #245d3c;
}

.signal.sell {
    border-color: #69333a;
}

.signal-direction {
    font-size: 20px;
    font-weight: 900;
}

.signal.buy .signal-direction {
    color: #4ade80;
}

.signal.sell .signal-direction {
    color: #fb7185;
}

.levels {
    display: grid;
    grid-template-columns:
        repeat(2, 1fr);
    gap: 7px;
    margin-top: 12px;
}

.level {
    padding: 8px;
    background: #0b1019;
    border-radius: 7px;
}

.level-label {
    font-size: 10px;
    color: #748197;
}

.level-value {
    font-size: 12px;
    margin-top: 3px;
    font-weight: 700;
}

.info {
    padding: 12px 14px;
    color: #8491a7;
    font-size: 11px;
    line-height: 1.7;
}

.count-grid {
    display: grid;
    grid-template-columns:
        repeat(2, 1fr);
    gap: 8px;
    padding: 12px;
}

.count {
    background: #101722;
    border: 1px solid #202c40;
    border-radius: 8px;
    padding: 9px;
}

.count-name {
    font-size: 10px;
    color: #748197;
}

.count-number {
    font-size: 15px;
    font-weight: 800;
    margin-top: 4px;
}

.journal {
    margin-top: 16px;
}

.journal-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 11px;
}

.journal-table th,
.journal-table td {
    padding: 9px;
    text-align: left;
    border-bottom: 1px solid #1d2737;
}

.journal-table th {
    color: #77859a;
    font-weight: 700;
}

.chat {
    margin-top: 16px;
}

.chat-body {
    padding: 12px;
}

.chat-log {
    height: 180px;
    overflow-y: auto;
    background: #080c13;
    border: 1px solid #1d2737;
    border-radius: 8px;
    padding: 10px;
    font-size: 12px;
}

.chat-user {
    color: #d9e1ed;
    margin-bottom: 9px;
}

.chat-bot {
    color: #4ade80;
    margin-bottom: 12px;
}

.chat-row {
    display: flex;
    gap: 8px;
    margin-top: 9px;
}

.chat-input {
    flex: 1;
    min-width: 0;
    background: #101622;
    border: 1px solid #263144;
    border-radius: 8px;
    padding: 10px;
    color: white;
    outline: none;
}

.error-box {
    display: none;
    padding: 12px;
    margin-bottom: 15px;
    border: 1px solid #713842;
    background: #1a0e12;
    color: #ff9baa;
    border-radius: 9px;
    font-size: 12px;
}

.error-box.show {
    display: block;
}

@media (max-width: 1100px) {

    .grid {
        grid-template-columns: 1fr;
    }

    .chart-wrapper {
        height: 560px;
    }
}

@media (max-width: 700px) {

    .header {
        padding: 0 12px;
    }

    .container {
        padding: 10px;
    }

    .stats {
        grid-template-columns:
            repeat(2, 1fr);
    }

    .chart-wrapper {
        height: 450px;
    }
}

</style>

</head>

<body>

<header class="header">

    <div class="logo">
        <img class="brand-logo" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAACAAAAAgACAYAAACyp9MwAAABCGlDQ1BJQ0MgUHJvZmlsZQAAeJxjYGA8wQAELAYMDLl5JUVB7k4KEZFRCuwPGBiBEAwSk4sLGHADoKpv1yBqL+viUYcLcKakFicD6Q9ArFIEtBxopAiQLZIOYWuA2EkQtg2IXV5SUAJkB4DYRSFBzkB2CpCtkY7ETkJiJxcUgdT3ANk2uTmlyQh3M/Ck5oUGA2kOIJZhKGYIYnBncAL5H6IkfxEDg8VXBgbmCQixpJkMDNtbGRgkbiHEVBYwMPC3MDBsO48QQ4RJQWJRIliIBYiZ0tIYGD4tZ2DgjWRgEL7AwMAVDQsIHG5TALvNnSEfCNMZchhSgSKeDHkMyQx6QJYRgwGDIYMZAKbWPz9HbOBQAAEAAElEQVR4nOzd2ZNkx5Un5p/7vRGZtWAHiJUAQTRIkE2yh+T0aFqjZczGtJhJD5LpQW/67/Qg6U16kUlmM6PRSDM9rd6m2WQ3m02CBLGRILaqyspYrrsebkRmFgiQIKpQWcv3mQU8kVlVedMjMtzv8ePHEwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAODWK+d9AXeCx7/+fH/s6y9me1Ay1KRsW4ae1Foz9ZJekvSaVltaSUpaxtaSJP18L/1clSRDhizWyerND/L9/+fPvZ4AgHvGC//km/3wyUdSD4aMNRk2LbUmq9Iy3a+zntJTetJL0lKS1AzztDhTTZKW2pNSSjabTbIYknGRg7JI+dVRvve//et7que+9d/8p3296KkHQ6ZpSjL/7EkylTrfR2TuoNr37XlcKQDcvVrt2dZ5DF1MJbUnLTW9tN38o2dRarbbbTLU1GEel3tPln3MQVmkXTvO4mCZVe3Z1pb1tM1iMaRtNymlpJSSdnaW0n99ymIMPz8lyeGwyPVrx7k4LPJn/8v/dU/NKQH4nDy/7M9995t54tHHcuXoWoaUZKjp2yllHFJaTyvzGP/Rltkn9c+nauuY1lpK6yfzrSSZ0jO0mgvDIv3qOq9974d550dvGNtvsfG8L+BOsDmo6Q8eZLvY5ni7zVQ2GVNSxyHTLsCZXtNK0uocuBtbT9kF83pp53j152doNeOUXCxjtgf1vC/nnvHEN7/U+1Dzzl/82BseAJyTw2ce7NPlZbYPLLIdWlrbppYppZRMY89U78/5X5KU3Y1wT00ynWQUn9wf9ynjOGazmZIxaT1Zl5bl5XtwanNpkc2wzWaYMi1aWnp6nX/OdnKPMPdM7XNyRO7TewcA+KxaklaT0lvGVndB+ZqpnI63Q+lpi12y3WLM1JI6lWzTsjo+ypuv/jgPPfRQLn/xiRwvejY12WyO8tCjl3J8fHSy+F/7fq4zx7mmMscFS99/htut9DkGeVx62kNDjtfb/P5//5/0v/6f/tU9OLkE4FZ6+Z/9UdYHLW8cv5dpnHJ4eJhSetbrdYZhOO/Lu7f1mlZ6WmsZe0mtNS09+33pi23Sj4/yq5/93OL/50QCQJKprXL9+EqO2pRaa4aDeWl/2zZpdV7476Wn5XSxv9V2sovnfrWtyfXWkiHpi56DLz7cV6+97xf1Jh08fDmHjz6Ua8PUr//pT/UnAJyDdVqO6iqbuspxbcnYszxIxlIyTdvzvrxzdJoGvw+4115v+P9MU3pN2qKnDy2r9SZTKzm8cCl5/qDnZ6t7Zn7Tpk22ZZNVmTJcXGTbe7a931glrLQ5cL1LAGjlYzcVAgCfqJ/ZjbePxc27/+cxtWfaZShO25552b9nMdT0Tcvxr97L0V+8WY7yZl74by/3/tBB+tCyrT3vb66mLmqmTKk9GU4SAObxfKpzjNDi//kpPel9ymazyfLipZTaMq17nvkvv9Hf+N+/Z1YFwMd6+b/7o36lHef6ap3FcpFxPJw3AE9T6qKm1Z7ef/NW//v93r3cVCWEeQbVSkvKvIFkvasEcJBF0rZ5781f5pd/9up93sufHwkASRZjzcHBMtNym9SeKT3b7Tbbtk09WOzKik3pyS4juKWdrP2X5CRr5f5ry4VFNuuk1ymb3M/B8Ftn3Te5/NCFfOHw2fz06IOeH0iqAIDbrWVKqTV1OaTUnqn2bHrLerPJMAypd8A87HzaM31Usi+VteuzeXtcrUPW05Tee4ZSMxyOqZtkvVrlXlr8TzKvRAw1bdhm0zdZTZssFot90f8k89FipfSUtjs6oe577E54PrVarVarvRvazGNo3y//l91xRDkJzPdpLuU7DUmyKzM7LFLXU46vHGXv7Z/8PE+88qUsLhzkuLWs2jqltIylpiUpbfcdd0eCtlJOvvf598P9247LZWobsmrb1EVJLSX1gYPkxcOenxyXAMAZT/xHX+3jQxeS4XrGxTLDsMh6u85qs86Fi4cZh0WuXruS5eIgJ+ccfkzb75Bx8Lza8gn98mnb2nuSnlb7SUWn5bDIuC7p19e58tY7N/1c88kkACTpm03KtM202WYqU1rpGYYhi8Nl1n1KT092r9nek5rd2ae7v7+/+bgTfiFv+w1YWoZSdx1jvn0rbKcp63GbXDjMs9/5Wl4ff9DzV+/rXAC4jYaWZGrp2ylT2SaLMs9+SknvPbvT3nP+87Hb3+53wPXdzv99wvycC1B2/98zjmOmaU4MKG3IWIbkuYOen987SQDbPmXqU4bFmClTlstFWtuV+s98o1t7UtJSdwkApQ27pOI74/nUarVarfauaPuu6Ukyzzd6mT89f2EuB9B7T61jhl4ztkWGXtKP5z+VJMd/8VZ5d3GhP/mNFzMOY8ZLB1m19XwWcE/Sdt+qJK3PKZ/7b39H9MN92PbSc7Q6Tqlz/HEcFllvNnnggcM8/w++np+t/qLnjf0hDgDc78avPtIffv6JXGmrtKFndbzOdugZx5rl8jCb9Tbb0rMYl+n7Rb98fNvL+Y+D5zv/+s3985va2ntKenpvmUpJLyW11yymZPv+1Vz9yZs5+rt3jd+fIxWskox1Puuj1pphGFKGmpYpm2m9+xPzNH8+7+v0pqH0pKXmtBvvtzZzRnXpKbXv06G5SWUs2WbKle1RNheGPP/t38+lb31B5wLAbTS9db0MLVmkZlHqXL49ZTcLup/nfzXpda6QtXvs+2P//6WU+SY6NdM0Jb2m1pq+7bmXFv+TpAw129KTIVmv12m7xYOSpKRn6EmyTwjImfJ5d8DzqNVqtVrtXdXe+LlW9vOQ3efquJup1ZQM6b2kb6aUVUuOp5x17U9+Ura/Ospyk2yvT5mmnpaSnrIL9Jc5oSDZJRbsHndEP9yPbTIMQ9q0ydhLtpt1hkXNprb0y8s89Y2vBgCSZPHKo/2L33g5x4ueaVkylZqDgwuptWa73dXqq2N67yll2P2t8x7n7s22JUkt6bVkuztivbSedn2Vqz//Za7+2Zv3VHzoTqQCQJKp9fQ6ZCpTptLTa0lrJXUYTs4A2Qfreikpvd9w9sXpDcf9pSSpbco4zeHw0qff+nf4FGrPtm9TxmSdKfWg5olXvpRxGPoHf+5NEQBul2Udku2UxbJm07Yp8172TLl/538p7eTk3b120hfzV3rvSSnzTvhak1rTtm2+uX567Hlze8/MZzYlKbVmPW2zWCzSy1wgsN6QujmfeVez66t+mkoCAHwabVfq/3TR/2So7TUp8xxkM7UslodZXT/OpfFihk3Pe2//Isff//VYymt/9cO89B9+I+PlZd7dXk1ZDNlup6QnQ+ZkxlJrShkyTZvUciYpgNuvtSzLYj5aKTVpPdu09KFk+cTDWX77C33957+4Z+aYAPzu6ksP98e/8nzWl8e0C8lq2qTXIX1eiU6pp9t7S627/ay/eego9/m2zJv68ctuPbX2HK9Wuby8mMNpTH/vw1z9t68Zs28DCQCZzy/dv5A/GtDcvwpPpvjt7Gfn4N5U280cg3HXtrVnnnj3ZOg9tSVSAG5eL2c/bplKUh44yMMvPpP1ZurXv+eGBgBuu153d377rdz35/wvux3tcx98TD+VXd3c0pKUlJT03ccnX7sX7RYf9ov/+z5Lsiv3XzPt/r/23a6DPn9Nq9VqtVrtb29LP43YtZIb5iGlJ71PqUmmaTNXI5qmDH3I4hMCVf0n75afXvrb/sS3XszhxZJV26SMQ8ZxmI+BalNq62l1/62M3+fW7p/n3LgQ08qcAFBS8+jzz+StK0c9P7paAsB96fKTj6Q+dCGrZbJt62Qo2Y8inyn+ccYdMR6ex/wrNxFHSnK8XmW5XObhiw+mX9vkwTbkb//8B7f7pXHfkgCQpPaaG8p77SKTtWdXvjQZWp1f+ElSTst4tjLNgb7MbyX3UztPvIcMfU4E2PcJN2f/Bjv0pJZ5F921usmFxy/lkeGFrNJ6+947bmgA4HM2lbmc/VT3u/6ziwDfv/O/s9qZhezskhaTvlvrn/fnTbv6AHMqQE3OBO/vBbXVlF4z9OnkznjY/YgtyVRyco7wrKf2nnrmNaTVarVarfa3t3tlN7do+wDzriZAyfz/27bNwVgzbFuGqaWu1vkkm++9XY4evtgf/cpz+WBomXpL6z1DkkWSPk3ppaWUZL9n8Lz74X5tWy9JymlCZZKU+diGKSUHj17OY195Ib+6/vc9rx+XT3rOAbg3Xfzmk/3BZx/PdHHMNEzZbluGUlN3Vatrbi7+cd7j4HnOvz5zHKmXLBYHmY6nDH3Ko7mQv/2Xf5L8fG2cvk0kAGRecN23883Dmf/vv/7Lnj6X8Uz6LpjXbvg37pe2nv11PrttnZuyz6MoPSlTTxuS49ay6ce5+MjFPP31L+e9VvrR93+p0wHgc9TLPOfZL+Sm7MbnXQb5nTAfO4/2JOmznN4O7or/7/5cSyk9rewm0xkyVwSo2Zfovdec9FGSsvu4l9NHy+l9xpzs2U6mz1qtVqvVan97u9fLvEHndA/KbmdfnY8fSqmpdT6m8vjaKu//4p38Ju///c/y4GMP58GnH8iH/Tjb7a4SwFAzTW0+0mhRk6ndEf1wf7Z19/E+DjkngNQkqT2t9KxKy4NPP55rv3g/x6+/HgDuHw+8/ER/7MVn0x+8kKOyySZT6mKYB4ubjN/s4x93xnh4+9ubix/VlG1yabyQxdE2P/nrHySvqtRzO0kAyPxinEoy1bktJXOAN0kvfX6LqPMbxXYX57yhBMh9+pJtSaa6K2c6z7rP94LuIfvXV+89ScniYMx6M+Va1nn40ct57PeezWq16tPff3ifvvoA4PPXdnPCXlp62Qd9780F7N9FS0svp2fn7dtpNxUcdjvy+i44X0pSy2kSxb2kn7mHaDVJ+skd776P5jlzkrSTMnofXcwAAH6zk+N1du3ZwHwvydRaSi0ptWa12WRoixxfv57Nq1d+86j75lTe+Jsf92cvvJLFpTEZk216eu1JrWl9m6SfFg3l9jsTg93PoUr6mWNcW1rpWdchjzz/ZH5xbdWnH6icCXBfePZCf+zlL2Z89HKulSlTb+lTy7gYs+3bm773nowmn1lpJWMfMm5L2gfH2fz5W3rzNpMAkNPF1rorY9/P1LI/+4psZ9pa5q+W3nO/3gX0XemtuTzu6c0XN+dsN9ZxyPG0SR0WWSyHrI83ud5rDh+7lGdeeTGv/f1fntt1AsD9YD9PnHf+Z5chen/O/T5qDsTvM0HbHJEt+7nxnMR4GrGtZ9p7L4mil91PVU4rAJRdF9TTTQfZ90lP/Wxn6Gm1Wq1Wex+2SeYZRU96mWsx9SQlPXNVyiTTlLock2HIerXOWBfZTsOnGse3f/tBeefBN/rDLz2d8dGLudqOs+1ThqGm9po+taQM594P92s7u3H+3Xezqv2kq4w1R9eP8vBjD+blb38tf/OD//t3m8wBcFd68MWns/jCg7lW11llm7HW7Kr+7zZX3nz85rzHwfMdfz+7oSfLTTIeb/LjP/vrm/8H+Z1JAEhOzrAf2lyyNC3pPRn2HyTz+Z4laSfTy57SS0ofUm/BL8PdqJWklTpXRSg1rQiG3wrzubE1vUyZhpKpJX27yXJY5KCUbLPN0bLm4uMP5pl/9Ep/49/9Tflt/yYA8LurvaX2nqG3tPQM6fPNUEru5ySAkpJekvlYrKT2eR449CSlpfSk9Cm1l/ReUzKmtpKh7fvt3kkAKL2l9vlnPo1C9wx9/tq8WnHja2U6s4Aw/xtarVar1Wp/W1tPSr/P8bm+/3yfv1Z6TW1Dpl5zUA+TVXL0/lE+rWt/8lq59MCD/fDCxZTFmD7M33Gx7SmtpNUhUz3/frgf234m6rUr0Jpekn1lrtKTsY65nilH0yrl4kGG7z7bpz99XbwM4B5Wv/Z4f+C5J3Jt2XM8taSUXVxiTt6rmdfvboU7YTw8j/ZmjFPN8njKq//fXyevHxuTz8H9G7k8o/SzNxRJyXwDUXpNek1t9cyf7btOm1+v9eQXot6HrZfP56FkvpFpqVlvpyyXywx1zLTZpNaaOpas03K8mPLoK1/MQ9964Ra8HQMAn6QkGXo5Wd+9M+Zh59PuZsAnc8Ha553steekf+rJ3Ppj5or32KzlNBG4pea0H/Z9cvYxtNOP9/2j1Wq1Wq32t7dnY3RJdm09id8NrWasi2RqmdabLDJke+U41157K7+LzS+vpL1/PctNsiy7f2+aMo7jPB/cbdhIkp56ch1Dm+dE+/bG6/9oMP38+/PubJOTJNLSb6ie2UuyXq9z4dLFtEVytF3nS7//UvL8YQ8A96Ty5Yf6l7/zStoDB7naV8lBTYakbbZZpKaUkrLbsDrfh9fP1J7/+Hd+7Vm1z5uo9zGNM1+54bGfDx1say5sat790c/THGN9blQASNLLLoW3zGdGJSU9JbXMxcSmMi/899J2kc05qzjZl72fA36t3F9tsg+IJ8Nu99P0uTxD95le01Iy1Z4hNW2769VhSO89vfWUkqzGljfK1Tz8hy9kujD1q3/8c2+kAHCLlKcO+7b0LMaa3nrqUNOnnlprWmnzkVA5//nYbZ//7XZand3Dv58X9t28uJV5Np3SMqWnlntu3f+MevLf3k93qM3JnGf6JKf91G7ovabVarVarfa3taXNA21JWp2PALgx+NzmIH9v6TXpbcpyNSVvXf+d4iTv/dXPyjqtP/yVZ1LqMsNyyHqYsmrbjLtBvu12ns9VkHbhxDPX00pS03ZnBreTf7t85Hq1n6W9Uen1ZI7Zas/11SqXDi9lWq8z1Z6v/dN/nB/863/X8+Mj8TKAe8nTi/78d7+Saxd6rg/rtNoy9TYvTtea1nZ33WdiFLPzHsfurnZfaSc53eQwz4NOH2m7OFnvqaWk9iF1PeXidsjqrffy/p++Zgw+R7Zw7/RkLv+f08BdS909brSftJ+92dj/3furbSc3On6Lb62+C5TXj1RaOFv2bCot7ULJu9PVfOGrz+ehP3zu3o2tA8A56PtH2d/+lBvG4jtjPnZ723kGuOufX5sAtvlzpWc+Kqrv/lw7SRz4pADu3WpOdjh1Nri/f920j3x85m9rtVqtVqv91O1ZZz63W4yf2jbJHPhf1CHrD65+zN/77a791c9L/+Aoy+MpfTVXZdymf/ziQflN17Zv9zPKT/q69tO1O7vJ1klVpf2nhyF1HLLablIXY1Z9yqpMeer3Xw4A95bHXn4+ubzMcVllKi0nY+1JLOLj/tZ5j2N3azv3bS99F+OZH2WXBNlLy7Co8/hbxvT1NottyfThUd764U/D+ZIAAHepoSf9+joXL1zI0UHJw199Pg9++1lJAABwi/VueAUAuBOUfmMws2WXlFjKXO639WS9zdtv/m7l/89646//LgfbkuWUTKttDg8PM5VkU0+vYWzZnUff0kvLVOfHPkHyNCnQtplbqpfUXlL6rl/7mf6tJds2nTwX67LN4889lcW3nzKZB7hHXP7Os/3RLz6VPs6l6ofdYz/unuxMz40bK/ndzYv885jbSjLV040gQ+/zo9RsNpsMwzBXC98kh1PN6z/8SaZXr5oAnTO/AXAXG3rS2jarOuX6Qc/jr7wgCQAAbhEL/wAA5+9kkX+nnNn5fXLsTp0/M0wlfb1N//EHnz3o/Oaq/N2/+dMcTkMWbcjR1etpNWm7KOp+sWEfVJ3K6WO/X67mo2X/uVkfTf44a9tapt7Th5p1n5LlmE1Nrqyv55mXX8iF33/GswFwlxu//kR/9usvZ72sWbcppZQMvd9QEabl1+cNfHYnRz7OBz3OVRD7mWOQejJte8Y+ZLmtudCGXHnjV9l+/z3PwB1AAgDcxeo4pGU+Z2WdlqOLNQ+/8nwe/AM3NgBwMyz+AwDcecpJwHkO8fcyL8xvek/vPcsyJMfbm/9GP1uVa2+8l4PtkAtlmZaa6czu/tqTYbfa33eL/6e7DmeSAG6ds/14duHhZGGi96SWtPQ5GaO2tEXy/upqlo9czmMvPZs8deDZALhbPXPQn/36S3l/WOdK3WS7G4SHliym0+O6PzoWc5N6Tc2+X8sNx6In8/i7qEMOe82w2qZeWeftf/EDz8AdQgIA3MW2raUMQzabTeqiZjW2bC8v8tTXXsyD33rajQ0A3EL7m8iPO+0VAIDPzyed6bvf6ZdaktQMU8kHb/3ylnzPt/7598vl7ZjL5fCG8sK1f/zCvjni+am1ptaa1lqGYcimTWm1pB6O+XBzlDy4yCNf+dJ5XyYAn9Hjr7yY/sAy18eezbJmnW1Sy8lBO2fHZQkAt1ZLPUnAPPncmUSLC4tlyrVNLq6SH/+vf6L37yASAOAu1UtShprrq+MsFkOmaUopPX1R8t64ytN/8Hu59M0nJAEAAAAAd7czW/o+eqpvL0lPzVDG1NWUK6++fsu+7V//j/+iDNe2OdjWDL2m9ppW6lx5oCS99MzffX7cEBxP0osziG+Fk37sv96fpSdDKelTy1CS3qektPTSMiyHrLLJtCh58LnHcvEf2CwDcLd57B893x989olc7essLh9kW+Ykr1Z6ftObuio8N6+dqX6078+pJts6Vz8ahiGrD6/nseFifvRv//L8LpSPZQYKd7Heew4ODrKZthmGkt6nXLt+Ne3CkKODnqe/+XLK71021AHAZ+QoAACA87df/z8byDwN7NdMU89Qxhy0MXlzuqW7z370x3+Zw03NwTYZ9sHvcnokwP5a6iecUW82eWucJF3kxgWJmiStp09TDpfLtO02wzCktZb1dpPl4SJ9UdIOap548dmMX33EUwJwl3j0u8/3J15+Psdlm8XFZa6vjrKZ1lkeLDK1djIu9I+MyRb/b42z86+a03F4qvORAJvr6zy2vJjrb72X6dWrdv/fYSQAwF1qP5D1qaXWkpaWmp7lYkirPUd1mw+WU7743a+nvvSwIQ8AfkdT+slNZCnzBxICAADOX+lzUKRmjo0shjGZWsbPow7/jz8sqzffzcVpzLAtGUvNtvT0ocxHD7Seoc/nEJc+VwSYd/9b/P/83FgJYOjJmJpptclYh/SpzZUBask0TenZZFO3ycMXMj52+fwuG4BP75nD/uDzX8iVrNMOajZtk5qSgzpms9mkDDVTTTbD6cL06eJ/S3U4z00rpWe7XWexWKSUks12Sl0uk3FMn5KLfZH64To//j//0uL/HUgCANzlanJy1k0yl5/rJZlqSy4tcnxQ89w3vpzli5IAAAAAgLvXPpTfy7zjvvTdrvzeMx1v8urf/v3n8n3f+v6Pc/X1d3JpOMy0aSkZspl6ek+GWk8rAIi8fO7aRxIr9rs89ykBw+41UbN/jfT03rPpm6zGli985YUc/OGznimAO9wTL7+Q7aVFthdq2q4Ez9DbSTWeVk4fH31Tt/B5a7TWcrBYZr1ep/eexcEyx8frTMfbXMwyef84f/M//xuL/3covwdwlys9WUxzpnkr8/kr+ySA9XaTejhmfOLBPP7V51KeOnSDAwAAANxl5hDmXAb+NLRRdjvvh1ZzUBa59u/f+HyC0D8/Lu/86PVsPzjOYVtkUWqWw5gkaVOfF5rPXNPpNQu93hrthkcrLb209NT01JMdnx/3SJLeW8aDZY76Ou3imCe+/Fzyew+IkQHcoR74hy/0h57/QrYXaraLpJeW2qY5wasldVebfiolLSW9nG6RNPLeQq2nlF01nVLSy5Daay7lMIvrPe//5K3zvkJ+A78LcA+bpilt6LnWVjn4wsN58pUvnfclAQAAAHxmZ8+A3+/2XvS5JPDn+n1/fKW899O3ctDHZNUyZMjQ57Pm99d1ag65lm454lb4uPOcW05fB/0jaR8frcRQxiEtLX3oee/6hymXD/L4l5/73K4XgM/u4Jtf6M99/aVcH3tWdco2LZtpSu89Q0vGlNScvvH3EsX+PyfLcUzbblPHMb3UbK9vcmk4zKVNzZWfvp3rf/mW3f93MLNPuJuVnl56pnpj+buWkpZkuVxmu9mk1prtkCyffywP/tOXZDgDwO+guZ0BADhXJzu5y36xt53s/q+tpmx7rr774ed+HR+89naO3nov4/GUcn1K7ckwDJlqMtXTeeM+PlPy6wvXfAYf2da/fx30ffnnXSWAs587+2dSS66vVxkWi4wHy1zv6zz49ON55I9e9OwA3EmeW/bnv/57uVbXWZVtpvRMu6NckqSUkiElaX0+Gtm7+Oeq1pLWptRa06eWg1ZyeNxz9adv5/1/9SPRsjucBAC4y5096yaZby5Pisy1nlJrpvQcl222ByWPvPB0HvgnzxsaAeB38NFdRQAA3G7113b4lcwVAMq25fW//8nnfwlvHpU3/vZHGVc9B71kWYe5NG5JpnI6ZyyZkwA+uhOdz2bflx/XnzcmAsyP/W7QfbxsPW2zWC7nXYxD0oae9aLn4S8+mUvfesqzBHCHeO7bX8/Vus1qbNkuk75746+1pgw1pZT03lN6n48CyJwEYKHzc1B6Nm1Krz2ttQw9eWA4zPZXH+adv33tvK+OT8HvBdylepnLy82Pkp75zJv0krElY0u2220y1Bxnkz4m07RJxuSxl7+YC//Bs25wAAAAgDve2YXfdmahve6qACzrIvn+r25PyuarV8sHb7yTcjyl9JqW/rEVAIb+69fO726/uFM+kv4xL/y3+ZEbd/zvkwD28bLWe8ZxTJ9atutNMtSs+pR2YcxTL33xPH4sAD7i2X/2+71cPkguLXJctimLecF/KKfDeyvJlJ7Sa2qvqa1m6PWGSkH7ZDDHAtycnmQ7TanjkN57Fq0kV47z/k/fSl6/ZpvMXUACANzFevn4HYn7AW+5XOZ4vcriYJmMQ1p6VtnmWt3kya99OQ9/WyUAAPi03DwCAJyPjx7JVPoc0pzKXPb9Yl3c1uv58N/+uGzevZpyPGXsQ+qZ4My8G72dXLPjpG6R/unD2GdjZb0k47jI0dH1HBwcZBzHrNs6/aBmvejJg4f50n/+TfExgHP05D9+uV966tH0i2NWZZvxcMxqtUrpPWnzEQBT72mZH8lcFSD59WMAVHA8dZIweeax//wnrS2dKqm1ZizLHLQhB9uaa2+/m2v//i09fJeQAAB3sdLL7nF6FFpymuk2TZssxyFtu8k0bdIWyXZoaYvk+rjJw994Ppf/8QtucgDgo95elSTpvc/nuk5TEgFcAIDzUEqfYxy1pm5bFlNSSs10OGabkl/83au3/Zre+j++V6b31hmOk0Ubks2UaZqyvHCYTaYcbY+zuLjMtk2fGGQ/G4xPbgzIf7rg/L1v3s1f0ktJy9whZ+Ng++oApbdf//zu0aeWxWKR7Xablp4yDFn1Ta6Pm1wbN1k89WCe/I9/T3wM4Dw8c9gPnn0004MH+WC6nmnsqb1n2fZl/kvKrgpAS5Jak9qz7dvs9/p/9H2fG+cQQ0vGaW7r7mtTyZxMUU8nGrXOiZXzMQvJUBZpR1OWmyGrX3yYX/7Lv7vPZyV3FwkAcBf7pAHtk3Yotuzf2JNtTY6WUx79ynO5/A+fMSwCwEfss8lbs/cfAOA89ZIMw5D0nkxtXvyfelbrbUopeee1N87lun7149fzQA5TVi0Xl4dZLpf58OoHKbXmwoULuXr16nzdn7GWlASA2ceVcv60iz3z1+pJ1Yj9J3vpmUqyGVvWi5LFo5czvPSg+BjAbfbCd76Z4aELeefqe1lcPMww1KyvH2exGD5xAVOU5tP5bckQ+7jXvOC/f+znHjU5nnKxHuZgnbz1vR9+7tfLrSUBAO5jPUk5WOTxV76Ui38oCQAATjx50Esp6f10eCxF9BUA4Dz0qaWmZGpzaf0yDimlpPZkkSH9Z0fnMlFb/9Ub5cPX3sqFLJJNT2stQ12klpI+zTsS98H15NcX9D+6sG3n4udr37+1JyVzYLwlWaVlfPBSnnzpi8mTS88AwG3y+D/9Wj948qFcL9vUxZhlGdK3UzLUbHuz0H8Tzs4pNjXZDsm0m5KMrWfcLfi3NqX0npqSsZcMpaQMNaWUHGaRg3XL26++nry2FhS7y0gAgPtVacmYXFlfS7s45plXXspD33jKTQ4AJMnbq9J7Tynl5JFIAgAAOC8nc7Na09JTa81hFll/eC156vDc4hm/+Oc/KKt3PsxiqunrKcMwJydsp00uHCzSp+1n/rclBNx6+z4ddqtKvczHZW6WyYUvPJInXnnxfC8Q4D7x4Le/2B998Zm8u7mW47bO4cWDTNt12mbKOI6ZrP7ftP0CcKtzReh9l56Mg7tNL7XOC/6ttfSpJa2ntp4LbciVn7+TK//mVcGwu5AEALhPzccBtNTDMVe2qxwvkie+9qVc/toTbi0BIKel/y36AwCcr7HWtDaX/q91yPXNNr33jNued1//RfLW8blO2N7+9z/M4XFyoR5ke7xOMh9ZME3bk3K6vwuL/rdW3e36r5kfZf/YLXy0sWY1JteXycFTD6d89VHPAMDn6bmL/ZEvPZ33cz3bw2Rx6UKub9fZbrcZ65DNdso4Ls77Ku9+fR759hWH+u546JNxsSe1lJNqRdM0pbSesQ9Zbmu2v7qat//FDwTF7lISAOA+tm1TsijpY3LUVpkePszj3/hyLr3yBTc6ANz3zp6FdvYBAMDtdzIPq/MxTUMfcjAlq3feO98LS5KfHZdf/ujnOVjVXBwOk2muVjBNU8Zx+Ez/pCSAz089cwxA0rNtm0xDz3rRsz2oefrlF5JnL3gGAD4H5fnL/dGvPJ/y8IWs65Rp6GlDz3aaUoaaxcEyaeIvt9J+IXh/DFHbtaUnpfWTr5VSshwPcpghw/Upr/7l39z+i+WWkQAA97GSns1qneVY0w9q3s1x2hOX8/ArzyfPXzLCAnDfGp+73A8ODk5KoJVdRrQbUACA26/3nmE3L0vm3fW1J/V4So4+e4n9W+mDP/5pefcnr+dCG1MzZLvdZhg+2+L/niMAbo1Wdo/khvOk90kAvff5rOkxmRbJhScfymO/9+w5XS3Ave3Rl57NxecezbXFNsPhmNa2WR8fZVzUZByybS2Luki5M4b3u1orLb20ueR/T5KeXnqmOo+LtSdp/cz8apGxD5neP8p7P/558to1u//vYhIA4D62qEOGkmxbS2rPuk75oF1Pf/RCnvvu18/78gDg3BwcHOTw8PBk0b+UctMBXAAAPpvS50X/fUWmWmv6ZpvN1aPk9et3THD6gx/+PNfeej8X+iK1l9SUbDab874sdnrZlT8+s/Ox9mQxjOnTJq21bIaWo7bKoy88nfEbjgIAuJUu/sPn++VnH8/2sGaVTVab41y6cCGlJ207pZdktVnPx/wMgwXMm9TLaTLh2aTCvv/aOKSUXfXLVtKnZLq2zpXXfpmrf/z6HTO/4rPx+wP3qZokbV7Q6Glp6RmGIa0kR4sp7ZGDPPtffMuNDgD3rX3Z1vn81mk+C624/wEAOC/jOKZPLdNmm8PhIL967e3zvqQbvb0p7736Zharnkv1MMm82FxKmYPurafmNLF0X2nqk+zPrOfm7Rf/e0l6SlpKak+GltTWUlpL+pQ6DtkOLauD5JEvP518+VBsDOBWeHrZH3nhqWwuDLnWV8lYMtYhm+PjDLVkqCV9anOVnzNVf/js9gv+w64CwNkZx74yTmrJZuqpGbJoQ6794r1c/X9fE/y6B5hDAtnfyQx9LoY21ZbV2FKeuJyn/9nvu9EBAAAAzlXvc4namiGHw0HKtqWv7rzd9dvv/6r87Hs/TDnapq97FnVMWjs5Sqq1lu12rmtcq9Ds7fZxy0lDSxapqSlzKeo2ZVW2WTx2OU987aXbfo0A96KX/8Pvpl1eZFW2WR4czElw6fORPrvHfpd6z+maBTfn7Exjf/TNfnV/01u2LVksDnJQFhmOp7z/9z8/h6vk82CWCfeplqSX/dv9nIleWzK2lrpLAthcKFk+94gkAAAAAOBc9d6TVjKWmrElqytHWV89Pu/L+libP327bN7+IAdtyNBqhlIzlJLFMGTcHTGVnFac+iQfPbOez6aXj+/JtouL9Z4MmZ+jodRkSI77JtuDkktPPpKHvvO8uBjATXjxv/puX18ek8Mx2/Rcu3Yt45n96PuF/6HPi5atnNmhzme2XwCeas9U56FsaMnQSmov6b2kjkPquqddXeXtv/lZ8uNrdv/fIyQAwH1sf/uzH2Br+vxo81ePss7xsufiFx/PQ3/0opsdAAAA4LZr6Sel8kuv6euWK796L5s37twg9a9efSvj9Z7FVDNkLvnfe0+tNWOtJz/PPhmA2+NsMLzvFpiSORmjTy2lz0dkppYct22OS8uzr3z5XK4V4F5w+ZtP9eUTD+R6nXLUVilDslwMGevwa392XwUgmd+juXXOzjZO+rn1LPuYxbrn3Z+8kdWfv67X7yESAOA+Nd/k1LRSUzNn1+0Nvc8Zd4ua1djyq3I9j77yxTz6hy+4KwUAAABuq1LKyUL5kJKxJZurq3O+qt9s+3fvlw9efSvLo5axJX1qadtpLnlcSsru5xnH8cbz6c88uLVqmx/7xaWWZCpJG0r6WNNay2azSd8lAZShZhp7rrTjfOd/+M/ExAB+V88t+7P/4OV8OB1nPBiTJLWULJfLbDabpJe0lDOVivfUwLk1elrpH3ucQu01h33MuOrJh6scK/1/z5EAAPexfZZz+q+/FdTdsLDt2+RwyLW6yRNffSGXv/OsGx4AAADgtjmbAFBLSW09fbM956v67d7/dz8t1996N8PUsxzGDJl3me+11lKr8OztUHZlpYczJaZ7SVpNVn3KVDKXQa51Ts5o8+aY1nvqA4d5b3stz//X3xETA/i0nnmgf+mffCdXlpsMlw9ytD7KwThk2myz2Wwy9SbZ7XPWyq///776TenJhQwZrq7zyx/9NHl78mzcY8ww4T62O/YlU50znvem2tJKy7TZZFFqaimZhpIPxilf/IOv5cIfPOWGBwAAALgt9vsAh5S07ZTV1aOsrlw778v6VN75+5/n6L2rSXa7/ft8nEEpJa2133gEgEoAt8pc67j0ORZWz5SXnkqyTc+mt7RaMo5jauYKDX1qmaYpx9nkuPZcfvKRjN/6gpgYwKfwwssvJY9czLVxyofHV7OoJdN2naH0DOMyy4MLmZco626DYr1hnaJ4t70pvcxl/3vm2gpl9/G0G/tKT9qV6/ngZ29m89fvmG3cgyQAwP/P3n/+SJJl+Z3399x7zdxDpNayKitLtp6Znu4RzRmK5S52SQK7CxJ4sP/eIwEu8LwgCHKfJcihWvZ0z/S0rKoumVrrEC7M7j3PC3OP8IjMUpmRGZGRvw9g5SE8sizMw6+Z3XPuOa8o8/WBf+ark8eAE6iralKerqVYoUmFpTjm7HffZs8PTuoULCIiu1zZ9CgiIiIiL1oAQnEqC1iKtG3LyoMluLDyUkxWN5dWbOXaXexRS2wjZDAPayv/SylfGuRQAGTrza6INIe6rnF32rallLL29SpGQhUYh0zuGw/bAWffOw9n+3pVRES+xIFvn/V9rx3lwWiZkgL9hT4eusE3pcR4PCbnvOFnnG72RYlv66aJgJM0ibVt9ntfdrym31tPfusiQsEDVQkMbj3i4V9f1hHfpZQAIPIKMy9rG0wz6ruzhrlRsk9Kn02+G5xhGPOwN+bYd1/n8B+c0Q2PiIjsTjZtDrop+K87UREREZEXyhxqjJCdnJ2YeoQct3u3vpHVv75mw8/uEceRKs3TNoW2KVRVRfHcrU7HcfOZyfyA+exUvzwtmxxUt64Kplt3lW+TdgDWZBJGtLAWfHKD7E7BaWnIvcIwjsjzgTPffQeO1ZoTExF5gvTWfp979zC3qmWsDsTsNE3TnduC0eRMioYxjUt01YiLrSdgycYAvznEAilPj0+3gDNbt22equp+zgnmeGm7xaBumEW8TdS5Jg0Dt//DR5rk2sV0BSkiX1uxQrbCKBVW68L+149x6AendUoWEZHdy7T6X0RERGS7WfFuzZoZ7bhh+eHydu/SN3b/rz8zfzim54kqVFiKNE1DSom1JRkO4DPBD03dbqUys8GkOubMtvl5HSdGY9yOoJ8YWgsLNfvOHn+Bey4i8nLovX3YT3zrDdq9iXGVWT+3PXn8lS83OyM1PV5h5rhtDvzPpg2aQ86Zqqpw6yovWAsLoSauFD7/uw+f457LTqCrSBF5Km2AcmCB/W+eYu976n8mIiIiIiIiIluv69NuOAEaZ/Rolfyb6y/lirVLP/slcystlmE8buml3oZ+9NEhTQtRbQpDy/awmc2BNoIt1ux/7Ti9d49oPkxEZCIcX/Q9rx8jHtuHxYBq2Dy7J5X779rYFLBC9EKYJFnEJyS1mU+SJwM8Wh3Q88RiY1z95Qfw4Z2X8lpKvj69/0TkqWSD1diSF2sOv3WG6o29uukRERERERERkS1XYle+vXKDYbPdu/P0rg/s81+8T78Yc1bjpeCTUr7mYW3SPtA9flVvX3kB3PAMderRjltKMJoq4Ht7HHj9BJye13yYiAhw4OxR6iN7eZQHlGiUoiS2rbBWQWES/J9eF6xdM5Qu+B98Y3UAgF5vjuWVAWDM9+apG+fB59fxD+/q6uIVoAQAEXkqxSBHYzU5w4N9jnz3POHEnG56RERERERERGTLFKDFKAR6BOrm5Z56GP/+jo2v3WePV1RW0WQnuBF846R+N2mrKgA7gZXuby6EQCGzyphh5aSji+w7d2yb905EZPuFdw94/+xBbCF1UegU6damy9PqAvzrdRTWKgAwc72AE3CCO4bDZPNJ1sDqYMjCwl4SNWHs7G0jt/7qQwX/XxFKABCRp1RoLTMOmUHPiYf3cOJbbxCPKwlARERERERERLbGerlbaJeGPLpxe3t3aAtc+//9xurlljJsmavnwAPTQvOzK/7VH3lnCFUijxv6dY05jHJLm5xhDxbPHqP69mG9UiLyyuq9sc9Pfec8tr/P0MeEKtHkFoLizM9qusp/mg44G/zf8JyZz6ffdzMsJGIJsNJwkB6/+Xf/+cXsuOwISgAQkafiBk0eQ4QQYLWM6J07wsHvvb7duyYiIiIiIiIiu0gxiBjNoxXa93dH2dqP/vrv6OdAGeVuhZ+HyeS+kW193b+SAHYANwoGxQlACpBxRqmQFxKnvnOe+OY+vVIi8uo50fd9b51k/theRrEhe0spLU3bgpna2GyxaRWAboX/+mln+lkO620CCpBSTTto2V8qPvovv4CbRa/IK0QJACLy1EIIpCrQti0lOstVpjq+n4M/eVM3PSIiIiIiIiKyBQJkSKGiLnG7d2bL5AvLdvOji6RRIXigWMAtdJP2oZu8nxb+1QTu9mpLJqXEcDgkWqAXK3Ju8AQrPqLsrakOzm/3boqIvHDV8b3Mnz7Aw/EKq82Quq4ASDF2dW1cZ7BnEbzb4PGV/1PTSklrwX+6x+ABb5x5r1i+fJvy8UMF/18xeveJyDNwSttSp24oGbUjmn4gnD3AwX/0tpIARETkpRZjxN0ppRCCLptFREREtoM5BAxrMqHJ2707W2p0+S5+Z4VExAnk1klVj3Hb4gYpBEJWB+XtZhYpBVJKuDuUTAIsZ0IyVhhz9ntvk945oLkwEXl1vH3AT37vbVZDS2uFfpVoSoNTCBgha0h8FuvNgWYDuRtX/pdSKAYlGh4MxyAEkkUixh4qyt0VHl68+aJ3X3YAzWSKyFOZ3oDnnCmlYMExM0p02FNTHd/PkT9/Q2d5ERF5KZnZ2uP0YxERERHZHtEDzeqIa59d3u5d2VJ+fWC3Pr7E6N4ysRgxVoxGDf3+PJ4LTdPoWnQHsJnVl27d59EhFjB3SMa94Qrf+tM/ZO7tg5oLE5Fdr3rnqL/+h99iWBWaAG4FbD1hLfo0cK0ktmcxWwHgSS2BUlVR3MmlYCHSti3RAu2wpRrD+OZDbn98meGFB7qYeAUpAUBEnloIEXdwd6IFohe8aTEKtlCT3jjKkf/+Pd34iIjIS2nzZKu7TmkiIiIiL1pwqIpRtUZ78dGum8DOn9y3R5dv0W9DFzCxrtpBLyZyzlDtnrYHL6tp8CVbt02TAFLpglzuTukHllPmwFunt3dnRURegH1vnaLs7+NWCHQl5wuB4IGomP+WKZPy/rPM15MBSukOdmHaOigSs7EQevTHxvDKXcYf3tx1107y9SgBQESeXnHMwc0gGMHBSibnhrE3jBcT6cQBjv3lO4qYiIjIS0Ur/0VERER2BgNSMcJ4904tNJdvM7pxjzh2ahI2KsQSqHo9mpK1fnKHmFYAmAqTKgClFEJd8SgPqA7v5fBfqC2miOxee3/yhleH5lnOq2SbBJ8J+Fq40UBnri01ezRtZmvblhgTAJ6hF2oYFRZKxfj2Qx7+4pImtl5hSgAQkadWStcHDbpsZzNIKRJjoFihKQ3jHlRnD3PkH7yrmx8REXnpKAlAREREZHvFAqkpPLp5Z7t35fm5MbQ771+Ae6vMtUbfIuPhiFBXDHPeEHSWF69YYTb8sr4aswvBuBvZC96vaOrAoTdOMv+DE5oHE5FdZ+7Hp3zf+RPkkAnmFCuTqiiG+WRMtGnCVFEawDPoVvUX8qS1glt31oH11gCBgJkRLWEtWJNZLBWrV25z+6Mr27bvsjMoAUBEnlqyQMQIQFsKDYUcwFPAzbHgtJZpFyvmTx3iyA/f0M2PiIiIiIiIiHxt5lDlwMNLu3wi++KS3f/0Mos5Yi2klBg3mZDUAmC7+UzZ/2nZZZ+UZc6T2XUzY9Q2rJYRS2XEibdeg9d6mgcTkV1j3/dP+tE3TzOuMh4zbtPwflf6P3gAD0xTphT8f3bT0v6bq89Mz0cppa4KgCUqAqmBXmvc+PgS7ecPlT74ilMCgIg8s+nqyBZnTGFMS8FJhEkptJZmLlK/dpgDP3pTNz8iIvJScHfcddoSERER2U4BCNnhRrPrJ7Kb392xh59e6Vou9iqakolRCQDbyQ3yTAJAdAhuFIw2dN+zGHB36rqm9UKc7zOu4NC509u9+yIiW+PkvM+dPcI4ZAajFUIvQexOy7HA5GwNTBKkApTgqmDzjNarKWz8+noFAPBcJkkBgdojty5cxT9b0pEXJQCIyNMrpVBKl8uXUiKkiAcDAjHGLvssGU0e05SG6sgeFl87zMHvn1E0RUREXhphZqWPiIiIiHxz08nr9fDAFz9n9nmBbkI7ta/OOsJr//ljW7SavNIyF3vQ+IZr0fXAyqYVgS9+V1853d/jxq9NXwN3p+SGfr/P8niVsFiz9+RhTv2j93QnISIvvYVzx7C9faxf09+zwKAZ0+a89v3ZCimaP9laG6rP0FWfWdtKoQoVITtpnJnPkbv/9VMF/wXQtaGIPAMLTojdTU7OGXMjFiNkoNBlPk+SACw4y6NH2P6K6s2D2Hv7dCkgIiI7lptR3DGzrrPn5OOCTl8iIiIi30QX1PeuVeDaJHYX3p8G+QuFEiFHsGh4LsQCVaxhVPj4Fx9s6+/won34f/+PdswWScuZulTUHomTa9E2QBuNEYVxyVgMa4Hp4LOpE5Nj7Jr+fRbTVf/m66WYy8z3uq2sbTk3hGQs+4ilXiEcXWTxvSO6iRCRl9bcn532g68fgTrQBKdxp4RACWHSDqV07QCsYJTu7FPA3JQM8IxCNmIJ3Tk+BKgijTlja/Eqkt0JZtioMNcm3v///CcF/2WNrgBF5Jk9fuPTbW3bkicBEyIQnDa0hIN9Drx1Ck4t6hJARER2JJWpExEREdk65SuurULopihbWvIk4dIcyqhhvu4x/vzeK3d1duV3n7CXOeY9UdqMmRFCwHPB3UkpkVKaLMjofsboqletlwsOX3ns5avNrr7cXIvi8eBWwYNTgtMkZ9yD+eMHXsBeiohsvfQHh33xxH5svqJEp8Upbo+3S7TC5hFSwf9nE4AYQhfgnyzAzDmDGdkCTW4xM2LrLJTIxz/9u+3eZdlhlAAgIltuti+Nma31UDYz2pKxENh7+CDHv/UGHJvTpYCIiIiIiIjILjZtqfSFSZbuWHEsWzePEEPXVz0XQvNqThs8/PkFW755l9SUSSWq7himAlWBNDmYG3sDl0kQhsn3Xp3WCTvNNGGDFNl78gj7/+T1V/MPWUReXsfn/NiZU8ztXcQNsq+3AzbrztebzZ7ntbDi2blPSi0zuQZwow6RGCPuUOfAnlJx/5Or8NmSjrhsoAQAEXlupjc7pRSyFzx0yQDjtmFsLXMnDnDsB29v926KiIiIiIiIyHMyu3oaNgYE1ioKFofSlbEFIBilQO2Ru9duvLB93Wmu/eb3jO4tU1vVtaTKhcoCiYA3Ld60ROumd6eJANNjOj3kCsC8eNOVsWYGwWj6xqHXTzL/3jElAYjIS2PvG8cJC30caHLuKvSE9ZPKWvsZNp7np+1S5Nl0NRW8i6tMk8oKeHYigapE+k1g6fId7v30cx1xeYwSAETkucleuoz9SRUAiwFLEQ9GG2C1ytTH9rD/77+tGyARERERERGRV1ABogUqC1QEcs4UnFIKfSJ3Pru83bu4fa4N7O7F69hqS2yNZAmzSChOKE7CCBjFvNuYBl26PsyyPaaVMN2d1jKPGNEuJA6cPQbHe5oDE5Edb9+fv+EHXj+B9SIldslMIUZCAKwQYK0agDxHsTvHTxPKshdK68Q2MtdW+N1Vbvy7DxT8lydSAoCIPDeldGXqYoy4WdenZpoUkKAJmUFqOfjmSRb/4pxugERERERERER2IZtsm0MFsysEzSfPK1AIpFgTs8GFwSs9sT389XVbvnqbsNJSWdUFlbMTY6QXEpa7o+pACU84pppt2RbTBIACUAdWGJIOL3D03XPbvWsiIl8qvXPAD547iS/WNMnJBjmst/qFSeVfbEOAcXr+mVak2diiRp6GmVHcu8RIHCxSx5q5kqgHmfufXN/uXZQdTAkAIvLcxBgppXQnp2Bk97WtLYVMQ7WQeJCXOfzu6xz8797TbamIiIiIiIjILrZWon4mOGDFJxukEAFIMbJy+8G27edOcv/jKzQ3H+LjQnGjGLhFAoYVJ3h3HLM5eRLx78oyF4IqATx3G0r+zzxC97qEFMkJxnOBxTNHmP+D05r/EpEd68hbp8lzxqAMGFuhoaWQyeRuwZ/TnX8mI5kSzZ6fMjm/uEF2w0MkWsIejln5/DZLH95UioV8ISUAiMhzE0JXuq9tW8yMlBIxxrVswRCNleEq1k8sM2DvmaMc/Ifv6pJBREREREREZBcJ3m1fxpisKAwJb53SOncu33gh+7fjXRvZ8NYDfHlE7YkQEjlnmlw2BJs3m1YA0ATwizObBGDWBcjGwyEWA6vWsFpnDr95cpv3UkTkyY78o3e8f2QfY1pyKJTQVQDw6Zg2HeMcmCQDzFLK2RaaJvTF0B374oRshFFmeOsht/76EwX/5Uvp+k9EnptSCiEEQgi4O9kL2QvuXd+aSHeOcgoEZ9Va9p87wZF//F0lAYiIiIiIiIi8IlovWAiYg+eCeaDKxuju/e3etR1j+be3bXTjAfMlUpFos5NTYGxOsS5OECcbrNf+/6rEC3l204DYtOz/Zr2YKLmlJBjGTF6sef1//pFeGRHZUV77H77ri6eOsGoNORQIoSvhH2ytBP3seBfCenhRpf6fj2l75bZtiUT6ObB67R63Prq43bsmLwElAIjI9rDugiFZdzPalsLAGgap0Du+n8P/4Fu6ERIRERERERHZBYp12xdNRMYYybnQNBkyLNZ9Qgtm1Qvdz53u7n/+2B5duIGtNszXcxQLeIg4htHNr8yuxlTwf/sFoLR5rU1mY84oFeKheU7/w2/rFRKRHWHfj8553ttnXDmlAnDa3ACPB/c3V5aZfr884bnyDNy688ZoTGUVfQ8s5sT45kO4tqIjLV9JCQAi8tQKTy7rMy0x90UbdDehnktX4s+hipGQjCENZS6w5/Uj7Pnxa7oREhEREREREdlFusnI9RkFt0nQIBghRmKB1Dh3rlzHbww1wb3J/d9fJi01hFEh44ysO47BjSpDKl07BWc98UK2V4wJKxDNCAGakHnQrLB46jCcWdDcl4hsr5N933f+JM18oo3rrWWSPR4+3Fzy//GAfwAC5tPtuezxq6NAP/ZJrRFWWlYv3GL1767ozC5fixIARGSbWFcqqHTtAdyMlALZW1baAavWcuCNkxz403O6TBARERERERF5iU1v7L+oJ32LU0JX5jaY4YMxS5duvOjdfCnkqyu2euUO1bCl9kRlFV2wBcxtw6p/t/UEC9leOWdSiF1orI60yRnVhfM//NZ275qIvOIOf+tNxj1o6kDLpI1vzqSUvtbPf9EiQXl2lmEu9oijQntvmet/9TsF/+VrUwKAiGybSCCESOPOuG0opRAChFggFdo5Y+7kAfrfO6YkABEREREREZFdYBqgnq0SaDGQvdCUTCDSzxFG7fbt5A53/28u2OjafdJKQy93x3Ea7AcjuAFGNsgKFWyrAjgBI0JxctNCyVTzFcs2xA70OfaX7zjH5zX3JSIv3P4/e9MPvX6CUSiUBBknu0Poys/Dxsq+3fr+9aD/tOy/W8BnKgbMnuPl6QQP9K1mfG+VvV5z8+8+2O5dkpeMEgBEZNtkvCvxFyYFAEsGHEsBKiMnh4WKA2eOUb17WJcMIiIiIiIiIi+ZaWDaeXJPenPI7pPgdaAiUAYjaF7wjr5kbv3Vhxbvr7LgkVgChUAO6yX/Z5MC1JN5OwU8RIoFSlPo92raZsS4GTKyzP12lT1nj9A/tn+7d1REXjHzPzjlh14/wUoZkeoIFAqZEAIxRnLOXxnE1/nl+TGg9si+ap7Pf/kB3FBKn3wzSgAQkS0XvsYG0GI0ztpFBYC7k0uhacZdNYAq0Du4h/2vHYdz+5UEICIiIiIiIvISmw0mTD+etgaMMWLu3L95G26saqL7K6xeuUV8NCR4F/hvQ7dNV2kWVAFgJ8gFSgYzo5eqSSsAo+ol0p4eq/3A/KlDpPOHNO8lIi9EfGO/Hzh1lNwLrDTD7nycCxYDbpAdbLKif3bl/5NMk/ymzLuEvycl/cnXFxzy6pj7V27R/O6OzubyjX29Jh6vgOgQSyCHDKxnLgWnS7WZMf1emfnvqyo44AFnPcNY5OsoQKwSOWc8F8wMs/U/ouyFUdtQhwp6kfnjBzhqkVtt61xe1l+biIiIyCvMLWBeNkxCFR6flHq179ZERHYOc8MmQerpPNs0QN19Yt38Upspg8zKvaXt2dGXzINfXrcczfd/7w3GsQv2Byvr85r45Bg/Po2yedXmNMAza1raWWWcn42Zk6pAssDS0hIhQNXvMWrGtBQizr6Th5nLkeuDsZdrS5r3EpHn6uCZY/QOL7JEQ9zTo23GOJkUKtq2xZuWXq9HW77eHdV00NL54ut70vk1ME2gCPTawOD2A+78+/d1TpCnogQAIOcRyQLWODEmWgohGCW3JIxQnGJQbDbQXQiv+AWweSAQAaPF8Fp/TtL5uhOt2dvurOYAjs3cfZpFPMIoZwrGXL/H/JE97HvzOA/DZefiSCc+ERF5sczBn5AdKiIvUJisMAlgYIXJqseN92dhJsg0O7GiEpUiIi/edCIbpqvRy2SC2wkGGaP1Qm0V86FiAaefI+Pt3OmXyNLfXjMW533/u2e5N1rBayMFg5KJOKE4Tpwkz63/3OxinkDBHGJZbx1QDDxMWzPYKz0H+mwKeHe8sxfqVANOaQrRurlUM2c1j5k/skj/5AFWrykBRkSen1N/8a4vnjvGwziiqbo5+pC6eE9pM8kjliKeu/y8whfP90/PDeYbn6HFok82ez/qBm3bsri4yOrqkGiB5EYeNcxXPXx5yJ3ffLZ9OysvPUVs6cqM1ZboeUXJ3UVZwDCMVGD2Inc64E0vmqdlTIzuQu5VegwlkIpR58DYndK2T/8iyCunO9l1b6DpeW+aab6WYR4SIRnDUctw5SH7eoscffMsVIGH7afOVRWyExGR50M3qyI71zQwsW59smljtbaNfY8VuBAR2T7TYXttTC7eJWsBmGMWKaXg48zqwwHj1cE27enLaenCLfYePsihE/u4N3oEtdG0meQQQ2A6Yzedxyy26TVZC+BMvq6c1xfKg0MqDLNx9PxJri6vevOhyj2LyNbrfeuoV8f2sRwaRqHFzTacB4J3bXrxgFuZpOx9+XI/3Wc9HXNIKTEcDjEz3B3zyP65PTQPlrnx4edwc6hzgTw1JQAAKfXxxggEkgHBiCEQvRCygxWcQCndSpNis/1L1gtpbXdA/oUnAHigzoG6Ac+B2iplZ8tTC/74ZO54PKbX61H3emSMkTcEg73HDjKXety4+pvt22EREREReeHC5P6rux2bNGWzbkWpm29s5QbkSYZpnHzDNnSnFBGRFyF8RSTZgCpEyE7OLQ/v32F47ZEmvL+Ji4/s3p5Lfrh3nqofaByoE9ZmvJRJH+ey9krEyYr02aAPbOzj3CXR6WXYCmtzXY8lMU6O9+RaJtYJc+f4udNcHYy8XFQrABHZWie+fZ5R3wn9SB470Y3gs/GuSQWYSTU1Bfe32sYFkbiRcybVPTwXyM54dcjtz64w+o0SweTZKAEAuP/rK0aqfIUx1IEcupIloTjBfRL0tm7gm6xR3jDw2avZWTJ4IBRI2SirDeNLDzQgydf2ZRcPAch0GXDj3GLmVFUi58JqO6ZKxp7jh1j4Fz/xT//lf9HfnYiIiMgrYj1hdOOqfzcnh/Vk5enzNF8lIrL9Cl1f+kK3mvDJT3KSBfop8mCo5SVPY/DbG3Z3vvLD33mdh22L1REPQN54zNeqAAChbAxId3Oi69+XZzd7PVKYqX654esOOOM8ptfr09u/wOKxAzy6qFYAIrJ13v2//X1/EId4v6KhQLC1RXnTFmp5Evyf1t2NKAlgK61VQp45ppYSngt1ifRK4N6Vqwx+dl0xD3lmSgCYuP+Lz/WGEnnhJm+7L7mIMDMKmVFpSWakZLTuPMojFvbP8do//xO/+L//VO9fERERkVfCxgvH9aCFrUX/ncdX162lcj/+DRERec4K0AbIVtaqaoaZisLBIbeZVIyqdVKj0PPTGly+Qzl+mP6RBYbjTMGIMVGmvZnXWpl2LRjcjDJNmjMo3rU/lednNgkAJoE2d9wy1Xyfh0srHFrcw9HzZ3h065ZzQeWfReTZvf5P/9hHfcP7PcYhMxoN6dUJy2VDMBom91OoE8xWcJsuKO7Ow3HTJU4JRgiJPGypCPijMSvX7r/o3ZRdKnz1U0REXpxpRqE55JwxM1JKhBRpcRoKTYBSwf3xCuMazv2Pf6A8RBEREZFXQA7rq1I2BP/pbm7NQ9e/2KcTLdPPjQ1TWJPv61GPetSjHp//oxuUSUvNaWXNrt5mt5kbNQFrMkt3HrB8/wHydPz6wG588Dn1SkuvDQQPtDNl52fz4KYVczb8/GTVZ5l8bL6+ybP7ojzEGCMFWBkO6O2Z49F4hXEFb/75D+F0X0dfRJ7J3B+c8N6hPQxCy5iWURlTz9XknNeeU9busdZbXk++88L3d7cKsNZuIUzOzQWHXJi3Glsecffjy5SP7yv3QraEKgCIyDYK6x86bL6g6NWJcdvQtk5KiaqKUAolZxqLxPkIORBC5OhPzvut//KpTo4iIiIiu5Qba2X+i9na6gnzQJyUrDQCGMTSXWdmm3w+mTrvfhaYrMTQox71qEc9vqBHQldemLAhoBwmSVvBElZaHjxYgita8fws2o8f2KP913zv+ZOwWNGUBiIEcwpgNlt62GFDcsDmQx8mr5UCQM+TU3BzLBkr4yExOXWVKK1z4ntvcf3Kb7Z7F0XkZfXGvB88f5LVMKZYpm0yc3M1uR2TYsCLd4lfGyqpFaKSv56L6Vl2eswLjpdCGmUeXLzJyq+v6RpItowSAERkW3xV9dUA5KYlxUgKRs6Ztm0xM0IIOIXGM+6B2K9YPHOU8sfZ7/z8gk6SIiIiIruYuREd6hzoZUg5AGHm+jJMehqHSe/KsJ4AYLMhjKBHPepRj3p8AY/T8rdl8hg8kMrGwIIVqKlZbY1V5Fk9/Pkl23Ngv9e9/TS9RPGWEjLmG8+Fm+dmpl+Pk0cFf56dzdTRNp95d0ySF3FocybGSAyJ3DRgkUEZsdI0HDh6gPDuIS8f3tV8l4h8Yyd+8C62f47GHPfCYq/HaDykeCZWFXky0PvMOGWTFeowGafkqfgkqQLYUHXH175nVJawcUt7d4mHF65u057KbqUEABHZcaYnw2hdA7pSnGBGCJNb0OnJsnTZ0attA/OJg+dPkXP2+7+4rEsTERHZEmaGTSbmpp+jiVCRbWEOdTY8F5JH0qiwdOkWaVSIRHLOhJDWJ1YMcigU2JAAICIiL9561ZbusZl8fW0S3Cq8aWnuLm3H7u1K9z+9yt65Hr1DfUYVlOyEqiK3mRCgaRrqusZLWT8/zpxEFQDaOtPez4HpIV5vX1SAFALFnTY3EMBLIQOxFxm0Lef/+Lt8PP4757OHeiVE5Gs7+t99y23/PCuxJToEjDJqSGZYiOTJ+F8MwmTcnyYqTQeb6TglT8e9O5G2bUu/P0fOmeF4RN2bI7RONYa6Cdy5eANuthrjZUspAUBEttX0vqcweyPUWSsJ2OUBPHbBEYJRKHiEoWWsF+mfOMDcuyMffHhLJ0wRERGRXcbMiJZI2WgeLbN04RpcGuu6T0REZJOVT+9aWZjzQ/0zsDcSEgxWB/T7fcyMuhfWKi1OhS9IdFUAaGvMtr6AyVyYgc9UBpj24QbIwDgVIPPaH3+bi5/9Xy92h0XkpTX3g1OeDi0ySmUS4J+s7qdsWNRQNq38j1rwsKWqqqIZj+nXNYPBgBZnz559LC0t0ffEfK648Mv3Ke8/0D2tbLnw1U8REXleSlfnj24rbFyVNb0hWss8nDxOB64Yu5VeZkYJxjAVqmP72H/+JOmtg7pcERGRZzbN1haR7ecGo+I0ATwGMg7jvN27JSIismMNfn3FxtfuM9dGbJjZN78Xz4XheMS4bWlyC8FU6n+bmRvmBhjBu/Jjbt2WrbBsLb6nZt+fv6FXSkS+kr2xxw+dPwELNTlM5t+t4FbWKu8UDLcu4yhMWqzFSeWX4HTPtaLkr2dgDl5azAuldVLVI1Y1K6tDFupF9tgc9z++quC/PDdKABCRbWG+8XEa+J9NApgtMfdFN6OlFChOCF1W9LBy0pE9HH3nNezsom6MRETkqbi7gv8iO1EMEIziRrAIVbXdeyQiIrKj3fn0CvZoyJ7QxwctzbChrvr05+dJqermVVhf/fmk+RcFgLbG7HzX5rYKs8d+9mM3sMpZ9jFH3jjJ4veP6SZFRL6QnZ33c997h7CnT5M2DhfTCiOzlUZmK/LOtn2ZLtmTZ9M0XdOjuq5pmoZePUciwqAhLjfc/2+fK/gvz40SAERk22y+qdycBPCkbVbOhRRSV4quFDBjSGbQK9TH9/L2j7//In4NEREREXlBgk8SdErBc+5q44qIiMgXKteW7bP/+rfMN4E4dhaqOUrOLC8vU0qhX/fp1p6ztgod1oNEmwPV8s09Nt9FWNvwaa3LsJ6EwfoGTkyGxUJTw+E3z8Br80oCEJEnOvLmGcreiqYutJYxL4RJJN8NmgB5U9l/mw38f8lcvHxzddV1YR8Mh8z3Fxg+WqXXGgd8nk/+48+3ee9kt1MCgIhsq83Z5dOLi/XGAE/e3LqgfzVZ9dW2LSEEqIxVMkuhYTwXeO9/+VPdFImIiIjsEl4KsUAVjGi6nRUREflabmT76G9/wwIVKQcqElWoqKqK5eXltZLPsytBQcGfrbRhvmsmseJJlQBmD7wBpW1pvWVoLeO5xIE3z76o3RaRl4h9+5DvOXOUVWsoFbjntXa6hS7wPx2LHgv+T74/3WRr5JypexVEWF1dZT7V7LU+H/7Hn8LNsY60PFeaMRGRbTPNd57c3bDe4+yLKwCU0G1uYGZr5ZnNDCcDhRKdVTKPYks8uMDim4eVBCAiIs9E7QBEtp85RINgNlkV5+AqASAiIvJ1NL+7ZzZs6IeEtQUvhSomelXdnWM31XruVqorCWBrPHm+6/HqCjNT9TMHPhSnTpGxtdieHr2j+1j8yZu6QRGRdWfm/Oy33+RRGRL3zDMYD7Hw5IV3AMEDYVKBxC3QBtY2t+5ey9Aw88xyd3Jtckuv16Nu4Or7n8LFZZ1d5blTAoCI7AizZzzzr3eDGS3QNA3uTowRilPaTAyQqoD3I0vtiFirN6yIiHxzKnkqsjOVUmjbdtICSre0IiIiX9fv/+V/s/JglZiNQGQ0GGPWpdWVyYpQnVmfnye1wnx8/uvxV2Ba9cjNWBmvUh9YZOH4AeK7RxWdExEA3v7zH5IO7cHnIsujFVJKWFlPlp6ONZvHoVmbx6OgEWZNmNmmvk6rhJQqRqtD5qs5qtZoHqyy/Pnl57y3Ih1d04nItikYhfUzpa1tfOUWCjiFFAJuhk/OtNGMui2kNuOlBTM01ImIyDflQDanuOMwmRgFVAlAZPuZ4VWEGNF1noiIyDdz+defULWRRIWFROvQ4mTvVqmHYJNEACOYU0q73bv80nt8vqts2B5vfDkxmetqzWm9UMVICIFH7Sq2t8/i6YPb8NuIyE5z8p/+gS/vMe7kFdpQCMFI1i24m7Ye2Ty3XqxQrBtzzAvRIRWIa8V6J/P2r7jZxIlYwLyraOzm5OCUYJRghBRxd0pTiEQIEbeAl0jf+sw3ifb2I67/9mO42erAyguh2RIR2VGmWYhflI0425uoy0J0ulJF0+87sUCcBGjclK0oIiLf3PS8ogoAIjvLtFyuM50e1y2tiIjIN9F+fM8eXrnFXDbqUBGIhFRBCEDXatHc8Vy6gEeM273Lu8KXrbr9UpMbk1IKbSkQ6Sa6esb8kX3s/8l5zXqJvML6Pzzh4eAcTSy0sZCtGy/cDXzjvdKXjUObEwRk3ewK/81xhlIKTdPQNA0pBHq9HmbWvQbFsBbm6dE+XOXO59fgsyXNMskLo9kSERERERERERERkVfEo48v0Vy7SzUuhNItpADI7mRfr37l7gStAN1WAaiIBDdKAI8GxfE2U8/3OHjmGAvfO65wncgrqP/tI378/Gukfg9rC5UbZtYV21X1wi3lBjl023ShSCxQh8BcVVGHLlmulMKobWhzJpjR80ActazcekD+zW2dUOWFUgKAiIiIiIiIiIiIyKvi6sDu/v4S9mDAvFd4Bp+Ue3YDQiBaVw1AQaTtZ6VLxHAzCl1gL5eGsWVKL3L4jVPbvYsi8oKlN/f56W+dJyz0GHtDLg3JjWhGCBEPRsFnm4rIM1hr0jKtQjzZvGlJGJ4LuWlxd2KM1FVFzwNzJbJ8/R4PLt3Yvp2XV5YSAEREREREREREREReIe3nSza8fIuF8aTncwzdytFJrWM3m+lRL9vJ8/pr4O6EAJYibSwMY8b2z3PwL9QKQORVcvSN05T5ilVrGPoYMyOYQ1u6yi2TpC55NtN2CCVMtkk7uuDdFi10wf+ciTF2G0YsUIZjmnvL3Pn8BlxY1oshL5wSAEREREREREREREReMXd+dsmWL9+iGkNFIliilEKhi3iY2Vo7ANk+bhBCwNy7vtIGJTjZCk3INLFw6PWTHPvxG0oCEHkFHPzj17x/ZD/DmGljwSOEKnQJQsWJk5EgBIX/ttLmdDhziDF258oYSHWFl0I7HFFlmCuJ659cIn92TydS2RYaAUREREREREREREReQbd+f5H2/gph7MQQYFL6H9RDeicogMeEEwilq9bg5jTe0pYWt8KIhkFo2X/uBByv9aKJ7GL7/+g13/PaMcYVeG2TpeiQg1Mok0+NUoBJX3p5eoH1KgBPMh6PaUumAG0peJvpUTFXEu3dJcYf3FbwX7aNEgBEREREREREREREXkHNtYHl20uk5YYqB0JImMVulbkpCWAnaChknGhGCpEQAhnvKgPUCU9Qeonl5Jz70z/Y7t0Vkedk7q2jvv+1k8R9C4xCZlTGZAqlFNqcyTghBCJGKWrfspXMH08GcAOqiFUVBCNnJ3pkb5yj3F/l6r/5jYL/sq2UACAiIiIiIiIiIiLyirr908+s96jBRi11qsg54+6klBRE2mZudFUZDCiO5S7YFwKEAG3bkt0ZeUOeT4wXIuf/2Z8qa0NkF1o4cZB4cIElxngCn7RosUlDeg9Ga07rpSv/XzQUbIVp4N8cjG6DLknOQmTUjAmxIlkiNE6vNS78f/9WwX/ZdkoAEBEREREREREREXmFffpvfmFhZUzzYJU69SgFrEBKabt3TYDZMN4k1td93QoWjYaWJjnD6OS9ifjuUUX+RHaRPT8854snDrLKiHHINN4S0+PhvUKXODQ7TsizC3QtWDYf17ZkjAjFsLGzL83x2a8/3Lb9FJmlBAARERERERERERGRV9zVX3/MkbQAw4aUEu24wbNrAnmHyLYe2EslrK1CdVvfch0Y9Y3Db5+C4z2F/0R2gT0/OO3H3jkDizWjdkSMRkiRpm0BMA8ED5hDCd1YAd1KdY3fW2daBWDyGbgRLREx5q0mDFpWbj1g9ZfXtfpfdgS9/0VERERERERERERecf77B/bg8+vMU8GosNhbwFRCesfwaVBvWoraAxAopZBSIueGVEeG1lIdnOfkt85t6/6KyBZ4bd5PvHOW3DeWyojQi7hnQghkX2/REku3Qh3WxwrZOvYFp8JYoE+FL49gteHyv/47HX3ZMZQAICIiIiIiIiIiIiLc/o+/t6Urt5lrwXLB83bv0aut6zldMLpAXwGCT7pQTx7dwczwUvDS4sEZW2bh9BH6PziuDA6Rl9iRd14jLyYe5QElZqqqIudM0zSkVIEbYbKZ21qguljXOqR86b8uX6Ww3lZhuk1Fh54HbKWhHjo3fqnS/7KzKAFARERERERERERERAC4//4nxNVMaR2Lmj7ebusr/rvPi238XghdFQAzo2ka+imSLbMURxw+f4p4Zr+SAEReQvt//IYvnjjAw7JC6BkxGYPxgJQSZgYzFVps0ps+OoQyaQmi4XvLFVsfg4MHUjb2hJpbH1+Ai6ta/S87ioYAEREREREREREREelcae3up5ewthDqarv3RibR/67g/ySwZwBh0gaASQKAEyjEnIkGKynDgTlOnDvD3PFDSgIQeYkc/KM3/Pj5MwxqZ1xBSAF3p21bCDZp+5EJ3o0Dbt1jyl0SgFshW1E7gGc0XfW/VgkAwzHwQChQVkfcu3yD1V9e15GWHUcJACIiIiIiIiIiIiKyZuUXN+3OJ1cJKwUriek08uxk8uZyyG5hsq1/XZPPW29zSW/P61+p65rxeAzBqOZ7PMoD9p47TnVk4cXupIg8vXP7/dAbpxjWzoiGut9jdXUVcPrzc4xGI0opVDGt/UjZNB7L1zdb3n/zMZytvrL+tUB0qHNgdG+ZOx9+9uJ2VuQb0DWYiIiIiMjX8aS7QRERERGRXWr4f102uzag7xXjJlOFioARDdrSdjPLwSaXyQGn2/IkCSDgmLsmoJ/V5D5kugJ1XfeVaIFA1/u7yRmvIjlAGbWEXsW93oC5bx2D07WqAIi8BN74/tss1Q3DOSAE2nFLqnsUjNy01KmC4rg7xQo+2XIotLEbMmzSDmBz8PpV4zMR/LBp676/ft7K0/OYrT/DcqZXJSjd6GsWCcWpm0hcarn1+0twI2uiSHYkXX+JiIiIiIiIiIiIyGMeXrhOGGT29xbJozEArRdC7MrOw+PRpTATdFL+7IsxG+TLk3LV3dcKw9pp5wNv//kfbdPeicjX9dr/8H0f9Zx4YI6VPFwL5n+Z8oTtSSvXX1VPeximwdPSZgoQUoTiVCXSz4GHV2/DhRWd5WTHUgKAiIiIiIiIiIiIiDxm5eMbduujS6TVllC61ZEWAkYkGZSmXXuueRf8nyYAFJts27j/rzpzyIMRVUq0vciZf/xthQRFdqiTf+8dr4/sJe5bYDAaEswUxH9mNtl4rIpKAMwLRiF6txkF8/VnxhhpS1dZZXXc0As1YVR4dPk29376uYL/sqMpAUBEREREREREREREnmj480u2cvUuC9bHWggEcs7EWAFg+FoQJUyCKIFu1aViV9tvvtdnOB7Q9ox4eA+c36OXRWSHmX/vmC+cPMJKKqyUEZlMUPRuy8x2dJxNBAh0rRJmt40tAiCkiqZp6MUE40I1LNz6qw8V/JcdT0OIiIiIiIiIiIiIiHyhpUu3KI+G9EvEcreespRCSgmYBlGmSQDTnzLcTG0AtlEAxqMB+/btYzU3DGrj/I+/ByeCkgBEdpCD506wHFvGPRiHTKq7cvOydaZJAJvPSV27BJ9s618vQHZocku/6lOVyNwIbn186YXut8jTUgKAiIiIiIiIiIiIiHyh0Ud37M6nV0iDTC9HeqlmPGrAbFLy39fK/5dJcEWB/50hOCytLtEmJ/cDebHm1I9/sN27JSITJ/7yW+6LNbbYY+QNsY60bYPq/z+76SH0mfPSdNvQDmBy/poqM881D/RzpDd0Hl66yeBXN3R2k5eCEgBERERERERERERE5Eut/vKqDa7dox5DVSoIRvbSBfytMC367wbZAA/dJtuq6teTQFYhh8IgtIR9cyz8+KyiiyLbbP+Pzvni2aOwf45haIkx0oxGxBjJpXz1PyBfIWAe1hIBpkfUzXGbNqrpvlms+3oxp4TuXOZuLMQ+7b0V8s2H3PtPHyv4Ly8NXYGJiIiIiIiIiIiIyFe6d+EmYbnFVlvmqzm6ZgDT8EkXQFHIaucowKgZE2PAzGjymKGPyf3AgbPHqN49qCQAkW1ir+/1/edOsFoXVq2h9RYvLSkEUhXA83bv4ktv88p+2HiOKtNqADbz+eR75lB7Ii8NOUiP6//H7xT8l5eKEgBERERERERERERE5CuNLz2wK7/7lEWvYJiJlsCM1qGEQgjddHOZrFw1U7xku4UQcO9CWjFGLATGFMpc4ux757HjtZIARF6047Ufe/d1mvnAamopVrDgBIyEkYdjYozbvZe7hnm3BboP1gL/3n1MMHIA3DALmAViCdQ5sNdrLv3q99v9K4h8Y0oAEBEREREREREREZGvZfTRHbv0m084EBfISyNKW7AQKCHSlgwlk0IkhEDOWsG63aZJGO6+thUr5AS5Hzn8+slt3kORV8+pP3iP3rF9jFKhWLdBF6A272qrBLVQ2TIB1toAzFYECClS3Gnx9bEyF2gLMRtxteXaB58x+OiestnkpaMRRERERERERERERES+tsGvrtng2j2O9veRLEEIa2WTowXSpBKAB8VMttM06GVmdLEtx+mSMixC6QUWXztG/3tHVAVA5AU59Pfecg7OkRcibSxghegQfP1tGEqYrFiXrRSmlQBmRryC42ZgATKEYvQ90c+RlSv3WP7FVZ3I5KWk8UNEREREREREREREvpGLv/iQfH/AXKmxbORc8GCEEChtS8mZKqiE9XabrvqfMgf3TA7QJCfPRY698zrh/F4lAYg8b2f7fvj8KcZ94/54GbcyWfHva0Hp9Z70ijs/q7J2LLtgaJhUV5hWA2hKXjtvUYxQnHlq+jnC0oi7n13drl0XeWZKABARERERERERERGRb+b6qn32d+9Trbb02kBl3VRzwcleujLK7muBFtkuxjSQGC1gBk6hJTMOzjBBnqs4cPbE9u6myCvg2//wz7iXB4xTIfUS5hDdiWUalDYKhpt1iQDbvcMvOTc2HEdzIxZbSwKwAski5gHaTJ+Kea9oH6x2wf+ry8rCkJeWEgBERERERERERERE5Jv79KGtXr9Pb1yYCxU+6aUcqkQVDWsVvtpOhUkAzJ047StuhgPZnCZk2uQMY2bh6H4W/vCM0jVEnpO3//mf+f08xOcTHgPehfrXStLPlqXPFsim8N2zcAOHtfY0s8d5eqxjjIQQ8FwI2ag9UZYHPLh0g5W/u6bgv7zUNIKIiIiIiIiIiIiIyFO58/llxrcewbiAGzkAMZDcqNw1Ab3NymSjdKuMAUo0muC00SgGoa6wuR6n3zlHOH9QSQAiW+zQ33vH7dACeaFiWMa4Z7zNXSDa1hOluioAgdYCbeiC2PL0yqbjZz6zTb7m7lhxaovYuOX+9dss/42C//Ly0/WXiIiIiIiIiIiIiDyVfGHZlm/cpTwYkBpIHmjbzDi3mFm36nLy3Gk5Zp/tyTyzydYzMzDHvdugW/0aMSJGzhk3GMZC008cO39mm/dYZHcJ5/f63lMHWWoGDPMIS5EUAinE7gk+Hf3WY86m4v9bYr2qQsANcoAc1sdBzwWKEYjUJGy1Yfna3W3bX5GtpOsqEREREZFNphnhYdPaF2Xfi4iIiIg8bulXN+3hhdvsbfv0xrEro5wCo1i6YPMkluUGeS0BIGAeiKXbHk8UcNy0GP1ZOQUzw1Mgh261a3CospFap0qJldGQXEeGVaE6sofeH53UgRfZCm/O+YHvnSXvCTSMqYORSiGPGgKhawJgRg5d9ZRubCxEL6RS0BD49MyhciNmx2Kg8cKQTEkBi905h+IkN6wFH7bc/OgKo08eaOZHdgUlAIiIiIiIfAXl3ouIiIiIfLnh7WWWrtxmsVSUYQtArOvHSjBrQvrFmiY1z/bCDg5xsllb6PcqRs2Q5WZAruHNP/gWvLtXoUeRZ1Cf3OsH3j5LPDhHYxm3gpmTQjcK5pyB9TYda+06WH9/yrPxUroS/w5VryalSNu2jNoGN5irapqVAYskbn5yieXfXVfwX3YNXW+JiIiIiIiIiIiIyDPJlx7a7U8vM7q3xLwlaJxSCm2AZjILHcr6BgW3Qg7dNlttq1v1asyWxJanU+hKLrh1WzHrvoYT3LHcEooTSmbv4gIlOvdGy5z7w2/Dic010UTk69p37jj7jx0hxi7oPG3D4e6EELrKHKZKg89TCQYxUErBm0zlRjDDg0FKkOFQvYfh5dsMf3ZFr4TsKkoAEBEREREREREREZFnd2nFbl24yp7Qpy6Rdtz1ly/TBAC6Va3TSels69t05asmrLdawEldn/FJr/Fp0LEYmEEymJ/rcf/BXdwKYa5iGJ0D7761zfsu8nLqfeekz584SBOcJrddkD8GshfaUtYSAOT5ciuEZFBavGmx1kkWiTFRWie1Rlgac/ODz7d7V0W2nK6nRERERERERERERGRLjH510x5eucOeMEftiUIgT+Jc5pPy85Nov0+C/5tXwQZQ7+stEXACbgHz0NVUmBzXYpBDgcpoyKyurrBnzwJt29KWhpzgxOunOPO9N/VKiHwTry36wTdPkPfWjGnxYIQqEVLEff3tlF3NBp+3UjLupUt0Sgkzo7QOORKbQFrOfP6LDyhXh8rGkF1HCQAiIiIiIiIiIiIismWu/vpDyoMB/Zy6PvMzIWTzLvg8S2Gw58c8rB1/Z5ps4bh1bRfGnmloCb0KnwTJkkfqDM3yiJXl5W3df5GXyomen/rOW6RDiwxjS4mGxYAbtKXgZsQYIdiGZAB5DswJEYq3lFIIKVIskFtn3mr2UnPj9xdoP3mo4L/sSkoAEBEREREREREREZGtc31s196/QFxuqNtA9EDwAARyWA9CQ5cdEPANSQIFcOueL08vOEQvxNlV/5PS/9PD7YBVicacQTMmN4UwyBzKfT7+l//V7n12Q8Exka9pzxvHqY/vZRxaWsu0ZNpSaHKmlNKtQMdxd0LQ+Pa8mTlmTrGu9cKYQko1c40xvvaQ4S81vsnupRFGRERERERERERERLbU+MObZg9HzDWBXstaEDpbWCv7D12Q2vzJE9VaH/uMbFpboYAVAgWbOaoOEBOlKfRjTdVE9nhNWhrzq//nXykwJvJNnF/0w+fPsFSGjGzc9Z4HzAwz6wL+wSilkHPGTG+x5y03LdECIVYUHDwwn2rsYcPVX3203bsn8lwpAUBEREREREREREREttyF/+MX5neXmWsjfasZj8dYDMRe9zHFJ6vUwbyrCNCt/lfwfysUwGoYNqukYFgp1ASiFzKGx0TbZCqr6Q8D1YMxl/72Az7/V79QZFLkmzg759/9yz9hNWUGNNS9ipLbtSC/T5Ke3H0tGUAtAJ6/ZGntOJcClUfy/RU++flv8OsrGudkV1MCgIiIiIiIiIiIiIg8F5d+/mvKwwE2KvRjDzwwHI6Zn18gpbRWAUC2nhsM2xFVr8K8UFukHYxIHgnZYZxZCD0W25p8a5nbv7tI+9s7CoqJfEPv/PgH3BktQS9Q9yua4YioNKZtFRy8FCIRdwgNLOTI7U8uw8WHGudk11MCgIiIiIiIiIiIiIg8HzeLPbp0m14b6VuPkjMhBJqmoR23Xfn/mThZt/o/oKnrrRHrCoIzGq4SitOLidoj89aj3yTC/TEPPrrK7V99Rv7onoJiIt/Qkb98x8OBBXI/MC55reT/LPMv3zYrk02ehVFZRfJELImF0GNw5S7NlbvbvWMiL0Ta7h0QERERERERERERkd3rwd9etmpx0RfOHiFVRoiRAoToUAplQ6xsPfBva58rFPa0StMSAtRVn1IKtSXKoLBQzdGuDLj+0RXGv7yhwL/IU6i+fdj3nzvBzcED0r45ymiAAdEMs6BeJtssWkU7LPQt0d5f4taHF+DmWOOdvBKURikiIiIiIiIiIiIiz9XtDz9nePsR8zniqw0pRNwgh27zSUjGvAv8qy3AszOHmJ3KI4RA8UCwmqqNtDcecefXnyv4L/KU5r991E985w0ehRG2WDNoBlRVRXBo2/axKgDyYpkHSutUrRGXx9z47SdwaVkvirwylAAgIiIiIiIiIiIiIs/XzaHdvXCFMMzsredohyPatqUYaxt07QCmSQDy7KqqxrNTRpk91TwsNQxvP+Ta+xdY+fCWDrPI03h9wQ+/+xrs67PsI6wy2lJwCiEEQlDobSeIJbC/3sOjy7fh4yWNd/JKUQsAEREREREREREREXnumg/v2N0De/3oO2dIIUHltJPy/tMV/wZEh2yqAvCs3GCQG3pWsTf28Hur3P79JQa/0qp/kWex5+xRfP8cS3lANVcxHA7p1xVNk0khElJFW8rXXoGrJidbLzgEN+7duMPDz65u9+6IvHBKABARERERERERERGRF2Lpv31m2Yof+fbrPGwHWFyP9BeD6AUVrl032xoBNh6Z8gXPmT4rFqhLzVyJ1CsNn/7yI/JHDxT8F3kGi98/4QfOHGdoLR4BM1IIlLYL/gPknLsnqw3Ac+O2PvaF0o16s0ljKQfiIHPlX/9CL4K8knQlJSIiIiIiIiIiIiIvTLm1ij9sqNtEHDvzqSbnjIWAm0NwQoDsZUOQZ1Zg4+T29Hmbt+0WNm1TX7Sfm5/rhG6zsBbgMnfMu+9YtMkxMwhG8EAkYiFRtRUnmgX84/t8+l9+q+C/yDOa/8EJP/6d8+R+oLVMwLCciQ4Jw7x0mxlmRoGvtcmX827gm46GuDk5OG00hp7JDlWsSTkwR4UPM3MlcfGv39/uXRfZNqoAICIiIiIiIiIiIiIvzPCTW3bv4KIffe8sgwpWllfpz/cZjgdUscYdSs6TPtpPFx57fFX89jH/5skI05j+47/9+i9UrDtOBQjBCR6hdKthq2DMNYELv3ifh7+7rMC/yLN6bdH3nT3BqIY8TbwxZ/rmmo41OyHxaLeZPaRl9pNc6KWKfuyx/OARC5YoY2dfNc/o9iO4+FCvhryylAAgIiIiIiIiIiIiIi/U8s8+s/585YtvHmfVA1YciuHBsUm4J2BfWOZ+c2B8JwffnrRPX7S/098rTEtbT5IHNicy5EmJgGiR4I5nI7vTi4k5S5Rbj7h36Y6C/yJb5PBrJ1k4sIdVa8jmFLrg//QNNvte3kkJSC+ztaop3p0L8swBjQVCdqLB0uoSe/YsEhtIQ2d8+xFX/tWvNPbJK00JACIiIiIiIiIiIiLywt354HOqgwvsO7aXpdESqUq4OxacFAz3jPF0Qf2XJfD2RdUBysxHs0kPwWZWwLrheFf23xKxGLbSMLr3kIefX2fwwR0FwES2wP4fnfMDJ44y8IYcoMUxM8DZPNRM359KAnh+jElylEN06NU1o9URVZqjXVnhqoL/IkoAEBEREREREREREZFtcHNs9z+56icW5uj3akYULHTRslIyIcQuwvMNPE25/efpixoYzDY3mA0Qbt736DAt+18M8mS9cXTAA03JxBiZo8JWhjz65Dr3fn5xBx0BkZdb+s5hP3LuFD6fGJYhlhIWAAPPecNzC4azs8agl1mZ1AAIj42kBQj0q5rxeExKFVWq8eUR13778QvfT5GdKHz1U0REREREREREREREtt7w1zftzkdX6HtNaJxgEXcn50wITxdFe1lW3H7Z5Px6APFJv0wAD5gHFtMcc20k31nizu8vKfgvspXOzPmpd9+AhYrlPCL0E61lChmfJCfpDff8deF+NrRcABg2Y8wiPU+ElZbVa/fg4XB7dlJkh1EFABERERERERERERHZNkt/fdH6+xe8OrGXXIxxKYQUab089UranZIEsLkM+Oag//Tz2fWts79zsa7Udd707xQLVCXAUsPo9hKrF2+x/PtbikWKbKH9b54i7Jtj4C3jkAkxUDyTixNwgsL/z9Xm8TOW7mvFAtmg4MzVNfnekPBgwIO/+kgviMiEKgCIiIiIiIiIiIiIyLa68+k19ngPGzvmRq/Xo23b7d6tHaGsBcGM4IFYAnUb6DfGo8+vc+v9zxT8F9liB/7kvB85d5qH4yVyBWmuYtyOADAvhBDWetHbZAusfzzd5Om4dfVPNh/C9WNqVL0+49UxtjTi2q8/erE7KLLDKQFARERERERERERERLaVf/LAPvv5b+nnSO2RZjgiVRXRAp4LFCdaN51dSsHd10pwP0lgZ01+u01WrsJj3azNDDNb+30CRqD7vDhYiAQC7ajFLFKT2DM27r9/geX/64JxZVXBf5EttO9P3/JDb55mhTFhLjH2hrE3WJz0pA9x0xu5K04/TQKQrVHXNePxmBjjZMyfHHYLuBuhgXmveHT5FlwbaxwUmaGxSERERERERERERES2Xfv+Xbv50QUWrUdFRTtuKaVg1sV1Silrn4cQ1r7+MnP3tYQGM1tfNVwcipOzEy3hY9g3v5deG8h3l7n4iw9Y+vnll/8AiOw0Zxd88eQBVuvCOBWyQQ4b03bWVvzPPAbf+H15disrK8zPzzMYDKiqCoLhFomxwlpII+f2p5dZ+fUNjYUim6Tt3gEREREREREREREREYDhxZv4yePMH1kk4zgFmwT7Syn4zGr5adD8STavst8uXxQInO5fiLFLapis/jczcCeYUaUepXXKKJMsUVZbxlfuceffva9gl8hzcvzd1/B9NeM+5Fwmb+LuLff4+/kL3orm0594bvu525lDDOClZa7XI+dMW6CqKtrBmAVqBjfusfpfLuogizyBKgCIiIiIiIiIiIiIyM5wrbFLv/6Y9t4qvRwJ2dZWyQNUMW4ol/+ymyYxhBAIIUBxPBesLVguhGzMlYre0Mg3Hyn4L/IcHf6zc77n1BFGIdOUZu3rmwP/m0v9F+s22Vr9ukc7bqjrmmacMYuQocqBepC59/Hl7d5FkR1LCQAiIiIiIiIiIiIismOUjx/awws3qMeQCl0p/KZdSwKwyer/GCNuPHHbKQIbJ+E372eT13+vgJEsEBw8F7zJ9EogrbQMPr7GzX/z6x30m4nsLv0fHPfDb51lhTGhcto83hD5n5b970r+B5hsTtjwnp5W99gdKUrbK5eWOkUGgwExRmqLxKawJ9fc++Q6fPZIY6LIF1ALABERERERERERERHZUZYu32b/of30Ti5iKdJitF7WSv+/bBUAAhvbEkyTFEIIGJP2Bg4VgV6qAKiayNL1+9z93QW4vKxAl8jz8tqcH3rzNCtVZuAt0Y1eSljpxpmv01JkLfCvd+rWyQVCl0KVYsRHhQUqHly8wcOffa4jLfIllAAgIiIiIiIiIiIiIjuKXx/YrflLvnfuLOHIPCklctuV5Dbr4j6llC9ssT0Nwj3es3tniTFCcZi0OXAzcs6MVwasPhhx/99+pCCXyPN0ou9733kd39vnURkS+4l2NKDf79NMEgDWqnh499Hj5f7DZMwpL83Y8zIIIVBKIVYV3hqpKaw+eMi9//R7jYsiX0EJACIiIiIiX0F9s0REREREXrzhpw/MDs35QjpGtXeuW2JrRohGhEmEzR5bcTst072d3Dbux5P2ESCXPCn9n0gGKRvjRys8uHSd8vM7CnKJPGf73jjNvlNHWEqF0OvTNkNSjIzHYyx8vRDaF72/pbN5TqWw8Zh90fHKOZNSRTNsWLA5FmLio48+fF67KbKrKAEA2H9svy+8doxlxlhwSmmJwdayi4C10ShPRqq4VvPl1Z0OzgZNLCQivYcNd357RRekIiIisius9e6zmas9c8y7CUYREREREXkxBj+7bscOHvO2F4i9HtGMJo+IAVrPYJFi3VwlGLFAJBAdgkMO5WuV7/4iYdLJe7ri1zfdD5hvnB/2tecBBl6cGCOFTG5bql7FaDQipQQFYgiUphDMqNvE8uXb3P3wAlwd6MZD5Dk78MPXfe/po4xrY+QNya1rOTJqqUI3tmyMVD95NNkcwDb1AQDArRsxrRhhcoyK0Y2NBm4GpZAsEG3aCsUpwTAz3CrakbPH5uk3cOXXH8FH93VwRb4GJQAAC4f3c+CNk8z1ChaN4i2WW9ydMOkvgoeZC0moJuN8sWk2Z8CtvFKP2QptMtLIiTceMbzzwJdvqBeViIiIiIiIiIiIbJ0L//aXdv5f/MRDCgzKGKtsLbhO6CZqgwe6sPs0Ah+A0j1+QdDu69hc6vuLVqo+tgJ48th4IVqi5EIIAS+FXl1jZmQv0DoLsU81Nh5evMn99y/CzeGunWPtHVv0+V6f+5dU3UC22bHaF08cwhZr2lQgd+NHtyg04CHyNNlDWv3f2ZAM9UVKwcwouQv8AzCJybkbySN1jMTVzKUPP2f0y+saN0S+JiUAACuWmUswSE62glOIAbqhaVqjySm2fsGXvKyVKckBQnFKKK/UI0CKgUnLre6CVURERERERERERGSLXfi73/HGH32bvFixmkeE4vSqHiNvAIgz5fbNJ/O2z/j/3LDwl66iQNy0MKxMen7P9v027/bHDayuGOaWUAoLc/OsrKwQUoQYsWLMU8H9AXc+vc7yL67u6uDWwR+e94VDe/Gm5f6lO9u9O/KKe+1H38UOLTAOhTxuqIIRslOKYyEQQiAXxTyezaSy9tpC2vWxMhRwhxgDFpycc5ckFQPFCxTHRoWeJUb3VxjduL2Nv4fIy0cJAID3K0ovMbKW1goWCpV1GUZgkxQA6y7qAKxQJpkA3YVeIccuWeBVejSH3LZEN2rrSrKIiIiIiIiIiIiIbLX8yX1rzjzwev4gA4f+nj7Lo9UuUEchbYjTlZmV+88WwNsc2DfvQlrmk3axT5gSDXTJAgVo25a5uTkGyyssrwzo1f3u3x0VFmOPlev3ufXBZ/DZyq6eXH39f/qRp/3zDGiwXDj+52/5jf/68a7+nWXnev1//pHnvTVNckZ5DFZIFidt/wLRWG8PLU9lNtg/TciafcMHoLh3yVJOV407BNwddycVY381R3N3hWuffA5XG40XIt+AEgCAXBqyt/g0TTQYTcnk0hIsTp4V1usBTAckCsWZjGTllXs0IMRESBFLca00i4iIiIiIiIiIiMhWu/Affmvf+d/+wsdVYjgcQwgYEEvopnVnekx76Fbmh2eO4XUxpzD59zcEsL6i0kBwSA4ry8tUdU1KNW3TYqstB+lh94fc+te/2fVBrbP/9A+dfT1We5lhFalKZP7UIfa+99AffXBr1//+srMc+8t3vN1TMZqH1lpKgcpCF5AuTgyGOZS2xWL8yn9Pvtg0CeCxFikz1VpynoygwXB3yiT2VBOwpRF3P7sKHz3UOCHyDSkBALqV65OS9sEMrEwG+TAT1A7dQDX9zAuBMEkLsMmV36v32LaZthjklia3T/cCiIiIiIiIiIiIiHwNv/1//Sd763/7ibcVlBAIJWPua8H/zaZlp5/W7M+WmX/LZ9rFzj7HZpIQAHpVTRmOoRjj1THzRBbjHM31B1z4t7/c1UGt3ruH/PR33mTYM/JCYLkMCXWPwThTzXdJAI8+uLXduymvktPzvnjiECtzxqqPCQYhOMUDlEL2QiwBzIjWxYZUB2BrFCBsGo9TSrRtSwmGAV4KMQQSRjWGa59cZPmXV3b1OCnyvCgBAEilkEoh5kz2gpeCOSSMUsokO8nxSawfJoOUgz/L1eMukFIieCLGCFEVAEREREREREREROT5uvyr33P4j9+mpK5c9DREVybzuHkyTdmV7H/62FE39bseBAxAG8va5913vVs5vP5T3ceT/21pGuZSTRkX5kOP8mDAJ3/zC7i4uquDWnt/dNqPvf06K4ypD8xzb/kRvT0LLI8GRAs08z36pw6x549O+9LfKsAnL8bbf/IDyt4+S8N7pD19cmnJdM3os4FhuEH0QLBA+4X1PeTrCB4odG20NzDvVtsGyMGxFLsK3A30SIRxZnD7IctXb2/DXovsDkoAYL2XS4yxy9wsGXfw8PWuO8qrennikEsmFZtUQhARERERERERERF5voa/u23NuRMeDi3gVaCE6RxtIZvhOFs1ZTvbVmCaWDBbVaBsfh6zzwlURMLYmWsD+cEjLv3Nb+DG7u5lvfdPz/qxt15jXDmkyP3VB4RexbAZUgfDvTAsY0oyjn3rHO1o7IPfqhWAPF/n/9kfezMfWB6vML9nnqE3VMHIBMAxM6IlSpvJOXeLHmVLTDpvb1AMihcyTjTDimG5oeeBsjxk5eoduD7SuCDylJQAwKSsfzByKZSY8RTIuUB03K0r8r+pV0mha/i0dZeSLyEDzLoWCjzex0VERERERERERETkebj5dx9x9AfvEE/sZYx3rQAikAJeCpRCnK7G30Ju66WsAXJxzCGmiqZpACOlREqJ8cqIqkBcbli6dpc7n16Bm7s3+B/e2uf7XzvOwolDDHsw9Ab3QkqhC/KtVWQw2gBNMpYN9r5xgsHtB87N8a49NrK9Dvz5eff9fUYp48kozZgUvEvUsUKxLgnAyyQdIAZaLXrcEsFnK6SsR9QcaN0Jdc24yfRDomc17cMBSxduMviVkoJEnoUSAFjv11RsNkOzYBa6SgA287zJ9w1e5dD/RADv0l51KhQREREREREREZEX5urQHuy95of3z5PmIx6McTumigHPheBOSJFSnn3mcnMF2NlGqCklSik0uaWqepAL7bDFc2Zf3ae5/ZCbn11l+Lvbu3o6uffdo370nddIBxZYLiOaMqTqV3jbzajbpInC9NgF7+bgx8mIe3ocePss929+sm37L7tX71tHff7Yfoax0ISMRSOWQPE8KU8/fWZYr+yxq9+tL14AykyLbeiOewwRJ9CvEwxaem1k+dYDHv5cbUFEnpUSADZx+2aB/dk+UK8iBf5FRERERERERERkO4w/uG1Lh/f6wrlj5H7sAvBNy0LdYzAa0HrhWZdx5bWp34L5eun/abXY3DpmgVK6XuE+dA7O7cFXxzQ3lrj9uwuMP3+wq4NZB/70nB944yTjucgSY2w+EUpgdbhKPyWMrhf4rOSFAliKsBjZ//YpVlZWfPyL67v6WMkLdnbBD71xgvrAIss+6to+u68t8HQDPBAmwf84Wa0+XTAaZ97z8mwCgHfHdXpI27YleMBCRb8kbKnh7mfXt3EvRXaPVzdyPWN2AH/Sx7OPgW7rLlrYkLH4Kj6KiIiIiIiIiIiIbJelz68xvH6f2EJyI7QFawspJdqcn+nf9kmgatoiFmbmhyeBwSolPENtFVWJzJdIvrfKwwu3uPLLD3d18H/xvWN+7B99yxfOHqVdqBhay6CMGI5GtN4Sq/X1h2srqydb7Drw0jQjSoL7ZcCp75yH41Ezz7I1jvf88DtnqI7sZWgtJTohQM6ZUgo+qXBs3gX/Z9+oqgDwIhiVJXpWkVYz841x4dcfwuUVHX2RLaAKAMwE8x3cA+aFMv188pyNfUq6CxSAbF2pommpmFfrMagEgIiIiIiIiIiIiGyfKwN7UF31E/sWsIWKuarH8uoS1f4FjLIheP9UbDIR7OuBf7f11azWFlJTWOzN4Ssj9to8v/2bn8Fnj3Z1EGv+D0/6wXOniPvnWSkjSl7FqkQvJprc4tmo6xrGDXjAzSYrq8NkcVkhFqiiMRwO6S/O8Wi54dQff5er/+qX2/vLya6wePIwi6eOMKoyo2ZMrCMApRRiVZF9fWCYpp2YQ7BJPGhXv4NfjOkxDDOfd4faCAVqN1LrLHjN57/8AD55qKMuskVUAWATW/vv44dm+pXZC8ZX+wCWtQtgQ6VwREREREREREREZBt8/sgeXrjBIomIUdU1w9GYGOMz/bPTVf5hZmWY2+z3AzEbC1Zjy2P2es1v/x//znZ78H/xT17zY996g7y/z6MwxPtAZeTSQCnMVTVVTIyHIwpGDt1CuvKEyXRzmJ/rM2jG+Fykd+IAx/7xtzXTLM+kfuewH37tBOPKWfUxVEYIgZwzIQQIgYJvaOsB64sfDZX/30rTyh+zhzM6+DBzoF5k9fodmt/c3tXjpsiLpgoAdIPOtKeLz3yOBXySBTa7+r8r/WSTwd/X0gXKq/boGo9FRERERERERERk+63+zRUbHNjrHFskHezTrCwR0rMv34ozq4RnV6/GSQXZ1BTmSCzduseF//O3u3vC9MxeP/b9N6kPL/CQBqsDpTi5ZIKDmWFutOMGCFQhTObajTJZShe9m2Eu0yoKuVCaMREYNUNWYmT+xEF6bx/10Ue3dvfxlOfjRO2vv/c2vq9miQElQC8mSi4UnJASeS2qY2vtjsuk2rM5RC9r1T7Kl/2/5EttDiFNP7fJ+NknUh6scOXfva/3usgWe7UXsD9BsYJPR3ZgQ+eXTele3VPWTxCv3GP3oCw4kV3i9Dvn9G4WEXkSm+Spuz1+9yoiIiIiIjvGtf/zffPVhryamavmKKWb6njSpfz0a0/6XljbnFi6+c+1xWPWBQ6tJKo2seg9Pvn5b7m+y4P/vXeP+bt/74/oHdnLcmhhoWJYxmRzMMOD4e64QYyRKkSiPWH94aSirE8W5KVeTc5OnSr6/T7L4yFNbZz6zhsv+DeU3eL8n/wB1eF5lsoIj2AG2Z3sBYsRi4GmaTDbGPyfpZjH17N5HH18ymS69n+qSxKKJVDlwHyOvP8f/uuL22GRV4gqANCVagru3cWHOWtBf+8GIygEHLyQrQt7F2J3crBCYVdf2301Uw6cyMuud3yvH3rvLA/S2Jd/d/UVH9RERGC97lHpVqsYypwVEREREXkJDK/e5/DiHh6VjFcVpMKoHXetAWKiFKeUAsRuxboZpRSMQrRAwCltBiBNKsQ6EEKkmOEZYklULVSrhd//7Ffw+dKunkuxHx7xI2+eYTjfMiJjVii5IYXJXZJPKiOY0c2mO07uvmjdlHucBAGngdVC12u9LQWLidxk3DJ1FRnR0OxxzvzzH/rl//1vdvWxla1V/dExzwd63CzLtP2u3nMgdO95M3AnNy0pGublCwP/00D2qx75cOsqYOO2YU6kMK2UHTYcwzDznodCSpHxeEyKkbbJEIxe7EGTmWsTH/70V3DzVQ+wiTwfSgBgdjJ3Pftw/TuTok7TogBWwGd/IqDTgIi87MbeMqidhbNHyKOhDz65qwsvERHYmBwKzF4fioiIiIjIznP/ry9YtTDnvZOHKVYY4cQYiXSBfs9OiAECNLnFi5ECBAsYXYAwxthFp82w4kQgZ6c0mYqaKjur1x5w/bOruz74f+ifvOvhQB9fqBh5QymZEANm0KVNdKv+18ql+/p8+2y57ycpAJMWCzYJzmKFbEbTK3gx9v/oNX/ws4u7+hjLFjnd99fefYsHPsLnE21uSTz57++rVvgr4rNu86zI1zFNBGibBguOu9Pr9WiaTMzGos3x8OoN+HR3j58i20kJACIiAkAToX/iAGbOYDhwrqzqAkxEREREREREXjq3/v0H9vr/+mOfq2vGAASiG1a6vt5lskUDS7HrBJ6dkttJGXvrtuJUMdEMhvSt5mhvL+P7K9z85BLDv7m+q+dN4luH/dwP36Odc8apJedMO1kxbe4Ud9qSCSk+9rPT1f3flJlt+NgqY++Zozy4dc+5oEChfInTc/72T/6QFVr6dY+VdkyKKJL/zNbfdpsPpU0qZEff+LWw8ceIMZILZC9UJOLYWbl1n9ufXH6O+y0iqmQqIiJYhCY5w8pJR/Zw6L3XCWcWtcRVRERERERERF5KF/72N/TG0C8JywVvWpIb0QLWZkpuSSkRDUrJeM6YGSklUl1BCLhFmiYzF+dZoM/oxgPuvP/5rg/+9799zE989018T81KGdG0LWVSGSHG9dYJKSXMt75furt3yQApkOcTx99+DY7Pa55KvtDZH3+P4WJFmEusDJZJaVL2X54rc4gzW5j5OkAIAS+GO4yHDX1PsDTi+oef45eWd/U4KrLdVAFAREQo5pQKBjTk2lg4c4gqBW6Uj5yrY12MiYiIiIiIiMjL5eKq3Tp+yeffPkE1F4mAe1kLXgd3rGSyF7wUigViiLgZbVtwdyKRxTSPPxry4PpNHnx+FT7fxUGr0/N+8N3XmD9+gDbAuB0S6kQmd/3TzSheKLngkwSAXPIT/6mnDb26r8f5swE9Y+HEAebv7Gf1xupT/quym+3/szd9vBBpaHAa5vbO83BlmbpfqwLAM5oG8t2e/PWw9vH0fWsbnu/utDmTQmIu9AgrDQ+v3sE/e7h7x1GRHUIVAEREBEp3A+zBGXtDW0F9bD/Hvvf2du+ZiIiIiIiIiMhTefjXF231+j3qodMLNZ67MtSWIiFAaVoCRggBC92K4TzO0Dj9HNnrPdJSy80PL/Hg3//ednPwv3rzoJ/+wbvMnTnMcC7g8xX0AiUYbtCWTFvyWoDe3Wnbdsv3Y9oGwN3JZEqCYcwcPHeS+N5BVQGQDfrfO+GLrx2l1IHGMiRj0I6o+7UqADwHbuvbrODdNn1OYZJ7YYFokb4n9lqflWt3efDTC7t2HBXZSZQAICIiYAVzJ+FEy4zKkEFqqE8e5OD/9F3dXImIiIiIiIjIS+nhp1dp7qwQWoBADlCirVUCsOJEC5OglVPFmj3VPHvaiur+iKt/+xH5b6/u6oBV7/vH/MD3X6c9Nk+podCwmlcZe8PYG7I7bgYhEFIipEQVI9GMwMYgw2yAcHOQ8Iu4+1piwfR1mSreMqIhHFjg4PlTcFotK6XTf+eIH3j7DM18JFSBfopkL4zahpQSuP5UnlXwQPDwhDYf094f023968UcN6dMBoY6VFRDY/XKbe5+cuWF7LeIKAFAREQAQoTS4rklBIMAbXSWbUjvxAGO/LMf6IpZRERERERERF4+n69Yvr2ErTZUoSJYos2FpjihSpRScDfMjVgCvRKpVzOPLt7k85/9hvaDm7s6+H/2v/+eH3r7LOHAAuPKWS0jPDhVFXHrKkYSDIuBQrfqv23bL1xd/Xig8KtNEwDWkjI2JQHEOvIoD1g4fpAj75x52l9VdpPT877/7dP4/j7j6DTtiNFwlRiMxfkFBqvLxBi3ey9faoGuoP8XDYCzb/Vi3bZW+n/6Q41TNUZYHnPjo4v4lZVdPZ6K7CRKABARkS7b3Y1ohk0SOFMwiLBqLf2TBzn0P35fSQAiIiIiIiIi8tK59/OLduvzq9gokzxiFruAlRvERLJAzMac1fQb49bvL3Lnr35vfmn3lvznWOVv/vM/c07so93XowkZ94xFKMEpJWPTcv+T6gjQrdAPIWwI0MPGQMM3TQIIIRBCwN0nCRm+9m9GM9q2xROs0jB/5gi8s1dzVK+43utHsaN7GMUWDy2YUVUVITt5NKaqemt/R/LsAkzmjLsV/2uB/kmLEIKRA7TFKQ4hRKJHqhzojQP+YED+9NHuHU9FdiAlAIiICABu66eEQMFzIecMvcDDMqR3bB+n/tkf6spZRERERERERF46K399yVZvP6QuEWshhZpSnOBGGRV6baA3dG79/iLLP7u0qwNV4cyiv/sP/pTRfGC1dsaxUKzgVvBJ9P7LVv5+4b+75XsKOWfMjFRHRtbyyEec//EPnsP/SV4WvXcP+cHzpxik7m8WClhXkSI4xLLej16eHzdo2haPgWxQSiHGSAqR0mbC2NnjPRbayMV//9tdPaaK7ERKABARkbW+bIXuAtm8ewwhkL2FysiVM3/8APt/cl6X0CIiIiIiIiLy0lm9fg8ejZnPFVWpsDaQSsW89+DBiM9/+mse/fTCrg5UHfvLd/z8P/hDHvZa2jkDCkZZmxuazglttnlV/yTs+oWfTytMPit36PV6jIcjql6N92BcF85pkcoryd7Y42f++FuUuYRbS/f3u953fpq6Ym4E39Vv5RdurSXAzDtvuvq/AI4RiYRiVK0xT0U9KPzm//1XeiFEtoESAEREBCxMbtLC5EKuK+WWqoh7huA0tNxdecjx917n4N9/UzdZIiIiIiIiIvJSGf76ut346CLVoDDXGPO5ojcOLF27w7Wf/hI+XtrVgaq3/skPfeHsUQZzgVHtrJbR+grqyczQhgMwCaBuRSD/adV1zfLqCr1ej7YdY+astkP6Jw5Sf++w5qdeJcd6fuy75xkvVrTWULzF2PgnUHb1O/jF6oL6bDjCwTcmAcS6oikZdydawkshtkbPE70xfPCff/7id1xEACUAiIgIgIGbAWHt5q71Qtu2pBDJwzFVClTzift5mcXXj3L4H76tmywREREREREReamMf3nNmhtLzA8C86vG0ue3uP9vPzCu7+LQ4emen/5f/sjbk4s8sDEDH5NSwMzJoSv7bzjBfb1M5Ezwf3abmq0mObvyP2zankUBmrYQUiJ7wUvGvFDN19wa3uf0D96levOg5qdeEXvPnSAd2cud4SOqaNTYpFpFWata4QbZAk4AV/jrWeXQbVPmRiy2lgRgBEp2giUiRmyNOSqqQeHexRtwbbh7x1WRHS5t9w6IiMgO4GHtJi6HQPCu9FspmWRGqCLj8ZCF/hyPBiuQeuw5dYT2L4o/+Ogi3Gh0MSciIiIiIiIiL4W7n12BQcOjlSUe/eby7p3TODbvh147zuLZwzR7a+62K/T2zDFuR6RUkdtCnITppwHUwjT2333dvKwF8svmf/8FGHtmYWGBBw/usTA/B7kwGg2J8xW5BF7//jt83LzvXHy4e19HYfEPT/mRt89yO4+o5msGoxG9GMnerrUzdQM8TKoAzAb/t+Mv9+Xntr76vwBxpj2IO2SDnDMAwbq2C1WBmDMPb93n7n/+WO9JkW2kBAAREYHSXcA50IZpRreRQkUZt9R1zaCMWG2G9Ob7jAdjQggcf+M1zOH+jU+2+RcQEREREREREfl6Bhfv2pWLd7d7N56r+uQBP/TaCeLr+xktRFJd0SswGK1S9RLLw1WqKmHF1wKoAMEgE8gWgEKaWV/ffWWjaZv1zW0C1gKyz8AtUNUVyysDFucXKG0DFCx2//5KM8AX+8ydPsTg4sNn+5/JjpXO7fMz33uX+3FEr6ooZBoK2Z0n/YnNBq4396yXb2ZaF2Ua+LeZRzMwd1IIUJzoRmydh7fucfvzq9uzwyKyRjVQRESE4GXtAs6BMjk7pNB9sLKywsKePXgwhqMRoa7IFTxiyKHXT3D4J2/qUlpEREREREREZAew1/f7wbdOse/t4zTzgVEqLA2XadsxdZ26VbvxSaHT2a+tl1V/Gs8a/J9q25YYI1aM0haqlKjrmuF4iPUqhpVz6I1T1N8+qbmpXer8D7/FIxsxDhn3TG66vwkP1pX5Xyv1bxTTav+tVzZ0BvG1pAAjWiAQ8dZJHkmjwqMrt+GzZa3+F9lmqgAgIiLM3iEZpavjZNCUFjOjrmua0YiAkULXd836gcGwIYfI4utH8dz63f92QRd3IiIiIiIiIiLb5MCPz/r8mSM0eypu9Qa4dyUfQ9Wt3/dcCGYE4lq9/0K38h+6L5kX4sy/+aSQ6hetqt7q8KuZddUEvFDHhDdOtkzq1TS0hDrRjjPH3jvD9abx9qPbmpvaRY78k2/7vbkxod/HMNrcYNYt8c+lYLa+xnX6t2de1nJZzJUQ8LTMoReMcVsowQgxkCOE7BgB3Ikl4G7gkVQi9y/fZvT+Hb0HRXYAVQAQEZEuc9McrKsE8MQc8A0ZtTAYDbDaaGNhXDn7Xz/BgR+eVba1iIiIiIiIiMg2ePuf/pHvP38K9tc0fWfIGLdCFxpdD4Sar29TG5/x+Pe3g/mT5qm6zxxwc7K3UBlxT5/Fkwe2YS/leTn2Z2972D9HOxdpyZTSdgktdIkhwSKP/3V3ur8dBf+fledMCpEUAqUU2rZlVFraknECISTKKLPoFbc+u8K9n36q4L/IDqEEABEReSp1TNBmAEoK5IWaA2+dZv+PzikJQERERERERETkRTle+1v/4s/94Z7Act9pkhFGLXM5EF/yWZppNNHp+pGXSQny6VqWgBGT0Vph/7HDLPxAi1N2g4Vvn/Z9504R5/uEEHB33L2rCGHdX8X0UZ6fprREg1QgZidZ6NovxECJxqAZs1j3GV+/x+pn17Z7d0VkhhIARETkqcQYKcUJIVDILOUhZU+PA2+cZPEPz+hmS0RERERERETkOTv0w3P+zl/+mOFCIC/28H7F0mCVuV6f5LsnQFoM8mQrBmFSGcBzwcwY5gbmEifePEv15mHNS73kDr95itXUkHFyzpTSvc4hdCGtaUKAPF9mRikF2kx0SCESYureh+7MxR6sjFm5cgcur+yeAUdkF1ACgIiIPJXRuCFWFQCtF0IvMggty/3CgTdPkN7ar6twEREREREREZHn4XjP9//kvO/79hlW9keWQ4snaNsxB/ftZ2llGaq43Xv5zMJkdsltfdvwvVJwwHqRJjq2WLN44uB27KpskTP/6Ds+XjBGPaNhY9l/UPD/RapjwrwL9nfJAE5uWrJ37WLrUWZ4/S6PfnNVwX+RHUYJACIi8lRCirQlU9wJMdJ6YZhH5ArKnh7H3ztHenOfrsZFRERERERERLZQODnvB999jUPvnOVhlbnXrhAXakbtiFIKKysr9PcsMByPHuuN/vJZ7/DeBf83TjXFGBnnMZ5gTMtqGbHv9CEW/ljVKV9GB//4NZ8/dYjcC+RQIHYl580Md6eU8lgrAHl+3J2CYyniMZJzxorRo2K+JIZX73L7P3ykF0JkB0rbvQMiIvJySrG7sbRYiBYo4zEhBEJVMWob5o8tsmd8gkfunj99pAtBEREREREREZFntPd7J/zQuZOU/XM8YkiYS9AYK8MV5vs13mRyzrgZFl/+CgBlZkbJJmX/N4gGGE1uiSEwTpm5/jxHz5/m8qNlb39/X3NSL4n+9074kW+f4yFDQh0opSVOXl+gK0XPxkoA8nxNky6s6kKJLUY/1tRtwO8sc+e3F7Z3B0XkC6kCgIiIPJXRcEi/38cNxm1Db26OqqpoRgPcCqvWsPfsUQ69dQbOzumKXERERERERETkGez70Tk/8q1zcHiRYVVok7M6XKFXJeo6MRqNyF6IMTIYDIi7IAFgWvbfvGDelf0P3iUGFIOmbal6FdlbLBkeAw/HK/hi4sDrJ7Z79+XrOtXzhdMHeRAa2p4xbIbUKeA5r636B7rFRyGs96aX5ysGCkbBKRgQqElUj8asXrwF14dKsBHZoVQBQEREvjFz6IVEHjeUAF5FRnlMcKhiJBQo0RnkMfXpQxwIzn0uOZeWdVEoIiIiIiIiIvIN2Bv7/NAbp+gf2cvynPH/Z+9Pv+Q4szy/83ufx8zcIwI7SAAkQBDcl2QuVVlZWZVVXS21ZlqaPqNWjzSjo3Nm+ePmxRyNzukz6pE0o5FGy3RVV5cqt8qqTDK5kyAAAgSxIyLc3ey5d16Ye4RHAMwkiQA8lt8nz5MBRDgIg5kv9jz3Pvd2ucWtL43f5AxtCwY59/v9PJymqmCPB0jD+oL/s8WkBFjYRisAN4gqMe5acs79jnFLWG2sR8vymRMc+5MLcftvPtF61C53+MVzLD17knupxWlZHjS06yOsqTeaPszaAMwnA8jj07/+jMhGccCDYT0k32tZ/fAat355Sa8rkV1MCQAiIvKt5GnZNQcKfda1bezzDyKgpGA9OpbOPkUUuN1+FMoMFRERERERERH5ek7+8YuRT6xQnzxEWapoK6fgYA7uJIwUDqTNQOkiD/gxCCDDRgUApw9O+rYEgdlji4GnvmrAiReepZuUuP+Lz/bbadk3Tv345RieP8WaOVFBBLTdmMGwZrK3c1j2PCcgVSQyFYm02rL2+S2u//xjvZ5EdjmlSImIyLfkpHCSQzW9GS8Juuy02YnwvidXhpFPOHzuaU5876XFHrKIiIiIiIiIyB5x/E9fiMOvPEN99hisNP2u/9Jh9AsxncW0FH4iRb8+U7mRfdozfYHHvlPcpskO9MH/zc0n094A068b3zfHzQnr16cmA+PUK+cWcOTydTSvHo/DL5wiH1li0o2pCAapoiuFNvbDM3hvm4w7MkYUpy6wshpc/83Hiz4sEfkalAAgIiLfSkl9pnWOfqTpRKtMM7CtypTSUdoxVZNZ68YMnz7Ms//b78fv/i+LiIiIiIiIiBxgp1M88+99J448f5r1obFWFdZjRBstQZkGu4M83fs+v9CS9tmqy/YARmzbd2zx1UGOkqAMEmu58Px/+Af77MzsD2feeIF2mFjrRn2Zh66QPEh1ReuxL5JY9ioLqHJmWA9JY2c4gavvfAxXVd1VZC9QAoCIiHxjYX2gvxikCLIHlUN2CBLFEh5BIRjUDT5pqWqojyxRjtY8+7/TpEtEREREREREZLvmrafjwp/9kOr0YbqVzDi3eOrwHETu+55nM3IkciSSGxaGYxQzStoMku/1xX+LzQFMqx30/z6LzU0pWx4z92fBGdGSDtekww2Hf3RW61G7yIv//IeRn1phLU/orFDXGdxp25aUKnKlDtaLlCKxlAZ0d9Y5npf49O9+y+rbVxX8F9kj9vo9gIiILEBMJ1s+rQIA08kW04kXfY+oqqqYTCb9DXwK7o/uUh9bJp1Y5sI//yNNukREREREREREpgbfPx0Xvv86HFtispxYTx1UgRmYsRHljuJYBInNBf7ZWk2x/VH6f2YW3Hc2N6M4/b97e/B/Xpr+2Y6OtTJiUgenX3wOzmStR+0Cr/yHP4zm5GHWafHaKBRKKeS6IqVEV8qiD/HAM4BJxyFruPPx5/jfX1fwX2QPUQKAiIh8S4Fb0CVoc/+dukBdjGo2G8sZazL3x2u4BdWg5ubqHVorVEdWeOM/+UeadImIiIiIiIjIgXf8T16KF7//HdZy4a6P6AZGqQKiEN71IwoRTliAT5dUYpYGsJkOMF+5ca/LEaRpk4NZkkP/m4TNqiBEeuDns9YAOSeoEjGs+ySA77zy5P8RssXSm09HPr7CrbJGRwcWBI5HwVOCqiICvNtPqSx7U5My5e4a9y5eXfShiMg3pAQAERH5xmaZ1Smmk6u5n6VZCbacGY1GBLBy5DBt6RhNxqwcXqFkox0EoyY4+x+oHYCIiIiIiIiIHEx2/lC8/M//OJ569Sx3bUQ7MMrAaKOjTPfyJwwzI6V+d3RKCdLW6P78LvjYB4F/2Bq8mA/uf91/nwXgQUQwigllmDj87FOc+YvXtBa1QGdeucBdn2BLFdSZiL6KaF3XdF1H13XknPvnueyYh71uZq+r+bYhs1EVyPc6vnzvIpMP7+yTdxWRg0NNVERE5FuxubtGi83s8pkoTlPV4DAeTbBcYUA36QBYrYChs/LMIZ76p9+JL3/1HlxrdTMpIiIiIiIiIgfDy4fjlT/5Puu1M84dk8oJ64P+FkEVhrG5VBIODxRGt81d0vNJAA8ri7/XOJDMcDb/PbOvbr6RIDA7Aw9LgrASWDZagsgFBsDhzNKFQ7H+yX2tQz1Bdm4lzr71Mhwd4nVh4oVIQZjhpb94CSOmyS1OoAv0aGavkfKQ6hjFoCOwnPrveUAxqjBKKdRjuPGbi6z/+gtdBpE9SClUIiLyrdi2Pmu+bcwe8zBhMLGOGGYmdbD89BFOvv7C4z9oEREREREREZFFO93Eyh+fjVf+5PusNh3rtTOpOoo5bgFEv+YCD27R/Rr2Q/B/Zr4I/PZ/l2/7+cN+llIiWSbVmWJOa4Wlp45w7MIzpNPNPjpTu9+x507RnDxE2xhepX73/0Oe1vvp+bubbF/HBYjov1FKISLIGBXGslWUO2vc+/UVBf9F9iglAIiIyBNn0VcIqCzh7kSTOf7cMxz781d0iy8iIiIiIiIi+9erx+PMH7zOM2++xGpd6BKAkwMq70eKaeXF/VLLf4GcROeFjJECSunIyzVLZ09y6Pkziz68A6N65UQcfvokpc6MupZJFNqu2/IUn5Wef9jv5NtxEk7q27UGJOZ3dAWDnGnXRywNBiQy4UYqRtwZceW3Hy768EXkEegdVEREFqJKmbYb4xRG0bJeO8dffJYT6sMmIiIiIiIiIvtQeuOpeO57rzJ85ji3Y0TbQEmzkv99gC5PEwBkZ4SBu0MX/UYUg3vtmNEgcfjc09j5Qzrbj1n1yrE4/eI5qqNLtMkpVWCV4fbw+g0JVQHYKfNl/7NPy//P2mkARDDINT5x6lQT45a6JC69/RF8uKoMJJE9TAkAIiKyEFVKdF1H5IQtNaymCeNBcOi5kxz7yYu6zRcRERERERGRfePQH5+P5777EuVQxd00JpYbSraN3twpghxBCut3/6MCAI8qDDAj54xFgAfWVIzMWU8d6fgKz7z54qIPc387XcWJF59l6cxxJjWMciFqoySwnKal6GMj6D8f+FcSwM54+PvI9Mx3haW6wYpTVkccGx7h2vufEe/c0buPyB6nBAAREVkId6euayIZLR0lB5NU8KWKY8+f5vifvqDbfBERERERERHZ255fjmf/g7fimdefp1vKeGOkQWZSJrRlsiU4t30hRAHQnZFzTRTv+5wnw4YVnoNJblk6c4yn/uJVnenHZPj8GaqTh1mvnFUmeA46K0zKpO8//xVPclXB2DkW00oY8+0WppUAzBJlUqg9c2JwmHJjldV/+6mC/yL7gBIARERkIcINyzUlOtoyIWWICsa5ZTyEYy8/i333Kd3ui4iIiIiIiMje9MKhePnP/wA7fYjVqtDVTtuNKe2YyqAywwjcgjZDSf3oKwKoB/qjsujLnHtXIBmeDXcnIsg5MYmWu2nC4XNPLfpQ96fnj8bJ86eJ5YrWCtRG5L4NQ84Zs2kQetHHuY/N8iscNqqN9Pr3FjPDzIi1lnq98PH/8qtFHKaIPAa6gxARkYVIdUXbtjjBYDAgpUTXTegodA2sVoUXf/gmvHpcSQAiIiIiIiIisqdUb56MF3/8FnerMe2KUQZGZ4WqSlQpkyPI09Bn0O/O7VIfpJt1Rtcu6EeXMLquw3JmMFwiiuPtpA96JrBBpm2CN/8PP9HZ3klnluO1H32PfGhAsUJJjmVo2zHRFTIQ7l9Z5cKVFbAjZu8hYVurAPRfDSNTlcTxapm3/+qncGWkMy+yTygBQEREFqKUguWEWT8RoxSSARZ9EkAVtA0899YrNK+pEoCIiIiIiIiI7A3P/LPvxgs/fovRCrTDxDg6Jt5iBhFOcicFRATBdOe/GY5tBOqMfmgB/xEVp8kVJZzJZEJtiYaK4i2Y4dHRJqddyrz4z/5Q60875OU/+T6jodMlcPN+uJNzpsqJcKey1D/H5zoBxDQBJswJ89/1V8jXkOpEKS0pJSICLwEpUzwIy1gHh6olrn90CT5ZU/BfZB/R/YOIiCzErOSUbWv3FUBYkOrEWjchHxlw6pXz1C+c0CRMRERERERERHav03V87//0T6J5+ij384TV1OGzLbgWG2shsx3/Pg32zxY85tdKZOfN1qAsgOhDI13XkuvE/dQSRxqql4/q7D+i5T84F+Oh0Q0SXfa5svOQCFJA9v46zFe52Hh9bPsq315bOnJdEcWJ4tPWCxlIUGDJGm5+coU7n15b9KGKyA5TAoCIiCxUjn7AdMJrPu3P1pGaRNtA9dQKT3/neXjhiCZhIiIiIiIiIrIrvflHP2RMR6mCSR1YnUgBlW8GOkvqR78zGnxjr/+DmyTk0c12n2+uP9lGhQWAqqr6IOlSZlQ7F/7gjQUe7T5wYSWeee08ZTkxygW3aaCf6Vc3LPqRo3/umyL9j0UYlCikZFhxaktUVvUtMSLRREV7Y5UvP7xCd+m+LoLIPqMEABERWZjfNamNcMyCdR+zljsGp45y6rXn4bkVTYVFREREREREZNe5desW7n2p87Z0RGzd7Qx90L9s68dtsblBYhYslZ0xX1Vhc7d5fwEMICc6c9a7CWmpxg8PePrfe0NrT9/SWz/5IeuVY8s1k2i3lPGfPa/nn+Pb1wZ97ntKhnl0s9L/AHXKEIF1MIiKalS4/uEl+PCOgv8i+5DuJUREZCG+8iZ+WoKtrmtGkzFUGa8S92Kd5WePc/b7L8P5oaYAIiIiIiIiIrKrfP7X71qMOpbSgCVqrED2fpdzmOEYbpu/JoxEH/hPvjUwOuuBri7oj6rPqtiowGAJJ5HoEwKKOyU5jtPRsWYtK8+eoHrrKa09fUNP/7uvx1rlxNC4P75H1WSc313KP2xzyM7q30+M8Nm7SIIOhqlhOE6sXrxB9/dXdeZF9iklAIiIyK4xu+MMoAuHbFiGkpwJHZM6yKcOcfT15+EZJQGIiIiIiIiIyO7ywa/epru3xnIa0IT1O/4TDwRCHxb4ny3W+3QUheZ2jBu4zc5ymm5AMUo4kRPVcMB6NyY1iVFMeObVC/D8Ia09fU2DPzgbK2eOs54L6z4m1xnvOoCNJJZvksyi3f87wAKiEFGwbP1aawSDkijX73Hrg88WfYQi8hgpAUBERBZqNqlNYaRpD7AwY+QdNqj70nldy6CpGPuE22nC8vlTnHr9FdK545oOiIiIiIiIiMju8eFdu/rhRbqbq1SdUSwxSQm3RJCwSCRPVCVReSJHPyz6oPQsUcCTdkbvhNnp61I/oN/5P2vFAJBzzdgnDJaXGK+vUQ9r8rFljr94diHHvNfYSyfi1EvnsCNLjFJHHjSEd9g05B/TqgvMDWf2va02vmOBoWW/R5GmbUVSgCejI0jFWPvyDl988BlcWdO7i8g+pgQAERFZqK+ayNZ1Tdu2lFJoqgozw1MQtTGunJMvPcNTL5+F06oEICIiIiIiIiK7x/jnV627scaw1KRIm33oAYs+9InNtkRst3XJXjuhH82s9H8YDw0nV1XN+v1VUqqYtC2pyngyRrmwdOooK6+d0RX4XU4P4+kLz5IPD7k1uY/VFY5TSiHnvPGwb5LMoqDVg77qnHzVeZ09PlnV/85hEBX1JLh7+QvaD75U8F9kn9N7qYiILEg/0Z1NZOenvRZgXVCTyZbo3CnR/zQHOC3XuM2hV09y4SffWcTBi4iIiIiIiIh8pZu/vczSek1NTWmdAYklDCuFLlraKLQ5aBN00+oAsJkskCJIodjzo3L6CKkF0x3pjtvmmpR3haXBErROIuHZGNPSWoGVilNvnSe9cFQX4isce/406diAdhCkpYoSHWGQ64pSCtAnvczG5grg9Fo88J3p+qDKXwDQvzPEA21CYPMUdV5wgpQSZoZF/3izxHrpsHrAwGuWu0y+O2H0iys6sSIHgBIARERk17HYHDPzyQEADBM3u3tMluG7/8e/0ERMRERERERERHaN8eXb9tHfvU1a7ThSL0NbCC80g4qUINcJcsKnQbxZOfTt6yGyMzbP69bKC9vXoJy+NYCb02XHDzUcOntyAUe8+x1+/el46oVnGBxboqWjLZM+Zu9BuJFStehD3Fce9r5gZuScMbM+4cL757YTuDvD4TLt2oSqhbizzif/9c8V/Bc5IJQAICIie44FxLijaRpiZcCdXHj9P/1zTY9FREREREREZNdYf/+a3f30GsM1J8IYdR2lC0rplzC6riOHk8IJc9yg9r76IfR96mWxIgdPPXeG+juntO4075k6Tr9+gXxoCU998DlhVKkv+2/FN9ovyLfnGI59ZTGEKE4OIwckn1YByH0bi7CMrRcORcOwGJ/87BdP9uBFZKGUACAiIntOAupc0XUdE5z1qjBZSrz0H/2xphYiIiIiIiIismvc/+Rzys1Vlq2hyQ2dF1JKpJTI9KXp07Q8fQqf/j4R9BE/VUFfHDeYWOCDzPPfeQXODrXuNHX05XP4kQF3ujXWfYxTyJZIGDhYGBamANQjmu+EsNEeZP7npa8dkh2q6fmPiI2WAI1nllvj6vufwsWidxORA0TvvyIisuc4UML7DOMEUWfuWQvHl3ju3/++JmMiIiIiIiIisjtcWrUb731Gub3GgJqUMkHCSyEzqz/vpL5bfd8OYCNMp3jdIoU5xQpdFbRLiUMvPLvoQ9oVqu+diqMvPMu9qqNtwJo+oaWUQtd1fUAaI+v5+0hmwf++RciDLKCpKiI2l0Ldna7rcHdyQDMudDfuc/9vPtPFEDlglAAgIiJ7UkefzZpzxs0ZW8d6HaSTK5z5J28qCUBEREREREREdoX1335hNz+6it+bUEcDGO24myuRHhv96Yv1Pehha296efIcKMnpGmfNJpx66RzNd88c6CuSXjoW5954EV+pmNRBVwWeDAcsYq4nfd5odSHfnk/H9iSANBspEREU+nPt7lQpMcg1dWuMv7zLJ//t3yn4L3IAKQFARET2nDCwnAiDdjyizkZuMnfHd0knlkhPr3D8x+ejPndIMw0RERERERERWbi1X16y8dXb1CXRWE2Va8gVsxBfigCCLjtdciBhoeX7RfMEozKhOjRgPRXOvfkSnG4O7HrTiedOkw4PuT9Zo7XCxAuT6AgLSIkqJYhEuG0EpeXbS5FID30f6N8vIpwuSp+EkQ1IDPOAJa/obq9x+e2Pn/ARi8huoTsIERHZk9ydqqqoqorxeExKsHL0EF/cvUE+MuTUaxdYefb4og9TRERERERERASAe1du4bdH2BhqqyilbAT33KCkzd2+sgtYEBGkuuLu+j26Grph4qk3X170kS3E4R+ejyNnn2LkE6zK5JzIOWGpbwFgCUoXtG1LIchVBUpi+dYs+iYgs7eD+SoAs+oghSAAqzOeMuZB1Rn5fsvalRvw2areTUQOKL37iojInmMBOQw6xwmoMh6FyWRCs9Swbi1rg+DIy2c58ZMXlW4sIiIiIiIiIgs3fu8Lu/H+ZxyaGNaBFwMPUqqIOuMGXdeRY7O0tyxWSokoTqoyLR2TGg6dO8HJn7xyoC7OoR+ej7NvvUzbGCU5VYLUOQaEd5Twfk966lsARJXownGFnx9J9q1tQMICn/uGlyBVFW1AcadKNU2XuPHBZe7924s6+yIHmBIARERkT5r1upoJ2LgjLubcLeuUQzVHnjvF4LunDtSkTERERERERER2p9Hff27l+hpLpWY5DWiaAW3bMpqMISdWBkMSRukCUl/SWxYkjPB+z3UCSNMWDYNEdWIFu3DkYKw3XTgUK2dPsp4L41TwbFhA8iCHk7achT7oH8wqWfhD/5Py+yX63f+z8/tVlUHqagBAaZ0BNTcufs7apS+fyDGKyO6luwcREdlzEpulrrZkwcLGN9Kg5v5kRDnU8PwPXocXD8ikTERERERERER2tU//u1/aygiaCXTjDqqKuq6hONHOVQBA7QB2AwOSGWZGZy1tXRicWOHUS+cWfWhPxFOvnGP49GHWU4un/gmZSlCFkT1hD5T5d8Jcz98dkGZtALavgU7Pq7tTug6bBCtpAKsdty99iV8e6cyLHHBKABARkT1n1g9vdrO7PQkgDMyCqGCUC/dSx4UfvQUvHlISgIiIiIiIiIgs3Nv/+b+24cjIXaZKNdky0RVKKf0DbLZzWjuoFymFkemD/xA4zsRbfCmz9NRhnvqj5/f1WtPSj56NQ88cZ5Kdlo6ooESHu5OsX5ibBahnFPTfeQ8L5IUZS80SqYWlklkpmesfXiLevakrICJKABARkb2pWD8FnrUCsDCmUw4AvCsMh0NWu3XaoVGO1Fz40VvYSyv7emImIiIiIiIiInvDtd9+wuE0oO4M1idUkWiaBqdf10hmWwKrsiiGeWwEuksUJjahLGcOnz8F55b251U6txQnnj9NO0yMfUKkAJyIwLNRbLoOF4ZNB0x3q+P92J9n5onxuVD+7Fz2V2H6g64wdGN5Ytz5+HPW//ZTBf9FBFACgIiI7EGz3f8+/RSzgDwds5thy4n7a/c4dPgwnRXulxG+knnhR98BJQGIiIiIiIiIyILd+bvPbO3abapxYbkaUlmiRL9kkTGyNv8vXiTCjellIWVINq0EkJ32UM3JN15a7DE+Jqdefh4/VDNOHZYhmxGlQEpQZco01Jzmxsz2kvXyzTnTSqfT3ydm2576NVEHulHH0Cvs7jo33v5oEYcpIruUEgBERGRPKrY5ALJDVTaTAEZdoV5ZYX2yDjhBocvB6jB44UffJT93TNMQEREREREREVmoy79+l/LlfWoSbjDpWsIgp9T3nl/0AR5ofVjbLPfB2ChUBAnHzemyM66Dw+eeZvCH5/fVOtOJH74Yx86cpKuCCYVsRvbAAlJKdAGRMsUSRNoI+Cf6XyT6IY/GrR/zCRVhm41BlpsBvj7m9qdX4Vqr3f8iskH3DyIisidtv/Hd3l+saRq6rsOin5wMBjVtKnQDY7xknP3uS/DMIc1ERERERERERGRxPl2z1UvXaW+vkT3RVAMigrZMYFpCfbaIP1v/mF8D2b7z+mFrJPJozKzffe0GvrmUVCxos7NmLUfOPgXPHd4f60wvHY0jzz3FZDnjdSYiNkZK/bOtbVsi6Yn2TTysUgJsfV1vf+2GOeCkYDr6VgspEtkrUgs3P7vKzV9d1sUQkS2UACAiInvO9pL/DpQEXd68UbbSUUeQpsWxOi84gQOTOujODHj6hxfg+cH+mJyJiIiIiIiIyJ5051dXbXJ9jeXS4GstZkY1HNCWjhRstAIIgy711RDDEikSNh2J7YHEIFSD/RE5kYJS2mkSgBGeMTIxXYAKg5IKSydWOPHKcws+3h1wporjz59icqxmfQglnDytgOA59Wtr7tRVwkqHRV8NYaNdJ/0vHNvsU3+Q2awiwuZmpq1JPX2thGL9CBJhm2kCMf0DORnRFRJGYzU+6lii4eYnV7nz15/qRIvIA5QAICIie9L2XmI+N+Z/PnvM/M+KOe3AGTxzhGe/9wqcqzQjFhEREREREZGFufbbj1j9/AZLqSIXo5RCrusHHve7Yvrqu/54+EZ4dfaLzbBKzkaY0+WgOb5E/dbpPX0FTr5wjpPnTzOqWu62a8Dm5puHrbvNzP9Mdk7CKKWl7TrqpqFMCtYGTy8fZ/zlPe59eGXRhygiu5QSAERE5MBJQCrBaDLBnj7C6T98c9GHJCIiIiIiIiIH2edju/7xZZbaRB4HtEFbOtrUVz0ESA6VzyoiznZe+7RMOBu7jJODTUuFy6PZ3Nk+92uDYPb9wK2vyjA8vMLp88/As8O9mQTw/HIcf+FZ1qMj58xKXSuh5FHN1fXfXua/f732tRJy9COFY+EkvP85kHPGm0ypjGQVw6jg1jo33r0Il9b0IheRh1ICgIiIHEiTyYTByoD7MSafOMwr/8mfakojIiIiIiIiIgsT7962W59+ziEaGiqqVOPTkv+JzXaIeW4Fo9jWndc5Ht5nXL65mAX62UwC2K7rOiwnSnJKhuHxQ5x+5fknfKQ747m3XqMdZCbZ6boJFloq2ylfVSFh++t6+2vXPEipgpy5PxrT5IY0dq7++kPK29cV/BeRr6T7ABEROXAcsGHDejdhMBziNYyP1Lz8n/6ZZjYiIiIiIiIisjA3//IjW792izwJKqtwEmW2Yzggez/gwR3FFpuPme8zLjtjtvt/i5Q2qjBMoqWt4PDpE/DqkT21xnTmf/WdqE8fZdVaSpOpqop2PNZzaAfNkkjmEwFmrRSyB9njgbYKAKUUxm1LtkTunDuXvmDtHz5X8F9Efie9f4uIyMGUjIkX3GAcLfesZbRkvPwf/8memqCJiIiIiIiIyP5y5dfvk1Zbcrt1t/92s8C/Kv0/GQ8rh59Soi0Fz0apgi47ZZg5951X4PTvunq7yKvH4/C5p7nnY7oq6LwlLKjretFHtu/Mt5SYSdGX+p+30XIiGeFQeeKppaOU22vc/ODTJ3jEIrJXKQFAREQOpFIKy8vLTLoxUSVK6igrmXJyyIX/+Ed7Y4ImIiIiIiIiIvvPpbGtXr7FYN2pSiJHgki4JUqCkiBsozj9Q0u1x0YXcdlJKTYDthZsbOF2Cp6MNjtdA0tPHeb46xcWfLRfz/nvv8btsk59eIgTNE3DaDT6yrYH8vWlSKR4yOtw7vULwewVHBa4BZ6CsCAwmpQ55DXdjfvc+OASXBor5UdEfi/dAYiIyIFUpczq6io5Z8BJlTEqY+6yjh9ruPDP/zB4plEigIiIiIiIiIg8cbf+5kPrbtxnqUtU3gecAyjWtwSYL/s/Hw106xMEHihVL9/Y7NzOgv2JzZLtG7u2i1OnDEDnLW04pYab63c489L5xR3813ThX/xx+HJFfWSJO2t3qZLRjicsHVqh9bLow9vTEpvPn9lzZybYfI26bR2znzmGFaf2TDOBK7/5gPE/fKFXtoh8LUoAEBGRA8ndaaoaJwib3YQ7noJRLsTJJc7+8A3szFBJACIiIiIiIiLyxF36b39l6d6EpTaRPdGFU7LRJejCMbM+uBhgMd0xbHPlwxUq3AF91/bNAG4f0rUwUhgJI6LfxZ2SQYaxT8hLNfdjzJv/2V/s2nWl/OqxsCMD2grWuwl1XWMRVCnRdR0pKXy082a7/nudBZ0XSEZUCSwR1pf9T0DuMkttxb2L1+h+9aVe0SLytekdXEREDrRZ5vbmL6AkZzIAPzzgzHdegtODXTtZExEREREREZH968qvP2Cpy1QlUUWmqhq6gKYeEBHT4P/m44uBp7ni4goZPrL58ztrAbDxM7YsKW0kYJQEbXLWm+Dwn76469aVqhcOx9k3XqTLQUl9koM8XrOKADMBpJzxZFhdMRqP6bqOuq4pbcvQGlbSgNGXd7h76fqCjlpE9iolAIiIyIHTZ8EnwvoyepXPTd6mM7aJF/KxJeqzxzn++u4v2SYiIiIiIiIi+0/725t246Mr1BOjKpmYdFSWmEwmWNiW4LTbLACdcNPS/06w3xe6j40mAWw0apj2ce+yM6nhxPOnqF47vquSAI5feIbhU0dpk4P1wX8LZyMVQCUkHpmzfb//XOuIqbZ0DJeXuLe6SlXVLA2WmaxNOFQvY2uO3xpx6+NrTD68o4shIt+I7gJERORAeuDmey6D2y3orDDyCevZOXr+NCf+9OVdNVETERERERERkYPhxr/50FY/v8mwZFIHuTNs2p59FqOdfbVIW3aoKwSws2Z92udtTxKY/TYMxrnFVxqeeXn3bC5Z/sEzsXTmJHfLOp6cMCdNKwDMx/31zHl0D3u+zCcBRDLWxiPquialCrrCMCqqcTBsjesffMbab64q+C8i35jew0VE5MCx+Rvt6Y34LAkg6H+fc2Z9MibXidIkli88xeE/e0FJACIiIiIiIiLyxF1/71O4N+aoDahaWGmG4LGx6x821zYMtiUByE4IgyBtDCLRh1jSljYAM25BR2GSW9KxIYf/5LnFX5ULh+L4hWfwpYpJ3hac/r3lDuSbCttsCwFMq3ZsnnQjQRhVbsgO3XrLwDPLXtHduM/6r64o+C8i34oSAERE5ECaTcyczSSAjZtyg67rWFoaUKyw7mPs0IAjz51i+INTmg2JiIiIiIiIyJN16b7d+Ogz4t6Yo/UQn7SYGQGUuRDhRhJAbPanl0e3URqfzaD59p3dvc1vBmDZmETLuAlOnH8Ge/nIQq/IUy9foD5+mPXoSINMsWnZ/4c8UfzBPy7fQJ8wslkRYlZ9dDZmqqpiPBphkThULzOIRNwfc+nX7y/isEVkn1ACgIiIHEh57ma7GLTZ6FKfhZvdcHdyznSlgBnhHXlYc/zV8+TvKwlARERERERERJ6s+7+6bNc/ukwaOz7xPgFgbjMD9OHnrOD/jgmL6di6gWR+I0k/Epsbu7dmBhQLSpPplhKnX1lgK4A3T8bSmeNMaigUSnj/b0mxEaTO0Y9ZZQN5NCX1Y8a2JejUOTNeX2dpMMSKUzmw1vLRr96BS2va/S8i35rewUVE5MCZ//DbXooL+pvwwWDAvXv3yE1NHmRG3RgGkI4tceaNC/D8YjO2RUREREREROTguX/pOmvXbnF0eAgKW8qJ97Rv+3Gane4HTjuwPdxi9BUmm6bBKXQ5yEeWWPnh2Se+pmQvHo9zr79CVwej6MjDiohCWDy0bcR8Uol8O/Pn8GHPlxTgXWHYLGFuDFJDrE24+PYH8ME9nX0ReSRKABARkQOnL/vvuPlG1m2ey8CF/gZ8MBhQSkvnLQwSazFhTEtZqXj53/kDeHFJSQAi+1gphbqu6bqOlBIRQYRe9iIiIiIiskBXVu3+xZukNRhEQ+WpDyRG0BGQINeJ0WgNy2lj48N22/d3xwM72r8qyH2wzHq2b+7c9t8zpmtL0xNY1w1t25Jzpo2WdLTh+IUzcPbJrikdOX2MermCKkGGcdcStrkmVpe+IuasXWaZq3Igj8YNzPoTWaZJF7M1hhxQhdFNWqpIxN0xk19f11kXkUemBAARETmQtu/4nw/+P+yxxWbD6bJzm3Uu/NkfwsvLigaK7ENN02xMyOelpNtnERERERFZrLtvX7LP3/2EvFpILTS57kv+pyDVFevjMUePHqWUwretCKAEgJ3hXSFbppQOstFaIR0ecPj800/sGIY/OBXHzj5NqcApWAarjDDbaBXxu9bF5NuxafvRVKabCZJBTkSyjdeWu5MDVmjwu2t88v/6hV51IrIjtIIpIiLyDQVQDxpYanj+x9+H55KmSCL7zHA4xMxwn/bVnO7+VwKAiIiIiIjsBqs//djKl3cZWs1k1NLkitI6k8mElBKTyYQom8H/7QF95+GbI+Tx6MKxpqLF8RpOnn8Ge/nY4z/jZwfx9PPnWDp+hNaCzoLi/ZWPCOaKFWx5fqTY2hJAvjkLaNzIJcCj32CUoLMgAiIg1wMmayNWRs7Fn7296EMWkX1EK5giIiLflDktHaOY0A6M5//sj+HZRtMikX2kaRqgz8afBf1V/l9ERERERHaT2x9/jq21VCWRSNR1TSmFwbChtGMGdf7WQX0lBOyMKiUsAg9IOVMiWC8tdmSJp14699j//qMvnqM+tsKqT5jQESlovd0yv3XbHLMkAJsNPQe+tQRUBDmmCRfWt+rwCDp3iETtcMgGXHv3E+Kje9r9LyI7RgkAIiIi31CfJV9YL2NKk4lDA174k+/DuaGmRSL7xHz5/1mvvlkVABERERERkd1g/YMbdvU3HzEcQ4wKyTI5Z8bjMU1Tf6v5iwK+OycBFv3c0nJi4o6noE3OPSYsnTnOoR+ef2xnfPjds3HkuVOMmmDNx1Abkfr5bc55y8UOC2L6+/649VzYCakEFWBpdo6NYoZXFSkl0moHN9e484srCv6LyI5SAoCIiMi3EGbkJjMqY9ZjQnd0wAs/+QGcW9b0SGSfmF8sm5X/VwKAiIiIiIjsJuN/+MLWLt1kJS3BuGAk3J2I6PvOfwsK/O4cn+70TinRdR0pJeqlAWNaJo1z8qVn4LnHsJZ0ehDPvPo83XLFeurwxvAcFG+pUiIbMG0REWxWAJhRC4CdEV6AzXMZZliuMDNSMcrNVT771W8XeIQisl8pAUBEROTbsOizpb1jVMaMlhP3VxJn/+hN7MWjmiKJ7BNmthH0VwKAiIiIiIjsRnc//hy/uUrjmYpM3TQUAnLa0tf9m9AO8J0xqyg3088pg8gwoSMdHXDkxTM7/vceffEc+fiQsXW0VqCa/t0eJMC7suX6xnT4jh/JwebWn9eEgQdh0+dEG8S9MXc/vkZ8NtbufxHZcUoAEBER+RZSQGknDAYD8rBitYwoy5n2SM1zf/A69sySpski+8D8Yk1KunUWEREREZHdp7103z779YcsdxWphXbcknPud5/Lwjhgs0RyD5qqAg/a8YQ6J6IKVpnw1IWzDL57asfWkerXT8TTL55lPVqiAqZl/92dKidqS0TbUVcVsxlv2ObQs2ZnOOC5b7vQPwf6lhBVSdRrBW6tsfb2dQX/ReSx0CqmiIjINzTrhZYwWu8I6wODrRcmS0Z3tObVn/wBnB4oCUBkj3J3cs5bdv/PyjWKiIiIiIjsNuW9m3b5799n0CWGVtOOxmD9ruN+17eR6AORs3nO76pwllDwYCd04ZBTv9u+cyqM2hKp6yCC0hirdeHUa8/v2N/57Bsvsp4LI+twCkYQpZCTkUrgXaHOFdbNh/ptOmQnteGUBH0zjkSdatJ6YeV+4cv//rc64SLy2OgzXEREZAfkcMApyRlXzr2B88af/9GiD0tEREREREREDoj1X101v7XKctQsVUtky+Sct1Q2m08AUILzkzXbUGIB2RMp4P54jW5gpKMDzv+z7z/yRpKn/73XYjJM+CBjTdX/vdGH9lNsHfM/29IOwPrS9fLoqqomVzWWK9yB1ZbBWuHT//LnOsMi8ljpE15EROQbciDMCOtroyWH5FC5k6ZJAHG4ZnQk88p/9GNVARARERERERGRJ+LST/+BwZrDqJDcsIhp4Hn6dZoMEBEP9Kef56gU/E4Ic8J8I/gPtiXoPhgMGJcJ91NLPrZMfnb47deRXj0aK2eewg4NGcUE966vYOlGCsOiv96b/z8ds+9v+ZsdNz0DHllXGK2N6QiGdcPhSeLGbz5e9FGJyAGgBAAREZFvYTYRnpXEyxEkguT9T1eZcL8uxIklzv+zP1ASgIiIiIiIiIg8fleLffCz33DUlkgjx7qtu/7rnEnTvvTuCvA+TrEZW9+QpkkAsx+klHAcT8F66rjw1qvf+u878/J51qzDG8PNKaWdtrDcdlxs3eG/mZwgOykBtMHh4SFqKhgV4uYao99c1+5/EXnslAAgIiLyDfUTuERY/zE6P0nKEVhAFx1tdm7lMfn0YV77D/9YUykREREREREReezKe7ft3sUvOGwD6jCy9T3o3R0zI89VAZgFqbcP2SlBbImu92tJJfXnuUxaqpwhQV5uqE4f5egfnvvGa0iH/uT5qE4chmFm3I1pmopstiUAFAYlGSXZA9d5vj2BhZ4HOyISTTXA2iCvF6q7Yy7+t3+nsyoiT4QSAERERL6FmA6i/yjdOmkK6pxxcyapsF4VypGGF/43qgQgIiIiIiIiIo/f1d9+QvnyHrkEdcpUlojiG7v+zYyUFB540tzALVEsUcxocgWTCeHOmJZ7NqE+fRSeP/z115BePBKHn3maSQ3WVJTo8K6lmru+s0qWxTbH9jYPsyqXsnO6rqPqYGm1cOVf/lzBfxF5YvR+LiIi8ghK6idNm7/ve6RZBDmgqWtW2zHrw4Q9dYRn/t03lAQgIiIiIiIiIo+VX1mzL967yPjuOqUUbG7X/9ehHeA7Z3t5/WJ9IkAYZIwKozawbJRDNfVThznzwtmv/d9/+sJzxHKDV4lJmZAt4V3BvK9S6fRVLDcTDxJB//uvtrH1RR5BHgzpxhO++PVHiz4UETlglAAgIiLyDVmA0Y+5706/9pMoA6L0/dYGywNGuWPcOOn4Msd/fEEzKJG9QiteIiIiIiKyR629f9PaG/dJq07yhFmerlhM2SzI25epj20/Ul/4R7P9HIZthtR9eq7H4zHLgyGTcQfAejehDBL52DL5wpHfewWW3zobh8+cIK00RNVXdsCdpaUlSim/d0obtrUKwMaxf41/34EwvYjb2yWkuYqg/XjwBZM9MRgHdz69zuidL3VKReSJUgKAiIjIt2DhGwNmZdP6u34LY/ptLKCUFjdnlCZMjiSWXz5F80dnNI0W2cVSgr4yZsJIhENKiYiy6EMTERERERH52m799jJ2fcSQIckqYhrJtATuHZgT5pTklGmg0wJyJGpPCiA8IpuuE83K7bv1v8rhfWw5V4y6QtU0TLpClQxqY3TEOPHW+d/73z/50jOs5paJtTgFvMPMmExaqKvpo6ZrWNO/N09/bfhGzNrZ1hJgetwHmgUxfX3MEmRSJGw6UiTCoKVgGSxDlI6mqug6p/HM6gfXWf03Hx7wEykii6DPbxERkcdkVl6v/+oUcyZ1MG6CU688T/7+U0oCENml5gP9cdAXPUREREREZO+6NrbP3/uU1Wu3aEoi2n4pIgXknPvHmG/9Ov25PD7zG8YdI8w21pHcnDJM2JEBzRtPf+WVOPOP3ww/VBODhFlg4dMUdutL/U8TD7b/nfNDvp4t5yo2w2oJI6VE64W260gpMVld52izgt8bsfrZF0/+YEVEUAKAiIjIYzGbtMFmfz2zflJgOWHDzPNvvkr+7ldP5ERkceZfwyIiIiIiIntZfHLX1i5fZ3kMK8MVxm0hSmA+XZKIROVQ+Wags1g/5MkxMyICdyelRDUccOaVF2hefPgGksHpo9hSDTlBBFa8T9xI1ld1sN/fAkC+nhz9ADaqArj17T+zQ6ZP4Mi5YikPSHcn3ProCqNPbugKiMhCKAFARETkCYtkjKJQhpnnf/A6vHpYSQAiIiIiIiIi8tjc+/klS3fGlPvrLKcBTdP01c4iYdAHMT2Row8auE3Hog98n0tp2kt+unkkIiilEBFElUhHh8Th+oE/9/p/9hfhyxXjVGjpcO+2VG0oBqHE9kczfX0QadrKAWZn1A3ASQThjkWisorUwhINX75/kfWfX9YFEJGFUQKAiIjIYzabxG1IgdXGuHLuV4ULP/4+vLCkJACRXcTmyi+KiIiIiIjsB+/91z+1I6UijQrjcUsxmwb/Z33N+0QAi36XsyfXDvLHbLbrf/73s++1dKxXzoXvvQbnljceNPzO0zEZwFrlTHCcWeVJcAvKNG1D125nbD2NPm2V4YQFVuV+7aAr1MWoJ3D30pes/a2C/yKyWEoAEBERecy2BxFLBB3BhA6axHhgvPBnP4Rn1WFPZLeYvW6VBCAiIiIiIvvJO//6bzmelliqhrRtIU13Ns8L64d2/z9+7tNgfcRG8D+l1FcGMMPrYD23HHv+1MafOf/WK4yrICogRb/RJBvk1F+3iP6/p1WmRzZbEZglU8xeFzE9uR5BZZmazHIMqEdw/bcfL+RYRUTmKQFARERkAbro+nJhKSg5uF87F/7pn8NZJQGI7CbzZRhFRERERET2vEtju/vJVZqSWGmWsUj0Yc5+lBS4BRAkNA963GZzzdmu//m5p5tjGdZpOfHSOXjB4tl/8YMoSxVjnxBRSBZYBMWgTYUyDVTngIoHkzvkm0lhpDCcvq2CWzA7qWH9Jp+IYBAN3B1z96PP4dN17SQQkYVTAoCIiMhjtr2UeEQwGAwAZ1ImTKKlrYNRAxf+yU/gfKPpmcgupCQAERERERHZDy79/96x9au3SOuF5IkgAf3u8WKGWx/jnIt1ymM02/UfEbg77r6RDDApE6KCycA58aPv0Tx1mDVrqZoM3mH0j+u8pfhmy4aEkXXtdlQY9Kkxs4oA/XWjDZrWWL18kxs/+1TBfxHZFZQAICIi8phtz+A2M7rSYgmygVkQKRhbx3oDL//jH5NfOqJpmsgusj2RR0REREREZC+79rN3GKwFVaqZFMdSwoE2HMsVsL33uTwOs53/7r6RCDA/98xmfRWA1GHHllizllIFUZyahHnfOoCciNTv+E82Df67KQD0iJJVlC42EjQiglxXdPS7/+kSy7bE5It7fPFX7+slIyK7ht7/RUREFs0CwwlzuuysV8HZt15m6Y2nlAQgIiIiIiIiIjvvamu3PrpCGheWmiW6gLDEoK6JUojpLnRZLIt+z3mXoMtOm+hbSuJbHpeiH323SUOhn53RhVMNGrIZUfrqDF3X9dU9c8OKDbD7Y6699+miD1VEZItq0QcgIiJyEM2m0L7Rmy0AB3O6nGhOHeF4Ok8hYvLODWUQi4iIiIiIiMiOuvuzTy1WBnH4xWfoKqMAVUpE12JVBiD8d/835PFz2wz3m0EEG6X+Z+tKFgkD6mIb35+uNMkjcIIMdJOW2hLWZCalkIphJeDeOrc/ukb70W2t3YnIrqI0MBERkQVL05562QOLPqP7jo+wEyucfuMF7GW1AxARERERERGRnXfvvYuMr92kiUxtCcYtVUx7m6sN2mJZTAP9viWQ47Y5ZgtGBmTv15dgGvzX5XskYdM2nt7i7tQp95UWOmfJGgZj49ZHV7n70090pkVk11ECgIiIyEJYP6L/mjZC/P0vvDHWqkI5MuC5779G9dJxJQGIiIiIiIiIyM76fGSrn35BvVZYoaH2TLZE13WU8I2d5rIYmzv8Ic+C+wbFEmX2MzZbAMR8coCu3Q5wzIxcV0DQrU9oSuaw19jNddb+9lOdZRHZlZQAICIisgvMT87cgkIhUtA2kI8t8+wbL5AvqBKAiIiIiIiIiOys0W+u2Z1PrsD9EYNckVJF6VQ8fuE2IvgJiwd394clLGyjsuRsbalYIjZCPwoBfVsWENGf9EjQ4lQpc7geEDfv88W7ny74CEVEvpre/UVERBYiYZE2PohLgi5BSd5P2LoWw6HO3O3GNE8f48J338BOLysJQERERERERER21N2//tjWLn+Jd0FrAckwU/hg0WY7+/sr0Qf23dJG+f8E5GmZgLB+bUml/3dOhRHFmeB0BE1dU+6Nuf7ep0zev6EzLSK7lj7BRURkI4MYICzYrEZv02TjRHzF42VnBP1EbZahPWwGdF3HpBuTl2vuljU4PuClH7252AMVERERERERkX1pdPU2rE9IHTRWk8ibu85tc8zb/r00N2SnzJ90n47pT2L+J7Z5LcznHi/f5Pk4//wNM0hGJpE9Y6vOrc+ucf/X1xX8F5FdTZ/DIiJCitnkwR+YuPWZxf0OdTef5hqHPkAekU3P+ezMW9iWESWoUsbCmZQJsWSsDlrunUic/Rd/qBQMkcfMrX9tqmeiiIiIiIgcFPff/dLWLt9kad2oSkUqRp5WL7SciJxwAicwM0psXUfqd6Mnsk+rHoZWjx6V9/Uhp2H8/nc5+mHTdbySpoFq+oQAw7Hw6drTwTZbx7R4MDElDEh90oRPtz5ZBFEcvH+Ot2YUh4E3HIsl4sYa9z67uYh/iojIN6JPYBER2egVxsY+fyfN9Rlz21YBQNnDj8Wsn9t8jzEzwywo5rRVMFmC7kjFs//Od5QEICIiIiIiIiI76su/+sBWr94krTupBfPA3fGuEKVsrFcA040L23ahm0rQP07za0cWm/UAfNvP5UGz87I9KBYRmz9LiZwzNk2oqFMmRaJpjcmNe1z/6HP803t6hovIrqcEABER6fuGTW9d+0xhNjJjbdprzOgfM+sxphSAx2s+AQDA3YkIUkqkYcPKhdMc+ZOXNaUTERERERERkR11/e0PGLTBMDLJKkhVv6PfEwnDzIgIIoIckKerE8WgS06X+lUjQwEIWay+ToJtWfecNwv+J/rnsVn/2C5BhGFj55DXxOqI6xcvM/6tSv+LyN6gz18REXmgxPXsZtgCcjhp282xmz4+HrdZ4H+eu+PudFa4nzuOXjjFsR8+ryQAEREREREREdk5n7d2/YPPyOstKSBj5OkGkWx9EgD05dKzb64jlWngtMxaAmjFQhZse6vT7Swgm208p6M4nTtOn+BST+CwV4y+uM3451cV/BeRPUMRHBEReeiNcJrb+Z/CN0vTA5q/PTnbKwFEBMWdNnWU5cTKK2c49CdKAhARERERERGRnbP6y0t9EsAkqCKTMcIdc8Ms9zumLWEEKYKwfsyqTIY5bqofKYsTtjmch2+AmgXI5tfd+j9rVJY4nBvGX9zl1idfPLkDFxHZAUoAEBGRPqhvALM74fRAv7bNrG0luz4ps3J60PcgSyn1pchSYBkm2WkP1Ry6cJrjf3ReSQAiIiIiIiIismPu/+yS+Z11qhI0qdko/W8e4EGabhTxh+yyfljAVWTRZs/JWWBsM+C/+fOUErUlGs9wd8Tnv/0Y//iOns0isqcoAUBERLbE9MP6iVsxKPOfEluytnXP+7htbwEwm5DMfla8JdXB2Ca0S8bSa2cZ/OiskgBEREREREREZMd8+eEl2hv3ScWord6SBBDR7/gviY2NJDmCHLGx61o1AGRR+mqmCYu08XzcFCSCSFAs6AiKQbKKikw9gbzacfm3nzL68KYWQkVkz6kWfQAiIrI7WCT6vDDHpzWwIiBPQ8q+8bgFHeABNJtUA7h7P8E2m37f6aKQKiNyomtqjr7wDPfWS6z/Wj3JREREREREROTRTX593VaHddRLS9hyRcqZMCdS3gieBpuVI+fXjcK0jiSLNXv+bbQCmJb9t5ju+jcjCEoEiUQGcmekdSff71j7jdbYRGRvUgUAERGBEkQJsvcl5gnDUx9YLskpFJzA3cGdOudFH/G+Nwv8zyoBmNlmCwAcq4xSWowgotCWCcOnjnL8Oxfgrac1vRYRERERERGRHXH/Z1fs7uUvqVuggFuiGHTERvl/iz4JIIfqRsrukafPy41EFAv6xhU998AxPGciGdkTKzQcbWs++X/+Qk9lEdmzlAAgIiKQMsu5ZkBNWWupUqL1wiRKnw2b+uBzVVWklOgmrT5AdoGIoLJEU1W4d9wfr+KHG8599xXyK08pCUBEREREREREdsTdz77A74851CxBASxTMGbhfmOzCkDSioTsArO1y+1RfJ/7RlVVlBL9phs3fNRR7q7z9l/+7ZM6TBGRx0LxGxERgSvrNrq1yrAklqzBJkGdMiklSH2frBJOGGSr0MfH4sV0suJdIXswyImE49mxY0Oe/fEb8FyjKbeIiIiIiIiIPLqP79rNDy8zuXWP7NO2halfnEhu/fcCwHBs2n99oUcsQoqtCSmzX87W1SKCjJFbGFKzwoDP370In7fa/S8ie5oiOCIiAsDF337I2he3OcKAYclUnsgOedpz3t0ppfR93ZI+PhYpMMKNXDVEBF3bki1RVYnOW+5NVpkswcv/9C/g3FDTbRERERERERF5ZOu/umI3PrrMoDMqMikSFmkaZFC8VHYft607/qFPAnCjL/3fBQOrGUyM5Ymxdu02o19d0ZNZRPY8RXBERASAePemXXvnY8ZXb3GoVDRtYG0h2kIC6rqGZHTheAS+6AM+4CIgpYzlTFecrrREBClDbjJj67hvY179938CL6woCUBEREREREREHtno8g38xipNG+QS23b5b8ZNwxJhCj/I4jh9sH/7olhYv7kGIAcMOljpEuOrt/niV+8+6cMUEXks9AksIiIb4oPbdvXdT1j74ibLUbOca5IHpev6B6RERDyQOStPlkVfas+7grtDnaHKdFFwL/QFGjomNuGutbz0j35E/cIJJQGIiIiIiIiIyKO5vG5fvP8J5fYajRspEk7aKKluDym3LrIo8xUA5p+bxfpEgCoyjWfSvRFfvPspXBtr1VNE9gUlAIiIyBbtR3ft8nsfc//6TawtNJahOCUCpx8550Uf5oFXRYYCJQyqjFeJko1CMGnHWIJcGeN2xCgXXv5Hf0j9yinNvUVERERERETkkXTv3rJbn1wmjfsNI3059T6gmiKRI22UWQ+FU2WBYvocTGwGw+afl9kh1sbc+vQq5aPberaKyL6hBAAREXlAfLhq19/5mHJjnWFUDKioyFgkPOKhk7f5D5TZzfXscYmtN9ryaBJABCklqqrCCSZd1ydnVBU5Z8yDiKAaNLTm3E8tz3zvJZaUBCDyjfQ7BNT0RGQ3SXpNioiIiCzc+OdXLd0ZM+igcoAEMbfyY872udT8elEoOUC+hm+znrj1+bX9eWikMCwge2IQidGNu9z8xWd6NorIvqJYjIiIPNz7a3btlx/R3RgxjCHWQQqohwM6n/abZz64b1QpYWZ0XuhSX04L+gBaVfqsWtAE71H1WfUO5rh34EG2RCIRJQgHyxURAeZE6hjbGDuWOfTWaeyt40oCEPk9wpguWG2lm2eRxbJpF88UrtejiIiIyIJd+q9+ZYdWYXncl1EvBF022ujIQHafJlUnwhJBotjmiLnWAQ9sHrEgTMsXB1kCLAKLIBEbNfy3JpFMn1sPSS4Jg5QSeJDog/6l7ahSTUND1WXWr93h6tsfL/YfKiLyGGjNREREvtqnq/bZz37DsM1U607qwDwoEZBsM3/WA4rjXekTA1Ji+xxNQf/Hx2JzzJRS6Nz7rI0MkQttVcgnlzjx0rM0L5zQLFrkd/C596z5koEiskBaABYRERHZdd77m7+jWSukSdDkBssJklFVFSmmQf1vcRunOz/ZMJec/1Xri181VZhMxhTvcHdSSiw1S9gkyCMn35tw9b2LcGWkVUsR2Xe0jikiIr/bxXV773/4K06mJQ6VirLeMqgGdAl8+imSMcz6e+WwICUjR1BFf/ddDNrUjwRkzeIemy0ZzoC7Y2ZEBJ0XclVx6MQxnvnOS6Tzx3QlRERkT9n84NJUVkRERGRX+HTNuttrDCNRk1gfjZh4oes6sk03iJiTw0nRf50NwzcCt872Yu02HXJQ9RUwIUhbkvR7CYu0sSGmrxYw/bVDmlYOaJqGZtCvY066Fp+0rFjF8ijobtyHD+7oSSYi+5JWTURE5Pe70tnbf/lTlibGoWiwNsCtrwRAH2yeDaO/2c6xmeEd1icLzBIGZjfm8vjMJwA4QaR+PtOWjnEUqlNHOPna83D2kJIARH6Hh+0u0PuXyGIE1t9TzMaiD0hEREREAPj0v/97G3ZGe2/EynCJpcGAyWRCzhnY3J092xQyGyrwJL/P7N5/u43n1Nz645ZWpdGnj0QEbdtSNTWpahhaQ7k7YrDmfPE/vqPgv4jsW1q/FBGRr+ej+/be3/wdK11FHjl1ZFKkfmd5OIXACcwDK5sZ3GF9BYAyW6iPLR3d5DEp4VhOxHT3P8kgJyIZXXbu+Bor505w8tWziz5UERGRr60YdGb4tOKNNoWJiIiI7A7v/N/+0g5FhY0KUWA4WGLStRuVCjfbF8Z0bF0d2t67fXurQzmYAtsyNnb5xyzIv1lJ4mHPmfF4TF3XdF3Xty8ddRy1Ae/+q/9FMwkR2dcUgRERka8tPrhrH//ibZbHMCxGQ8LIOEGJfh+emW2Wb5ubuMmTVUohpUTOmRKBu1PC+2tRZUoVjHLH0tmTHPnzFzWlFtlGLwqR3S2+YieQiIiIiCzOJ796l2GbqCaA+zQ0u1nKHbbu2J4JrR/JQ3zT58XDHndoZYV23OITZ8kaBp3xm//pr3f2QEVEdiElAIiIyDfS/uYLu/vR51S3xzQTyFbhKVMMwqzv72ZGSbMyXX2O7qy8G+aEucr2PmZm/axnNlFyoHOnc8fNyU1mzJh2yTjy4hmO/OQFxTtFRGTXy2HkSGRPpEjK1hERERHZRfzXX9rk6i2WvSIVg2R4grDALdh68zb9/QPfn+3wnt7vycEWaa6aaD9SJDCfrjFuXfuafZ0lC3dtIQcsW0M1Du5e/hKuFqWaiMi+p09QERH5xm7+9FPzG/exexPqzsieiLBpJYC+GsD81C0FJJ8mAMgTYWa4O13XYWbUdb3Rew+gbcekOjG2jraB4y8+y8qPz+sKicyZvSC0C0Vkd7DZ/YT3v87KJhQRERHZda6+8xFxaw1f76hSDWzOqWZVnOYrOX3VQsSs25MCGDJjD6keMaswUWyzDemGMJIHTVQsl4q7n13jxl++rxm+iBwI+vwUEZFv5dpvLzK5dptqvVCVRLYKSxWTrsPqeuNxKegbBdjm/XWE4sxPSkr9R33nBScwMyKCqs50pSXRV2SY1HDy5XMc+clLujgi9G005s2SakRkcRJQYeQSVOF4aTHNaEVERER2l89GdvXdT1guNVULFgn3AEt0HpQEqcp0XiD3N3N9gkBgEX2Qd6H/ANnVpjv/YRb8D6gSHUEbjuUEySgeVCkRo+BwWmb1sy+49T8q+C8iB4c+S0VE5FvpPl+zGx9dZu3aLZpJsGIDunFHrhu6riNiLjM3YkvQ30z324sWEWT6lg2RjK6GslKzfPYky3/8vJIA5MDTrn+R3cm8T2ZLOePJCDUVEhEREdl1Ju/csNufXmUwSTRdIls/Ukp46QO1VJmuFM295CtZbFaCmK0x2nTFalbu33JifTzGcqKqKkoplFKwgCoyx4eHaW/c497lLxf4LxERefKUACAiIt+aXx7Z9f/hHbObY/zeiCVrqHMF9OX+c5ku1Hvf681tlslt+gB6RLMSZ9tZ/O6x8TgPEtZXBDCn85YuOdWJZU689AzLf3ROSQAiIrKrOFASdAnWKYyyw1ArxiIiIiK70d0PP2f981vULTQl4ZOCRUAynD5wOx/8nwV7tV4k8ywci+kqmD24EhZm5LrCLNO2BdwY1ENqq2Ds2FrL7Utfsv7Ol5o4iMiBUi36AEREZO+7+N/8zM7/ix/FcDhkdTymThU5NstxhUGJafl56yd0slgWfSuAMCCgEIzKmEgN9XLNyZfP4USMfnZZl0sOLNezX2TX6QiCjlTVLD11hJXvvkr9GtGkTOdOl/v7j+z9/UYAbonZUnJ2Jx3QqgHb39Ni7vuzxXczwyJRl0Ru4f4XN7j+dx/vinfDp7/zfBx67im63C36UET2HDcIUr8b0vt3RDfHDdoEYYnsMCyZuL3Ox//2N7vidS8ie9y1kX3x/sU4d/Q1mqqhtIVojLqpGZcxTurLtXtsBv/ntiJs3Kss4thl13lYYkhY376vGTS04w7zYFDV5M6Jzmha44sPr3Dnsy+e+PGKiCyaEgBERGRHXPwvf2qv/qd/HnUT5CZREv3MLU37cZnhEdg0AcC1v3zhwqBzh2Tk3CcEOIUugw+DZ7/zIhcnbXR//4UWAEVEZOHCIFeZtjhthmpYUR1fofIEGMVbbNAvDabpSrFFou88NPsoC7oD/Km2UTbVfOOMJKBMkwC89OfOqFmiYRJlgUe71dLJY6RTK3SNbiJFvh3v3wPKXDnlBJYSBlTFiPWCtXqNicgO+viu3TvzZTz98lmqeolRdMS0GmFxJ5H69yYgT7MVw6ZB/+n9ielt6UD7fc+BiMC7QnSF5cGQqoVubcRyHhL317nzbz88wHf/InKQKQFARER2zHv/xV/Zd/8v/27cbteJOmhnqds224MHHtOZneoALJSligjHCSyClBPmQYm+/14eVqyuT7jw/df4qOvC376pCyYiIgtlAe14QkkQFpRZMD87OdUUS0ysI8ypSJt3GtEnBYTNdrsf3H1kG71TgRQOJNycoE/eTLnqT5I71k7oYvfstm8pdKmwpgoAIt+ahdOwORNzS7QJIIHVWAVhuyfxR0T2hzv/9mMbHhrG0WdPUVJibVJIVSaSE9GvFaXY3P0/C/5vVC9SEsCBF5vLiltYQJ0zXgo5JXIAk47D1YCqGJ9+fOlJH6qIyK6hBAAREdlR//B//Z/shf/9D6NNCa9m9+dGin7X/2zx3dAE7nH4Or3yZj2U+4sRfSuACALHwwkzkjlWG2uTjld+9APej1+Hv6NKACIisjgJaOh3+7ckUhjZHCIo0VFy4BbTXWPRLwACxOxuBMz8QOcgRl9llxz9aUnT6kxu/dfOCtkSJYISTrLdc7I6L9QpMN1AinxjYUGZzsGK9W1SADwFkcAi8NbxrnDv5q2FHquI7E/X3v+Y3NQMTh7BcCwbdZUppWAxbQEw+4if3n7MEgCyPvoFeCADwDZ/kS3197BrLcOSSQ7XL15m/Ktru+dmVkTkCVMCgIiI7LiP/80vOP8XPyTMKE3uF5YCrC/0BnMTu1nAerYXL7bdmltsDWof3D17O6vrOlJKJIywvvxeWF8M1Czh7rgZaVix1k54/vuv8FlK0f3mqiZPcmBsFDEJtgQM9T4ksjgpJSICKw4WROp7Wrt5X8UGgD6xrcD0xqKQot/pTkA8bPvQARHT/yv091cxPRtBENEnAAZBVBWZCv86mYVPSKrTNGlR78Ii31SwGTYp+LSccv+a6kujBCnDZDLh9s8/1f2+iOy8T0Y2PnU3jj11kvVw3GP6PtRBGDFN4tzuq3Z+y0HUPxm2P09K25GtIlkf+F+2mltXPufWX36kzzMROdCUACAiIjvv87CL//Ov4vyf/oA42XCnXadebiilxdpC0zR0056yG4kA0x5vsx60s8B/js1ytT5tVqkecF/t6y6J1waEbzzefboQaAln2mc5BcU7zBJ5OXHuexe4tBzR/VQZ1HJwzN5rjP71pSe/yOJsfMaZ9cmB0ZfzL9MXZurTDCFsfkMQ/bdi9lPsAL+SZ/lMCTaagMf01waY952bOndaykMX4hfF3clmpK9V70hE5gWbiY2YTedTToq+mkoKo3RODr2+ROTxufG3n9hgeSXq8yfpIggPslV4dPSbEQKz/uaklILjNE1DuJL/Dqow6EqQcyYlNpJBIxk5oBBUKZP6B7JcDfAb63zx24uLPnQRkYXTnb2IiDwe11q7+De/ov3iHsfyCr7e0VhNzjVd92Dv1t8V0E8K9j9WDzv3EUGJwA1KckoV2KGKo88/Tf3HZ3RFZN+btSqZf/8Je7BKiYg8Wc7WZDff9r0t5WPlAdvPjW/7WSL6ygDmm313d4kwJWGJfFv9e2OavgdsfSWl6fvmMNes3bu/kOMTkYPjyv/8G5vcvE81KcTEKa0TYX0VQqaf9zlRVRVVSnSTyaIPWRasqirMjM69f564Q3G6cCICd2c4GMDYyZPClQ8vwucj3TaKyIGnBAAREXl8rk7s6i/eoVy9xXGGlPWWlDMlZ7oEXdrc8T8LquUIKg8SgU179rpNK1Pq9v2JsWlbAMxwgknXURIcPnaUsy9doHrjtMIrIiIiIiJ7zGYiUCJs1mM7UUYtdz67urgDE5ED49pvPuBkNOQSNE3DIFXUOUNKlIC29BUjcxjJldx5kM0qgpoHTuBAtjSt/JWwlIFEtz5mmczF33zA6q8/1+qhiAhKABARkcft8sgu//K3TK7e4lheZjJqCcDpS837NAnAmSv5z/QGn822ALPHyOMz2zVpsZkAYGZEMko4o8mEFieGmfPfe5n08klNw0VERERE9pJtkbRi/dJgbgu8e1tBExF5/D6+a1ff/oiVNlFNIBXDC0CC6fpD54UEfWKAHFgJSF6wKJgZKSVS6j+3YprANsgVdWesX7vF+JcK/ouIzCgBQEREHr/LY7v86w/obtzjaLVCjIMUEHP95mMa4LeA7P2Yld6OLVUCEqbelI9sewnlLcH/ALwvo+bR9X3WqkRkoyNoLfClxNnvvMDK66eUBCAiIiIisuttTaeer7JmAXWnOZaIPDk3fn7Rumu3GKx3eCm0XijhYJlc9yXfIxyLUABD+kQAA6xvV1kiiDBSCYZeUY+Ca79+f9GHKSKyq+jzU0REnoi4uGaf/upd7PaIE/UKdUlkB9iaBPBVZf7Ve/vJ6ifb0fdTI/pWAMnwbHTZWY8JgxMrPP3qOYavHFcSgIiIiIjIHhMYAaRI3Ll2fdGHIyIHzJfvXqS6N2E5DxkMlggSk9Jt7PSO0FLDQdenrkVfodIDilMisFxTpcwgKvzWGp/9+j34vNOqoYjIHCUAiIjIk/Pxffvkb9+mujliuU0MSl/yH9Jmmf+5QL/F7P806Xvc0kNGNutbMERQpv/rKHRWsCZxL9bgqWWefvMF6gtHdZFERERERHa5WZW1zezqhIXx5fufLuqQROSAGn123268/xl+b0RNRc6ZUgpRHAv6TQlqB3ngddY/D7JPlwirhFWZqjPyasetD67Q/fqmgv8iItsoAUBERJ6sT+/Yxz/7NcsTY9Almi5tJAGEgZvhc7ftswWqjdL08kS4OxHTLGuzjQoMlhORE1122ipYTS1xfMCJl56BM0NdIRERERGRXW4jCYBE8mlltmutgici8sTdfucLu/HRZfzeiIqKnGuyVWQzunCKNoQceAFEsn6Y4SQoEKsTJtfvsPrLK/r8EhF5CCUAiIjIE1c+uWufv/0R1b2WpZJh7GRLuMG4dOSmmasCYOSwLQkASfO/R/b7WirMSu4BG4kAJKOE4wStOVEnxnR0Nayce5ojLz7zhI5e5Mkwswe+qgyliIiI7EWWE+4OGF1XyLkGD3JA1S766ETkIFv728/Mrq+SJ4VBVTMej4kImqbR/EtguiY1cWfiQWM1tl44wZCb719a9NGJiOxaSgAQEZGFuPN3l+3Wx1eoR86RvESMCykSTTNkNBrhbEanLTY/sBT8Xzy3YOIFz0FVZybR0jZw8qVnOf7jC7pCsm9ExMaYmSUDiIiIiOwlpRRSSpgZdV33yQAFmgIfv/Peog9PRA64Wx9dId+fwGrLYLBE55DCtAYkpOkOFrfE0nCFWGtp1guf/PId2k/uaYIuIvIVlAAgIiILc/vnl+3WJ9eo1pzlqEmTwDqnqioAYlugTR9aOynRt13oh/PN+uqluqItHQB1yox9QixXHHnpWY7+2YuaosueV9f1vzi/BgABAABJREFURuB/exKAiIiIyF7j7pD6ympVykRXyA7LqSF+eU0BFBFZqPF7X9rt9y+x3CUyfbn30nbUZK0FHWAJGKSKMimU1H+WDSewfvE6o3e+1GeXiMjvoM9PERFZqBv/9gO7+cnnLI3hcDTY2MmRgT4g/ftK1cuTs9GGIYw6ZcKnFyYn2jLh7mSV7lDN8VfOcfjHzytaKnvX2eUYDofknLfs+FcigIiIiOxFYZCq6RzLnVIKEUGdMnWnexsR2R1Wf37Z7MYaVWdYJFIkEloQOvDcyJ7InZFHDrdH3ProyqKPSkRk11MCgIiILNztv/7A7n56jZWJcSwPYH2y8QG1fVe6Wz9kZ22vBLC9IsAs+G8BOcAnHYNcExht6Uh1RWpq1vKE1dzy1GvPsfzHZ7WaKHtSVVU0TQOo5L+IiIjsDykZEY6ZEV2hypkqMneufLnoQxMR2XDtNx8Rt9YYtEGTKvyblCqU/ScSk7YwrIccKhXVnRE33/8Mro01URcR+T2UACAiIrvCjb983+5+dpWmNZarIRb9R1TYwwPSshiJPgEgdU41vY0oBlZXeA4m3jKpnFETPPvmS6z8UEkAsve4OymlvlTulBIBREREZC+LCJzAcn8Pn6zCinPl408XfGQiIpsmF+/al+9+zGAUUKAzLSkcdFZlKMHyGL58+xPW31PpfxGRr0MJACIismtc/e3HfPnpFaoCKWa70sHTZiuAMCdMqQCPy1e1XEjTMasCMLSG3AEeRE5MkrNeJlg4dZ2ZpMK91HL6+69Q/0hJALK3RARmhrs/UPJfiQAiIiKyFxViy31MCsiW4P27urkRkV2l/YcbtvrJ53gpeDJtBjnA3MDqhm7Scv+Ta0zeuaHPLBGRr0kJACIisntcbe3mu58w/uIuw7ai6RLJE0TaCEpv9KF/iIcFruXxce+ICBL9jumqqqhzxWQypiSnzc5aKpx740WO/NHzSgKQPcXMCDYD/gr8i4iIyF6Wor+fKaX0EZXiLFu96MMSEXmoG3/9qR2NIfUkkaZrQhsVCecep3WgvWO2seR3fW/79cyeaMbO8jhz7a8/0NUWEfkGlAAgIiK7y+edXflv/t6W16BeD5bTEhaJtsR0whfUZuQIEpuN6TcrBPS97GfTiBT9mAkLQiXksPCvGA9PsJi1YJid5zYVPBtmhrlTO1Ql+oSAlIgIclPTlgldDs6++QLLf3ROJ172htQvjuecN6sBRCEZ9GkBIiIiInuHERAFopAwqlQz6DJXP7y86EMTEflK7/5/fsrJ1ZrGajovZIdcAgvIOVPCsby5YWR7RcOvqnAoT9YDG3qm63KJwCKIUqhzovMCyaiqhuiCFIlhyRy/V/HR//0vdSVFRL4hJQCIiMiu9Nv/4q/saKmp1gupg6W66XtyG0y6duNxaVssLmZjNsGYjtkH3maiwOP/N+xHs0SAYv1IRF8+1Ke7ijYeF4zbEdYk2uSs0nL65XMc/tPzip7K3qKWIyIiIrJPWEBKCfMgT4KbF68u+pBERL7atTW7/OsPaTqjIZHIVFWFuzOZTMg503Ud0K/xPGxDg9Z+di+frs01TcNobZ2maei6jrVR/+s0CYYtvPNvfrboQxUR2ZOUACAiIrvW2//5v7Z0b8KhkkgjJ3mii4CmonzFJ5hbP6APSM+C09lnH3qGoxngzrGtI/qvVVXReaEk8BrGtDSHh5x+8TmW/uAZJQGIiIiIiDxhHkaEka1iaTCEyWTRhyQi8jvdffuSja/cZKXNmGVam1YijCCn/utsK0iOmFaLnOlbSspibU/KmG0e8enGkq4rVFVNFKirAVY3JDKHSsXtjz/Hr9zTIp6IyLegT0AREdnVPv6Xf2PcWmPZK2K9I6canxbh9m8xBbDQh99O+apOCn2FBSPXNcWdNpyoM/fLhK5OnHvjJZbeOqMkABERERGRJyAAI2+06kpAuzYCy4s+NBGR3+vq2x8RN0ekFtpJIaWKuq77tm1pttVjcxPIPEWOd4fE1mqcszW9MCjhG9fTItHkCibO5PptvvyrD3QJRUS+JcVARERk17v4r35hk2u3OZKG1KWiTArMJQGkaZm35JBw0rRQvVs/YlrCe1YO7qv63MvX9/vOX9d1pCrjBq0XojZKDWvW0g0SZ998geaNk7oKIiIiIiJPiFkfR4kILn3yGVy+r8CKiOx+n67Zjfcvw+qExmoigjDDfbrWg5PCN2oTyu4yH4By5jbzRF+hM1UN40lHVTV0bQujQrm1yrV3Pl3A0YqI7B9KABARkT3h85//hvXPb7LUGstp6cESYrPhm8HpmJYTK2muLQCQFXbeMQ/rjm7RLy52XQfJSHVFizOKQqmC9dTRLVc899YrrLx1WldDREREROQxsumWy9J1RAS1ZdZu3l7sQYmIfAPrb1+30fU7LEeFFcMd0rSKyayk/KwZgOweic1NOM60CsD062x9zt37tg4eHM5L1KsdNz+8TPepktRERB6FEgBERGRvuOZ27e/e497FLzlcKnIk5nvOzyYUs8kF9D/q0uYIg+z90AfgowlLhE3TLh7SU69KiShORMEsKNHReYunwBvjbqyRTy5z6vXzNK8f0xxdRERERORxCyN7IiYF63QLLiJ7y52PrrF+5SZpEpgbJOsDx/RJACU5bd4MNMvuVKZLSMmnG3S6Qp0bUgvV2ClX7lJ+9YWuoIjII1L8Q0RE9o7PJ3b7wyuMPr/FoEtUnkjT4PNscvewJIAyHbPd6ir//2g2Mranv571bYPNc1/ajkFdYwFt21JVFYPBACcYd2PqQwOu3b+JH244//3XWXr1hK6KiIiIiMhjNKhrGsuM7q8S66NFH46IyDfSfXrbvvzwEtUYKgfa2cJPwueqPyr4vzfk6DfoDOuGGLcsR8X61Vvc/ODSog9NRGRfUAKAiIjsKfHJbbvy6w/wL9c45DV1ZLrWIVVMPMg5gweJwKKfDM4mgK7J4OMTm5UAEpAtQedUGE3KRHG8KySDlI3WO+pDA+7bhPFy4tnvvcLSG08pCUB2jfkeubNfz39fREREZC+xMKwDnxSuX7oG1ya6qRGRPad7/6atXrlBPQqW04BUgGQUS2C5TwSIfvtHSokILTMs0kbZ/7lPnBSbG3MsIJegnjh2d50v3v4Av6LS/yIiO0EJACIisvdcvG9X3/6I0Rd3GHhmmAe0nbN8aIX7q+tUlvpS/9GPFH3gfzbxcE0ldtTDzum0OQB5OrHL2+bcEUGHUxpjrSqMlhJHLpyheeWkZueycFokEhERkf0mIsiWGJKpi+51RGTvuv1X79twrWBrHTUVEVAICn3ydrY+5OHuv+e/JE+Cs7UipwXMLyFZJJbzgC8+uggX17ViJyKyQ5QAICIie1J8cNeu/sNH2P2OyjN1bri/us5wZZlIfQ+47FB5kCPI3gequ9QPTQMfkXk/2BxuvlEJYGNSN9eSIdEnY8wGEVhOlGysLsHguac4/tpzcH5FK5KyqyghQERERPa6TCYVw9dbWC+LPhwRkUfy4f/jp9bcb6nGAZEo0S9RZDdS6tckIgqmHpC7wGyLSG+2UadM2za0bcvqrTus/vKqgv8iIjtICQAiIrJ3fXzfPvuH9xlMjKaF1PULW2aZmOYT2yzgPP31w8qPyTczH9z/uo9/4HtmuDsRgaegzc567VQnVjj5yvmdPWCRR6Dgv4iIiOx1CUhmRNux+uUtRnfuLfqQREQe2bUPPuOQDWioqK3CioHHljUItXBbvNka3GwdbnZ9PPVJAODcu6fPJRGRnaYEABER2dPK29fty99+yqEWjuUhZX2Mu1MSlNTPLlJAmrYE0AffzjBiy5hN5nw6gtQPezDhom8XYORUgcdGgHWtmzCpnUPnTnLqL15V1FUWRotEIiIist9EV6itZnT3PvG5SiyLyN63+utrtnbtDoNJYikGNFZjbkT0axSz9QpZrPmEjK3rQ/0PUlOzdHQFTutiiYjsJMVBRERkz7v700/s7sdXGYwKK/WgTwCwzXJi0Af/8zQR4JvsXpeH2yjjv81sIjdLBID5JgGb3+vcyTn316I42RKVQWdBDBJHL5zh5D96RVdJFsLMNpIAlAwgIiIi+0G40eSK1KkZmojsH5/+v39pky/vUY+cxmog4WzO5x62biFPngUPbByx6H9QLBgcO8zp77y26MMUEdlXlAAgIiL7wrW/fM/uXbxGrLaYGcVgkjcnFikg+2YigHx7s3YKhj9kMu2E+ebEjrlJXgI3w8367013/ieMQQQDIFkw8o7VAay8cJrDP3lJV0ueuFkCwPbgv9oBiIiIyF6VU2L93n3G90eLPhQRkR11/YNPaW/dIwdEMnw6b8v0LQEUAFms2bqR02/U6VK/TpenG0tGUZjUxpHzz/DUT7QRRERkp+jzT0RE9o2r//p9G127xXCSqbwiRQI264tZQIpgVmYsHrKxN7H1w3F7CfvtPz/QYvNMfFVFhYed49n329IREVQpYZ1DccyCzgr3Y0w3zJx48QwrP76gCaA8UWYGcwkAW94TlAQgIiIiu8Tvms/M379YJOowbl2/yejD2ypvJCL7Svvbmza6dR/vfG6+Nv1qqnqyG80qc1pASkBTcc/H5KNLHHrxaU26RUR2gGIYIiKyr1z/H9619Q+/5NB6Yrk0JK+YTDqsykRljH0CGcJiOrYG97MnsidSJMLSRnZySf3js/fjIH+A9rv6jTDDtyVYbA6fju3f7/8bYUbk1AdYfZaU0Y9IQaqCNUb4oYqTLz/L8I/OagIoT0yJgJxovZBSAg8yhqWkpoQiIiKyEInNOUhYP1cJpl8NSEbgWEwf505iWomrQNNlmnKQZzEisp+t3b1Htz7uW52YUUpR8vYuUVJfKTLR7/qvfHP3f4p+La7rJnRVsHL2BMsvPr3oQxYR2Rd05y8iIvvOzQ8usXrpJkujII+DIyuHaEvHWjtmuLLMpGuBr961vt384zwpexz6JIBveia2JwE8TNj0gcno6LgzXsMODXjuzZcY/uGzmr3Lwmwmryz2OEREROQgC2ZLeTHd5x88eH8yC6oAmGUyRuWJdnX8JA9WROSJOXTiGIPlAVGcruswS+ScsVR947ULeXxs+vm0fV0oUhCDintlwvD0MZ7+x69p/UdE5BEpAUBERPad+HzVbv6Pb9v48k0Oe0W3NiFVDdWgYTSaUOcGg+kIbLqn14E2OSU5mJPDabzPTq6mM8bOEpP8zYPfstV8WdKwvv/bbAQw6TqqpqFZGrI6WaNNwYXvvMLJP3tJk0AREREROfA2kmunv+6ravX6++rNrIBkxr07d7l37caTP1ARkcfs1D95I5bOHCdymrYZzFRVRSGYlG7Rh3fgfdVmkI11IIBkeASTaMnLDcfOPk3zxkmt/4iIPAIlAIiIyL519f/7G1u/eoMlr6ArRAnqqiE63yw1Nrc7ZtYOoNhmgD871D7NUAZK6od2AX97W3q9Tb83O/ezHUw5Z9wdp1AM1nxMGWaOX3hGSQAiIiIiciC5we9KRU5BH0Shb3Xmc3+wrE7g0qpmMSKyrzz9Zy/HkedP44caRj4hzKmqhLtTSln04QmbG0Bm60Buc2tA00+liKDzljxsWO1GjLLz9Evn4Nyy1n9ERL4lJQCIiMi+dvW/e9vi1hrVyKi9hpJwUt+7nq2BaIsHJyGzx+S5dTbNPh5d9kT2rbchPnfOPQpmRlsmRAX50BJ3yzr3Usep157nzJ+pHJyIiIiIHBy+bX4y/+stibURmzsqZz/vnFoZzCKyz6QXD8eJF57lrrWsWcskFdz6wH9pOwKjbgaLPswDrV9rS1g8uP6zUQXSgojAUpAqo7VgUgdLZ07w3HdfXsyBi4jsA0oAEBGRfe+zf/ULa1Y7mnHQREVlFfDwRbT5Pt/OZmYyzPXRfDKHvW8l2GjBMONzu//dwMzAoq8EQGFUxnhj+FLmLiOOvHiak3/6spIAREREROQAcmyuEsD84t78fbVZxtyItuPejVtP/ChFRB6n53/wOndtwsQ6vILIQeeFiKCua3LOqgKwi2wkqm0bABFOzplxO8KaBIPE7bLGoXNPc/x757T2IyLyLSgBQEREDoSL//Knlm6OsbUy3XmeCIywzVB0H5jenIZ4CsrG6B+XvR/be5fJt+dsLlLOLA+GtKMx3nXUdU2JjrFPKDWspY77tXP6zec5+oOzuhIiIiIisu897KbX8I0k5TSXXhsGJMPMsM6xiXPv7y8rj1lE9o1z/+wHUQ41+JKRakgJ3Pr1G8uJnDMWidJ+ddsUeTLmW2/ObC//n1IiSsG7QpgzoWPVx6xay1NvXHjixywish8oAUBERA6MS//1LyzdGTNsbbP8fDz4UTgf3HeDMldCM0c/5PFJAeurayw1A3JKdF1LVVVYToy7MT5IrOaOOz7mmTdf5Oj3n9UVEREREZF9L9jspbxdiuhbmgFgEIkUiVwMa7UDVkT2j+N/9kpwYpn1qtAmp0QhSkdEX0UQYDweU0qhaZoFH63MSv3D1o0fMwbklCCC4aAhoq/ksHxkmbtlndGS8d3/8/9a6z4iIt+QEgBERORA+ey/+qlNrt1jOWpiXLBI5KqhI2hL33c+potnM/OZydn7oQ/QneHMzu9mJQYLaFKG0u9oqjDcCxCkbLg7VM6qjVg/ZJx88wKHvvOMJoMiIiIicvBYn6rs7v0OymBjXpMwcglirV3wQYqI7Aw7fyiWTh+jLFWs02J1JpmRwskYKTZ3lFeWiE4JUIu0UfER5hrXbG0KaWZQnCr6r0aQzZiUlqgz44FxhzEv/dMfaN1HROQbUPxCREQOnKt//z7rV25w2JYZpIbxuCXCSFWmK04y66cjsa0awMKOeH+ZnwBuZ7E5DZyd/9n3Nq6FTcvD1cbtGDFaSpx45VlW3jyjyaCIiIiI7Dvbd0w+rB2ZWf8gJ6iqBnfw1qk7uPrxxSdwlCIij1f17OE4/eoLxOGGcS7kpmY8HsG0HcpsbL5Hpm1fZRHmKwB8VTtNm1677NNrSP/VzWmzMxmAH244/F2t+4iIfF369BMRkYPnyqpdf+ci61duUI2dQWrIuQbL/cJZ9Fnjs3L/873KSuqHPJrZBDDmJoH9sC0rnBs9TacTwFkFBu86cs6UbKzmjnj6ECffeE5JACIiIiKyr22vVPawJOWUEniQAho3+PDeQ4oui4jsLUdefIbh2ZN4ZZTSkivrqzhO1xe2V2ycDzzL4mxb5tm6/jNfCYDNdZ9ZIkcCSmkplTM+XHPylefguSWt+4iIfA0KYYiIyMH08V278dtPuPf5DZY8kz3hXZCtArbuPp/PIPe5ITtrdo7nb07mJ+uzJAADkhmlFHI2Sg7uxAhOrvDUa+dYev2UJoMiIiIiciDMB1UiArO+ZVYyY5BqbKTy1yKy9x3/g3OxfPYk66mjs77NiXeFpqoh+lWEtG0Dx+bKjVZwFiVsawXINB3bKwHMHjez+fMgKHRW+ioAx4aceeMFOF1p3UdE5PdQAoCIiBxcn6zZ7Q8us37lJvWoY+DW7/jf9jCb+15YIkwfn48qzAnzrYkWzJ97I2xzzEL/aZo6XuWaKI4Vp8kVkYL7qcVPLHPqjfPYOWWEi4iIiMj+tRFUmVYBcOvL/+ecia6QMSqHW59/sehDFRF5NOeXYvm5k6TDDRNrSSnR5ArvCl3XTR+UmK0bzILH23eey2K4OVifhDG//rNxnTAcw80INtd/LPprWaUEFEY2YbUqDE4d49BzZxb0rxER2TsUwRARkQPNP7xtNz64BHdGHLYBqRgRMQ08P/zPKLK8s7YG/nsOFOvH9ooLCShtS13XWEDpJqQ64easpTHpxBJnv/cKnB3oUomIiIjInvdVPZNn5uctKaXpn0nEpOP6xUuP8chERB6z08vxzHdfwU8s0aaCpX5HOEBVVbhvvkFubzUIWr/ZLZzp2s+2KpuzJA1PbLRy2C5Z4BTCgqiNtSo4+eJZVlT9UUTkd1ICgIiIHHjd+zft/sUv6G7fJ5fAYi4AnZQx/iTMssCJhNNXWQjS1ong3OPdfSNxIIqTS5AJwpxVJiw99zRHXj8PzyoJQERERET2B2NreWvfdo9sZnRd11cBiMAi4Fqn2YyI7FnPvniB4bmnGA+NEh1VBO4dxVtIRs657yUftrGJoG8JoLDHbrF9TW3WahMSRKJYvw7kbCYBzP+ZEk5EkMywFLTZSUeWOfXqC1TPH9Oaj4jIV9AnoYiICHD3l5ft7sVrDMYwjIocAP2sw9k6WzFUSm4n/L6dTA8zW+BcHv7/2fuzLzuy7M7z++5zzO7gE+Z5DASAGJPJTGYmK0l2kVVUV0vV/VCSlqpraS39c3rU0mP3Q7WqusSiyKwkmcw5MkZEIAAExsDkwx3Mztl6sGvu1x2OmODAdcB/n7UsHPApDGbXr9+9zz579xkNhuScmev1ICeqeozFSF0G7o6XOfz6aY68cxGOdxQQyneW9XJZREREdpF2d+TGAsrmuKSuawor6CVjzssXf4IiIjvETi35wfPHeVitUoeMByeEQIyR5E5d15g9mZiZ3kWutM3sTY/VhCd3+k93A2iLAKY5TYFb4Uaqaopuh4fVGvFQn/LownM+exGRl5cymiIiIhMP/+Ga3fvgGvtTj3IcKGsj0uyeqXNuAo5gmPl6AcDTCgHcJrvYp9txsn27+71oevG/DfCaILBp+G+emylwPtUdYOrzR9WYstshhEBVVbgFQiyoySRLxF7B42qNuZMHOfTO6y/4XycvO7fctBecbh1pG7MIRURERF6k9bhj8vfggZhD00p5+vN8sjjmmbkRXP/9J7M4XRGRHXHpX/+QO+WAUBghOxmnCs1bMyNagLxdvf9UXsHzNh+XF8Uc4iSnk2m6bG502mzyPtGbo837TI+BdDOq0HQJCBmKDF5XhK7xOI45+vZ5uLyoTR8iItvQGoSIiMiU5V9ct89+9QEHrE+3DlgNZSiJsSR5pq5rcv76APJpu9u/y673vWA6wGttt/j/Tb9XwslFYBgzSycOceQv39CVl29u69xIrfmLiIjILuGTVN52c5RjjIQQoEqUtfPw19f0KkZEXkpv/Ic/90dhBEsdsm0sIG+XO5DdbTqvs/UefrO8z0YhfnAwMtkyVcwMy5qLP/oe9vq8cj4iIluoAEBERGSL1V9+YXc//pzu2JgvOqS6JuOEWOLWtB6zdqYmG+3MNu/ub6vNmXqPFhJ3jIfJXL/t1Z6hiCQcOgWHzp5k35+eU0AoIiIiIi+9bJDD5iWwhDdzk90xD8x1+jM6OxGRZ3Psr9/21C+xsmA0Gs36dGSGzKHMTMZ0bowOaP+eAoznSg69fnY2JygisoupAEBERGQbd//2E3tw7RblGHrWIY8SOSWKorNpxtx2Vcr65friBZ68F8lrvICVPGIQao5dPM/Bv7ioIgAREREReals7I40smW2fUFrhtMMVg7ZWX34+MWepIjIDui+fdTnThzkYR4yoiJGQ3v+9y7zpu1/kbffVJMMlvOQ/omDLP7JaeV7RESmaI1CRETkKe7/7RW7d+ULFnKHuVBClYBmVw00DcgCTsBh/Whs7gawQe3qdkaeCvrahGhTBNDcjxiNKie8AHqRVSqqrrF07hhLf/aagkIREREReamYG+bN4sd0EcB6EWwIuDsdixQpcOX9D2d1qiIi3825RT92+RyDIlPun2NQD7E465OSWQpAzM2BOckcnxyQm4KA+S7DAo69eYHuxYPK94iITKgAQERE5Cs8+JuP7OaHV1nIJYvlHGlYE4jYpOw42+bFaHlxtrvuRpMEdXdCgBonhUzuGCtWMeoHDr5+ikN/dklBoYiIiIi8NKYTeOs7IC1Pvc+o60zMgX4oGH38UFGKiLxULnz/TXyxQ1U4g/GA3lyXUTXWKMU9zByCb+74mCZ5ODdwyySvGVnNWqg5/4O3ZneyIiK7jAoAREREvsby335iDz/9gmK1pltDgTW/QJtoA8fWCwGyOdk2OgJs7QTgFnDTr99nNZ0AaP+8HhgC5pkQjZxrRqkmF1CXMA6JYc/Z9+Yp5n96TkUAIiIiIrLrteOuAushCNk29xVL3nQFiMlgWM/iNEVEvrN9P33N02JB7gfqXJFyRdHpEEq1AJBJB06f/h0I4M3vxjoTY6TqGKOFgiP/6k3lekREUAGAiIjIN3Lv//uRPf78Nkuhhw0SMW/+FerYpiEAT+sKsHVOvXx3btuPUwgOAYOUMTOKIpBwMokUnaHVPGbM8bcvcPgvLuqOiIiIiMhLZdNrYGsWQHKGTtGlSMbag5VZnZqIyLcW3zroS2eOMO7AiIpuryQAa4NVctYQxb3MbfPATfPNnTgNx1JNt4ikEh7lIXNnj3D4X15WrkdE9jwVAIiIiHxD9//rJ3b7g885VCwQayN6wCYD6ZoqZCN5Jk1Ck+kigI1fuBlzBbDPLrPd8n9bYBGyEzGiO+REIINlnIQHI3ecZYYsvX6Co3+l6nARERER2e0CeNh4BTzpOhYmr2TNDM+ZUGeu/fb9GZ2jiMi3E19f8nN//CZ1v2Acm/buKSWCGWWIRNT/f69LoTnAMLf133vQbAApCYyGQwhOsdhjEDPx4ByLbx9WrkdE9jQVAIiIiHwLj698wRd/+JS50CVUwDDRDR0CkbquiWWnSb5t87X6pbsz2pb/663f2DwSYL0IYH0cwJbOC9YUaVSFs9bJ9E8f5Kiqw0VERETkJWUOZKfwyHzZh6SXtiLycjj02klGHahiJoW20L8pbgqen5j/LntLZlIUsiXnEzPrpSFmRgiQcmZUDckl9A4tUR5ZmsUpi4jsGlqLEBER+TZujezh31+xB1dv0s2RA/0lxitDrIb5/gLj4ZAYi6kvmG5WJs9TUwRgtFXhTWW4Ed3WiwDaw3ONxcAg1AwXIvOvn2Dxp+d1o0RERERk12mWw5oOY4GNZJ5NdcQqQ4nVTl4dw+2xtsyKyK5X/skx7xw/QFU4OTpmjpuvd1OMORCzFjD2MjfIFpqD5rEQJ3meOPkVWJvjMWDBySlR12NyacyfPMSBn55TnkdE9iz9/hQREfkO7v+nDyw/GsDqmDnr0KWkHlR0yx51XdNMIttMjf93Rlv9P70LoL22bk9+rnnzgme6ACBguCdyMFbTkNWYOHzpDId+ekHBoYiIiIjsOtk2UnjmTtgyVixgMK659fn1F31qIiLf3uvzfvKt1xl3IUfHQ7P4DxtbKJouALM7Rdkdsm0esdl2AFj/OJA8EzDKshnTOaqGMF9y9NI5Om9oFICI7E0qABAREfmObvzslwxuPWDJupTJ6VqEKhFtY09Oazo9t3WRWr696cX81nqzQNs4pk3vlirKSEo1nWCUZcmqDxj1YeHcERZ+eErBoYiIiIjsKk7z+tacbSdiW4Ju6PDgi7sv+tRERL6dU6Wf/+G71F3DS8Mtg/nUc5uRMbI1h+xtbSGIWyBvWs6adICMgeS5GctpgU4ZySTW8oi1WHP4tZNwoq88j4jsOSoAEBER+a5uu33x2w9ZvnWfsjZs7MyVfTxtLE5PVypvtygt301g84L+9KJ/Wwgw/f6tX+vuxBDwlDEyRVEw8op6LnLk8hnm3z2m4FBEREREdgW30LyuZdLNyjMGzaIZG92uOiHCuJ7lqYqIfK2l189hSz1GuSaRyOZPdFDcGuPL3mSTxf82rbP+mJjK87g7MUbcoEo1GceLgBeBodWUBxc4/Mb5WZy+iMhMqQBARETkWdwc2fXfvseDL25TWmA8GDaJt63MAX/q7nT55poEpxPz5hTB9LVtK8MzAXzLAeScCSGQPeFVRWkQQ6AOmbof2H/5NJ0/OqoiABERERHZFZrFjua17HahRHB4/OUDuD1QpCEiu1b5g+N++LWTPBosU3YLPKX1DidtAL6+oYJAMi1f7GUBiA4xN48DB5I1j4s2r+YpU4SIxUjlmWGuSQYWI0WnJHciB04fpf/H6vYoInuLfoOKiIg8qxuV3f/8FoM7D+mHDlZD9NDsUH/KSr99g7Bjuy+d3vW+1+Vtrs/0dW2v39bPywAWSJMigKIo8JTJuSYFGISacGCew6+fprx8SAGibPJNfnZFREREdlI779i8eY2bLDQLZR7IBuaBMsG96zdnfaoiIk93+YCfe/syuTTKuR7ZIIRJYdNUnLWp09+LP0vZhYy8bb6nlXPG3DdyPAZ1XTOqK0Yhk/qRQ6+fgvPzekiJyJ6hNQQREZGd8OEje/jxTUb3HlPmCCnjKRMtELMRMYIZqaqIkwr24GH9aJf2m/aeYctu9uZ/0e58N/c9/Qs8A26Gm623Amx3CGwcGfN2GEAm28bRfA9vvt6d2jM5gps3bVRjYERFcWCeg5dPwesLChD3mmAkHDPD3SEE6jxJOGQ9HEREROTFCmS67liqycGoo5EtYBZxjJCM+TrQG836TEVEnu70G+ewbmCUakKMjKuK5D6Z5D7Z6d1UN00qAjKYBgDsZU3+J09G3uRJIUBztBkfM4PsmEPBxp9DCHiEuufctwF+qM/Bt87N9h8kIvIC7eX1AxERkR01/uCePb56m7gyppci/aJHVSVCEXGH8XjMXL9PqqsnfgGHyZri1t3FT8yv19ojwHqgN+2b7szeek3bL8uT2YLJMtYJDPK4mRV3+Ryc7evK72Ea2yEiIiKzZg7BnEwm0czLdgd3I1ogrw7xNVUAiMju1P+LM945sEAVEmOvGKYRlEYsi/XPaQv6p6kDm0znf77q8bB1Ywg0cfzIKyqrGPqQpWMHOPUvL+tRJSJ7ggoAREREdtDg17fs/vuf06sC1eqIstNhUI3JwZifW2BtbY2imAS4ljcd7dT6ppp5++A3hc073+Xb23xtDXN7orBiWFeEssDKgqXDBzn11iU4o1Zxe4Y7Pjm2MlMlgIiIiLx4mbTeKrt9jZJzExUUVvDw/iOGnz3UCxUR2XXmf3zGT1w4Rx0msXanJAWwGKhzwmkrrqcOVWDLDimLSDcW5FGFmbFw/gScLpXfEZFXngoAREREdtjyb27arT98xnwqCHWzK8dCwShVFLHE3bfdvd4u9rcDAbYWAWS23/ku39121eNuzYiAHJ0RFeOYWTx+kBNvvganuwoS94KphX9316K/iIiIzFy2ZrEMd4JvFCWaB8xhuLI64zMUEXlS741jfuT8ScJcyVo9pDIndCM2GbPWFjJtR7v/ZSeMB0M6ZUFZBIY+ZiUmLv3rn876tEREnjsVAIiIiDwHK/98w1au3qU/Csxbn/F4TJWacQA5TXbssH0b+/ZoCwFabRtyFcI/u6dd4ybD4HR6JYN6TIpGKiOPfcTciYMcffciHItKQ+wB2+3+FxEREZmFJm5oioiDQ7Bmh6yZURLIoxoqvXYRkd3n0PkT2EKX5XpE7hZ4NzCqK6qcSJ4JRfH130TkOzKHXtlhuLxKiBHrltTdyH0f8/p/+Jf6xSkirzQVAIiIiDwnd//uY3t89S79yuhR0o0d6jpDsE2L+ZntF/anOwK0VADw/EyPAUgpQXC8gCokRiFRdwO94wdYeuv8zM5RXrytu/9VGCAiIiKzkGF9RFHEmi5FRAqM8coaa49XZn2KIiKbHPqzS949PM/IagZphPUi2WBUV8SyIITwlfGVch/yrAJQEujEglGuGXlitRoQF7oM+8a+v3xDAb6IvLJUACAiIvIcffk379vg09ssDI0wSHTLDilv3s3vBsmcbI5PDmgOtbx7UdpZg42qGhPLkuQ1Y68IvYIhFaOQOPTaKeZ/fMY50dPdEREREZEXJuOYG4EA2TEgJGP4eI386SMtlYnIrrHvj0/5vjOHqbuBVDTF9clraq+xCCE0mx48pSe/2NtefU/07BP51vK4agroYsDLSKfXZVCNWQs1xZFFODuv3I6IvJL0G1REROQ5u/03H9jq5/c4GOZgUFNYmAS0jaYIwHCa2Z7t+1ptq/rp8QDy/BjQ7XbxVOPuhMKoyAypqGNmHBJnv3eJpddOzPpU5QXT7n8RERGZmdAECGZN6393JzgUGdLacMYnJyKyIV7c50cunSH3C8Yxk0KmKIqmg4kZ0QL1uMJToghxPeex3iWx3SzB5FB5kzyDEAJlWUIwRvWIVI8py4h3AuX+OY698zra4CEiryIVAIiIiLwAd//mA7v3h6ss0qNMBV4n8IARcTMIgWRGoqlKhqcFuaqA3wluAbcwmae6oR0D4HUiYM2Vzs1nhQA5OFVIPMhrHH/zHAf+uwsKEl9xbatdeHIcgIiIiMiLtF6MmB2zCNmwYY2N8ld/oYjIi3Ks9PnThxnNRUadxCgPm+cuz4TcbGiIDqUFikk3k/UMh2/eLCGyE5IZ45SwyePOzMleQ07UlglHF1m8eGrWpykisuP0G1VEROQFuf/3V+z2Hz5lrg7M0aPIAfNA8EBdZ3LOECJ1TuudAKa1XQDk+WqLAKaHAkxf9xQy3o88qFY5eOEUh//1Zd0VEREREXnuQgjru//dHaszg4fLrPzzNVUpisiusO/iGY5cOsuodIY+JnY7QNOxJNAs/rfHdtsb2t3/Ijshs9FZon38NZ01M0bGLTMqnH3njsGZvnI7IvJKUQGAiIjIC7T6s6v26INr9MfQyZE4dqxyurEgxkjOudmdzubAd3oBWkUAz848Y76xU2qjE0Cz7B+8PZr3BDbmL2RglGpyv2TQcfZfOMWBv7ykuyIiIiIiz49ncm5mZbs7Zka37BGG28zPFhGZgQM/POOHzh5nGBN1B4Z1RZ3rTeMMtzvanf/Z2sh8ctjkEHkGmebx1T7e2sKTQCZbxguoOsaZH7wJJ7vK7YjIK0MFACIiIi/Yg599al9+fJ3OWmLBS8LYiTlQWkHOzc4eN2O7qCMoFHnhtl5zN4fgVNSMLPM4DTh04RSH//WbujsiIiIi8vxMdv67QSQ2rYxH9azPSkQEO9H3Q+dPkHqRR4NlKIxuv7v+8eCbj1a76WG7LoibigREviO39vEVmk0e7TSdyX8ziTE1vRMHOf72hZmdp4jITlMBgIiIyAw8/vtPbeXaXXp1YDF0m7mdlVOGpgtA06bMNrUra6kI4Nlt12oQpjsBtJqOAJnNNyHnTLdbMkwjxsEZdZ2lM0c4/tcqAhARERGRnVdY8+rVzDAzUkqMVtZYufdoxmcmIgKv/ck7pPmSodX0F+YZj8eEECZdDpsF2Omj0e7838h7uIHhmw6RZ9Eu+LedNtu/uTUFdeaZshO5P16jc/IQc+8c14NORF4JKgAQERGZkS//9iO7+8l15rxk3jpYDYGITy00u21dkJad9E1eCLXX34311ENZlgwGA/pzc3gBy6MBNl/SP7Kf4391WcGiiIiIiOyYQLOAEbGmJtUiAGurqzz+8I6mZYvITB3779/x7vH9VF0jFQ5kPCWqqgJ4YoEfNuLrvGXDQ2u7bgEi395Uw397coMNODFGktfUhTMIieNvvYadX9IjT0ReeioAEBERmaEv//4j+/LzL7AqUVjAUl7f3bMtzcB7bppAMOAWpqcObvkcw82o65pu2WE8GlAEo+wW3F9dpuoZ+84eZ/4HJxUsioiIiMiOsewbuxjdCSEoLBCRmet9/6TvO3uMW8NHjCNkd8bDIfNll2iBstvBJ1312hi73eiwsfif8W2e0Iy8PrNd5LtqC0ncIE0d7aMyk6lpfq+GXodqX4/zP3hn1qctIvLM9PtTRERkxm7/lz/Yyu0HzHlBkZudPd/kF7RvU0XfCluOrV8j387Wa2bejAEoQqSua2rPxPkOw47zOIw590dvsPD9EyoCeFnpZ0Rk19r6uy96oMyB6M0RthxxmyPA3nw7ddjkWL8u7TXMkZgDRQqUyXbVzN0i05w/u+R66q3evsC3zQzsSElJrCBW0M8lHS++wU+PiMjzc/TiKR7mIbbQobZEtkwZC9ybFxHj8RjYHFNPd9lrta85Nr328Olshsh3l6c7T6znxQJtxszdidEYVUNS6dQLgaUfnNpFr4RFRL49RQoiIiK7wJ3ffQxA/8xBHo9GhDIQAtS5IlqgKEqqqiLXiW6/x7iut13INw8Eb9Yvm1Z5mWwQJhXO2wXYe9F2m6XMn/aRzdfKHLC4fn1DCLhlKs8QwTuBR/WIY2+fpwzRH/zyupaTX0JOc6+Ds2nqZABNoRSZkSZZ5xAD46qmYyWdOtC1kkGqIYT1WZ7t8/lG29g2gZwnsz7DnnybJl2Ggk/aiW/zhObJmfNIJ0G9i1IGvVwQaqOIYebXUW/19kW/haZQJ9RG10qKHLBh4qP3r876R1NE9qpjhR//4RukhUgqM8kyIUM0g0nBvAcDizB53bGV+VPamHjTLSBY81c1O5HvLm/qNLHxOLT1uN4yRAtU1YgiFtRpSGehw9wbx3m88qXz0dBmcuoiIs9o90TzIiIie9kXQ7sTPvGDljjy2ikejB9Tx0xZdsg5szoY0Ot06PRK1gYDYllirt38L9p00iLbk62UMpBCZkii7HfZ/9oJxuOxr/5es1lfNll3TGTXMQfPmYARLRCqxOjOCtXIqciEIpJSwi2vJ4rjesI5sNfTx20BgBvEDEZzbYJvPOfFGMkZygTjysnLg5me87SHt+7RLWtGxd6+j7J3hVBg2QjJ8VHCRgk+eaBXLCIyEyffep3y4AJ5viDlMc0qP5Nq6Y2npmfZw6/f+LITthsv8cTnuBPNiIVRjcasVon+oTnO/viP+GL4O6+vren3rYi8dFQAICIisltcH9hy54YvlF0Wji3yyEfUDh4Ni4HkTsCJMTZBtEN+ooo+ry9MJ2O9SCCzefF6L+/+f1bTRRd5Mo+wjSfbj1lZMMxOb98c+y+cpqrx8QcqAhAReRYBKD0QUtP+urq3zINffwrXRnp+3QMefnzT+PjmrE9DRERkz+u8dcjLI0vQK1kbDrAyrn9sulA+5LaTiZbyZXba/NfTNtDE2Ix2dHdybh6ro7oihEC/X3Dizde4du33L+hsRUR2jgbpiIiI7CLVlcd2/dcf4g+GzOUuwQN1nSnKLm4wrKv13f9t696NqWUbkm0c06F2fEq7X/l2nI028O3uyZgnOyonOymHVvPIxtjBOY6+cY7w+kFdeRGRZ+TJ8eR0Q4eedaDS2r+IiIjIC3Oi9AOvn2HUDVSFkQJgzvQrsjYH0cbJIrtZVTWL/WZGzpmyLOn3+2SDQaroHt3P/p+cUz5HRF46KgAQERHZZfK1Nbv93lXsy1UWcjPjc5xq0qQTQJ0qwAneHOaT5ehJVUA7/9gNctiocm7i8mZurl4A7ByfdFowmilywZ0qjfEyUneNtZ4RD89x9I0z2PklBY0iIt9RBrxXkoqCFCM5BM3CEREREXmBDr17iXh4gbRQsuZjik6HMCmKn5ZpY2QtQMjuYL79kXMmhEAoCuqcqXMi0+TV6BSsdTIHLpyg84Y2dYjIy0W/f0VERHahwft37N4H1/H7A5Zin5gDuW7a/2dvApFsm+eUbxdwZ6ba0k+C8q2fJ9/OxmL/BreNjgDZmhZyKVV4AVVILFtF9+g+jr/1GuHcou6AiMh3VGdnlDLjVFNnh6ACABEREZEXYeEnZ33x1GHGXWNomZFXVKkGNvINoPpMebl0Oh1yzuSc17sAVFXFaDSiomZUZNJiyYm3XoPTXeVzROSloQIAERGRXWrw/h27//7nhHurzHtJkSGlRCzLpr1/aI7tg2vfGHQG6x0BZGe07f6hKbKoAtQR6sn9KIoCTwmqhJkx9opR4XRP7OP49y7O9NxFRF5mZk5ZNC06J++Y7QmJiIiI7AVnur7/zFHWrKYuYUxNLAvCU4oxp4vkRWZpu7GZW9V1jXuz6abodOj0es34zRhJllnLA3x/l96ZIy/ilEVEdoQKAERERHax1d/dtIcf32R+4OyzHjZ2UkokM/IkoN7aCaD9Y/Anf9Fv/Vz59gJPtjJ0oynKmBzj8Zhu2aETIiEnyjJShcRaqCmOLHL+3/0L55gqx0VEvi1PGcve7DDLehoVEREReREu/viPKPbNkQtIXhMLI092/38VpymaF9mt3B0zoygKcs6Mx2NSSsBkE04RSMEZxsTRS+dY+NFpBSEi8lJQAYCIiMgu9+hX1+zBB9for8FimIMUCUWkcieZQYy4O9mgsEBKibA+z8wnx2QcAArAd0Y7YKGxUQBguBkhBCxlQl3TSZnozeePi8xqmUn7u5z7sx/M7OxFRF5GASiAImciThHAXL/RRERERJ6nN//9n/loPjKgwi0TkhNrpwxNLgJYH1M4LVsmm16ryWxtzt48yd0JIVDXNWZGKCLJMwTDzJoCgSIyjpmqZxw4f4Ly8iEVAYjIrqcCABERkZfAl/901e5+cJVeZczToVod0e12MYuMRiOwph1yXdf0O11gY5f6dAyuDgDPLvPkSAWb6rYwHVjGPDUL0TLJMnWoqTrOuA8n/k/fV9AoIvIMNN5GRERE5PnZ/4OzvtqFQSdTT+bgBc8UuYlzjafnGTSKUF52bkDKTSFAbEZf5PlI/8S+WZ+aiMjXUgGAiIjIS+L+P161u3/4nMWxsa+Yo14bQ+X0u3OYGVWd8GAkmtbI7TFNAfjOaFv9Q/NiKjqEvP0LK6PtxjD5uGWGVITFHvHoIkf/x++pCEBE5BtoC7BScHLIJPPNVW4iIiIismOKc0t+7OIpRh1nXDo5OEamzFBmCG2CYcthinBlF9mpPJg51F6Tu4HF00eZ0ygAEdnlVAAgIiLyEnn48yt278MbzFcF5dDoJChzJNfNzLIQC6qq2lR9H6Z2p7cjAFQE8N1N5zYymxf3t3YCcNt8vdtxDO6JoQ8ZlIny6D6O/h/fUeAoIiIiIiIiu8OJwl//4Tuk+ZKqcHLYSCK0MXC70D8dI09/jsirwC00oy7ccZpOGMW+LvvOHqH3zhE90kVk11IBgIiIyEvm/s8+trvvX2VfKtgX+6SVASEZRdEh4xDj+udOB+XNO3JzyDOalFKY41MXOOZAyAHzAARSgDo27RDd2pEMTjSn9prQL1hhQP/EQY7/m7cVOIqIfCOGecDcwBXSioiIiOy0Y+9ewpdK1kK9aWF/a5dB2Lz4v7UIQIUAshtsV6TyjQUj1c0YgBgj41wzoqLcP8ehC6d2/FxFRHaKsiUiIiIvoUc/+9Tuf3IDWxmz2J2nDJGUEsmt2XnOxgy+oKB7R03vdmivc9racWFyvfPkSJNuARsfczrdkuF4jdgtWLOKpTPHOPgvLzknurpbIiIiIiIiMhOdPz7u/WMHeGxjxqXjlpuxd1Ofs3Uxte2ANy2gxQeZrZ3ofuluZAwjEGPE3RnUI8YxUxxcYOnPXlMOR0R2pWLWJyAiIiLfzaOfX7Wcsx+5fJbYj7jXuBk5Z4I9OXcvAO5q/78T4uTaJpsUWlhzbePUOOrgkELAASxjvlEE4MFJqYKUCQXEfpe7j1c5fPk8yQKP8jXn9prulIjIFhkjEHACbkG/1ERERER2UHn5gJ+8dI6BZep+oLJEyJNY15v4N9C8zYTmpZgzCYQ3ug22I/IAgk1/RGS2NkY0Nm+nxzhOv3+9qwVgZs0YSHeCOe5GbYlYBI5eOAuV+eN/vKLARER2FRXhiYiIvMSW//Ga3Xz/M+IwsRj7hOSEENhYht7sm7Tg2651X0sV/JsTGd/GplaIZlh2SgJFiKyN1si9wKM4Yt/5oxx96/xOna6IyCvFJ4VXWek1ERERkR1lZxf85BsX8IUOPlcwzmPcfVP8274We2K3v/ZAf63AkzmV7VrTf5O8y/TXtV/7tO+/l7WP3WfpihmIxBghOzklzCJFUZALYxhqVoqaQ6+dhGN9/RSIyK6y13P4IiIiL73BL27Y8qd3CCs1nVwSqmYOfWGRwiJmATOD7Jg7RYjAkwFjc7S7KrcEk+YEmq839z39AqJpbThpAeft0e6GyPjkyJYxzxh5o/CiuZiEFIg5UhLJVU2MBkVmLQ5Z7lV0X9vPif/hHQWPIiJPEXwyZEUzbkRERER2xMKpw/jBOVaLxIAhZYzE3OyLblv8T7+FqZh3Ou6d+vyNz927fHJhzCFOXr4GmmuTpg7Y2HDQfg5sP7u++dpAmuRwIGAORWr+H4Htiwv2oukxjtNHqx3d+LSvw5vWF8EDkdB0e8yZisy4cKqOM5pzTv7JGy/oXyQi8s3s5fy9iIjIK+PR331sj67epT+COS8pPFJXmXF2anPcnVA0VcvVaPTE129dP3laF4Cw50P3xvRV+LrgcWuAmQEnkC1sXN8mqiSbU8fEaqyJh+Y499+/q5UtEZGn0Nq/iIiIyM5Y+t4pP3j2JLkXqWNu4tacN+3sz1vetrZbVG0/b69nEKbbyLe+SbeE6WKLrTv84ck29dPft70XKr7YWZO9MUx33HRzcpkJcyVhX49jf/22IhQR2TVUACAiIvKKePxfP7S1q3eYHxulW9MiuQjURUHlmZwzZkaMEcObY6qyfD1oB8zD5Nj8/0iTRWsFkc8mhUmVf5jMUPTm6LTV+iEQF/p0jx/g6F9cUgApIiIiIiIiz0X38lE/dPE05VKfYTUkJKcbC7I7Oezx7eM7oukEuHWUlTnEqWN6x38GcmD9+k8XWcT1PE4megZrSi2yPbnrX4s/z65NyKSp62o0uZvoUI3GrI5WsbkO+04epfPmMeVwRGRX0O8AERGRV8iXf/OhrV6/RzFySgqyBTIORaDOmbquKYpifcF5uoI80Lxvu8p92AhUNXf52bg1AWTe0uqvDR6jQwiwWg1YK2HfhRMs/fi8AkgRERERERHZWcf7fuzN8/j+OZbziHGqCWZEB3eHYHu+hfyz2NotcGu7/+kCgCfHJzx54dtcTnQIeXP+ZrsCA9kZ7UTHdjPM+j1wKMuSZFAXxhoVRy+emeWpioisUwGAiIjIK+bW7z9hcPMh5diIGeq6hhCIZUHtibqugKfMQSMTyJsKAcxp+wWsH/Ls2gKAHDYC9JhZv2cpGlXPeBgqDlw6rSIAERERERER2VGLrx3DD3ZZjmPGRaac62DmTR5hsnNddkazo39jIXl9R/8kD9Ba38XvzThHcHwyNrDNzgR3wuTP7ddsHRkA23d2lG+v3cjhU8UV7X2LFii7HVbziHHXKA/Oc/AvLuuqi8jMqQBARETkVXN7ZHc/+pz8YI3FXNL1AlITqIQQsEkV+fosui3xfBscbjeXbms7Ofn22jEL7fXNTIoBJq/KDJp7FGGYx9TdphDg2BvnsAv7FESKiIiIiIjIM1v40WlfOneMYQcGoSZbhmDUOWFmFEXRjBJUFPrcTOddpneYm0Mx6RI4/fHWxjz6J7++PZ7W3VG+OTfWRyxMvXfT51RVxbgekaNTF05VOEdfP8nC947r6ovITKkAQERE5FX02bI9+ugG6eZDFqtIkZxcJywEKOJ6W7itVeLri/vWzJEL3g4JmBw+OeQ7CzSBfBvop9B0AqhD82cHyhBIVYUxqTIv4bEPOPX9i8TLBxVEioiIiIiIyHfWef2AH3z9FHmxSy4zFiGRGNcjkjsWI9EKQlb8/6zMp4cvNpoRjBu7+lPwyXgAx90IyShro1MZ5rbe3r/NGWxOCjQdArRZ4/naWnTR5tRC0dzbTqdDypmxVQxCYvHMETjbV/5GRGZGv8FFREReUcM/3LH7H31BZ6Vmzktialb7q6pa/5w0CVjaOXRb65qB9XEAsnPW58VN/pwn7eTSJKAf1RX9bhdyIgZYqwaE+Q7x4DyHLp2Cc4u6IyIiIiIiIvKdHLt8jrh/njXGDKohIUAZI0WIhCKSUiKNK6JGAOyI6ZzK1vxKChuLyXggZihToD9uCgDiVM6gLQLIT1nsn35f8O07O8p3tZEta4stskHOmRgD4/GQEKHGGVPTObLI0cvnZni+IrLXqQBARETkFTZ4/459/qv3KZYrlujCKNEtu+BGnfKkWjmScTK+3u5vve0cedLuTHaSTQLxkNs/N7MV2wCyTbgUGOSaogyMqVmNY8qj+zj/g7fg7JJCeREREREREflWDv30oheHFlipB8ROpIyRkDOeEu5N/G9m6+MD5dm0i/AhTw4cc19fzE9mZDNGKVOWJUUOrN16wNr1+3SHTqianEFyJ8SChDP2ROyU5Ly1NX3zPwtMdqyjBaCd0O7+D5O/ZazZRGNAMNx9kr9JhAipcIZlpn/qMIf+7JJyNyIyE3r+FxERecVVHz2067/+AB6PODK3n2plSBlKihCp65pxXROKAjdjnBNFp9z09e1idTu7frtZc/LttFX5gaYTQJzqstBW9bef074/W6YOMCoy9WLJie9dgJNd3QkRERERERH5Ro7+6UXff+4ouV+QQmY4WgNc8f5zFtoOgDzZRp5gDMc1C3Pz5GFNXh6xfOMOd//Lhza6/ZDFog91plv2qKqKEAuKsmQ4GhFi3PT/mf7e6gDw7DZdzy0fazdwtJ/X5nag6e44KmBQZvafPw5nF3QnROSFUwGAiIjIHlC9/9Buv/cZ1b1VDhSLMEwURHpFj0gkJYcQsRioUnqylZy1wwG2GxIg30ZmY+SCORR547C2EwBtEUAzy2/6q1PIDLowd+oQJ3/49mz+ESIiIiIiIvJyOTvvc6cOkeYK1vKIGCO9TveJReN2XF27Q112zvS1bvMu7s58t0e9PGTOS2ylhl/fN4C7n95kdH+FMMrkQU2uneQZi03XwDgpALBtRjfmqQVqeRZP5sHcINPu3GiOmNuii0y2TLLMuMisxJpzP3l3FicuInucfoWLiIjsEYPf3bHrv/+YzsjpeYGNnTIFSjdyVUN2Yoy4N1Hjdsv82gmwM9qZfTBJsOQtXQBsI1jfGrC7ZaqYWbaK4vA85/7dn+iuiIiIiIiIyFc69tYF6vmCQaipLVF7vWnn/9bd4j6ZO7/drHn59qbj/fXd426EHGCUKcdQDBJ3f/3++teMPn9sa7cfcKizjzkrWOovEmgW/80CVVVtdG6Y+n9p28bO2i4XFp7y8TaH45ONH1XHYaHD2X/7A+VuROSFUgGAiIjIHlL9/p598bsr9Cqjmw1GFVY5/Vhi2UlVTacoZn2ar7Qm2M/ksBGSt2MW2hdmeTJTLpvhGNm8mRM4meeXrWbgQ+pewA7Oc+H/+hMFkiIiIiIiIrKtuT896YtnDlN1nXEEyqb4P6W0sSK93TKyaSn5WbWF/dML/9nAJ9e6zDBH5FhnkXsfXYMb400lFw/+9ord//QGtlaTBiOoHE/Q6XSIFje1+p/UFACQLDe5hxf5j31FNTv+N7QdF6Y3crQbPdrr33TSyFgvshbG9I8uwRv7lLsRkRdGBQAiIiJ7zPKvrtuNDz+jHMN80Scmp2MlZYjkuv7Kr1Xl/85oA8dvez2DNwMCAk7ZiaymIYOYyPt6XPz3P1UgKSIiIiIiIpv0f3TCj146y8DHpOhUuSK7YzEQQlifTd9qFzsV/++stpF8O/IPmuvepaQYOb48YPUfr2171Ue3H7G/nKOTAv2iA4Anmk4AvrGpoLVebPBc/0V7T3s9A80Cf3u0H0tT1zzk5r5kEmNqlvOQN//kj178SYvInqUCABERkT1o9R+v2Z2r18mrQ0oCXieCQxkLUlU3O83NmTT8aw71/98xGzv9myrxFALJAniTetmagNnYjdGMC+gQICVyyKwxZtgNrBbOpf/Ln+smiYiIiIiICADdU/t96eQR6vmCERUhQlEGsmUqhyo3IaRNLWLWAXIAcMxdqYAd5LZ5p7g51GtD+jnw+//X3z215GL1d7ft9qfX8WGNJwgYEaMIYVMHgOn/j5uKOHZGkxObvpbmEHMg5oBN8jh58nOTJ8mcMkORYTAY0JnvkrsFw5h56//2F/qJEpEXQgUAIiIie9TK31+ze5/cYC6VhMrJo0S/6BGmWv41i9NNFbNj65XlLb2Q+PbaazidYGkvabbNH9s0R27r98hOURR4DKxWA4oD8wy6ibP/0w8VTIqIiIiIiAhzpw/SPbzIShpQ9EtGoxFkJ1oT9xdfMQJQC//Pg2+0658sIi/FPg8/v/W1X3nno6vMeUkYJUIyPFjTVcA2OgpM0/178bZ2XAgOC/0+g8GAOmSGHWfYhdP/h3d1d0TkuVPeXkREZA9b/sdrdut3nzKXSubLPsPBgKIomjmAwRlbc4ROB7OIV5mOx8kss0mbuW2+70a1edj22Ou2FlJky2RrGwLmTUUCm4sAjGRGPfmzp0w0JxTGclplPB/g8Bwn/u33nJOlAkoREREREZE9au4nZ3zxzVMMY8JiZFRVhKJoYszsFDie6/V2/+3i5UYsapi2kD+7YKSUKEIAd8hOSYA60aFgcOcxN/9/H3/9hb45sgef3aSXIh0KxlWiDk3HhrarAGYQmm9lHghJ9+9ZtT8HmzZxGKSQSSHjtpHHiVM5nPaehHFmvuySozOMFcvdMelAycK7R5SzEZHnShl4ERGRPe7Rr67Z57//mLA25kB3kWowpCxLfPIyIWdnOBwC0Cl75Kwpci/CV1XrT7fyMwesCTrHRWZY1hSH5jn2zutwoqOAUkREREREZI/pvLnfD108xbjrbLcGvF1nuq0fk52R60SnUwCOmWFmpLqm4wXdOnD/2u1v/L3u/sMntnb3EcXY6RUlkYj7RldBd8cnYxuCg5kKAJ6HvOWAp3R7NPCU8TrhnkkFeCcQ9vVYOHmY/sUD+kkTkedGBQAiIiLC6Jc37MsPrsKjIfs68+Tk1HWiR8FC0SUmJ9cVBKeezD7buktgmhIGz5d5mByTLgy5vea5qfrvFuw7eZTjb1yAYyoCEBERERER2UsOvX4W6xTklGZ9KnuepRpyZpzqZpq8QRkKelXgy0+us/L7m99qlf7mex/Q90A5hlDnSUeHZvGfyeJ/u1HAgtIBs5ZjJFtzf4JDwLHCKI8ssv+1U7M+PRF5hakAQERERABY/scbdufjzynH0PFIyAFLmY4b3VgQgHEak+PGDoKv6wWgIoDnK0wd7bV2y4y8YpUxi6ePcOzt12d4hiIiIiIiIvIiHfkf3vZ4ZJEcnRAAUxe/WQlAtyjxlAkxYjGSUqYfuhSDxINPrn/7b3p9aNfe+4Ru5cRhpiA2nQUIzY7/3CQHpneny2xkwEKY5NAMy05d14xTzbgbiQfnmf/BaWXOROS5UAGAiIiIrFv9hxt2673P6A8CS3SxGoZrIwo3ylhAMDyG9Rb0063oYfOCdCNjrpBzp4W2nd/U0V7zDFSFMShgpcz0Th3k6F+9rYBSRERERETkFXfgz1/3hbNHGRdOik7O9axPac8LBsEdi5FxXRNro3404MGnX8AX4+/Uo3/wixuWvlzhQDlPkSBORgvgzVt3pQB2i9qcjBMwCgsEjBSg7hppscORi2coXj+kGyYiO04FACIiIrLJ6s+v24MrNynXEotFvwlOJm0D3Rwnr7f//yrTLzLUCeD5mL4Hm4oADHLHWCsS415g35mjHP6Li7oLIiIiIiIiryh7Y78ffeMcD8erVKVT54qMwsBZa/MpdV0TPbDUmWd0b4UH/3T9Oy3+t2689wlxtaKsrUkCEJpqg4mM6/7PmBuTOxCazo1mzShNc3LIVLHGFwrOvHthticqIq8kFQCIiIjIEx793RVbuXIbW6vplz3MInVOmxbyv10RQNsJQE3odtrmIoCmHUD2moqMdwPDmFgrag6cP8GxP1MRgIiIiIiIyKvo9Pcvc7dehV4kpYqi38U6cdantedZCISyAKAbCjorNaObD575+6bPVuzae59QVBBrI+MkC1OdGh1UADBz1nZnAMjebNhwx1Oiys34xuLQAos/OqebJSI7SgUAIiIisq37n91k5cZ9ijFEa2bJxRAIybe0+f9qerHxfGTbKKeYHsUQHIqiADIJZ0zNmlWkuYLFM4c5+GMFlSIiIiIiIq+St/7PP/W00IH5kmSJGAOD8ZC6rlWCP0MZGHsieSZ6JAwStz78jNXf3Hqm3f+tld/esmKUiTXE3GRffJKFMVM2ZrdwwN3xyTjHGCCGgAXIBayEMUfeOA2vLSpfIyI7Rr8FREREZFvp1sAefHYTf7hGyE1LOctgbgSag+y4OyE0BQI556+cNdfOq5dnkyepgnbhP5mTJxfWHEiJArCcsBjwjrGSR4z7gcOXz9J755jugoiIiIiIyCvg1L962x91ElXhjL0CmsVGM2tWGuW5yjljZut5kekd3zlncgiYBTqVUSxXjG4/2tH//8e//kMzBqBygk9yMzT3P2j5Z+a25sgChmXDUk3ESCEzsppRx7j0k+8Tjs8rXyMiO0K/AUREROSpxp8+tGu/u4ItV8zRIQ8y3djBUobs6wv67k7Ozb6CEPTy4kVoF/+3G8MQJlXlzf3JJINxkRkWMOg4h988R/nOUQWVIiIiIiIiL7H++SX3/T3q+ZJUOGZO223cPX1lgb7sjHYzREqpWfCf5EbMDIsBCwXjYcV8Knjw2S3qqys7svu/5R8/tkfX7zAXOljlBCJ4oIwFdV3v5P9KdkBwiBnwSccGz1gnkMqmE8Dpdy/M9gRF5JWhDL2IiIh8pfqTB3bnd5/h99aYL+bI44x5oCASiRRWEM0IbLSk33rIzppe9G9GABiOrb/fJgUARWYyriHjlhkVmbUy4wfnOHTpDMWl/coGiYiIiIiIvKQOXzpD2NcndY3sTfP3jR3ogaCI77lrN0G0xRbTmyICEbLRqQuqO8us/fMXO7r433r46RfkhwO6tVFksEm3RtkFLGPk9dyYudE+CLJBjJFU1eRQY/2CcGyJ3g9P6+aJyDNTAYCIiIh8rfEf7tq9968RVhJdLylzJCQnZMdzbhacJ+3uFGS+WFtfzG0qAgBC3hi7kA3GMbPiY+KheQ5fOkP3oooAREREREREXjZn/upN7x4/QFVCIpG9blr/NwPHm85w6tD3QrQjAGKM63mRnDOWnU5l9Crj9odXn98JXFuzu59cpzOo6daBwiP1qKYsy+f3/5Sv9dQxmG6TjRzNz2j2RJVqQr/gcag49dYFyouHlasRkWeiVwAiIiLyjQz/cMe++O0nlKtON0dKC1hyrM7kDEZskgvBNrWnn96tHtCLj53R9FYwZ9ORrTmmr3tg4+NhUnMeOjBgTO/4AQ6/eQ7O9hVYioiIiIiIvCTm3znq5amDrHadiho8AW3sZ1gGMNwUgT9v05sg3H19FAA0XfkWUuTx1buMP3nwXHb/t8a/+sJGtx7Sr40+sTkf3f+Zm/RrBNpcTSCFsN4RoK5rOp2ClGqGaUSYKxmUibPfvzS7kxaRV4J+A4iIiMg3Vr13067/9iNsWFG40S87RAvr7eXWj6fMppedFX3jAHCaAoA0OaBJALWf0/y5aT8XCmMt1NjhBY68+zpcWFARgIiIiIiIyC7XeeOgH758lsdFZlhmEgkzI1ogYOsJ/6TufC9EO3LBzMg54+7r3QBKCuzxiEc/++SFZEgeXLlO9XCFIgfKsktV18rN7CKZQLLmaIozmsdMCIFQFoxThRXGOCTyvi6L/90l/RCLyHemAgARERH5Vqr3btkXV67y+NEjgkMZ43qLQdiofm9m08/uPF917a7+mDfa/bsZGaMORjJbrygPk8+L3uxAiMOa0mBkNSudTO/cMQ68fQHOqAhARERERERkt4qn5n3fpTOkowsMYiIHw+NkARrHPE+K8g2zSM5f/z3l2bg7ZhvJjxBC09Y9Z+rhiNvvf/bCzmV8bdXufHoDqxJkx4OSMrMWvDmgyZHVodm4AayP0xzVCSuMECN5PKLsRL5Ma+x/7QT73z2rPI2IfCcqABAREZFvbfzzm/b4s7vk5YoiBcyblxRxMvdumooAZsttc3AJUJSRwXhI7Ea8NO6PHjN/+jAn/uSN2Z2oiIiIiIiIfKUTF84wd3gfy2lM6EZyroHJQmJyPDUr/mYGwcho7fB5aPMcblDnRJoUXkCz4BI9ENdq0oM1Vv5w+4VmRVZ/c8vCMMFqxZx11vM10yMZtxvbqJGNz1/btTHbRn4mhIi7U9dNJwAzY1iNif2SqgNHLp+jOL1PP8gi8q3pOV1ERES+k/ofb9sXv/ucODS6XuKVE4jkOmEG5hlw2kJ4MyOHSNUGOV9xbLVdcLqX2VT1eAqTRX422v0XeWMsQN7yeclgYBk6BbmusVRTdiKDvIYf6nLx//EXCixFRERERER2m3Nz3jtxgByhjAYpU4RAdMNSIrhRhgJzyDjJaDoEKIZ+Nm37PQCa1u3evm3H7wWwAJ4SZezQqYxDVZ+7/+vvZ3L1r/y/f26Hc5/OcoKUsWgUIZJHFRCIRYdhztSh+bdAs7Gj7S44+RcSVEDyzJohjM3DwMhEbw7ano3ZCQSiRTw1IzVDYeCJKtQsLyV6bxyb3T9ARF5aKgAQERGR76z63U279t6nFCNnPvaohiPKsoTcBIltnJzx9Yr4GOOMz/rVkacO2Ljem/ITWz5vayGFNTXopJCpY2ZQZt74v/+5onwREREREZFd5Pv/6qeMS6gtkUgYmVyn9UXarXEgqID+ufCAMdnNTdNhz4pISolOUTJeHtKpjI9+/ruZnuaN9z7lYDFPh0CuMzlnYqfEzEgpEayYjI6QF6X5Gc2TDTNb39/8eSPHk8lWU89F5k4c4Oi/vKg8jYh8KyoAEBERkWcy/tV1u/7eJ6THA+Y6c6RxwtwIGMFsfRaeu0NOk0rnzYvSW4+ttktkyHdjk/lz5k+OBwjeJDHSXIdL//6nuuIiIiIiIiK7wNv/4S98NY0J/Q6152ZhP0za/E+1o/ct8V3cLsCW76S9ttEzMcNk4z+WnXo0JsQCEhwqF6juPiZ/+mima+uPfnXVHty8Qz8XlDlQJ8djQTZIKdGLgaJ2YoZAxi03XSPYaFOfVR0wUwGoByN6c336Jw8T3jygPI2IfGMqABAREZFnNv7VF3b342uEQabIgZANcyMnx5NjNikImPWJCjApAJj6e9vmDyCFzHK1xqgXePPf/4VzoqsAU0REREREZEaO/Ks3PfcLhpZYSyPGXuGW6Xa7pJSAjeLuvKnb2+RQRPfs3DBvoubmuubJTu4tGxbGNfZ4wI33PpnZqU679pv3YWVMjxIIVDmRaHI0hRuxromTf8emjoEYjmYw7gZlNGqvGRTO4UtnZn06IvISUR5eREREdsT4n2/arfeu0B8FilQQKXFvWstFhyK0BQBNZDzdiv5px1bqBPDsAjQdGrwp0mi6NbTXtdlJMi6Neq5kuZM585PvwcmerrqIiIiIiMgLFt485Iunj3Bn9Jiw2GVsNbHXIedMVVWEMBnqZuDm+CRgbmM8xc87YCo5ESZxM0yucbNMTlkUWJWYsy63P7pKvrq6O1bOb47s/me3CINEGTskDDeaTRp1TZHbx0jz2EnBSV+Tl5EXqywjw3pI3Q/Egwsc/qs39FMtIt+ICgBERERkx1S/uGlffnydYpTpWYde2cMsApOW8yTc04zPUoAndiq0O0ZSyFgnsJZHVL1APV9w7k/egVPqBCAiIiIiIvKiFK8f8rNvX2SViv6hRR6sPqIz192IqXMm2kaZ/dZ27cHbBWvZCcEBy1ML496MWcAI40w/F6SHazz69a1dtWz++B8+t9WbX1JUTjd0CERwx92JMawnBZqd/40M4AEtH83eqK4gQo7OuDTmzx5n7kdn9ZMtIl9Lz+AiIiKyo1Z//rk9unKb+sGAMgVKCgAyTnbf1JLw66jifOdNt/Vrw/k8OepAU+3vTlXX1IVT7p9nNB849vYF4um+gkwREREREZHnzE70/dCF43QOLxIWu9xfe0TRL0meqUZjihDodjrUdb3+NW0RQJ7dab+S3JrI2SZXNhmkyaJ5M04vUHqkWK6488FnszvRr3Dv01tU91fppUBIk7A+GCk0jxu3rY8aLf7vBhkgQLffw93JBax2Movnj8GxUvkZEflKehYXERGRHbfysyu2dv0etjwiJsc8NAv/IRJCMevTky3cIIfpwgCjKAPJa5bTAF8o6R7dzzHNmxMREREREXnuFk4eYvH4QR6Ml1lLQ2JZEkJgPBzR73bohMhgZbVpPT/1dZt2cMtzkYKTQttxwcijisXQY3T7MeP3H+7OLQyfPrbx3cd0RtCpjJKAB6PyTAp5vXtE8yYQNnUM1BLSLOWcGVYjIFPnitSLpIWSY+9emvWpicgup2dvEREReS4e/93H9uDKFyzkEqshJyeEQHaH7OvBZLTQvD9nUkqE8PSXJ6pB33ltR4A2QWQOnio6wQgBMokUnXouUhxZ5PRfv6kqcxERERERkeek98fH/fClMwxChZWGmWMBUqrpFBFPGR/XdIpyYzc3AIabqYveDosWqOuajGMxNp0N3SFG3J2ed1i99YBb//WjXX3lH/3dx/bwkxv0c8THmRqjijRdACZn3o6NMJpDIyR2h4gRYyQbjKoh3iuwo4sc+1dv6Q6JyFMphy4iIiLPzcp/u2pfXvmCOS/pUhAJpKpZ5Hd3cs7UdY2nTIyRoijIWXsVXpQ02fk/vfi/XuWfm0INaHYD5BLyYofy6BKn/1JBpoiIiIiIyE6zNw760msnGHSgjo5PVmDbRdnpP7eLs9Mfa7mxvqtbnk3OmbIsm7F5ORHMCDQ5DctGtw48unpn1qf5jazdvEexOmaxnCPnjFsgWcAJk1yArY86WH98qZ/ETEVrNszknCkChAgeHRa79I7tw052lZ8RkW2pAEBERESeq4cfXGV48z5LNodVgRAKCisoQqAIYb0YIGCYmQoAnrNA28avnV3Ytv2H6M1hk10j65+ba+pcMeo4o4WC/pnDLP3ojIJMERERERGRHbTv/HGKI0sMugmPvt6KPWQjuGGbtvfbxjF5v22K0jL5idnu8m1ZdoqiIJnjBoUHrEqYB7qUDG4/YPk3N1+KcovxJw/szpXr5OUhhUfM4uSx0zy24vqmgIwGSewObc4Mz5gZISfqeswoJHyhy1v/4gezPkUR2aVUACAiIiLP1+3K7rx3ldHNR3TrSCeXUCc85WbWfAiYOymlZjSAvRRx8ytlY7Zfw41mN4MZpQUCRkqJkScGIbNS1hx/6zXKNw+qCEBERERERGQHdP/kpPcOL7HiI7w0UnDc87aj8JzNO/ynC71lZ2Vrdv4nb8YaxuSUtdFLkTh0bn14bdan+K0s/+K6rdz8kjnrEJMRvNn93z6Gtj7eVAYwW5nmB9vM1u9R9maTxpCKaqHkrf/pR/rpF5EnqABAREREnjv/fMVu/PpjeDCiGCRCDpAyPikEMDPcHXenCHHWp7tnbNr1PwkXfdIVIHkzAiA4FGaEYHiAFJ2qAytFxenvX6L/7mEFmiIiIiIiIs+g/+ZRP37pLDbXoaKGYGSDxOZwyw1SMFJourb5NkUA7bH14/LdtJ0KQ2gWyosUWIwdumuZR5/dIl9dfumu8uMbd+DRgF6KFDlgvrFM1OYGsmVS0PL/rNXZyVgzksEdMyhjQRmMZInVIrEyZxz58XnlZkRkExUAiIiIyAuRP39kd9//HHs4pmeRTlE2gWXKRGvGAeScm9Zm8txkNpJAITfHeoBPs/hvZhCaoox21hw0MwDdnDpmVq0iHpjjxLsX1QlARERERETkuzre9VNvXyAu9RlR0esUeE5AhrCxttw2ZW9HuSXbeF9ru24B8mzcmo4LZYxQJwqMOTqMbj3g0d9deekW/wHqjx7a3Y+v0xs3BQ1NF4DNj5wcVESyG5gZIYSm40fK5JQwnCIE3DJrVsG+HsXRJTgWlJsRkXV6PSAiIiIvzPCDW7b86S1Wv3zY7PYvCqCZadbONVMBwPPXJorK3Bxxcsnr0BxuELD1cQy1N4UZlp1QZ0b1mGK+w5f1Kmmpw7F3LmBvqAhARERERETk21q4cIJ6qcPQK9wTRYaYnIyTg00W+QNugWyBNDmc5u9P55NDvis3mkp4y00BfXIKCwweLfPgk5er9f9Wg1/csEc3btPJEKeqSJolZCebkzRXYvYm3T5aMYPVmZTqZixFt2TFK8Lx/Rz/yfdnd54isuuoAEBEREReqMfv3bCHn97CHo7ppAJqsGyYQzQjRMPWExVNsLke7HhoDtkx28XzdV3jk/mGxIDFpkCjsEAwo2ORnBNewINqlXh4kZPvvAbnF5QdEBERERER+YZ6f3TET751gUd5jTpmQhlIVU1hTez7TXZfu20/p10bt7/a1m4J7W739Y55TMYptCMAslN6pEyB1TsPGX668tJf4vsfXaNXNf+mkJuikhRodpuTsW0fWZuv1fT12u6aynfnBskzyTNGoCgKyhCbmhQzYhEgOJUlhjET9/U5/udvKC8jIoAKAERERGQGql/dtce/uUlcMbrexRO4G0URqaoRIWScCreMRSPj1NkxD0QrCCoCeCbton8KUMeNoDw6FBmKEJuPe252nWAkh+QQ3JgPJTaqAQj9god5FTsyz4k/vgiXVAQgIiIiIiLydXpvH/Tzf/wGKz4k9ArG1NRkPAaSg1mYWtnPmDcLstGbwyYLtNMj3TaNBHDDtAI7WcX3bRb3m7b3lpsrWeOkqc+Jk4551WhMJxTE2ijHMLz1iDv/9ZNX48J+PrAbv/qYhdwh15AsUFsg4RQYoW4eTVsfRm5s6kQBAXMoUnPdAk8WCMh34xbXO2amlAHDrPl7dmdUVcROSSKRS6NzZIGOOjSKCCoAEBERkRlZ/eCWffaP73GwWISRk1IiZ6csy2bmfHZwh+wUIdIpSgjGuK4m1ejyrLYmiCZ5kScSSFvVdU1wiBYwM+qQGceKcHCOI5fPES8tKtgUERERERH5CscvnmOVMXXMpJBxa7rgbSyabt1PvTlmm47d5Nm1I/C2mu/2KIjEGnqUXHv/ygs+s+dr7Xc37LPffMBSOUf00HQEtEAgUhSd9c+bfjS2j7vpxf12+nz7uHxaZwr5doKz3glzu5/3EAIpNRtovABb6LL//HHC60t6dhDZ41QAICIiIjOTr3xpv/3PP2N/OY/V4NlIGUIoKIoOhRuWMpYywSFbJkdHYeTsZCAFI8fmZWSYzKesqgqKyOKxwxx/4wKcn1ewKSIiIiIiso3jf/2mx6U56JXrBe7bLe5rgX8HTCoqtha7J8sky2TLmDnRm6547edUoemaV41q6tUhRW3c/vQ6vAKt/7ca/fKG2cM1ehV0Ymdjd/l4DGx+PLbXKEy6UWDN1oH8lLEA8t2ZN6M8jM3XMhvr17vtDmBmJINcBvYdO8TBM8dmdNYislvoOVhERERm6+oj++A//y0HwzwhNS0KczIsG4HYtI9LmZQrMolYfrM5iPL85GB4DJAz5EwZI+YwyCMGoYZ9fS796ffhVF/pKhERERERkSkH/+yCz584zFqRqAoj2UaBe7vQp6T989UuUucAhCbB0C5ux7ZIYDKBoWORpXIO1ioeffL5zM75efvkF7+hP4YwqumXPVLKlGVn0+cEb47ozWaA6aIKt42FaVDxyk7ZWgiUt4xVcHdCCFgI1Lli6BWpE+kd2UfxR0d1F0T2ML2WEBERkdm7OrKP/j9/z5L1KbwkJaPOBqkJZIqiwAxSqgHFL7PkBnVOGwHnZERD0SnJwRiSqOdKBh3j4p//EE52dcNERERERESAzrvH/ODrp1grnXFhrNXjSWzlzeL/VPTULrbKs5nejT69GJImLerbj0dv5tdPdwlwCxQ5EAaZLz78DG69wgMJr6zY3Y+vsZQ75JUxTT/5OCmWaMZTNI9TJ7gTJn+GjXb/vmlxOmAeVAjwjNrngY1CCyebr2fG6lRhAZxENkiFM7SavFBy4vI5eE2jAET2KhUAiIiIyO5wbWRX/svP6A5gPvboFl2yBVJqWsmZGRYc94SKAGarbS9HaLoxJM8kdwiGF1AXzqDI1PMFb/3lv5j16YqIiIiIiOwKR147xVrMVP3AyGo8tour8jw9rYvg182pD8mJI7jzyXWq3997dRf/Jx79/KrFlTG9OtChoK43rs70NTSe3OHfXsv20AiLZxfYGAEAG10WnKnClhAwM3LOuGViUTC2mnFIFAfmOf/9N2dz8iIycyoAEBERkd3jytBu/eJD8oMhIQViKKkc6twUAQSMYMYrH3XvYuYQLUB2Mk4ORuWZOifa1EmdK+J8h2HMLMeKt/7nP3eOae+KiIiIiIjsXcf++k2v5wJVB4Z5DGWEYOtzvmHSQn2mZ/kqespQBZvsYDfHzdcXVyEQPBCyUaYAy2OWb3z5gs95dq795kPmq0A5dnqTEQDttUmh7QMwbeP6aVzj8ze9+A8QMUJyzJ1gkEjUJIZWs+pjfF+X+H2NAhDZi1QAICIiIrtK/d5de/jJDdL9NQqPxFg21ePWRDielA6ZtWiBXCdSSk3CKgYshknleRNXjusRQx+TepF6vuTCn/4ATpUKOkVEREREZM85/Gev+dyxA/QOLTK2mtoSZv5EfNsu7inq3RluGwvW7c70wKSwPW8es5CsOQBCdro1dOvA3SvX4frKnlnaHn380K7+5gPKMdggYR7Wd523RQBb59C3pt+nERY7a7vnBXMgO54zpTU5mZwzBKPolFQxMy4z5793GU51dDdE9hgVAIiIiMiuM/rFdRt+epv0aI0ydnA3xlVFURTbfr6ZbRQIuGKa583cKUITXLo7yR13b1rOuROjkVLCCqMOmRUfURxZ4vQP34azCjpFRERERGTvKN856L2T+/G5guXxKiFGOrHAq5qCqfbeGG62vpP6FZ42PxMbi9PN/PoiQ5GbToM5Z3IwMkawglDBXCpYvnqLtd/e3HN3YvTePVv+/A776BCTU1jEzbAQqT2To62Patyw0RtgunW9FqB2RmZS0GLNlW3HKxQYEYPszcgKA8Nxz+SQGYfEoEx876//HE4qHyOyl+j5V0RERHalx/901dau3yM8HlGmQKfoMByOgQBu+GTRGbToPyvtTL92B8Wmj03ekbxmRMUgjgmH5jn6zkU4NacbJiIiIiIir74Tcz5/6jBpoWRUZJKBk7CUidDs2t0SHbULfbKzfGoppN39bw6eM51Ol7rOxFBCnShywJYrHnx2c4ZnPFurN++zdvM+c3QYrqwRrWgK/71Z5rcQqFPa9DXTeQF1AHh2me1GLmwwpvIyvvGYho23tSWsX/Blvcap711+/ictIruGCgBERERk11r+u09t5YPr9KtAGBvd2HsiE7K1CGBzBbo8D9OtE6M3uyamA/2cMwEjmhNDUwyw5mOGPeidOMCxy+eIpxaVChARERERkVfa4UtnWTh+kNQLjKgJAUJujsID5pN90tus+LtlsmkYwE5ym1ogpVkwjTQj7goLhOyEyujnyBd/uAJX1/ZsgmF85Uv78soX2GrFQuwRKpq2FMG2bUW/dVNAVheLHZEnzwPtU0R7ra1tFYIR3Ni41JO/u62PCBimEaOOUxzfT/8HJ5SLEdkjVAAgIiIiu9ryL67bo09usJRKitooiAQPm9r+y4vlU/mp6S4A7cegKcTw1IwECGVBMhj4mNVYs3DiIMcvn4PT6gQgIiIiIiKvpn0/OO8HTh8l90tGlsk4Zkagadtt1oxO20oJ+53TxKuB9qo2i9RPbirwBJFImSNzXrL8xQNG793b8wmH4Qd37dG12xwslyiqTEi23gkgpUyMcb3V//TFUtnK89UWW7TPFdme7BLQdmosykidM+VSnxUbc/SNc4TXl5SLEdkD9HpCREREdr1Hf/eZDa/cYTF1sLERJov/KgJ48XybzSnBp6vQm8X/EAIZp65rAGKMhBDwGBiWTu/EPg5eOAUnuwo8RURERETk1XKi8GNvnGHcgZFX1HljSTRP/hxCIPnG+803ugO0C3waBbAzzLcsUBskgzRZHenEgl4u6IydYi1x+7/8Xld+4tGVG6zd/JLF0KPIEHNYLwIwt02t/o2Nx2yy3HSxmNmZvxraHMx0zmWjpGXj4ykYKdim54zApMtFERilEalj1P2CY2+ef/H/EBF54VQAICIiIi+FG//1fatuPmYxFYTkavk/Y24bsynbtn5thXmum10sMUZCEcn4JDkAkBl1YNg1lk4dZv+F0zP6F4iIiIiIiDwfp3/8fZZDxbhsFplDCAQMcibnpp13MnDbaKfejlhr4ypQAcBOaWelTy+apjApBHCnEyL1yhq9FPnw57+c5anuPjfGdu1X7xMGNd0csToTQlMEkHPeNIO+1S5Ka/F/500v6LXjGZNtHO37WiklsjuD0QgrIquMKQ4usP8n57QZQ+QVpwIAEREReWlc/d9+afn2Y6zyJmniildetOmqc58KMtuK9Dg5cs7kYFBEcjByzhRVgjrhZWQtZtJcyf4Th1n4/indSBEREREReSWc+x9/6L1Di6RewSgmHIiT1v/Q7vx3kjlWRvLkA+0iasxNTNU09Vao9KzaGoo4Nae+nU+fDYqiYHV5jf3dBT74xa/h+qrKLrb6fGA3P/mcLgVkW3+stjvMw5aHaVsAoAKWnbDxPDBdHNTKFnACyZojW8AtkAlNIUAMhBDodTrN9+iW1IVx+PXTzL9zVE8wIq8wFQCIiIjIS+XT//gr6w2hVxllbYRstC9ppoPMJ9qezeRs957gUJYl5Exd1+u7/wNNoivGSJWHpJgYh0Q4MMexy2fov3tcgaeIiIiIiLzUDv30god9PQYhUcdEyhncIU/ioklMBEw6ATx9hbTdtS47o72UPnVRzQ0fw6G5fQzvPoI/PNCS9VM8/ofPbT4X9DwSK8gpURQF2Wy9K+D0w1WP3eejzXVlvlmBRVXXhBAgO6muqXNFVTj1fMHRN88Tz87rTom8opQLFxERkZfOZ3//O3pf1uzzPjY2CisIRUmdE14YKTgpOG6+XiFt3pQBuOnlz7Oa7gKwvoOCjTaKdU4QAtGaILPpFtDMo8tkCjNicEax4qGvMdhfcPTd88z98QkFniIiIiIi8nI61/P9Z46w2s2sFRUhQIETaGKiTNNuvs5Ng+6AYSlvavefQnP4ZJi6aQv1U4VvcEBTaBEKw4NT0Yyrs+yQMqVHFqzH6M4q1/7XX+pif41f/T//k5Ujx0eZhd4C41FNjVMZVOa4QSgiASNkn3SykGdhU88Dmc0t/s3ByBiZ6M1hZMybz3IDioJRSsQc6NDkaVLILIcRo30F/XOHZ/MPE5HnThlwEREReflcW7W7H15ndG+FBetA5VRVhcVIbpMpniet/qYjzqAq9OdoazAKm6v+M02LRVINnnBz6pgZFZm8WLLv7DGWfqBxACIiIiIi8nIJZ+b8xJsXGPYy3oOaGgtOytVTv2a7ourtYip5NjEaKSWSN7PrfdKvPmajUwf80ZBrv/toxmf58vj0l39gHyX12phoBW4BjwFCJNHkZjzlpsDFtQD1vG3doLE159Vsgtm4C2HyDFMHGBWZhVNH6f1ImzFEXkV6/hUREZGX0toHd+3WR1dJKyNIGc9GjCXBochQpkCcZE3qAHXIBM9PzKaTF69tDxgxQoaUEnWA8ug+Dl4+Q3zrsO6SiIiIiIi8NA68doL+mcNU3UjM0MlGTkAoZn1qr6ytBRPbHS2fdFows6YnQzA6VlKmwMqte/Dhl9r9/019cN9GXzygGGW6sSBm1kf+GU0XwGjWtJ2XmYsZokO2jFteL8poc2PFvnlOXH4Nzs8pDyPyitGzsIiIiLy0xn+4a5//6n26A1iKfarVEYFAzLbtQn+2DKa9FDPlhlkEAhEjmpFzzVoeMSgS1ULJ8bdeo3jjkIJPERERERHZ9bpvH/V4aIlVS6QiUNc1ZYiklIgxzvr09ryca8ycIhjuTkqJwgNhlBneeciDT7+Y9Sm+dD7/T7+1+svHlLVReCTXjtfNCMZoATPDzMi4ulnMkHmz0L+1K0D7/mwwpGLUCxx76+JsTlJEnhsVAIiIiMjL7aOHduu3H8OXKxzoLODjdnZiU8DfzERzoIl6FHzOnmN4Bk9OtDBpw2gMrWa1SPj+Lqf/6CJzbxxREYCIiIiIiOxaxbn9fvDyGXxfj5HVeHSygbs3u81dIc2sRbNm8X9SAFAQKHNk/OUKtz+8CtfXtPv/O1j+7DblckUZIgEjpwS5WfhP7tS5xl0ZmNl7oh9Gc1jGDYa5ZlRAeXiR+Z+e0xOWyCtEBQAiIiLy0qvfu2u3fnOF4vGIfcUc5s1UMzfDbVLxzCTM2Vr6LC9cmwhrk2EhBCw4OTp1zAxjwpe6HH3jHP3LGgcgIiIiIiK70PE5P/rGGeLBOaoOFJ1IqmpCEanIxE5JSmnWZ7nn2STuzDljBHrWIQ5hdOcx/vFjLf5/Rysf3LPHV29TjqFHScfK9TELALVniFp+2g0CTy4E5sl/QxmoQ2YYM6ffuUjn3WPKwYi8IvQMLCIiIq+E9OF9++y//RZ7OKBITXiTDDK23t6srXB2hfgzFYjNGIAwaQnoCXIm5Aw5kcvM/XqZfLjP4XfPEy4dUAAqIiIiIiK7yuJrRylP7Wc1VORQN3PmU5qaQe+YFkCf2XaLl8B6bP+0Y/3z3Mk5k90prYBBzfL1O6ze+PJF/RNeWXd+fsWWr98hjjIdi83mi0n7f2iK/WX2Mk0hjLlPOmY2m2PcaMaVVDVxrstyHnP6e5fgeKEcjMgrQM/AIiIi8ur4dMU++2+/pl9FyhQwD01wY0Y2wENTka5QZqZybmrNzQyfJAYMKLxpzZhzpljoslok8oE+p753SUUAIiIiIiKye7y24POnDzHoOKlIFEVBPR41I85oFj5Ho5EWQHeBtgijIBBrY3RvhYcff0H+Qq3/d8LDT2+SH6wRK8gZLDeXNTtkjcCYuU3FMEYzomTyd3NnPFhjYb7PsBoytETVNY58/82ZnKuI7Cy9AhEREZFXy9WBffq//4zumjNX9kkZKk9YEQlmWNIMulkLDpFmLmDOeX2HQMiJmBLRjColBlQMO05xZJGTb12gvHhQ2QMREREREZmtU3P+2o/fZTwXqEJFwPC6powFEYPc7LQty0jyWh3onrMQwvqohRACYdIFsB05V3umKArKOsDjESs37sE1Lf7vlPTJY8u3VuhT0ItdqqrC3el0OiTlX2au3QAzvfjfPicZ0CtK0mgMZIpOZGiJ3tF9nPo37yj/IvKSUwGAiIiIvHqu13bt57/BHwzpUdLvzlGPa9K4plN09AJoxqYzLdPJsPaPOWfKGOj0SgbjAQ8Gy8yfPMTZ778B5xcVhIqIiIiIyMycfOc10lygmOvgBoYT82TsHBtv5cVw9/VOCykl3H29yNzMCBbxYU0/F/jDNUa/vq3F/x12+x8+todXbxEGNR0rMYvkDEVRzPrU9rw2/5UmBQBp8g4DYgbLTjMkABIJLyCVhu3rw+V9ejYTeYkp/y0iIiKvpk9W7YtfvEdvAGGQKCmIVqgF4y5jvtF+LlkgWyCGQK4TeTimLAo6/Q6P05DRYuC1n3wPzqgIQEREREREXrz+u0d96ewxhjE1i2UpEbxd9DdgMn6OpjCgmbs9wxPeA3LOxBib6zzpvhBoFj5CdqIH4tgID0fc+t/e0+L/c3L3P//B5seBwgrcDR/XUOnBP0uBjQ4AyYwqGD55X1u05FNjGoIDlkkh40tdDl08PYvTFpEdogy4iIiIvLref2Q3f/0R5WqmUxlzscd4MJ71We15bXjZvhC1yfvWCwFSIphRYBQ4bpm1NGTFKuxgn9f++I0Xfs4iIiIiIrLHnQh+/kdvc796TB0zVarWP9Qu+mebtNpmMvpM65/PXTtWDqAIgRACnjJkJ+ZAtw4sWY9PfvbPMz7TV98Xf7jCgnWII2e+7BNdC1C7QWBz239onp8M8GAQmvemSVYmeU1VGHNH9nP0Ly7pWUzkJaXnXxEREXml1b+5Y48+vkFvAL5aYx4w10ugWcqWwaZmAXqzU2ayR4ay6OB1pgiRIkSq8ZDYiZQLXe4NHmH7+1z4dz9xzi8oEBURERERkRfi8r/573jAEDqBsiyo65qyLCeL/ka2Zt95sqnFNi2APncBW9/5b2bNjueUKUOkQ2Chitx871O4lbT7/zlb/c0XdufDz1kIJT6qCJg6YMxcAA/Nc5FDcMPcyNbkXwhGcsdsehSAk7wmdwP7Th1h8Y9P6y6KvIT0+kNEREReecv/eM0efHYTG9TMxZ5mMs5YuyMmw3prxq1JgUzTCQCg6JTkXFOlMbFXMOpA2Nfn5NsX4Uxfd1NERERERJ6r0//2j31UON4xanNGqWZ+oc94PMYxMpu7mjV/agufFbI8TyEE3L3pBODNTHMzowgRxjVffnKd5V9c1+L/C/Lobz4yf7BGrA1PoCWo3SFOOpK0uRc3SGGSm8kZgjW5mmBYCCScjDPsGAfPnYBzc3oiE3nJ6NlXRERE9oSH/+0zG917RF4eqgJ9htzAzcnTVRgeCB6IuenOMByO6fT6UEQG1Rj3TBGMUCfKaIxCYsVqin19Tr17mXB2n+6oiIiIiIg8F90fnPLu0X3UXlPXYygCozzGgTo3RctugWSBFCbztdtFNjbGA8jOMn+ykByamebRAp4zj27f5+7fXdEdeMG++N2HzYJzWaj8ZYamy4+KHChS0wnADdLkcHd80j3DDWpqvIDSIOeaccjE/fMcfO00nOjqdoq8RFQAICIiInvG3b/5wEZ3HtGrCrp1IE4WnmGjRePX+aafJ1+v7QSw1fz8PMPhkCrVdDqdpqWmOzFGqmqMRaisxhc6dI4sse/cETjW2/FA1C034wpERERERGRPmr98zI+/cYY1G5MsU/a6uDshBEajim7ZWf9cn8QObcI9mxb/nwdvRyxMKgBSs82cEAIhND3mSiuIq5nVm/dneq57VfXRY7v1/lU6IyPm5p603f+2Wr+fBNzC1N+1eLUTpp+DjI1r2l7n9XEl1vwh57z+94xDv2AljDlw9jhL5068uBMXkWem51ARERHZU+7+l/eNW8vMrQXmvUc9TrgFLAaSZ2IRNrYSTLYThMl/3QJOUBHAM2guq02O5n15stDeLLZnxuMhZQxEC80YADcyRp2bRBteEzqwbENWepn+ucPMvX58B88y45ZJIU86FWzuViAiIiIiIq8+O7vgR985Tz0XqEqnjpkq1ZChpCBabLr8O5hnjIx5now7a5fagqrIn0EzLq7pFAdhatHScWviNoJTk3AzUgYjUo4iD397jfSb+7r4MzL8pxu29tFdOrkgeKCkgCoRopHJWLSN+0lYP9KkCCDgmLsWsJ5Zk2tpf14ym1Ne5pMxGilD9mZ0RnaSZ+rojG3MOI5Y7VYsnj4EJzvqAiDyktDzp4iIiOw5V//jry3dXyUMMgd6i/g4k2qn0+syGA4nn5UJ3gSdLYWeO+NprRq/4Vfj3uzyaBfpmS85cPYocz86vaOBqG+qlPf1bhEiIiIiIvLqO3LxDHXfqEMmWSZNdTCbLmhm/X1bvsH09lrZMeY+da0zMQSKEMkYuXZ6dLj98TUe//oLXfwZW7txD1se06PEU6YoCkiZogyMx2PyVE/A9V3/z5QvkKfJbO7AuF1eZvp9mY1im9gtqS0Rl3pc+tMfvqAzFpFnpSymiIiI7ElX/+OvzR6u0R3mJhjNmXFO0Glm1DUz6SFOIiS3reGSzEIGcjSyQdciRXLquiYuzbF08STdPz3z7KmCtgWekg4iIiIiInvS3A9P+r6Th6FbaA1/htoZ5k2MTjNXfnLAZOd4As8ZT5le6FA8HLF27e4Mz1paq1fv2b0rN/BHAwKR2iGl1HR0CM3c+dA0cVhX5OYASEFZmFkyh4AxHo0oLFDjrPThwF+/rWyJyEtABQAiIiKyZ135X/7JqnsrzNXGvHVJVSaEyPoK8MT6Lo/2UKgzU04zjy5mCJ6pc8Uwj7F9fY5cPkv5g5PPdoemdvqbb300iIiIiIjIq6z79mE//dbrjELTAjsrIJipFJqxcVh+onW5eaAMkcIDPUrKsXPng6twbVV3bZcY/OKarX7xJb1cYskIBOq6ptfpN7kVc5qhGZngmehNN8Zsm4bxyYxEh8IDKTm5COSlLgtnjsDZRd0ekV1OBQAiIiKyp135X/7JOquZubpgzkt8nHFrws8UmmQDTM11dNWfz1rODm5UqSbhdPodcnQGPibNFZz43gV4c98zBaPmgeABc2tGQUwSEEr+iYiIiIi82o5cPMO4H1hlSA6TOfMyE00L8kxeX8XYCPNiDkSHcZ2wbHRH8PDjG6z8Vq3/d5vV6/eov1xh3jqUocTdMZrODa04WfyHdvHfcDN14JihAOQ6U5YlRbfD2CuWqzUGZeKNv/rRrE9PRL6GCgBERERkz/vw57+m87iiO3QWrIt5IAdIZiTbiDYDevG0G7g7RVHgRaCiKdiw4CSvGVJRdQNn/+QduLT0jEUAut8iIiIiInvJvp++5sXBBdZ8jPUitSW1IN81Nod3zax4I3qgk0rswZBHf3dFy8W7UP3pI7vz0efYypiQA51YksY1IYTmrppvGcE4y7OVaTbJiT1eXiYFmFucY5THLBcVl/7nv1AXAJFdTDlNERER2fP8+qp99Pe/ZO5xplxJFCmQCdQhUIcm+Aw45u30QZkls0hyyEXAY2A8HpGqiq4ZRcgMfUhaLDjz43fg0ndrSxem2krCZAeCKREhIiIiIvLKevuQL1w4zlqs8dIIRUDx32w1MVkThydrOvQla3aGZzOCB+biHL4yYuXqnVmfrnyF0Xt37OGnN2G1ovQOOWdY33CRMZoAvLnHAAHzoBGMM5QBJzBKmd5in16vw2h5mYV+j+U8ZG0Oln5yWndIZJdSAYCIiIgIkL5Ys8/++T06q4luHYg5TGbBb7xc0uLv7hBjZDgaUaVE0SkhBsyMEAxPidiJrFYD0nzJmR+/i7156NsFpB5ob7Vv+YOjx4GIiIiIyKumvHjYz//gbfJCh3GEYRoxTmMgoxXI2TKaW+C2UZidJ/35Qg74yojHV29z/7fXFantcg//4aoVK4li7EQr8KkfrXa7hTpu7C5eBNwgpcTayiq9omQ8GlDOFQyLiv0XT8OZQk+SIruQCgBEREREJsafP7ZPf/UHitWEPx7TIRAwMo7H2LSnc8U1s5bzZAYdRqrq5p3ByN782auKsggM85iqHzn1g0uU3z/2DDeumTuoOy8iIiIi8mo6fvkco56x5mM8NEuRMcZZn5YAhQXqaoQVEY+Buq4xM2qPFFaS7yyz/POrWvx/Sdz74CrF4zFWQwgFnjNF0SGFzCg3YwECRp0SIUYV4M+QG6ScsdB0YujGgpAzEfCcyMEZzxvH/uQdilN9pUxEdhkVAIiIiIhMyVeX7eo//Y6lKtIdOt0cIRtVVVFnxTO7RbsLZFOLfgBr9gvknCE4KTrMdzj8+ik67x75ZjfQm+/R7CzRDgQRERERkVfZoZ9e9PLQAnVs2o+HyHrxd0pp1qe359V1zcLCEvVoDBk6nR4hFJREbK3m9odXZ32K8i0MPrxndz++waHOEvXqmG7Roa5rhnUidEoCvl70PxqNZn26e16eFGAEmlGJrZibUZlVB9jX4/Drp2dxeiLyFVQAICIiIrLVlWW79o/vMbeSKNcSXUpiLHF3LBazPrs9b3rhHzZ25mcLOM04AHDKIuAkRnlM79ASR948S/yjbzcOwE0t/0VEREREXlVHf/Ka73/9ONV8oEpjomcC1hQcm+FBwcCsWREZrQ2YL3vkYY0RqQcVnZFz/Xcfk68u6ya9ZJZ/dd1Wr93laHeJalQTYkG33yOlhKdMGSM5Z6xQF45Zm170z1OdEaNnzKEajYlzJd2TBynfOahdMyK7iAoARERERLZzddk++dkv6a1lOpU1RQBETCMAZi74RhDaLs5vFAGAxdCMbXAn4qRUMchjODjH0Tdf+0adAJwnF/6Njc4DIiIiIiLycotn5/3AhZOMOsZaHgGZ0sBTnox+C8RYqiJ4xhxrZsVXzlI5R7U6Yqnsc/fDz/Hf3NbNeUld/+WHxEcVfe9SjWo6oSSYUdc1IRg5Z3plZ9anKTR5kMykS+LkcJvkZoBxTqz14NS7l+m8tl8ZE5FdQgUAIiIiIk9zfWRX/ul3dFZrikFF6ZFcu15AzVAAjLze6r9hmBthsl0/WXNUVQVAr1vilhn4GN/X5dTbrxMvf0VQuiWobcXcHCIiIiIi8vI7+b3XWSlrhkVFCjWxMAIGOYMbjpE0Bm6m3KBKNZ3eHKHOdBIcKudJ91YY/7erWvx/md1cs0/+6XcsWZcwytSrA7pFCWVkWFcUMVJXlQrwZ8h8kn+ZDEZsiwDcIFkgG/Q6XVJdk3oF9WJHowBEdhHlr0VERES+ymer9ukvfkO/jsTaKK0E10uoWWkDzmltQqBdm69zoigKLAaqnMgGKWTGXlHHmjxfcvZ7l+m89ZROAJOA1qe+ZxP4NofuvoiIiIjIy+3kv37T81KHumvk4Ex6gFHnmgwURTP6zdUBbuZiKKmqRAwlPkzE1Zob//T7WZ+W7ID6kwd2/9MvOD6/n+AwGo0oe10yjnkmJP38zVo7grFd+M9TRwpQ1zUWwYMxINE5ssjCD0/pxonsAspfioiIiHydT9bsk1+/R1k71XiMthnMVjInW9OJIToED0DArTnIhlmEIlKZM/CKikQnBAKwXA/oHtnH2TcuEM89vRNAnrrRgc2jB0RERERE5OW0+OMzPn/uGOOu4R2DnMATiUTlGy2/3CdxhcxUCIGcM6OUIcPn//x7+HxNYfkr4s7//gdLd5fphg6VO+OcsCISslOaFrB2C5sUSW2USzXHMNfkYJAyMRqjjnHk4hm6F/YpeyIyY3r+FBEREfkG/HcP7LNfvMdiVVKkQHSIHtZfTLXV0BoP+WK0keR27QBt0v4/50wsCywEYlEQY6Sua8qFLnfXHsK+Hmf/6CKcWdz8XXzS7n8yz85toxOAiIiIiIi8vDqvHfT544d5WA+J8z0GoxEAhQVijBQxYtbMH08pYUHp82e1NWaefv/XxdPmwDjT9YL5ssejW18y/uC+ou5XzEc/+wWsVhzoLTJeGVBYxGKg9oz5Vz9Oph9H8nxt94PX7fea3AuOu5M7hi91OfeDN174+YnIZnpuFBEREfmm3ntgN3/xMXG1pkiBMkdCanaF1MmJZQFhc0i0OTgNk93q8izMDXNbHweQrZlJZ94cASfaJFmUHXPDk1Nnx2NkmMfkfuRxMcaPzHHqRxfhtd5GEUBO9LJRjqGgICfIRcGIGi9UDCAiIiIi8rJaOHWQ3qEFyrkOg/GoKRi2pp91rKBMU4uNERJJRd7PIADmTsAnc8NpurYRMG8OCKQMFBGPgXE9JpaR5IkiBLpesJA6VHces3Lj3mz/QfJ8fJHs4ce36K0Y+8I8oYIaJwUnulNkXy/OT5Ojza+0j6P1TIs53vat327HgHw3k+SWbTlynTBvniuT11TUjMvEg/lE/y/P6QaIzJAy0CIiIiLfwviDO/bw6m16qYRRolf08NoJU7vOpxf9p2NOtY/feV+3GL813neDUERyyIyKzKCoCEs9jr55Hrs07+0XBYwYI5HYjAIIBqHZCSQiIiIiIi+fg392wfefOUplziiPiWWB+0bAYO2huG3HPC16mo6N3ZoubsPhkLqu6c31GY/Hk8IMp1Mb1f3H3P7wKvUHX6oc4xW1cvMhy9fvMp8KfJQIIWAhMP3j+HU/m+1iV/MgUez+QuRJGsUMK4xEYugVeb5k8dRhFr5/Qs+oIjOiAgARERGRb2n1Z1ft4ZUbzHX6DIdjeqGgdKMbSwBSaCrVm4VkV0u6XcQcPGV8Mp8uG+Ro7D9xlJOvn4WzOEVmlTGVOZVnMj7pKhA2tR8UEREREZGXQ/nOYT9w8gidhT61JVLOzcJiaGICt42Z1tC85m9Hgqkg4LtzgzoYdTDMocgQPYNl3DLJMtYEaXTLkiIEqlFFCJFu2cXGTt8DK7cfUL93V4v/rzC/vmx3Pr3B6P4yc1ZgdbOEX0WoA2RzrAnXiQ6QyZPHULKNxf6N4hJF7i9CmBqTYma4O3VdA1Au9Dj+xnmK1w/oWVRkBvQsKCIiIvIdPLxyi3tXbjDnBYwyJQVeN4vEbWQzPafefPt5afLixcn9cHcyiZFXVOZ0D+1j8dJrcHCJcXDGZFJoxgu4O9Eh6i6KiIiIiLxcTvd9/+mj5H7Bw/EKxIAVkaoeEWPEoSkMnhwtcwjaRPzM6tAs5NrTCipy04GtjAXRAiklohX4sKbnBat3HvLwxt0ZnLm8aH7lkd29+gX9XGCVE3Igm1FPVrGCbxTnwGQkYJi8nfo+6r744phN+i3kSd4kRsya7onjXJPmS05cPjfjsxTZm/7/7P3pkx1nluf5fc953O8SEVhJgAQIkuDOXCqXrsraurumpRnNtFnLTGbTJr3SP6cXbS1raTQmk7olTU13VVZXVW6VK5lcQGIj9n2J7d7r/jxHL/zeiBsBMJlJLBEI/D5lXhGIG8F0+EU87s9zznOOEgBEREREvo6La3bvb05Ze+EuC1GRm0LJeSNQ7DHrTz+dpAYbC0uycxyo3KmTU9pMWwrUiVHKND1YOv4CSyeOUoaJUnXVAdwdSoGcMUI7gEREREREniFHvnmS4bHDNH1jQqGkwFL3Wht5Y/d/9q2t3Dy6HuPy9XVzYidPdwl3c6nYPCywnEkGzXhCzpnF4RJ51GLjgq20XPz4DHHhvmbSz4nxr6/Z3Us36TeJKhIFyLa5wSKVzSSSWeA/P6QNo5IAno7Zbn/oNlm4O1VVYWZkglHK+OEhh/7spN4RkadMTzAiIiIij+DSf/nAxjfvc7BeIBWnioQX25h0woM7SWRnTSYTknm328cgktNYMLJCDGqGh/bTODR0u/9nvScNcFUAEBERERF5Zpz4l9+MxeMvMu5lmsrwQcW4HVNKoderiTYDmy0AynwgcXpoAf3R+FyCfGxc326u5VGozLsqAN41z7NipGwcHRzg8qdn4fyyJmHPmeu//ZxYHtNr7EuTcGbB/9C/jh1VSiGlREqJUgo5Z0rp6jFYglwFkzo48varLPzRS0oCEHmK9PwiIiIi8ogu/qdfWdxapWqgSmla1tCoSrfQka0re5i963MoOyvnTCGwZHjl5Ghpo52+R1DcaEomYjpxLV2vQXcnQvNVEREREZFnQXr7hRi+foRRXVgtYxpawoNSCqXkByp7xdwuY3k8LOh6tpfu+rbOxrwLIEXBPLqdw1XC3JmMGhaoWbl0m+aDGwrvPo+uTOzO2WtUa4W6QArDokvJyT7b8T+tIEHgdB+732DbPJQd8MTNSv5HxMYxSwTIETR5QlsFa3048NZxeHmgYVbkKVECgIiIiMhj8Pn/48d2yIdUa5kq+7QNQDdJ3brTQXZSAQYLQ3LOm6Xq3LDkeF11SwZumBlVVVGZE9MqAAHkKHofRURERESeAa9/731We4WRtXivJqx0O//7Ncmd3LZUnh5IBNCu4sfHogv++0Z1vOiqLABMk+NLKaS6YjJpiTZYSn2GOXHuP/1C78JzbO3XF6xabhg2Tp0hTSsBZIMyjWrNqktsL/k/qw4gT56ZUUrZWF+p65qqqnD3rpoiUA9q1qwhHVzg0Huv7uTpijxXlAAgIiIi8ph89H/5G1tYNxbp4dmIafAfukmRmVEILSbtsKbkjSB/RECZZam3BBmLjEfBS/dampb9b6MQSY/PIiIiIiK73bf/T38Vk/01a3WQkxEUIrrm4Z6DVKAK6575mbVvM8JMwcPHyAEvQYpuTsw0sXqmGHSzsKBOFfUEFibOqf/1v+7UKcsucvo//cy4s94lAVBRMlS9mhxBuBG5UJWuysSsDeP876/WXp68WZXElBLuvqUFgE+PpmmICsapsP+NY+z7i5OqAiDyFGgFU0REROQxOvXDnzK5dp/F0idZRdsUklVELjRNQ6/Xe2CHiTx9298Ci80H49nnFnQLVdNvnlVx0GKgiIiIiMju9fK/ej/u+Jj1lGl91mu+Kwo+2yU8v1t4FiN8oCXA9NDz/6OxuV3aMX0fIoIcRjaHqqZtMr3s7KfH9U/Pw7Wi0K0AcOHnH7LQ1sRaQ997jEcNbckknCqljX9f2ysAKPi/O2y0UfRg4plxDxaPv8DgnRe0MibyhCkBQERERORxutra5V99zuTqPepSkVIfK0GdetSWyONmp8/wuRdzPQEtbGOhb7ZjYLY4tRn8NwKjTA8REREREdmd7M19sfTGy4wXEhNrSD7dhfqQUuGbf57ND7Ym/5bpIY/Ig2IFSlAVSDnw8GnndqcAPa+pR8Hk2j3u/Pi0rrpsutDY8oXr7PM+3kK/6lN5TSmFkmMjuWdGCTu7i3vqqn+Urhrm2CZUh4YcOHl0p09NZM9TAoCIiIjIY1YurNiVj86wfvUOw5LwbFgJ+qmiTBo9gO1C8+/J/CLfrBIAaBeBiIiIiMiu9spCvPyNN1j2ljxwrDIoLV627g4uDwnuWzxYAUAeXQGyd/Mop0uy9oCEYeZdy4UmGPqQ9vYaF3/7+U6fsuxCl//2U7t74TpplOmVRMqGkzZef9ivrubvu8OsFWYpBXfI0TKxlt6RAxz4s1c16oo8QVp/FhEREXkCyvllu/nhWdrLd7EmKDljbWGQ6p0+tefebHFvfpGvsLlA8GULBb/rNRERERER2VnHv/UW9ZH9jLxlXEbUCaJtpwvg3S7/sAcP2OxVPT9PCCuEaT/xo8oWFAssglSCKhwzxyyRomaBAWl5wt3zV+HCqmZb8lA3PjpNbwzVqKVqClUYZkZ2No4tc/Xw7pAdU4Bcuo+FwAx67rTRUpYSh999jfrtw0oCEHlCNAKKiIiIPCnnl+3Gp2fJt5cZlqrLUHetZ+xW2bqjm5yKiIiIiMizYuH7J6L/8mEmlRFWqICSG3rJsYgtib+FL3/unyUCyOMR1gVmt1RbMMOi+2LVGr0G7l64xvqHVzVZli93rbEb5y9Tjwr76wVoul/qbNb9Pk//9Wwk8oAa+O0C2cFShbsTpRtx3Y3xtFLL0XdehZeHSgIQeQL0PCMiIiLyBJUzy3b/0wu0N+5RSmFMUXB5h3l4d8x9bfvu/3jYgqB2EIiIiIiI7D7vvxhLb77MijVkL1SlMCBBk7ve02w+34dB8dkzvxPmFLrnfLUAeDKCLjg7251dDCICawvWZNau3Obu5xd39Bzl2XD/n87b3Us3sHHbBfe9C/5n39oGQLP23SHMMa9oc1B5wgNybggrlDaz1o6oXz7IvndO7PSpiuxJGgtFREREnrDJqXt269MLcG/CsPRI0yDylwWgZ6+lePj3yJP1sGs8v4NAC4MiIiIiIrvEsYV46b2TDI4coKmC1jL9qiZPGqqqYtw2XzqH2v5Yr7nWw83mobPqCPNVEn6fNmmzl4pBdqOEEcWoW2MwMu59cQ0uj3T15fdy69Q57l26Rc9qIk//PWqOvmuFG+PxGIA6VRDRVQFJRtSJtZQ58varHPj+Sb2LIo+ZEgBEREREnoLm03t27+dfcPBeotc6yQzHuocxt43M9UJ0wf8CVYFqOqEN2yxRr4WpR1OsdMfc1+Z7fc6O7SwKFqrfICIiIiKyWxx5/wS9wwNWyzrUQVihKS1WJZoATzUF23j23/LMz+z5fnN+MF8F7MvmBc+T+QC/xXSemrvPw5w8d8wns28GHQJvW2oDqxKj0lI8kYpzwBcYXbpL88lNzXDl93etsfVry6RJYkCfPjVWgtqcUlogsJRo2pYSofWTHVUgt/QHCcuFaDO11VBsmhBUmFQty7HOkW++Bsd7z/mIK/J4KQFARERE5ClZP3vTzvz0Q/qjIE3As0EJcs7kaRa0+4OPZ7NFJ1UAeLJ+V/BfRERERER2mfcOxOLLh2m8C+DPQvfZuwOMsAcnUHru/8NsT5yGrTuut89R56+rAQkjcqFpWpLX9LxmEImbpy9y7e8/0gxX/mBrH160a6fO46sZJgXP3ddTSkR0/wD7/f5GCxDZQVamY8LWATeAsCBHiw+dUWp56y+/vxNnKLJnKQFARERE5Ckan79tl35ziv5KyzD1KQVSqumlHpYLloNiQfag9e4jdO0AkhapRERERERE4LVhvPnPvkUZ1oRtnS8FRmBYgKuA1yN7WLn/YgAFn1ZR6D527wF074NPfyj1+pSYJrvngq9OqFdabn96Zkf+PrI3jH58xuLmfeqSCJzSZsBpozBpGwwoOWsNZQfNJ1rNErM2E7S613pV3VVrqBxf6PP6f/99vWMij4kSAERERESessmHt+zO6Sv4SsNCGpCKk9sWCyilW6HqyqFt3U1hSgIQERERERHh7X/+J5TFmrFnwgoe5YF50tZS9PIoZte2TIP/8/NUp0u0SNFVBtheHWB9NMY9YeHsq4YsNMadc5fgWqPt2fJIbp+9QjUuDK0HGCSnrmsiCpRMz/VPbLcI5tpabnttfTImpyAWauzAgPSNF7XqJfIY6BlIREREZAcs//yy3fn8Motjp9cYedRilvAqbfm+LhGgqwqQ5nZUiIiIiIiIPI8O/3ffiHZfzaoX1qKd7kbvOKGE6ceo28G7mUqxUQFgy/cETuAxq73QfV6mb4QlxyxRN0Zabhhdu8vtn51XZFYe2fjUTbt16gt8pSGRaHMhADfDopDMFQDbYZstLW16dGOI0R1N07DvwH7Wo+XuZJ08TLzy3hs7ecoie4bGPxEREZEdcucn5+zGJ+dZaIyD9SLRZlJKG9spYtshIiIiIiLyXHv/cOx/9ShrZcJqs05/2COsTJMANmdNSgJ4fObLeJfpsRHEiy6It90sKSMM6tSjFxX1OBiMg0v/+bcK/stjs/JPF2107R5VrogMbZsxMyIXSsk7fXryFXq9Hitry3jttFWQ+07aP+D4v/mORnGRR6QEABEREZEddOcn52z14k1665mUjZjOT2NuGWV+l4VaWIqIiIiIyHPp9UG89aff5n67TpMKg34f6OZIXWnpbk/pRuW0cIqWvx/Zw8r6x0bCxdYY3azMd8yV+bYWxnfXeMGHXPjNqadz0vJcWblwg3x3jQE1ldd4Srg7Zso12Q1mSUTzx8zq6grD4ZBihdRLTGgZpULv8D4O/POTSgIQeQR6AhIRERHZYVf+5mO7e/4aB6JP1YDH7BHNtvRW3FxoEREREREReb6c+GffYpQKaVhjFngU2tE68PDS9EVzp0fmbJbqht89Jy3WHTH30YtTZ+fY8BDnP/ic9VO39a7IYzf67Jbdv3CTaj2os1PaQtu2RCh+vFvMAv/zrS0DGCwMaSMzHo9p2jGWIHrBuCr0X9wPry7qTRT5mpQAICIiIrIL3P78Isvnr3Kg9KiKdysmbpg5FKi8InvQWlESgIiIiIiIPFf2/fPXwpb65CooBORCyZl+3Zt1p+/+v82qAbDxZ3k02ysAQFBs83pjRhuFhkJJRpgTGBZGlYMDNuTsrz/h/i8v6N2QJ2b9Z1/Y8rmrpHGh5zVVVXXjwTQJICI2EgJm1QFKUY3FJ202BqeAukAq3dgw05ag5GDQ6+EYJVraaGnqoH5hieN/9A4cVxKAyNehBAARERGRXSCurNvdM5dZuXCdpeh1lQCKd6XRSjAej7sJrDLYRURERETkOTL8k2OxcPwwDBKtZaBgZngUos1YxAM71GfxJdP06YmLCFJV4SkxnjTdzn+Mdr1hwfusX75Nc2Nlp09TngP3//6M+WpDv3U8EmAbwf5ZO4CI2Aj8uys89rT4XPn/WULRfILWfGuALpGr0KTC8OgBFl994emfsMgeoBFOREREZJdovli1y3/9kTVX7rEv96jbrnZi1aux5IzX1ulXtRaxRERERETkuVC/eTAOv3WCdHCB1nIX8AfMg3DrCqeFTXeVdj8zXwHANXd6ZLPS/vPmg3ltyeBGASw5TmJgNft7i9TrhZufX6T94o52/8tTcfEXHzNYD/LqBAvHpgH/iNiSBBARSgB4KjYj+7N2Il0SwGZzEZ99bf6nLGi8cJ8JL737Opxc0Ggu8gfSCCciIiKyy3zxn35pvdXMYunRL4lm3JJSYqE/oBmNd/r0REREREREnoojb56AxZqJB01uATDrAnnb+9HPgkjyZMyKpc/v1AWoPBEBOWd6VR9GmXZ1QlptOf/hKdZOXVPwX56es6t26bdnWGJAXRIU21JJcVYRYL4SgDwd24fn7UH/2dc2vt+AgTGugrf+5NtP+OxE9h4lAIiIiIjsQp/+T/9ow/WgWi/0S8IL5NGEwUaPSxERERERkb1r4Z8dj4Vjh2iTkR3Cg5Rso3d3oQtKl21lpD0cD82aHocChBWybQ2U2vxhTm5aKq9JxelbYokek2t3aX55XcF/eerWfnXF7P6YQU5U5g/d6T9LApAnazYAZO+OYt1HwgGfVmno3p9uvJn/uSB7YVQ1lH09Xv0339EbJvIH0JOQiIiIyC7123//QxtOYJCNqoGUKmBz54WIiIiIiMheVH3naLz83knWq6C1IJcCbl3Z/2k5b9yw5OS5SgAOVGWz9P/20vXyh5sF5eYDcx6bR2kzpRR69YCYtAy8R7817l68vpOnLc+58x9+RnNrmRSGe5cEML/r38w2WgLIk1eYtmfxubEkHqwoMkvs8ujGmkJhrR2x7i1Lx4/g3zysJACR35MSAERERER2sc/+w4+s3FqlGrUsLCywMlrf6VMSERERERF5ct7aF0ffeAVfGjK2LtgfpdCWQkuQo9v9P2sD0Hp3AKTSJQDU06xpJQA8ulnwf3vLhVngzt2pU0XJGYqxdmeZC6fOMv70tq6+7Jhy9p7d+PwC7eqYnPNGsF+7/p+uWTJWGBSH1pxsvvHa7Ajz6dF9v82+HsHC0gKTOrg5us/b3/8WHOvrTRT5PSgBQERERGSXu/SLj+F+w/rNFfYP9gHd5KkrmLZp+4KMiIiIiIjIs+bgK0cYvniA25P7UAdWGcm82/FvRszt3G3b9oGft22hIc2RHt2WvtxsTaxwoLKKsjZmX+rR3lll5Z8u6qrLjht/dNPaO2uktZa6OJVVJNJG8lAhKK4xYifNX/rtUX0LyE03xjeRiWEiL1S88+fff2rnJ/IsUwKAiIiIyG53ubHLvzpLXBqxOOoTYXjllLbFc8EJCqX7GoWYbsXYnMT6xhGmxz8REREREdmdlv7keBx55wQrsQ59yJ4ppcUscLqe3Rtlu0uQzEnTneiFrrx0mwrZu0LSRnkgIUD+MFUkaAIv04CcQ3jXmzsMcgRuxr40IN9aY/XsjZ0+ZZEN9z6+wMKdwrCtiVHQq7o1lSa3RJ1oPGi9G0ScmAuYaf3kcSh0O1U2yv1TsCgU646YfrTYOl7HNEljUNW04wkpJVqHe3md5kCPl//qPY3sIl9BI5iIiIjIs+DCst2/cJuVS7dZKjV5fUIv1UQEKSXqumZlZYWqqjZ+5GELXVr8EhERERGR3WjxO8fj2HtvsFrGNFUmHLDyO39mew/pWZ/pMve6PKLpnLOa66HeRiETFIPKneb+KrYy5vbZi6yfvaX91LJr5HMrdv3zC0xurbKU+kzWJ6RUY9NKALNe8/NmQTONH4/X/Fg9U7a9vvXrXeJAmn4sZJpUaPuQXlyi/0dH9Q6J/A5KABARERF5Rkw+v2w3z1ykvXKXA6UHOLlKgBFNZtjrE7mbJgVMs9eDmGZVi4iIiIiI7Eov1XHk7VcpCz0mlC6xOReSpjE7rpQW8+iqLwT0LVFTEZ7AnF5UHEwL3P38Iiu/vqLgv+w6409v29q1W6SmMBgMGLcNKSXy+ph+hqp0ayjzyUOu0PKOK9PDPQGbCQKZIB1c4Og7r8HJRb1TIl9CCQAiIiIiz5Dy+U27eeoi3B5TZ6eKmmbcUtrMQn9AyfmBn3lYlrWIiIiIiMhucfz734ADA+5MVog6EQGOAa4F7B3m3r0DbdtiZiSvsDCsgLUwLBVxd527P7+k4L/sWnd+dNaau2uk3I0tbsagqkkF6ukySmz7F6wkgJ2XIyh0a1qO4e6My4R1a/EDQ15++7WdPkWRXUvPTyIiIiLPmPazu3bpw9Ok22OGE8OzUdd9mnFDZY5RwArFoFj3wKeHPhERERER2Y0O/tXb0Tt2iNW6EMMeJGcymVBVNaEA3I4zAwgiCmZGzpmmyXipGOSa9sYKZ3/+8U6fpshXuvDBKcryOmkS0GTcuzQjA9J040TY1rL0srPMbKP1CIA7tBasWcN6yiy+fJh933tZdwqRh9BasIiIiMgzKJ++b9c/vUBvpXCwXsRaKKVMd2d0j3izGdB8BYDtGe0iIiIiIiI7ZeF7x+LAG8dYqTJjaylkIjJ1qmhLty1XwbgdZEGJFiiQHNxoA8BZsj4LjXHxg8/g0kgzTdn9zi3b6sWbvFAv4G0h50zQrZ1YbEsCmB6yw9y696MUIoIwsAQkaKtgXBVeeud1Ft9/UUkAItsoAUBERETkGbX+0TVbPnMFvz/Gx4Wq6tGWWaR/MwkgbPMQERERERHZFU7048DJl1ivM+s+ITxo2jG0mZQSbSlkzWF2XEzLMJgZuFHM6Vd9+mNj9Ytb5NP39S7JM+Pu35+2dGedXnbquqZJkL0r95/mso20frJLTDMx3J0wyJEJhzo5UBhXhXKgz/43j8Gri0oCEJmjBAARERGRZ9i1X5y1+xduss/7lPUWD4cw5ueqBe2aERERERGR3eXFd1/HDy+Q+0ZDizssDhcopXQtAHo1BcVzdpq5b5ThjlzwcHrZiTvrXP3k3E6fnsgf7JP/+49sWComyyMC25JoNB8wUxLAzosIzLoEADOjlIKVwAJyafF+4s5kjfrIQRZPvLjTpyuyqygBQEREROQZd+0fT9nNzy6wSA9rAg/HcCIHpQSlckrlWBQqLaCJiIiIiMgOO/yDV+PI26+x7i1taeinhOVM20xIdYVXiZwDI+30qT732igUA08VzbhlkZr2zipnfv4hcW1NIVJ5Jp3+2a/ZnxapIhFmlOTk6WvJfKPyheyshOExTQSI7r0xCqlkapxxbolBYr0qHP/Gmyx867jeOJEpJQCIiIiI7AG3//GM3f78EkvRIzWQoivL6O5MJhNKKVTulDbrAVBERERERHbM8N3DcejkK9yZrFA8sAhSAaMrwz1fwUzR5R0WRoR17eYmmUXv019ruXfuKlxc0dsjz65PV+zW6YukxrFsNDmwugIgNw1V1ZWYl93FmGvVYAUIihdKz7jXjjjy9ivYiX1KAhBBCQAiIiIie8byP5y1lTPXWGwq6lJT2qAU6Nc1FUbOXS9NERERERGRHXFsEIfePE67VNP2wN1JAVXZDOoU7w7b1pNbdkbyijxp6UfFICfGl+8w/vlFBf/lmbf86Rc01+6yEH0CpymBY0QuWIArBWnHFesOpzvqDKl0Yc1Z64Y6VTRtS9SOHV7ghXdf2bHzFdlNlAAgIiIisofc/LtTtnr5Fr1R6SZFTdCLaiMBwJIrh11ERERERHbE0fdO4i/uYznGWF0R1gX5fbpfsyjetutUVtGsTVjwPuOb97l56oudPiWRx+Nqa7c/v0K11jLwHrkpuFcM6h6R2y4LSXYNi82qMIF3R0TXrsEK1nNWaVg8fpj933tZb54895QAICIiIrLHXP8vH9vqpTsMS2LofWJ9grUFryompd3p0xMRERERkefQgb94IxZeeYF2UGELfdYnY3LOD/nOmB5lWuJZdoqH49lY9AVsZczts5dpLiwrTUP2jk9v271z1+mtZRasB7lA6sJmEYoh77z5pjBAGGBkM8K6z9u2pfJE206wQaLpwUvfeovhN4/oDZTnmhIARERERPagG7/9nOWLN1iiYmA1Fk6qK9qSu/mSiIiIiIjIU5Le3R+LLx8iDxJNHTS0mAfuTvZu539YVwlgdoA6cO80A8hwoLfAvYs3mHx4Q7NJ2XPu/cPntnblNouRyE2hbVusSkRkVQHYJWZvQ/bumAmDXq9H0zSYGW5Ba5l2wXjx3RM7c7Iiu4QSAERERET2omtju/XhaZbPXyUy4EZb8kYmu4iIiIiIyFPxUoqX33oNW+oz8UyJYDweU1UVlpxsRuu2Uf5/1hIgLCgeSmDeYZ6DW5euc/fcxZ0+FZEn5u7ZK7R3VqnCaKJgprFnV7DYiP6HQZ4e4BBOmOFVIkeLJ8ijEe5ws6xRDg859ufvK4NDnltaARYRERHZq66M7fqHnzG6dZ86O76e6VMBbExk5x8Gw7Z+3X/H6yIiIiIiIr+Pkz/4DtWLS6SlPpNoyKVhoT9gMplQStE846nbOtubXf8H5oLhVNkZTpzrH34GF8d6l2TPKqfv2tqV2yxETS8SOQdG+p2D08N+d1JsrqNoXHt8Ztdy/poaYBGMRiMWFhaYTCa4O4XABhWjlDn8zgkO/dHrSgKQ55ISAERERET2sqvZbv7HD62+3XK4HbLQVFACrxxKhlIY9HrgxriZdGU36XbdVHm6+4atE1tNYkVERERE5PdRf/uliBcWGfVhLY8gQTKI0lKnamNzpwUQRqHr61wwLGbHTv8tnm1O4MR0LteF9z26A5zi3Y5naqctLckcbyCvN+yLPutnb8L5Nc0CZc+79aMztvzFNfojx5qEew047hV4Ahyz7lchR6EQZA/Cut+xVIKqdOsooPWTx2J6EQtdS5jNe0bBouAYjpGbFq8q2tS1CPAMxY0r9Tr1u0fg+FB3EnnuKAFARERE5Dlw9n/+iQ3GibSaqYvTTlpSqnBPrK+vQwmGw+HGZLbMZ1WH2t6JiIiIiMgf6PgwXvvOu4yrwiQVspWNl+YD/9vnG2XuP6F5yCOau4Cz6+xz1zQMSimkOjGZTBgOh4zX1lkaDDmydIiVK7e58uNPFMKU58aNvzll/XGQJhATyDnIUbCAiCByBiClhPuD4bXtQ5bGsCfrYfePWaJAGJQFp1lwXvnGyR06Q5GdowQAERERkefEh//hb83WWvbbgH5JRO4mRO7e7frPgeUgW9B6MKm6bHaYlrEr3aEJrIiIiIiIfJU3f/BdYpAoXr76m+WJKdYds/Lk818HqDBoM8Nen9H6Or3egGZtwvrVO1z5//xawX957lw9c4F91AzogvwBJIyKriKJA2ZAKVQFquk6STFoEzTTqJuGvp1lAWmSWRgOGZ44yqG/eEOrWfJcUQKAiIiIyHPk7E9+yfLl2wwap6Yit0HyGjOjGU82yv1n7yaveW65Z76XnYiIiIiIyJd59X/4blQHhqw2oy3VxeQpC9vYkTzb/d8ldJeNI3KhMqc0LQln6H0G1Fz+9OyOnbbITlr7xWW7c+4Kw8boWYIIohQStlE1kQiIIM1tksgOrUOZJQDszOnLlFOwpsEdllPLvjeP0f/mUSUByHNDY5CIiIjI8+Rqa9d/9Qmjq/fo54oq6mlJO8OSw1wJu2Jbe9Z5OBauB0gREREREflS1bdfjP7R/azaGBukB0piy9NmBLMsjAK2dVtySgkPw9qglxOMMnfOX6N8dFupG/Lcuv13n1m+vUodBiWINhMRG2X/rQQJw6eJNWFd+f9Zao3sPAvoVTXjZsJ6Clb6xsG3X4GXat2W5Lmg9VsRERGR582lxm6cusDK5VssREXPaqwYeEWZLs/5th5qm0kAT/90RURERETk2ZDePRzHvvkmN0bLpKU+De0DAWfZGWGbZf+796Tra25mRIa+96jbxPqVu9z6m08V/Jfn3tXPvsBXGoYk6lQBm2sjpZSNsv+zY2PdZIfOVx4UEbSlwXpO6Tvp8CIH3j+506cl8lRoLBIRERF5Hp1dtlufXmR05Q6pCRyjEBRiozTkrJRd8a4VQJ5fMBIREREREZn3yiCOfvMN1ofO8Og+lkf30V7YnWfTSm6Fbk4X2xIymqbpqgCMYTAKbn1+cWdOVGSXGX9yw259fgFfntCzmkxMN0jYRtn/WQvFYNpzPsCnv2Ia/XZWGLS0WJWwkqkqZ60ODrx5jPq7x7S9RfY8JQCIiIiIPK/O3rWbn10g315lENVGwN+maetzRSI3gv+zknYiIiIiIiLzFl4/znhg2FKPUUzwysE1f9hpPheoDJsrUT6NYFZVD29gsVTc+PwC5ew9pX2LTK399JJNrt/HmoAWYr59otvGOslMii7opsDbzisAdeoGwVwYjdbIVbBWFY6+dQKO9ZUEIHuaxiERERGR51j+/I4tn70Kd9YY5kTKRr+uaSYtUSDVNaWUrrxdcpqSd/qURURERERkl1n8/onY/+pRbKlPGy0FaKMlIm8EmmVnhHfx/IjufQjrIpThTlsy1kK/JG6cucS9X1xU8F9km5ufnmPYOj0qEonAaaLQsjX4b8w2VXR/VvBt5zVRKIBTqMzBg5yCsph460//aKdPT+SJ0hgkIiIi8pxb/c1lu3f6MosTZ6FNTFbHDAYL4MZ4PKZOicqdtm3pDfo7fboiIiIiIrKLDN57KRaPvwBLFSUFQcYJzAySlp93Wtu2mBl1qihtppSy8fVe1WdIzerV29y/cH2Hz1Rkl7rU2Cc//RX91shrDRTDU40lJ2yzgqLPZQMo72n3iI20ptK9Lx6UvlMWa179l+/qnZI9S09gIiIiIsLyry7bzY/OM5wk+vTJOQic5DVeIIXhDm072elTFRERERGRXeTIuyfoH9lP1EYhYzmwopjKbpHqCgDLhRRQpYSZQzF6JeGrDSuXbpO/WNbuf5Ev8+k9W/7iOkupTzRdEs24abCYrpdMh7xsmwHnabEN2UGzigyF7n3xkvGAqCAPnKVXj8KrPd2wZE/S+CMiIiIiANz+xQW7c+4KC1FRN0ZVnIRtLBQ5Rs5qASAiIiIiIp3hd18OPzCg6RkNLVBwoguwRJdUPLf9UnZIKS05Z3qpImFE09KLCl8v3D57ldFvruhNEvkKt354ymy1Ycl6VFbRS72H7vQvdNUAXGHlHefQVaOZtkLxEkRuyKUwSpmV1PKtf/GnO3uSIk+IEgBEREREZMONf/jc7py7wmLrLNLDRpmaioigaSf0evVOn6KIiIiIiOwGbwzi+LfeZNwPGiZEZJIbyRzHcK+6wIvsqHYyxsy6o3Jyk/EG9luf0fV7rPz4rN4kkd/TxQ8+Iy031OuFumwNsJW53f/FukN2nkWA2/R+FFAKmczEgrxQs1oF7/0f/kzpGrLnKAFARERERLa4/ben7N65qwwnsL8eELkrb1dVlSoAiIiIiIgIAO/9ix8wWXByFRQrG02vcxRydHMIxb92XuVOnRKFQlsylTn7qj6+Mube2Ss7fXoiz5TyyS27/vl5+k1QTcDCKUA2I9gYBolpMkDZyZMVoKtGQwnMDHfHkneJGh4st+tMBkazkBh+92UlAcieogQAEREREXnA7b/7zO6cuUgala50p0GqKpqm2elTExERERGRHfbmv/3TWKsyTSqEFWIjzFVoCbKBh0EOLUDvIAf6XhG5kA0mJVN5ItZbbp36gvbTW8rREPkDrf3TRePuOr3pFv+y0enEcLrfuy4pYOfOUToFh3BSDlJ070k7e5PcqHo148hMhhUHXz9OdfKQkgBkz9Dzl4iIiIg81PW/+9zufHGVhaipstOOJgyHQ8KCsNhIbZ/NnWbCtrb5nH9d7T9FRERERJ5tB/7irWgHFWMPJiWTYy5eYglLjnt3RCiW8rjM5lK+7dhuS0lyoCmZSdvQq3r0qEkjuH/xBvd+cVmzM5Gv6drpC8T9EVV2LLogM3TLJBab6yLzxx/iy36/5Q9jybvWDNMqABHzRyFHS1QwjpbFF/bz2jff2OlTFnlsNIaIiIiIyJe69cPPbfX8DQYjp2p7RDZwo5AJK+TSksypPJEnmZRqsjnZuumqRzcZtvCvPfEVEREREZHdYem7r8SRk8cptVEiCLfu+d66LbCRAwpEQFsKYaYS2I9oexDRAlKBKjupOO4VBSNHV72tC0AGhaA4jK1QDwfYKDhYhgxXgzs/PKNZmcgjGH1yyy6dOk+vcaqJ08Mpk4aqqiglU0rGKn9oIkB3+PTYtk5igRNYdIcCeF9fV4SmJVIQycnWtaVJGI5hQbeWlTPhLWMmVIcXOPyXbypzTfYEjR8iIiIi8jvd+JtTdu/iTQ74Aj4Cx8gEbduy0B+QJw3teMLBgwdZX1/HA3xuuhTTAzaz4UVERERE5NnSe+/FeOnNE+QacuWkXv3QBN/5Z34l/z4Z83OqUroUC8eo3Ls+1xhmRpiR6poyzgyiYnx7mXMffL5DZy2yt+Tf3rJ8a5X+BHpUDAeLjCZjvF8TZEozYbYasn0d5GHrIlvGS1Pq1OO0UUFlet1nl7qUgjtYlWgt0zgsHjtE+s4LWrmSZ54SAERERETkK9399Cw3T3/BYiSYBH3vsVAPacYNXldYr+Leyj36/ZoqClUUoJC90KbusICkKZSIiIiIyDPp0PGX6B1YYlRa2ihMItPmvOV7tpatnpbGlkcyS6iYLy2eHbJ3861CxqKQrNvR6rF1R7G3ULVGneHCqdNMPr2ptAyRx+TSf/ylveALtKsTJpOWnIycjDpVkAs+rcix/ffYNz5/sNh/MQi66gBKA3hEc+0ZHqYQRDKKd+9b48HwwD5eeuM1ODnUCpY80/QEJiIiIiJf7Upj9374ma18cY190cNGhZ71SBiTtoGNnSZdOco0naXOT1id6UR3h/4KIiIiIiLy9Qx/cCIWjx5kLSa0tUHdtQWr6oc/3Ttbq4LJo3G6ZGqnm19lg9a7w6zb7W8BVoJSulYAOQIrRjUODleLrF6/Ax/eUfBf5DH7zV//Vxap6Vki1X1WVtcwSwyqugv0s5kcNR/u9y2JAFv/m8W6Q74+m157Y/762sa9abZeVYA2CpGcsWdGZAYHlzj0xms7cdoij43WX0VERETk93bzv5yytQs3WYoezXpDE0BV0+SWXq9H5AwEZa5v3SzwX0AZ7CIiIiIiz5iF75+Il995jVisWfcMfae1THGbe7bfjF7NB7KUBPDoZvOp+bYKs+B/ts3v8QBKdIFDusSAOoxFesTdEddOnd+Zv4DIXndlbCtXblFnJ08yi4uLJIxm0lVI8XiwkodRHijzbwGBbTnkcfGNsZT56zrdpdJGS2NBqWCljBlXhf2vvAjfelF3MXlmKQFARERERP4gN/6XT6y5vsyQmlScXj3AvGJ1dRWv0pYs9RTgZa5UpeavIiIiIiLPlP0njtAMEqsxoVTQWmbUjjEL8jQBeLuH9beWr2d2Led3rc7aAGzfIRwGuJFSojanzk5vVDjz60/gzIpmYyJPyPX/9SPrjQvD4uT1BgqklDZen2/LMbP9d3ve7Pu3/4x8PR480ArAAiICMwM3mtIQdSL6zsgKeZB47VvvwNuHdEeTZ5ISAERERETkD3b5P/7GxjdXWPABk7V1DEh1xSS3ZC+EFYzuSNGluOdpAoAmsCIiIiIiz4Z9f/ZG1AeHjGhoq6CkIICqqjAzUqXl5aetWFdxDQKP6T7hmO78T0ZYwsOpGvD1zJ0LN2hO3dIsTOQJO/Pv/sH6a4V99Jmsj0ipAtioyjGrirgluG9dJQCPbU0CvqJ3vXx9FuBhWBgJI0rB3bvEKjJWG61lJrQMXtjHi++8Bq8MlQQgzxyNICIiIiLytVz5pw8ZXbvLfhvAuFCnCnMn8C/tVafgv4iIiIjIs6H/nVfi5XdfZ1IH1IbXzrgZUaKlTol2Mt4sbc3mQvP8rlX1sH50X3YtN657dOX+s0GZvguRu+B/tdpw49zlp3vCIs+xa5+cpV7LLNZDStMyX25+VhUxzyUEzJuNp/L4Pexe5EAyJ08aHKPyRNu25JwpBFE5t9eXWTp6iMGRA0/9nEUelRIAREREROTruTyx6z/7iPHVOyxZn9IUvOoy3COMEgaWiJTIJaAUatPjp4iIiIjIble9dShe/fY7rNaZnIxMJueGuq5xjNK2XQJwiY0g9LwCFHtYiEv+YNElWYdtXk2Prt1aKhC50OSWMPC6ohTwFnqj4PJHZ+CSSv+LPC2TS7cYXbnNoK1JUXdfNKcQWHImJRPJKQ45yuYIOa0EIE9Omau8sJFoUaBONZYLFkHljgWkZLTRQm2MveHt738L3tqn9Ax5pmgFVkRERES+vqsTu/7hWdYv32LJhpT1titzZ0YxaKIwaRtSSvS8okxaZbSLiIiIiOxyL7/3Ou3QGU/be810u867oP8s8P+w4D+of/WT4tOd/6lM3wN3BoMhTVtoxi1975FGmTvnr9CcuqN3QORputba7XNXWL56iyE1no3S5q5KR870ej3akhk3DYOF4ZbEHgu2VFUxVBHgcfmye5Ezvd4BXroDZte9UFLQ0HKnXeOP/sUP4Fitd0SeGUoAEBEREZFHc37F7py6QHP1Lgu5R0wKOQKva1LdZbxHLvPd7EREREREZJc69C/eiP7R/SyXdaLuEnu3dKcO2zjmwlQPLWktj67YZg/x2c7VVLoKAB5QSjAej6m9pm81aZSx5QnLP7uk4L/IDmjO3LfVy3dIa4VB9KlI9Ko+pQ3AMUt4lZi07YM/PK2cYtEd8qjKxhFWNv40nxDg2w6LzXUrd8hesIWa9Tp46f23n+rZizwKrb+KiIiIyCPLp+/brU+/IN0f028rUiRKhojoSoNGkJuWytNOn6qIiIiIiHyJF39wMo69c5KVGFN6QY6W7WH92W7J+Z2pClM9efMBq/nIfq/XYzJq2Fcv0G8NX55w+cPPnvr5icim1Wt3uX3hGnULVXZsXKg8EW3Govu9HY/HD92ZrqDd4/NVFRQedj+bv/5t2+K9xIiGe+MVlo4dZt8fn1AVAHkmaCwRERERkceiOXXHbp+6QL3Wsi8GpAbyWkOFUaeKmKZRa3FQRERERGQXOrkYh988zopNyP3AKyjRbClRPe9hgastQepQ+erHYXsrhfnLXgzaklnoD2hX1hlMjGufnINzK9r9L7KTrq3Z7b8/ZavXbzMsTowm9KMiYZALkQtVVf3O/4RaqDwGFl3bmun9aDaezlcC2D7Gzlez8WS0uWESDb7UY1QHr37jHXpvvqC7m+x6SgAQERERkcdm9cNrdvuzi9jddfZbn6H1sGJEBJmgUQk7EREREZFd6c1/9keM+sZKHlES5NKQpqvH80kA88GS+UCJglVP3qxHuEcX/A9gnAvJa5ZSj/sXr9H89rreCZFd4vq5SwypWfIB3mQ8G1VKRM4ks4cmSWnV5PGZjZfbA6EPu4c9mBQQmAWFTNWrWM8NuU6s0nLi3ZOkl5aUBCC7mhIAREREROSxWvnFJbt36iJ2Z51F7wMwjgzJyVG0MCgiIiIissu88q+/H6MBsK9HVNA0DT7dNRkGgRPmFLYes+Xlh5exDhzFRx7drCf4Zn9q6BIAskOv36dtGsrahOt/+4lmWyK7SHx8x25+cYmqQGoDbws9SzhGzhmYlUqZO1Q65QnYfp237v7fXg1gdowmYwaDAZPcUjwYp0zuGf39Sxw59tJO/oVEvpISAERERETksbv368t274tr+HpLahNOoqpqzPxrz2W3l2UTEREREZFHd/Av3w07MCDtH3JndZmqVzMY9MhRwLc+gH9V2X958jwMmx6pGKw2LOSK0//Xf9Q7IbIL3fgvn9n6zXss1AtUVtFOMhSj8gSwkSY1G0td8f8nwmPWQsUeWF+afb69jU2v6kMT9LIxjIpBTtStcfvqTa7+6rTGXNnVfneTERERERGRr+nWT89ZvTCIxRMvYtZjddKSUgV5QkqJbBARRHQfLZyUEmW604jpnqLus678WufhZfJEREREROQPdHIphscPMukHlif0+zWlbcklKCmRMRJg0x3oD/NlXy8oNgIP7sD7spYJs+v4wPdb941uRuQMGGYJmkI/nAOlx9mf//Yxn7WIPE4Xfv0ZbxzYhy8kzKAyaHO78QufrQtO+8bAML8aIl/X7D60Ma4GYM58cRp3p2nG1HVNRMHdaZuGyhORjcpqBk2hlytYHnP50y9Y/+iKbnCy66kCgIiIiIg8MVf/9hNbu3GPXgPVCGoqer0+k9yS2xY3w6cBfUuu3UMiIiIiIk/LsX4ceusEZTFR+k6kwCh4BB5d2f9iXWBaCbiP5utcv9nPRC7TAFWDmeF1D1roU7MQPW6fvUJ7a+XxnrCIPF6X1+36ucvUJWFNxqJroWK/YxFE4+7j1aVSdCFRmx45Z8y6agzNeEI7aSGi25xSgp7V+FphIdeMrtzh9P/8E1PwX54VSgAQERERkSfqyv/vA7t74ToHvEdqYL1pCXcGqUc/EpYLlTspGU2eYNH1uGT6//O0NJsXw4t2/4uIiIiIPA4H3zrBvhNHoFcR1TQoUrqHbQtI00Me3cNifF+WWDGbDW2UAyew2XZVd8a5pY3CsCTWr93l5pmLxNWRAlIiu9zqj8/anS+usuR9YlJIGA6ksI2xNlt3bI4E8ihmAdC8UfK/kGLzqFNFaTOJRD/1WEh9yriljcBSjY8LCySunr7Apb/+QOOsPFOUACAiIiIiT9ydv/nU7l+4TrXWUpdE5TURRm5a0rQkW86ZZFsfT+f7sjl6eBUREREReRwG3zgaB157iUkNE8/kaGlLQ8Q0AcCMhJGKdqE+aV/eQmH6uhnRZuq6xlLFeNLSs5o0Llw/cxEurikoJfKMuPvRZ5T7a/QtUUUiFZ+OAQ/+Gmv949FtT6Fwpglu03tbZU60eeP15DUpavqlRz1xJreWufzxGe79wxmNs/LM0RgiIiIiIk/Fjf/8sa1evMlCTvSioplkxqXgdQVAbht6dbW5FcYCCJUcFRERERF5jKo3DsSBky+TFyrWfEyhISITUcgelOTgjmeoimkB+RGVbcfM/HWdn+/MkqDDuu+36JKlw6CUwqDq08vGvYs3KJ/cUlBK5FlytbWrn56j10Ad2yocqifiYxUGxbsjRVCVwKJbZ4LAI4hmQvKu3c1a0zKJYNAbMhwZo7M3uPHbc6z+/JLeGHkmVTt9AiIiIiLy/Lj9d5/ZqLRx6I2XqRcXWcsjmgDM5jKvu2WxmPVmm06IVfxOREREROTRHX3nNeqj+7lb1vBeghLdzv/khBmBQemCJY7Rpp0+473L+fJ5zkYsMOj6U+eM58IiA5av3OTu332ioJTIM2j8wXW7vtCLI+++Tqmd7HQxaSsb6x9KvHo8Cg+/lmU2eppRSuCWGPZr6mzk5RErF29y97NzcK3ROCvPLCUAiIiIiMhTtfb3Z81LxKG3T1Av9Fgv61iCVCXapiUl6yZjViA2p2phs4QApQKIiIiIiHwdh/75O1EfO8i4HxBgEZgFQVAsCE/d1wksHMO652+FQL62WSD/ywJ7sz/Pz3LmNwJbciiB5Uyv9Ghu3uPuZxee0NmKyNOw+pOLtu/A/ugdP8wkoHghzEkb6x3RrYuoKsAjmY2v2cCsK/0/EwZUidxkUjGGxVm7eIflizeYfHBZF16eeUokEhEREZGnbuUfz9mtM5cYTKBPTWmDwLAqAY7PugBMv19zXhERERGRR/TGUhx84xijKlgpIwYLQybjdbxkLDIRQSFTCApBWBd81rP4zoqAnAt9avZbzejKHTizrHdF5Bl37+INqmxUxfFQqO5J2NJehc32KtmhYLRtoRcVg1EwvnyHWx98ruC/7BkaVURERERkR6z9/Vm79/klFnJi6H0mbUN4t/s/pZpE6hYhfTpJi4ASX/0fFhERERGRB7z5599jvReMq0LVS0zGI4Z1r0u+NcNsM+bh7rTJmFDUiusxmQWeClt3+5dStlx7s2lf8BK4OzmCQerTnxjLX1zn3s++UHBKZA9Y/+iGXfrkDD7K1Dh1qmhyl4xlZpA1+j4qMyMiiAD3RKRENqOYYZYYlIp9bYVdvsfNX5yCi6saX2XPUAsAEREREdkxd09doO73WThxmLbq05Z2GuTP00natB8pYBjuTkTe2ZMWEREREXnGfO///N/G7bphJa/TG/YoAVaCZEa2rhO9Aals7hcr04C1KQf3iaqqauu8p9Bt+48gKHg4McnY/QlX//oTBadE9pDxzy7Zyv6lOHDyJdYn69TDPt14HFRVRatNEI8krFtL6tU1pcD6+jqVJwb1gBhNGLbG9VOnWf3pJY2tsucoAUBEREREds7Vsd3gdLxUMgdOvsT9gLYKsgdeAovAwzbKkYbT1W0TEREREZHfy9G/ejdGg2C1mXDg8AHur9zDLOh56p6xAwgnzZWgzgbt9I9VURLAo/iya7elCkAEzmYlhgAww0lYhkGbuHbq9JM/WRF56tYu3uTgiwcZDCpKQCZoJhPcHU9JVVgeQVu6nf+TUYMVY99giaoAKw39dTjzDz+FK62C/7InqQWAiIiIiOysqyO7+ckF1i7eZiGnjf53Fk4yo7buI7DRi1RERERERL5a9c7h2P/qUW6Pl6E2VtdXgMKg16eUQsnQLRE7Fmw5YLMKgDw5pRQiunL/7g4lsIAqjLoY+xiwcuUWax/d0Dshsge1n96y659dYJAdJoVEorJESmmnT+2ZZgE9T3gJ+lWfntXEyoThumE31jjzo18p+C97mioAiIiIiMiOy5dW7Vr5LF6pvkHvxQWaQSIiU0ohWcGMjUMVAEREREREfg9vHIxj77/BujdUS30iCpNmTF3XTCYT6pTIbQvYl+wSK3ho9/+jml3b2S7ehyVUGN3Of0oQbd5IBqhbY+XidW78548VpBLZw0a/uGzrBw/E4JVD5AStO20EYfrV/7ocyKMJ/d6QKIa3mXotuPPFBW799IwurOx5qgAgIiIiIrvDlZFd/fgs1XLDYBT0oiJZRYuRbVoOUwuQIiIiIiJf7cQgXnrrVerDi5S+s96OaErD0tIShaAtuQtIu29ZIA7ryv+HQYrukMdj+0J8zFVXiIiNwzF6qcLNaNdGXPvN50/9XEXk6bt++jy+OsbHBbNEDg3Aj2qxGsLamN4448tjLv/2tIL/8txQAoCIiIiI7Br5zF27//llyo0V6qZgZuRkXf/RCCyr+52IiIiIyFdZOP4Sw5cOs2aZUUywykmVc3/lPrixsLTIaNLgXpGKk4oTONmc1rvAtEdQTcvRy5M1C/6bGSklzIz1+ytcP38Zro0VrBJ5Hpxfs1unL5JX1qnrmtTrqwXLI7CAPGlYSj3K1btc/qePyR/f1BWV54ZaAIiIiIjIrnLvNxctIuLgsKa3v0/bc8wDzDYWIstsyvbQ2bAzK7A5e3m2aLn5ioiIiIjIHvXegdj/ymHKojPJmVRXlGkfrVlp+fX1dapq69Jwsa3dtmbl/930DP24OFBi6zQmpUQuAeE4Rgqw1cLq5Ts0P76iYJXIc2Tl19esOrAYS/uXKKnFqm68mK1pzI8dFlt3+M63GXmWE7e2/x0f8LAvTn/IwzG6+1ednWqUuXb2HCvnrsK1VuOpPFeUACAiIiIiu879Dy5ZjhLHvvE6aV/NWoHGITxIALmB5HhVk5tMKYXaa9ydUqaLl7MFTOumwSliuoBpWsAUERERkT1p+OaBeOFbb1EODlhhhVxBTB+Mg6DyitJmkjkWQSntlgiSsVn2v2AK/j8GZXqBPdiYm8ynWkQEliraNqioqBoY37jH+IcXFawSeQ7d/bszdvzYyfBe4j7r9Pb1Ga2tExHU/T45Z8yM0mRqKmyWVGSQvTywEWInxcZJdCc1vzmj+4bNG9BsHSfPjXwe0d2vLCil4ARGgBXa0rVK8CrRTjK19ehZRYwydTiLueLjH/4UrqxpLJXnkloAiIiIiMiutPrhFbv08Tn6o6DfOqlUVF5TSgErXZnMUnB3qqrq2gXkrTNco5tgzmfLawFTRERERPaqQ68dZ3hoicYbqn6NV/5A0az552Pono/LttcsNl+Tx2xbVK5pGhyjZzVVC4Ncc/P05R06ORHZDT764U9ZaJyhVazeX6auKlKqmEwm4E5E0KuqB4L8u61lwJedzu+6t/jc38mt6tZxSsGmrVIiumQAgByFEoFbws2ItYb9DBiswsf/7m9NwX95nqkCgIiIiIjsWusfXrULgxSvfettaDPjMiF6DlZjxYg2Y+bgTnYoFMwMC6gKzLcCmJU03Q1Z8CIiIiIij1v/j0+EvbyfCQXHWF9dI/V7ev7dYbNg1mx363Sj7lykzkkYqSlU65nLn52n/Ux9qkWea1dX7OKnZ+PY914jW41ZorWWMOuqhrQFS05Mq4pk37pzfnPv7w6ncYVtSQKYT1Ao06ooszOdtUiZlfCfbeCIEngEbkYVRo7umywZuQRRDMewFhasx5VPz3D/x+c0hspzTxUARERERGRXG/3TJbv0yVn2R02/rYi2QHgX6LfucbaUQiFjqfsZp+BRSAVS6YL+wbQtgKaBIiIiIrLHVG8diiPvvkbZ12e9tLhXpOmOSNlZs3dgFqjbKIgdYOHUqYJJYVgSa9fusvzzL/SmiQgrvzxva5fvcCgtwRgiQ683gNIFzUspFNsM/s+X/t8tiV8+d2w3O9+y7ftTbB5BBus2eiRzwIiAHE4Jp7YeVXEWS5/h2Dn9sw8U/BeZUgKAiIiIiOx6qz/5wq598gX7o0dqExSH1jAzvKrAjbYU8kYB064vnDE/693YayMiIiIisne83ItXvvMO7WJFUwfRM0aTMYuLi5Qm7/TZCV0Ls82A3HSmMk1m7pVEPYFqdcK9cyr9LyKbLn10mvUrd0mt4VFD7sLlKSUgCAuydR+LxZbgv++SJICHmW3O2NygEUB3/h6bGzm6jR9g3iU8NBSagMYMoqJqE/uamsGtCef+/jfw8R0t+ohMqQWAiIiIiDwTbv/4tGEW+987TmPQRMukbTAH3KDEtCJAN8sNAIPtc97dkgkvIiIiIvI4LL71CvbCkDFj2iZTp4rixqRtulLRO32Cz7ntFcjC6CqaAak4qYXeOLj8yVkmZ+/r7RKRTRfW7crwQrxy4Jv4oObeeIW6X2NRyATFbKPl4bzdGPyflfgP27ouMyv1v/2cPaJrBeBGKTFtceBUdU0dTi8Dyw3LV25y57PLcHlV46fIHCUAiIiIiMgz4/aPPrfoWSwcO0xvqU8TmZwzkej6ZmJAt8sp+2axq0JXXrObZO5wDzwRERERkcfEvnEgDr59jPs2oU4OuQUK9UKP0aih1xuQc7PTp/lcK9NIl4eRgYKDgYeTilOtt6xcusnKh9cUvBKRBzSnbtny8dux9NqL9D1BtumO/2nwfNpexKdrHrup7HfBu8D+7BzpkgCw2LJZI5gmMVgQ0yoGHuA5KEBjQWuQvKKmol5pSPcnrJy5zt1fn9fYKfIQu2ksEBERERH5Snd++JmtXLpJfwKL3qemIoVDgZiWwyvW9cCbHWHTyeMuzIIXEREREfk60rv74+0//x7tgpG9pW0nDFJNKYXxeEzVq2mLWgDsFlvTkB0vTlUMX2258Q+fK4AlIl/q1udf0N5a4YAvwLgQZVoXf8pjd1Y7nFUnmB//nOn5srVJY7CZCDD7udlGj0SXMNXLTlpriRtrjM/dUvBf5HdQBQAREREReebc+6+nbTJq4uX3XiP6ifXSkuqKtrTdbNKMYt080MOIXCilTPvkiYiIiIg8+459912Wew1taXGDyoycWyozzBO5TEMutjsDQ8+LXqoYtw0BmDsE9L0i1hoGbc2Z336206coIrvdxWW71j8XL/kbLBweMknGpKxhybFsuDlt21ClmlTXTCYtPh1vdkpMWzLONmQAWGyeUAooybZ8PxglghKBm220e/QCB1KfuN+wcuEG44t3mJy9reC/yO+gCgAiIiIi8kxa/9kXdvP0ZfqjwiI9bFJIliCcjFFKIedMKQV3J6VEKSr/LyIiIiLPvuP/6r0Y92HsmWLxYO/k6cftfaHl6RuNRrj7NBjneDg+LixFj/O//Yw4c0/vkoh8pfb0LfPlEcNwJqtjklVEWyhtpsIY1D3MjLZtKaVgtnuGlpg7lfnqjNFmIoKIoJSycd4pJdwrcht4NvaVmt79htXPL7P8X0+bgv8iX00JACIiIiLyzFr+0Rm79flFemvBgveJpjArIleZU+NYDiwgPGij3dkTFhERERF5RAf/+NXoHTuEL3Tl/lMBo3RtsOZWew21wNppDlSeqMyx5EwmE/pRMWyd9vp92l9dURBLRH5vF/72QxvduM++asBCGjD0PrWlaUvEIOfc7bivjLwL1j+KbU1Emy/97wEppWnwPzBz3CosnNIGMSn0osciA+o7E67+8jPu//QLjZkivyclAIiIiIjIM+3eT87b7fNX6Y9hUCo8G57BcZIlHCilEBFY0uOviIiIiDy7lt47GgffPM5KavG62lLaf9YzebbT0maBFiUB7JgCuDvr6yMsnH39Rer1zP7ocen/+0sFskTkD3b+l79l9dotYq2BJqi8xizRlkzOGdgMrO9GMT2KgZlhZrg7Fk7KgU2Cfuss0mOhMW6eusiZX37K6JNbe3rMPPm/+U7sf/+V3fmmyTOp2ukTEBERERF5VHf+4bS5exw4+TLuhfVoiFzAoyt7Z13jUzfrVuFERERERJ5BB984xmSxotROzpnanEKhWJfoGkDuHn1xCoQef3daC/T7fdr1MXVO7M99PvjPf7PTpyUiz6qrE+uPItKo0FYFek7U4FWPlBtKm6EKzOhuCjtoVoVmlohWpm0JYvqFps2kutv1H01Lys5C6lMVqEaFS5+eZ/nne3zX/7EU7/2rv2C9NLz60rucjkmMPr2xt//O8lRoC5SIiIiI7Am3/utndu+LayyUigV6pOKUDJYS7k4pBXZpBryIiIiIyFc5+Ocnoz68SK4dEl2fZ2KjxPJs9/+WCgChBeCdFAZUDu4Mo2Y/fc5+8DFczQruiMjXduk//9YWcmKpHlJVPZomUwjqugYLSindZogdZHPl/mdm96hiXbJamWYoWAlScZbSgEUqmjsrXP74zJ4P/vvbS/HWv/wTVgbQ7OtxJ4048u5r+LGhFq/kken5T0RERET2jFufnmP50g3SqFBXFeG2uQiaC64FUBERERF5Bi1991gcefc1Rn2jSgZtJpLTbksAKAYWobr/u0QYrDRjchSGVjG5eY/Jjfs7fVoisgd88h/+3kb3V7Doxv6mbQnr2o4Y7IoNEA9LRCtsJqyluupaNpZCjRNrY259cYVLH5/h/i8v7eng/2t/9Y1468++y2R/zWq/ZdwvlMVEHOhR9qt4uzw6rX+KiIiIyN5xdWJXP/mC5vYa/daps0MxSikU6/rgQWw8BM/vkPoyemAWERERkZ1UvX0gXn7/ddZSoamCSdPg7rgD3j3Mzsosb/8oj86nR2yrsDD7+szD5hYWsFQN6TWJ5t4a5377KeXKeE8HtUTk6bny2zPEypjF3gKV111lmFJIBB6PvwHM9nHOH3JsVZhvRLP5WVcbwDG8hWGpGLYVK5duc/V/+cjaT+/s2XEyndwfb/wP34v9b7zEuA+r1kBlrLcj1sqYMky8/5ffh3f36U4uj0TrmSIiIiKyt3yxYvdOX6XcXGNf9KmzE2GQEpOSuwkmdIul0woBbcmUUqiqrVnWzma2+u+TLCAiIiIi8rjte/sYt+oxTb+AB9nLZoS/BBaGhZGKbXxe2KyE9fhDQM8PB1IJLIKwIHvMtVhwUnESadv1LhuTCC/B4thYWIPLp76AcyPNKETk8fn4tjVX7tMbFSoS7hWeUrej3rrwX5g/cMzC9Q8L4HdHbByzijLzSVDz4+DGWFi6zz28GwsdWnI3dlJwN5LRJSiEUZeEj2Cp9KmXM/c+v8KNH366p8dI/86ROPq9t4hX9nMjjVlNbXdPbzN9S6SUGNNwK9Y5+cffgmM9JQHI16Y6EiIiIiKy56x+etXCI15KJ+ntr7B+Te4748k6bZsxgxyGJcfMusB/CXLTwLRP3nyVABERERGRnXD4z1+P+ugSzUIiEpRcKFaIsG7/ZGztrzxPgf8na5aDkXOmqitaWqCrOuZmBJAmmYVJ4vbF6+SbKzt3siKyZ938m48s978Vw+OHWC8Zrx1zJxPM7hAW3drG1+kO4/Hl95PN/4WurP/M7H8ncmG4NGBtbcTK+hoLCwv0Uo9oCt4Ei/S4d+EaqxdvM/7kxt5dfTlWx8E3X2HhlRcpw8R9H+G9mja3mBkWkMyI6K7ppDZSBC+8+Qq3rpzd6bOXZ5QSAERERERkT1r7+JpdT8Tht15h+OI+1ialy06vnYggAZG7nTzmjrtT2pY0nb7OdkuVaSaAawVVRERERJ6iA997JQ69cZz1vtFaIeeuatU8s70bL9kNCoAbhc3KYLPAVvYy/VpQOeQmsOREQNO01CnhLazdusvNsxfgyrreLBF5Iu6cvsjBwwdICz2W19YY7h9yf3WFXt3HouBAia8uCb61RP/m989+fiOwP1/xBMhzrVFmH63AMPVoV8aYOwv79tHkDOOGfdGj18LdM5e4c/4KXFzbu+Pj6wtx5OSrHDpxlLJYs1rGmHWtGhzbSCaL6NIpwg0zo1TGgddexlri5k/O7t3rI0+MEgBEREREZM9a/fCaAfGCnaB3aEj0ahrLFMv0cCygyS2lZCwlcIfoMtzzXFm7QlcY4Otky4uIiIiIfB0vv3+S8dBoack5ExG4+0bQvwsWyJOWt+1qnc0JCrOuYk5pMxZBMqdtW6INeiRYb7hy9iJcWFXwRkSenM/u2Z0XLsehN48x7Pcxc1KvtxFcht+/H/iWKoixWT1g/udLPLxNYomt/1uRCxSo60QpRowzi/SoRoVbp69w/8fn9vTYWL9/OA69/jIHXz5C48H6ZI1Sg3l33/C0dZ1p/q6ek9EsVLz83knuXr0V7fn7e/payeOnBAARERER2dNWP7xmZhZH33uD6lCfWzGhJVMDtXU98iaRyVGwaanO2Q6f2ezK5/rcqRKAiIiIiDxpb/ybP4771kCvphBYdDsC3btqVrNDFQCerDCIaSjLo0y/tjVa40HXZqza7J691BvQnwR3rt4lf3xbb5KIPHF3f3zO3IkXv/EGN1bW8H4FkTcC+LC5w/+rkgE2vj+69ZDtnNjSFmA2Ks5+LjPrrmgkd5pRSwUciD6jG/e4de4qzW9v7umx8ci/fCcGxw4RCzVrVTCJhhbw2QW1rkrlRjsZukWo7r4DJQUjMu1ozFt/8m0+Pf+PO/VXkWfU75v0IyIiIiLyzFr54Krd/OwC3BuxZH2GqYYMTdNMe63N6vwb2TZ3/89KfG5Z49vTU1QRERER2Wkn/rtvRzo8JBaccZlsCbKUUiilbAT/lQDw5BmbxbDDHtz1OqvMkLymNIVUnH5JrF6+zd1T53fmpEXkuXTv4g0mt5fpW020gcVmCHDLvWTbn7/MRmvEh3y/TxOgYHPzxGx8zA7FwFL3yT4fcDgtkC/d4eZ//ND2evD/xL/+buw7+TKxf0BTw7q3TFIhKggPihXct4Znt9/PSwQTa2ExsdbLHPtvv6WyP/IHUQKAiIiIiDwX7v/6ot369Av83phhqai8ZhJBw3RXTwkcm+tl13EgaZolIiIiIk/Bwh8di8XXjnK/aomekSM2Av4x/RxQ8P8pmSUD21y56zI9sM2Q2Kw1Q+RCv1Ss31jm+kfn4GrWmyQiT03+YtWunbnMYOIsMSBti9qHxZajWLC18PymQrdWEuYPrJPA5tiYSnfY9GtltoMdYzRpqaKmHsHdU5e48tcf7e0x8eQw3vy3Pwg7ssh6v7BcRoxT7gL/KWhpaSnTe0l37cu2XpPz1ShTMkoN4wVnePww+7//qlan5PemFgAiIiIi8ty4/+vLZoMUB98+QVoaMLYWLFGsUAp4LuBby92lWb+70n00TbdERERE5Al56d1Xucs6bd/IbUPdq8ht2Qj2z+/8L6WQcyaltMNnvbf5dAdttmkLAAwjugCXQaHgZqRi9KhYLDXXr92FC2t7O9AlIrvS+q8v282FhXjx7eNE7YSVjSqHYbYRcPZ4cH3D6dZBZhURYZoeMLcWUqbft706ok+D/1XpxskUMEiLpOUJ5z74jPbDvb3r/4U/PRmH3z3BqC6MPWgtUw1rmjwh5xZLCa8qcs4EhWS+0VKmMKsAYGwkZESmbQM8kT3wnnHw9aPcv3YjuDza09dSHg9VABARERGR58q9n1ywe6cvweqEhXpIWyAXWOgPiDao8M0eeW5Y6nbzeAks/z5F8kRERERE/nAv/+vvxGR/j4lnLAEWWOkCAbMKALPPZ5UAFPx/shyIXDaqhbk71XRvZjGnVBWtWxfYaQtLpaLcWuXej04rOCMiO2blR59bubWKt4Wq6pHb7r6R6ppcgqZtsSptJDJtZ+EQjrtv3HPm70GZIBu0BC1BNsM9UYVTTYLhxDiYe/RurfPFv/+R7eng/0vDOPhXb8fSN15lfX/Fcp1pmJAqo7QNjpHMsSiUyJhDuFGm17BsVFfo7vOzxAzH6FU1bW6oeomJZ/KhIUe+9dZO/43lGaEEABERERF57tz98Rd28/RFbHVCLxs9r2maTF3X5BxdZnsJcs5M2najr2flSQ/QIiIiIvLYvfhnb0R9ZIlJHZQUtCXj7uScd/rUnnupMjyBlaBMWqLJeO4CYW0uWF3RtgUbZXx5xOn/10/2bqBLRJ4Zd89dpd86Zb1loR7iJNq24HVFuNPmvGUX/2yHv7NZhn6WbDYLYiczkhmYgRtROd7rkaMwGTUMo88BhvSWW8rVFc7+Tz/d0+Nh/80X4ui332L/G8dYqQt3JytYbaRhj6a0c9eyW2fybRUXYtouIW9rr+DRVZ/JTUNKiRyF7ND0YPDyAQ7/4HXVppSvpPVLEREREXkurf7ovK1+fo2FsZFGmbruM57u8K+sorZud8/8A/Ms411ERERE5HEZvns4Dr95jDJMNN7gVSJyprKEmZZvd1IBynQOYHTtwSqM2hJuqdu5GUYqzlLUnPv5Rzt6viIiM/c/umTXzlxiQE27MmahHpLHE6wEvV6PUqIbw+Z+ZntLgCBjlC4BYEvLAKNgjHNmrWmoqyEL9ZBYHtPeWGb186uc/X//ak8H//f98WvxwnffoH7tEHmhAu8qxVCiC9gTQJAiSIWN6+dbEgGMsPksjAJWmDUDsIDaK0rbVQYa0xALFYffOE46uV8LVPI76QlSRERERJ5bd3582u6cusBSW5EmQUUikbrSeIVphnv3yFyI6SEiIiIi8nik44PY99oR2sWK9bJOjkxK3fNnREx7AstOymQKGShU5tTTlmGWC72S8EmwFDW3zlyiObesN0xEdo2VH5+3cneNuoWqgZqKmBQ8ulaHW0oAzJkvQ2/WbYyYbwUQEVgxBqnPgvepJ1CPjbg35ovffMbNX17a02Ph8AevxeFvvIod3cd6VVhr1nGHYV0RuTDJLaQHw6+zoP72RAuYtQHYqpd6MG0F5O7kaFlt18n7Kg6dfOnx/8VkT1ECgIiIiIg81+797JzdOX2Jar2hT8ICcs6009L/s0XXHAGux2cREREReXxefP91hq+8wCg1WOpKLOecsZRoCxR1ANhRYWDJaad7ZM2sC4I1LTRBv3WW2orl89e4/eOzezrgJSLPpksfnWZ/9PDVhqVqSN9q2vUxyXwaIPzyoWs+WF2IjXwBJ1GHsdAm0v0JvdWW8dW7XPzNKcr51b07Fr6+FPv+t+/G/m+/xup+535eIzzTcyPl3N0bgORVVyVmusO/2+VveBi25XgwGaAAYaVrDUCQSxf8jwgqM8Iy6z5h/8mjvPDP31IVAPlS1U6fgIiIiIjITrv1o89tUpp44e0TpGHFKBVKOMW6km0q/S8iIiIij9vBP30jFo+9wHof1mno9Spy29CMW+q6B5amwZbCg/sC5WkJC8JitgkTHJyKisQgKvKtZW5/en5Hz1FE5MvkU3ft4sLZOPb+G8S40OvXtB7dzvKHbUUHykagf/rn6dfMukS1KpzUGtU44ystty9cYvXne3vX/+Hvvx71ywfwo/toFxNr7Tp1TVfyH7DoWipU7pQChUIY09e6/4Yzd03nLv32QgwFwCCmnySryO0Ed6hSxWjSkHoVS6++yOj741j95cU9fe3l69EWJhERERERYPkn52313BVsdYwHlH7FqIaJB5GMCkht1gO0iIiIiDwyO3kwFk+8yKgPo5hQyOQoRC64GY513YNda/o7rZSCWVAcGgtaB68rqgK+MubWZxfgwkhvlIjsWsu/umh3rlyHSQu50E8VRGyJ/8eWjgBd4tns9TAobhS3rjJKW/D1hsVR4cYHn+354P/iD96IQ++fZPHEi+TayOMRdQR1SuTIjErDJAVedVUlqwa87a5bNii+eW1nu/7T9PC5r83eg9lRPFHwriVDCciFYhAerJcxeaHH0qtH4JVF7VqRB2j9Uh4b1xDzWMxne2UvhD08w/tLkvNERETkEdz4hzM2uXYXlls8G1GMNgq441W1pRKAszV7e7vtr2+dTIuIiIjI8+yVb7xJfWCBsQeWEiklmqbB3Rn2+kQEudv6p2fIp2h2rbdc88iklLDkXZJGGJaNstKwdv0+6x/f1DskIrvejR9+avvTkDTKMC74XOn/6d1m4/PZ+kU2puXrIYXTy05v7NSrQW8l88n/7cc2Ob28d8fAVxbj4L98N468e4LxIqzEhJICd6fCGI9GuDu9QR9LTpMzEd3rtadHiuEEgAeFTCmFylP3bJAzdV0TlTHxzPCF/Rz/9juP628se4gSAOQRzUqQBaZSZI9FGBSL6Q22kL1L//Iu75vZjVhERESejGv/5VNrv7jNcDVYokftNTkKk5Jpk2888VhAKt1HZ2uW9u/zuoiIiIg8n1771/8s+keWmFRBoTuidCV+wWnbtgsgpEJERuX/H9HDmiwz/3zumwdbPwIkc6LNJHMoRiqOT6C9N+ba33ysp3sReWZ88A+/YDFX9LIxoMbCcXewoG0nZAqWup3+rUOunUgGk8JSkzi4ljiyVnHzF2c4+//8+Z4e/xa/83K8+lffpv/mIdYGLSMfk70lT/+vAFWqsTBKW6B0LRLCoPFMa3ljp//sNjSfXLGxy3/uf9PmvtfongE8QbFCAdy754Scu3vaJBqaGgZH93Hkv/mGAkeyhRIA5JFsZoF1H+Vxevjkrph2/4uIiDxpd85eYXLlLosjpxoH1oKnuluc3fbMM18F6WFB/tnrun+LiIiIiL9/KPa/cph77To5BVHNXukeID2mvZctNgIAeo58Mn7f65pSIudMaQsLdZ96AsNSceeLa0/2BEVEHrczd+zip2dZ8D7t2gRyIeeMmVEP+rhD27ZQCsmcZtxCC4f6+4h7Iw6UPh/8+7+z0ac39nQ0qP/dI3H8O28z6hdGvUxTZYoHxWYbYreazzMrc8f217a//rvS+7pngK0/B1vXoOpBzahdY2QtR06+DC/XemKQDUoAkK+tC/o7xZzWjNb1z+lx8NLtFvTpbkGP7tBkT0RE5OmJK+t287MLNNfusDR2+o1hJUhVb8v3FZsebGZuzz7PBtk3k/dSdPd53dNFREREnl/f/pPvc3t9hd7SAuPSwnS34DxX2ajHKjCCh1/PLjBTNg+2fgTIBQInmWHjwlLU3Dl3hfbja3qTROSZs/qLC5bvj1jwHlWkrhWAGW3XYp6UKmpL2ChzwAbsiwGT2yvs8z4//3f/ec+Pey/8N+/G0W+9zWhoRC/h7pjt/F/b2bqeFEAhmJRMb98i9yfrvP+XP9ip05NdSBFbeSTbd7zJo3O6AMEsSOAKEoiIiOyIOHffbn5yHr+7zlL08ElAxMaE62El/+cfrsO2lnebva4HcBEREZHn03v/9i9jJcYwqGgtb5QRfpjtC/3y6B5ItODB5/PZNZ//mCMwc6rsDBtn5cotbv/dp1oJFZFn1pkPPybWxvTC8HCiW+6glMCz0cvOMCd6o8Dvjbh19gqf/Id/3NPjnp1YjHf/x7+IA6+9xGTgrJPBbCP4H7EzN+Xta03z1scjFg/u5/7qfWxQwb4er/3339HTgwBaf5RHYOFUxamzk8JJRf+cHo+utNtmBQCbm/ApA1xERORpmpy+Z3c+v4TfXWfYVqRx4NE982TbPGA6KSub/d1gsxLALBFARERERJ5Pr/zvvxvrvaAZJNraWWsneF1RiGlZ39ioADkf+FcSwKN5WIsueDDBwre9Np8Y4FZR06NuE71JcO2vf6snexF5psWnt+3m2ctUE/C2i/W4VSSrYFKoxsG+6NFfLZz72QeMfnpxT4979bdejG/87/6M+wuFlV6LVUaJliATERvH0/a7ngfCoOr1WF1fwYYVK2XMpIZ0eBH/zhE9PQjVV3+LyMN5dKXqU9DVG5HHoswFETTJExER2XnLn9yw1iJeeP8N9r+4j9WyxiR1r80vJKbZLqGyGfifCYMyTe77XT3eRERERGTvse8eCTu4QOlVjGmYBKRBRVsa3B2iPHRtTc+OT8bDtjCVbZ/PEgQcJ2cYeh9fH3HxozNP5yRFRJ6wtX+6aOMjB6N/ZD9t5YzblqH1WehX5LtrrNy+yf3Ld+DM6p4N/qdXFmPxtRc4+OYJbjDC9vUYNSMGblgYsDOB/+1+V5yoAG0U0rBiLTd4D46+8xpXV8fB6ft79r2Tr6YEAPnajM0kgBJQTFOSx6FYEF9yLee/quQAERGRp2f945t223tx1F6ldyCRHdpuLkixgk8/T6W7R4cB3t2754skZXQPFxEREXme1G+/GIffeIVJHYS1lJ7RlhYLSL2aPJlQYdMO9Vqnf1q6Z/buwbzY1vwLm8vyTQHeGl4Kt89fZ+2Da3qTRGTPuHH6Ikf9NeoX9hMBvQR5fY3b569y/9INuLB3g//1+wfj8Luvw6EBK72A2mjbMb1eRdNMcIz5+/KsFYCZbVQEmH1tJ1hAUxqquia3mQinqpwIo6r6HH3/dW6tnY18ZWXPvofyu6lmu3xtXZC6UKzQpkIkJQA8DpME2bsDNncWflnJMhEREXk61n572a5+fJZB4/Rbp9f6dCroXaB/+n3zSZKwtR2A7uMiIiIizw97ZX+c+Mab1AcXSQs12QtNnlBVjpkxWV+nSulLE0S10vboZhU2Z8f20v+z4P/8utv8M7sXZ5GayY373P/H03qaF5E9JZ+6a/cv36RabTmcFqnXMpc//YL7Pz5nezn4X33zUBz97jvYS0usDyD3giZaoNC2Lamuu0qOdAH/+eD/Tpi/L83aRwOklGjblqqqaPOEcdtAbUxoGbywn8GxQztyvrI7qAKAfG0FyFUwppD21bz87bdoT46CJjBLYAZWpmWzyvThuotqx/SjTacyHt0D9/P2MZtjFngJWsuUYUW1f8jYWoqljWvtZpr0iYiI7AKjj6/buWjjte+/R7VQk9uGqIySjFIKkRLRths54k5XKUmBfxEREZHnz8E3Xyb217QpaNox4eDm5NKVjaqrCnLZSCCdTxwtQNh0/Sy0KvRoZnvgHryOAVhymtziljAzHIcSRJupA3rrcO6jc0/zhEVEnprVX162yeKBSOuZ85+foTl1a8+uYPixYex77SiDV18g769YrxqKRVeVJ8C8+6u3pQV/sILjfDuAp5EMMKsUXQLctlao6RIBAtyJ3FLXFV6gLQ1WQUtw4OTLrK+Po3x8fc++p/LllAAgj2QcmbpybLGm7tX0D+4j4RtZUWVjhJw9YM8SAKYZU8FzXQc32tJNLEp3U2mqIA+c0kuEBaV0iRPdpG8zePAcXzIREZEd13xy277g43jzT75DUzmWnHHJBE5DofLNbOzuWQe8KAlARERE5Hni33gx9h17gXaYKDbZ+hqxUXP+geCCza0DTb+mx8hH97Dd/zOT3FKiy8DopR4xbqA4i/1F7P6YCx98Bufu6G0QkT3r0t9/bOmlxcjX9u6u//rtw3Hk7RMMXz7IuA/LZQ2mLXhSFDCImMavtu243wnzlaFhPglg83t+172tTYX60JCX3n+Nq+ujiHP39+x7Kw+nBAD52mLaAiW70XpQ3PDasRJAkAki8nSAKtOfmdW17z6W53jI8YC6V1PjpOgG8NaDnIImMm1pqc2/NFjgqBSciIjITmk+uWvn/JN49fvvgwe5clpaqBNtWzYmYbD50crmgq6IiIiI7GFvLMWr33qTydDIqZC7kD7YNJi/sVBvW54bH1gDmu78QxtBHruwafl/AyeRKqe0AbkQk4LjeCncOHeV9d9efo5XMEXkebGXg/+8ezBe/Nab9F8+yP28TtOM6fcqaHNXqdm7e0KZ3pBnVXl240bM2bNCV3FybqPt7JPp58UKa9aydHiRfcdf4P65+0/9XGVnKQFAHom7kylME6RwczYr1xcsORAU27rzf5a7vAvHz6cmgNJOaMxIxcm0ZKCYU6xMc8/mvn/v3n5FRESeSeOPrtuluoqXvv0WbQnqYZ9RM8HMCS9QtmZi+7SqjysJQERERGRPe+M772H7h2RvWMsj6uQbQX6Y29H/FQtjuzHw8KwK40uvd0RQWQXR4C0M6yELUTG+u8zqlZtP9TxFROTx2veDV+PwW6/QLtbcbVaICryXyDlT0ZX+j/DuFrGLYjD2e7aTnH9WmFWj3Nh80oPl9TFH33qF5es3Ij5f2UV/Q3nSlAAgX5tFVyBlNhAFQZjhdOX/MaOUDJSN5+volsGZPXF3Wc7P5xJ4AazqbizFgmB6HT3ADA+DEg8d6P0h/z0RERF5+tZ/fdmumcWxb77JOBUaC6KaTh6tbCnR9jxXPhIRERF5Xuz7i5PhBxdYY0TURirdw2AB7e7YBWK+fHLMSjIYpWnxcFJrDC3R3lnl2ufn4cyy3jQRkWfUG//jD6IdJGKQsMqoMCa5oZhR1RW5bYHuvpAwLG+2Yt4t7Zi3VCKIbZGhjTtUF2Pbuu4UYIXeILG+1vL+n3+fj1d/FFxpdV97TiiOKI8mojsAMyMM2ii0eULbNhvfttEHFzBiY9B6XoP/M23J5Ci0ZGJ6/SKm/QBK2dLDZfuNZr5EnIiIiOyc9V9dshufncOXx+xPQ1LxjTnYlh6uBlm7/0VERET2LP/20Tj61gmWY0zuGaNmTF1/vf1XOx1w2Cu6TUvTSpwPed0CKnMiQ68khlSU5THXzn7B5DfXFSQREXkWHa/jrf/jX0beXxP7KpoqWB0vM2nH1HVNqitGkzHZZ4H+bqNr2sWl/7d76CaTsLnNuBA5M27HNFUwHiRO/OC7T/EMZaepAoA8kmhLF7D2Cks+TQLoAvyVG6XNG1lIPj9obvQheeqnvGvMqiYYYG6YQcLxaRKAYXh013I3lZ4RERGRBy3/7IJ5VcUihXpfD2BuIgmZrdnjz8JkUkRERET+AMeH8fb3v8k9RlT7B6xN1qndiGYC5t0K0Lb1ndkzYdnY4lEe+ro8Ipte17mWXPOiFGpLpDBSDtZu32P92p2nfpoiIvLoht97JY6/8wbsr1nPKzRNxqtEf2lIG4XxZAzu9IcDctNS6OIyM7P2jQAWO7uFYzOc3924HhZP69aanNlZh9nGpt0KwyqnaTOrNAxfWII/OhJ8cEMRp+eAEgDkkaSUiAgK1n0shVIKTmDhmHd7/i18bpIzrbUVvlE14HlkdDeWiNi4dkF3PXxuaIdu+M6zn9PkT0REZFe696OzNmmbePkbJze+1jqEffVC7pZSpCIiIiLyzHnrT7/HGg02rFgdj0h1VxWqbVvcVMNxp3TP2F1gZPbMXbY8eHctOPupIo0KeXnM/St34PLoOV2xFBF5Rh3rx4k/eo/h0QOsRktYED0jVTWlFNbGIzCjrmsKMJlMSNP789Yw+/TzZ2CNZmtsbTMJALq/S2kz7o5XFePcEsl444+/zdnRr4PPbus+t8cpAUAeSY7ZKNiVqzfAp0H/rrcZXT97uqr2ndknebMG13PMprcVm34WzGeAd1nJZe5rFlu/LiIiIrvH+s8u2s2qF4fePI4t1LRlQphRoqWXErb5QESZPgPM95aDzXv+9n1gu6X/nIiIiIhsdfiv3ov1fsaGPSYxAgoRidaNkhIW9hXPcFrheXKcntW07YTsBZIRJWNRSKkGDKdmbWXEIRa4fPos499eVlBEROQZYm/vjyNvv0r18j6W65aJteDebYTPXYnl5F04NHIXyErmm3GY6caNrSH0XeCB3bO/X6Ugm2a8mRlVVbE2GuG9PpMU4IXX/uQbfDH6RXBhXfe7PUzpp/Lkhe2uQXOX2z5oly95TddURERkd1r+0Rm7+vFZFnLFkvWJccOwP8ADcs4PBPhnZokA21sF+Je8LiIiIiI778D3XonFo/thsUexQrhhZhtVMs308LaTPGC0tk6/P8TdaZsxvV5F3e/RNA3RFsq44cXFg9y5eJ3xby7pDRMReVa81I/F7x+PY99+k/5L+xnVhbFnikNL/tL1ky9rzbgXYi4ba05zf3lLCXeYRMMoxjBI7Dt5fIfOUJ4WJQCIiIiIiDxmk19csusfn6VeaThQLdCuNxR3rK7IG0/gW2echc3JptMtVqbSHQ/vCisiIiIiO+r1pTjy9uv4vgElBW2e4AGVd7sKPXftH1XBaecUK/QGNZPJiApjkPqM1sasrY0wr+jXAxa8z/jGfe6eOr/TpysiIn+AI998g8PvnGBw9BBtP9GWTAqoMLwtz/3918xpS8GSEwZ1GCkH4fDiqy+x8L1jz/kV2tvUAkBERERE5AlY/tEZcyJefPcEYzMaD6x2IheKBf4HTrMsuhZAIiIiIrI7vPT+G0wWExPPTKIlIkgY7okUQZTAstbWd1qxQsktbgk3w72i3+vhOOt3VjmU+5z99UdweVVP2yIiz4KXh3H8O28zPH6YUSos5xEtLWaB41gYlVcEz/dGCjMjl0JxsFLoWUVKQds2VP2aF958BSsRq7+5qvvfHqQEABERERGRJ+Tej85aVXu88M5r3G7HjEsmVRB0LZI8usmoBVtqcxXAbPNL1rWo2/hcRERERHbW/j99LYbHD3Pfx0QF7o5HQAkiur67KRyKYR7PdQBip+WcqZJ3LRmSU/d65CZDExzp7+fqrz+H0/cV/BAReQa88Cevx8KxF0kvLLBWtUysBSAlxzFyzhQCd6eEFlCAjdZEZgX3xIQJZkZ9YMjSKy+y+purO32K8gSoBYCIiIiIyBN06/OLLF+8zj56DKmx2PoI7rOjbAb3wyAb5G7NeOP7kuauIiIiIjvu0HdOxItvvMKqT8gDZ+KZSBAOOTI5ZyK64ENyLb/utKpOAFhy2hKMRmMswzAqbKVl+ecXFfwXEXkGvPr/Z+9Pnyy5zjxB73eO+72xZCKxEzsBkASX6qququmu7ulWqUcyyTRqmbWNacz0QfoHZZKZPrXJRqMZm5kudfXCYhermsUiCRAEse9bbhFxr/s5+nBvLJlIcAUQuTyPmZtn3BtIeF7PSH/Pe97znv/6j/ulbz2ZxROXcjDOOcp6s7KiborvWvq23f2Q9T2eP2lJ5t6SWjOUmrSeeZ4z9yllSOax53CYUx/YzyN//q17/NO6O4lAAQDgy/TWUXn7736R+d2r2VuXDO14yn+j9G17/9xYADDV06OXZGibQwAPAHB+dr5+qd//9cdS79/LYdZpQ8vU56wzZd5uA3BWKeaWz93c0ntPH2qmNqf0MQ9ffDjD1Tkv/j//rRsEcAd47r/9p70+9UCOHljkrcOPMi1a6piU3tPnOW2asp7nzCXptaRnk0u5l829pJQhQ0/GOmQqLUdlTh9L5trShp6yN+aBZx/P3h8+rgjgLiN/CAAAX7Y3D8pbP30l+eQwe1PN2JK67QRwPCC9VRHAvD2OW8Zq/w8AcL72n3go9f6dXGtHWe4uMq0OUzcLzNOTlLGkDEN675lbNnvvSsGen9IzlJree9bznN3d/dy32M9Hr7+TN3788/O+OgB+jQvffrg/+6/+UV/t1Vxfzrmco+zev5ejrDL1KS1zaq0Zl4sMw5BpmjLPPeMwfKYD472mlLIpgJtbxlpTak0rSaslq3mdljnzUHI4Jl974evJk/fJOt1F7u2//QAA8BWZXv6wvPa3P0v/5CCLg5bdukxvJS01rW6SxEOpNxQBtLIpBGj19NcAAJyP+//ps/3Rb389672aVVsl05RlapatJG1OsonZWmvppSRDPdnOifMzz9vJoTok657Fqqd/fJTp7993dwBuY4/982/3R/7gudRHLmTeKzmcj1JrNpPavSe1pNeSqSbr3tLSMwxDxlJTpvbr/wd3u1pOOhP1uW3yS0PPOlPqMGQ9z+lDz7QomS+Mue+Zr53zBfNFUgAAAABfkf7Ly+XtH7+c/WlIv75OnUt6alp6emrm1XrT6r9vjtq3SeRsCwGkKAEAzsXOHzzSLzz5cK4veo7KlLLc7Km76GW7/fB2VV2pJ0WbvbTtr01CnJteUkrJUGra0ZzFKrny9of56MVXz/vKAPg8z1zsD/z5C33v2YfTHtzL0dgy15ZhKBlK0tqU0np6aZ/Jk5Se1H783L23n79nu0jevKCkl5ZxMWQ9r7Lq62R/J49844ns/vHTugDcJRQAAADAV2h+8ZPy6g9+kv15kUUfM5RxM/mfnmG5k9KToSXjcSFA2wzUpro57u3hKwDA+Xjw2ceyfPT+XB+mrI4z6tOcodUMrW7bDJ+mWnVuup2UDKnZ74vk04N8+vJbyRvX3CGA29H3Hu6XvvdMLn33qawe2su1nZ6DvkqSjCkprW9yJcOQnGnxX7JdSLH92gKKU8dbS27yTZtP6OznU8ch6z6lXVrmkReeSp6/pAjgLqAAAAAAvmLTS5+Ul//Tj3OhLbJcJ8NcMpYxyWmyuPSktpxsCXDcBUAyGQDgq/Xgnz/flw9fzKrOWZdtu9zW0lrbrjTcTDzUbbr8bLxWpNDPVU1SWsmwrtlZ1Vx+/f3ML34koga4DV36J1/vz/7xC7nw9UfyaT3Kp9PVHPTVZrK69tS5J+s5aWVTAJBN8d3xM/jm7RTZOM4lHX9OtdfUXtOTzL1nGEpWfZ1PV9ezeHAvX/vmk+d9yXwBFAAAAMA56D/9sPz8P/xt9g7n7E0ly9Ss1+vMNZm3UXpNUnvP0LvAHQDgHHztn32zP/z8E5l2Sg76KnXYvF5KSR3GpNQkJbWXlDPVmr0kpbfo33S+Sq8ZMybXp1x984Nce+3d874kAG72xLI//L96vj/ywtPpl5Y5GqesypSWOcNQMo41pWyer6XUpJfM65bSS2orGdr2Gby12YKnKQLI5nM4NtzwWZXUOqSX5HC9ymIxpI4lB1nnwhMP55H/zQtKGO9w8ogAAHBefvZR+cX3f5QLU80wlYzDMnOpmcumRVuy2QJg2HYDKN0qMgCAr8wz+/1rzz+VaafksEzppaXWmta2kwqLIXNvSepJnFZzOuV/46YAnIfak91eM316Pe+/9Fry7uqenw4CuJ3s/eHj/Yk//U7u/+ZTOdrruTxdzdTWWS5qxnHMUGp675nnOa0kizpkUWrK3DO0mprTFe69nD6D5U9u7CJ53LGo9tMc02paZ7m7k7btarRYDFlNR1nttDz4/JO58Cdfv8c/wTubGBQAAM7TS5fLi//xb1MO12mtZS7JaqhpdTNwrf24CGDTCQAAgK/GH/zTf5yrfZ1rWWWqPbUkZe4pPem9ZN2TVSlpJ1P9NaXXky2cSu8pOjmdq9KT6foqn7z1fvLGdZP/ALeRh//X3+sP/cFzaY9ezCeLKdeHOXVnzO5Y0w4PM7aeMs+byf/W0ntJUjO0mmUfU3pNes1caqZSM22aA8if5HjyvyfZxC3DmY9jaJstADYFjSV1WXM0HaWs1xlrybWscjDMee7b3zq36+f3J/4EAIDz9uKn5fXv/ygX1mOW85ihbQaxvSSt3DpPebbC/djNq8x+1XsAAHy+5/+bP+vzfsl6d0gfa+o4pG8n8xfDmFJK5nlOGepJzFXv7bmGr9BpwcWt2jvXJEOvWbSa5VzTPrqWw795x+Q/wG3kvn/2jb739IPpD+xmvVcz79b0oWS1Xqe3lr3FMrX1zb71tWYYhpNnb+89tW4yHK0kfXs+67grD6daufFz2tvZzeHhYWrddFvovWdYjGk1OSjrXB1X+fa/+ueimzuUv/8AAHA7+Pm18tq/+c+5+GnL/fNOhnlILcvMJTnoU8rOIlM2467jif/5TBHA6UqzfKYFXm56DwCAz/fwf/Xt3h7Yyaf1KKvlnLn3TWumXjL3ntZaSm8ZSlJ6S9vuNdzK5vXjuKuXkl5K2q/9P/Kr1PTtsfnq7Cs9Nakl63mdnpadxSLz4ZS9vsjuvMj8/kHe/O//zuQ/wO3i+d1+37/8Tr/0R0/l8GJyUA7TMidtSlrLYjsRnXn79NwmNTaP4ZY29ExDy7rO2/3tW0pahjPP3xbP381nUVK2n9/ZHNFcNzHLvJ6yrEP69rOehmTV5wxlSKsl1y/2XLk056n/3fcUAdyB5P8AAOB28ctPy5v/+eWMnx5lrw2Zj1Ypw5i6WORovUr5nG4At2K/OwCA397iGw/05YMXclSm5MIih21K8uviqnbDJMPx97bknp58+EKcfPC3/iSPo+OdnZ2UUrJarbK/3M98sM7iMHnrRz//Si4TgF9v/IMH+1P/6Hu59Oyj+bQdZD1O6dviudpO/01Pr9vtdW7MgbSbj7J5NhznP84+qz1/b/xMbv7sftX3JJtii4O+Sr84ZLqwSP3WRRmmO4wCAAAAuI1MP3m3fPDS6xmurHNx3EubNwOwoQwnldvHA7ThzCC35bQjQHLc+vR0IDdvOwYYBAMAfI6n9vqD33o6u5cupPeeOT0t8y3bzHP+Sjax7rye0ltLm3t62+wPvT/s5INX3kpeveLuAdwGHvhHz/Rn//i7WTxwMYfzOjs7i9S2yVss2uao2wfuVDeH/MX5KT1Z9JJxHFPv388j33o2eXyhCOAOMp73BQAAADe6+tevlzqW/uC3ns7exZqDuaQnGYYxbbsKbbhp2LXtSpu2ff14S4BaTt/rRWcAAIDP89B3ns3u4w9kWial1ByuDjKOYyJ2Oj+9ZHMDjjcA2HRbKElqT1JahnHMZpfolmUdUg/mfPrOJ/n0379s8h/gnNVvPtQffPLhXHzsweTiMgf9KEfzKjttJ0Pf/Ft+nKPYnEt6TVrpGZr8xXmpSYZxzGq1St0bs/fEQ3nwytfz8Tsvn/el8RtSAAAAALehy99/rWQ99we/80yGizu5Oq+ScdjsLZuW0k4Hyr0kbdvbq9VtR4Dt5P8xK9cAAD7f7j98ol94/MGsliVzn5KxJHNSa0mfzT6cp3amBfSmy9XpmtCeZO496T3LusjuNGT+9CDv/w8/Fv0CnLMLf/RMf+CbT2T54F6ut1WmHCaLkmUWm7b/vZ/Zo/64Yfl2Wx1JjHPX+5z1PGcYa4blMntPPpQr//BKn/7ze27OHcAWAAAAcJu6/MM3y8c/fzPjtXUu7VzIvN4kO4/HwaWftj49HSqfbgVwq71oVc8DANzk+Qf6A994MtNOzbrOmcaeqcwZFkOmaRI/3WbqdsVosu10NZQcHa2zn2WG6y0f/vyNc70+AJJHv/VMf+bb38j4wH6ujy2r3ZK+KJn7nMxThm0So5eeuda0ctxwp5q4vA20JPM8Z1wOmfqUo6yzvjDmgeefSJ69IDK6A/g5AgCA29jlH7xePnn5nYxHJXVOkpr0XxHGl74ZQN9UBFB6Um2gBwBwg+GJi/3h7z2T+sjFTLWl1qSXlpaW3uW3bwfHW1kdK+kp2cS8vSTDYpGdcZFcW+fTX76Tg79/18pEgHO22FmmLZJpaJvCutoy9zml9owpmY5WmWuyHpKpbo5WtkVerWZsVQHeOZtrTxmGjHXTDWBe1AwP3Zf7X3gmeXLX3bnNKbKxvo0AAEBSSURBVAAAAIDb3Cfff6Vcfv293F93s5jrdsVT3Uzwl8/vjHc2WVoj+AcAuNnD33o6+088lGtZZ64tKS3zPKXWmvX6KDs7i/O+xHva58a5OY2DV9ePsptFLr/1fj78978w+Q9wG3jrxy+XmpI2TWltU2CXJGk9pZQMw5BWTif/5zP/eg9nOr1wfkqtmduUUkqm3lIWNasy58FnHkt59L7zvjx+DTlAAAC4A3zw45ezeuOT7K9KlnNNyZC51KxKyWqo6dv+eUMrGXoR6AMA/Brlew/23ecfyVGZU4ek15LWWpZ1SOaW5WKR9Xp93pdJalpLFsOQcRyzanNWtWcYF0nruZBlFtfnfPrqu+d9oQCc8dO/+Mss1i17rWY51wy9ZsiQ1WqVMg4nbf/PLmwo2Uz+l5jAPHe9p7WWKT1lSNq8Tq3Jtb7KM3/wreTJhTKN25ifHwAAuBO8fa28+Z9fzPT+lexNQ8ZWM889dbFMqyWHR+tNFX1PxrZp96/lPwDA53hipz/9p9/N9bFtVv7n+NhundQ35/Krtl7iS1d6UkrZrD6cpszznLIck1IzTVN25kWGK+u8+5NX016+bPU/wO3krXV57a9/nN1V2XQzXM/pc8vu/sUczVNaSVopJ89cbj+llBvOKS299kyL5Pn/8k+SxxUB3K5EsAAAcKd460p57yevZHrv0+xMJWMfMrWk1SF1Mab3ktKTcU4WbdM2b9gOxeZyY0s9AIB72XN//l/kaH/IvFvTSktNS01S+iZ42hQB1Aifbg81JWmblYi9brbCqvOQvT7m8M2Pc/TDt90qgNvRT6+U1bufZH8aMmbMej1lrsl6akkvGVo2x5m2/3Nt6aXFmobzNZxEQT29b2KlUsqma9KQLB+6Lw++8My5XiOfTwEAAADcQaZXPi1v/fSVrD+8mvvqTurU01ctw7DZn/Z4wFz7ZhB9rJfTAwDgXva1//0f9PbgblaLnqO2StK2q/1PF7Edx0xV8HT+Wk8pPcMwpPeeeZ4zZsyyDWkfH+bTV7T+B7idvfH/+VGpV45SDufct39fDq4fZu/ChdRsJimP8xclOZn4N/l/e+plc3fa0HNlPsjD33w6eX5fF4DbkAIAAAC4w7RfXCnv/ezV5OODPNDG7MxJWc8p/XTvvJ7NWRs9AIBTw3MX+4VnHs3VMmWaVlnWzQR/Lz11u/qwl5KWklj/f5toKb2nDDW11gxzz14bM14+yoe/eDPTG1r/A9zuXvz+3+a+spN2fZ2dcSfr1ZzSa2qrGfrxE/d02r9LZpy7lk2CaehJek8rp0dqTxuTa1nl2//iz877UrkFBQAAAHAHWr/4YXnz736e9uG1PFh2s9vGZE56auaazPW0GCA53sNWQQAAcG974Z//aa5mnaP5KLs7i/R5PgmQNnsRn04/nG4MwHkaSknvPes2pSbZaWPqtaNcefO9XP+bN03+A9wJXjso7/z8jVwoOylT0lvbbrdz/A0tvWQzuVy6ErzbzHDmjvSSTKVlnSltWXI09jzz3/4T2abbjAgWAADuUKuffVDe/cnLWb//SXZbydhL5pqsas16SKa6SWDXbPbTGwzHAIB72JP/53/cr14cM489u4sxbbVObX07+dBOiig3anqq7ZNuCz29JnPblGYsW3L97Q/y6S/eOufrAuC3cfnN93PlnQ+zTM3usJPkeOFCy1ySedvNkNtLO9kWadP+f66b+1UWY9ZtzvV5nVxYZPzHT7t9txEFAAAAcAc7+tlH5b2fvZp++TC7WWRomxC/3ZSwPrvyXyIbALjXfO3Pv9lz/04+na+llZbS55RyGhS1UjdbKJ1dc1jsQPxF+rxEdC+3jk+P94Zezy1DHTP0kuU8ZHmUXHvn4+SdtagW4E7y+rXy7s9+mdWH11JXc4ZWN0V4/fQJIV9xeyqlpJZy0rGhpWc9Tym1ZlgOaWPNY994OuW7DysCuE0oAAAAgDvc4U8/LK/99c8yXpmy18bs9jG1bwZoqWWzR1taai0nCdbPS7T2UrfHje8fJ2ABAO40iz95pM+P7yWLnkVJhm1lZO89qUNaynbifxP89O1qxE0vJUUAv69tL4VNq+fcGFP2kqSWTG3O3NtJUUbp2++vNcOwSFu37E6L7ByUvPLDn+bgxx+ZIgK4E/3iarnyy3dycVpkXCfLskgtJaX3DMOQaZpT6nDLhEX9NQdfrLN5od57eitJakqvqccxUy1Ztzmpyd6F3TzxradTvnVJEcBtwM8EAADcBfovPi2v/Ke/y+L6lHowZ2/YyfpoSu89pdaUUrKap/Tyq8dhv+ZtAIA7yu7zl/qD33gyw4P7abWllJ66jY3m3jcJbVPJX5lbxZrTNGUYhozjmFJKhm0RwNxbpnVLn3oW85iLWeaDV97M/JNP3DGAO9jh+5fz8S/fzn4fszpYZVEWqWXM4eFhdnd3k2yeAdyeas9Jv6SWOWVI1pmyKnN2HryYC08+fK7Xx4YCAAAAuFu8fLm88u/+OvdnmX71MPvjMsMwpLXt/my1pvTNiqokN6zCOh0YtJQzA+3jim/r3wCAO9F9zz6W/QcvblcVbosjS9ms/s9mhfnn2cRNX9WV3r1ayqbLwudM2w+lZkxNmVv6etp2ZqjpQ80wDNkrY3bnkvGo5fK/e9XkP8Cd7s3r5cOXX8/q42t5aP9SLl+7mrkm+/v7WR+tMq1XWYzDZ/6z9msOviT9uEvS6dGz7TiZnjaUtKFn1dYZ9ha5/4lHMvzx10RQ50wBAAAA3E1+eVhe/B/+f3louJDdeUg/mlPrkHluJwnuklsns88ODiS7AYA73c6fPtF3Hn8wh23Kap7Stu3mW2tpbTNV8KsKAPhi3NBCeHs++6mPddh8vZ6TuaWmpJTNpELvPfWoZXeV/OTffv8rvnIAvjRvHJQr73yU1eWDXNi9kPSa9dGUmpJa60mhHufv8yKlk25KSfpQsy4916dV6v5Onvzm11Oee8BNPEciXAAAuNu81cpP/se/zO5hyX7bSTtoWe7sZWotJZt2bcd7sebk2Pi8IoCziVsAgNvd8O0H+qPf/npy/26mmpShpo7Dyer/45WC8zyfrPS/+eD3dxxDHneUulnpSVrLPM+ptWaxWKTWelqk0XsulmV+/p/+PnntSDQKcBf59PuvlHblKHXq6euWxWKR1lqWy+Wma0/JLY9f58Yuh3xZ5t5TxyFzeub0tGXJQVnnsEzZeei+PPTs4+d9ifc0PwMAAHA3eu2o/PyvfpT9ueZC3Uk/mlN6veVoud30kk4AAMAd7cnd/tQfvZDhgd0c9FWy2AQ7vfeTyf9aN+3l53k+32u9Bxy3Zr65CODsBE3vPRlqaq2Zpiml9+yOy+xnmStvfZj58uFXft0AfPle/9d/VYbrc+5b7GZ1/SDL5fKkSw+3h8+7G/M8n3TsWbc5GWqyHDPVloO+yqWnHsqlf/ysrNI5UQAAAAB3qf6zT8qL//Fvs3vUszPV1PXxAKCc7MXaymb9fyv9huOzRQBq6AGAO8Pes49lfPhirvajHLVVWpL1PGdq7TNJ7F+1BYDo54tRe03tt/okN52oai2Z0zKlZ9XnzHPPsiyyXJccfXglb//8teSd61b/A9yl3vjhT1M/OcilnYuZVqtM05TFYvG53//bdgTgd1VPj1s8xxfDkD5vyvzKkEx92rScXJQc9lXa3piHvv617HznIUUA50AMCwAAd7OfflJe/7ufZ39dstfGjPNmCHCy2iplM2jOzZsBfLYTgG4AAMDtbvHdh/qjLzydK/Nh5tqy2FmmbSOcYRiyWCw2e9bO82Z/+V9RAMDvr/SkZHMkN3YBOI4vey1pvafVpNUhY6lZtiHt4+t572e/THvlU9M7AHezFz8uh29/mMVBy04WWdQx69UkB3GObsgTlU3nyLPFFqUni2FMmzb3aaxD+jxnmqYMw7AtAlhn+cilPP7tZ5MnRnfzKybCBQCAu9zqb94pb/3oF7m4rllmTDuasxh3Ms9zWnrG5U5SS+b+2cZup/Xex81bAQBuTzvffbQ//2d/mNWiZ1U3bWmzbSNchrppUTtPN0z89/75+WjRzxdjaDcWkvZtx6lj89RShiF9GJNS0udkZx5y5bX3kx9/YvIf4B7w7r95qVx5473s90XmwynL5TKllE2h2NyS1jMMg8K989RPOwHUJPN6yrKOGXqSac5Yh4ypmaf15tuHOZfXVzI8eikXX3jm/K77HuUnBQAA7gGrdz7NJ794J/3KOvfv3pejKwfZW+5lKGMOrl1LS0kZh5Mq71sluw0eAIDb1fLx3f7g84/l6jCnL0ta5pOV/pyfms3q/7qd779Vq+bWWsZhmWma0qae/WE3l9/+IKt3PvkKrxSA8/b+i69m/fG13Le8kL5uqa1nKCXjdtJ/Xk9p28K+X1XAxxfvuAvAWccLRoZ+etQcP/N7WknmZcmVssqDzz+ZvT972k37CsnhAQDAPWB+91p579+/VOaPrmX9yfU8sHMxWbWUKVkMy5TeP9vPLV3rfwDgjvDAt57KhScfyXqcMmWzwr/PLfX3SH/aW/iLUY+3Aeg3dwLYnIdhSG8tizZkMZXMl4/y7s9ezfq1az59gHvJ64fl8mvvJ9emDFNJ7XWz8r/UjLVuJv1b33QGKKePCM/rL0lpmyOnRyvtpBPA8XP9846Wlrkk0zJp+2Me+9YzybP7MkxfEQUAAABwD3n7Ry+mXFtlOJxTrk/ZH3azyJh53TKUszu0njKQBgBuZw/92bN9/5lHczisM4/Jel6d7EW7GIbfqZhR/PPFu1UiupeSIUPKque+spML85CPX30n+cUVdwDgHnT5B6+X9156LTvTkEUv6dOczC01m04ApZTUW+Qt+GKdnci/2XEngONYqfbPHklSa83c50y15cp8kHl3yNP/4FtfzR8ABQAAAHBPeWdV3vjXf1P6J4e50MaUo3WW2VRun7bQ24zi2k1V9PbABQBuNxe++0i//4Unc3Sh5KAdpael903UUmtNv7lf7W/A5P8Xq93cZCrHW05t3piPVrk07mRxbUr74GoOvv+qOwBwDzv45ftZXFlnJ4sMpaa1lt57akpK75nn+VcW9x2vV+f3U9JvOI47LZxsB9Bremra9uuzR7KJw0opmddTxsUiB1lnfOhivvbnL+gC8BVQAAAAAPegl//1X5X9vkgOpgyt5r7di1kfTbeu7o7BMwBwG3pstz/w/BNZ7w+5XtbJomaa54zjmHmeMw7LrNfr3/m311L499eymew/DjFrTvtNtbp5f1mXWaxLhqtHef3//UOfOMC97t3D8t5Lb6YdrjIOQ8Y6pLS+2VKmJ6Vvtvrhy3Xziv6zermxE0C/6de9JNM0JUkWw5CeOXV3zNGi59IzX8veHz+uCOBL5icEAADuUT/6f/ybct+4k/ngKPPRKrvD4jPf0870fJMABwBuJ/vffDL9kftyvU9JLenbLPWwnRRoLckwppfN1ze3sq25MTlqwv/Lcbwa8Ozn38tpgemYkmFq+cUP//7crhGA28vVH79Z3n7trRweHmaoNX3ePDWGYUgpHtZftprtc/sWy0F62SwTOS4COF400k9+XdJSNl0me8+yjplW6/SxZl4OuTxMeeSbTydP7ioC+BIpAAAAgHvY3//P/y6765K9LFKmZGib7QBuzn6f/fIzifHym/QHMK4DAL449//Js/2Jbz+b+cIibegZFkOm9Xoz+T+3LIYhh+tVxsVnCxx/U5+39+29puazxRK/jbMTBb0k6SWllww9WbSanSm58ub7aa9eM6MDwImjv3it9A8OUqaa1pI5ZfMs6T2ttRue0Z7XX5J++vQ//ozPfta32jby+LVhsUztJfN6nd3lTq4dXM26zml7Q3LfTh74xhNf7rXf4xQAAADAvezNdfnlD36S6+9/kr1hJ+k9pfeMtaa0ZEjJYrHINK1TStkO5Go2Sdx20+R/v7HfW+oNg0UAgC/C4lsP9Ye++USOxjmrfpQ+bPYDXpZFxrkmbTMxsFiUzG2dpP1G2xyVfuvjXrZZAdgztE2MWHPaGer0qCddFj773mYP4JaeqbWTFYKl1+xMYy4cjenvX8tb/9NPTf4D8Bn91SspB8m4czFTSlatJ+OQ1ueknO5Pn2yL1fqmZO3sc4nf3uZ5XdLLZjX/8az+jTFSS+mfXRByHDvN83zye6znKcNikSlzpmHKajHnkeefyP3/9Jl7PNL68vgJAACAe1x7+ZPy3k9eyfqDy9mdh+zUZab1ZhBX65Br165luVik91snzzf6Te8ZagAAX4LHl/3B5x7PajeZyrxdXX5zHJIkfZur/lXxC7+J2vNrOz7dvArzhgKK3pLeUoa63RO4ZexDduch5doqv/hPWv8DcGuf/Pit8v5Lb6ZdPcqQIXW7zc84jjlbyldv6DpYPfu/ADcXSv72buoftA0MekmmoeVw6Hn0uaeSJ5fu1pdAVg4AAEhevFw++LtX0j8+SD+cN4Pqccj11VHGYZHlcpl5mrarwNqZpO5mNdjJkq3St1X3N/72vSSxrgsA+D09+PxTeeCJR9KXJVNb37BKv5VkrpujiTu+MJv9fevJXr+n7X5rSt9MstR+41YB2/WXqenp05xSSuo4pJdkkZr9MqZcX+eDV99O3l25WwB8rsO/+mVZfnQ9e21In5N53TLP88n7x/mHlqSVltrbZ3ISfPXqmfig9GRoN96raShZL2qe/qNvn+dl3rUUAAAAAEmS6aVPyls/+nn6laPs1mXWR1NKGbJ3YT/XLl/N7mKZ5HQQcavBxGYwdzrSLtuvmgIAAOD39OA//UZ/5OtP5WjsaWP9zCT/cczRymk0YgXg7+e4Zf/x5P9Zx5/tcTL/87ZOKD0ZS808z5mnnmVZpBy1XHnrw1z+wesiRAB+rY9++lp2Ducsp5qdYcyQmtrrtuX/aYFa91S5LdVsUkJnY4RpaDmsU/YefTAP/JfPidi+YAoAAACAUz+/Ut794Yspn66ynMbsLndy9erVLJfLz26Se2YP2KT92tawAAC/s+f2+qUnH0m/b5nrfcq69NRhSCnlpMbweJK6ZbPfrMn/L0bLZv/f452Wj5P3xyv7yvY7bnb8fYs6JEmmaUrtyTiVXHv7w3z0yltf9R8FgDvUwSsflzd+8JPcPw0pq56SIaVvnklnC/+SbHIT8hO3ndqToSdle7daLcn+Ti6Xde5/7vHU7z4scvsCKQAAAABu9Itr5Y2//Jtc6ovkYMqF5X56K2nHg7Ttt51tAXt8PlkZVrTcAwC+II8t+qPfeT7t4iLXsk7fqZnKnFZubAF8K5Kfv7/fZkXlzbFhkvTe0+eeZV3mvsV+yvV1Pnrj3fQ3rlmnCcBv7OinH5ZPXn4rlxb76auWpKaXGx9SpX8Re9fzZbihc1DpmcucVZnS9oasdmqe/PZz53l5dx0xMAAA8FmvHZZX/qf/kPvbMv3qKrX11DKevH28/n+uLXNtOe0HsGHFHQDwRXnke8/lvqceTVvWHPV1+pj00jL3dpLdPG4re7zXLF+kenKUfqbl8naFZT/Tdvl4y4CzWwfMSUrr2S/LlGvrfPL6u5l//KHJfwB+a+//5c9LvXyUnbJIbTUtdTv/r/PP7eq4IOPmYsLee+bWMo892R2SS8s89V//obv4BREPAwAAt/bGuvz8f/mrXFjV3LfYT1utk348hCibpG7qDfvt3mDbcq8kNuIDAH4nl/7oqf7wc0/mejlKX5T00rJeH2UYhvTeMo6nBYol25b0PanbpX9WAH5xPm9i5TixPx9P+J85t5Qs6pAxY8ajnstvvp+r/+6XAkMAfmcv/tu/yt60KUo7LVI79dt0ruHLdTZPdPzr44LNsQ7pmdOTXFlfT91fZO9rD+TCP3pCEcAXQAEAAADw+X55UH75/R9nvN6y7MuUKRnLmFJKWtum1Icxc01a+me6AJwk4ZObNuUDAPj1HvuD53OlrrMeW47aKnVIhlLT55Zah0ytnYQYtZcMrWRskp5fltqTkpayLa3YTP73ZKyZ0tOHzUrMqc2p47jZoqGPWbQx84dXc/kXb53vHwCAO98bq/LKX/99FmXItG4Z+5C0zSKF1JJpWqdWFQDn7XjCv9XN0bcRRO3J0JJMU3bGMdP6KIudZa6ur+doJ3nkhafP9brvFmJhAADgV+q/uFJe/sHfZXF9yl5Zpq/aphCgjuk928RuTTtu/XqLanut+ACA39Yz/80/6Yd7JUdjy1xy0l2o5taxxckWAP30fSsAvxjHn+0t1ZLVNGVcLtJLcrReZxiGDKVmOSwzHU3ZaUMuv/VB8so1dwSA31v78GquvfNx7ht3M7TN1jRDGdN7z3K5zHq9Pu9LJKdx2K22ABh7SV9NadOccawZd3dy0FcZHtzPs/+XP5NF+j0pAAAAAH6t9vcflY9/9mbqlaPsZ5FlxpQpGbYjuN57hro456sEAO4Wj/5vv92HR/ZzfdkzDS19O6NfznYXOrGpPjye+D+ZrC49WhD9/krfNvkvpyv/j23aLJfMvaX3ktaSnXEni2GZo4NVyrpnrw35+NX3cvkHr5v8B+CL8fZh+fjF19I+vJaxJWk9fZo3MUApqdX053nr5XiToLOvJa2UbDZuKlkOiyyGIeujVaY+5bCv88n6Wsp9O3niz78riPs9+AkAAAB+I1f+5o3y8S/fzngwZ6+NKUctY4Ysy5jMPUPkdAGA39/w7Uv9wacfy6er61mX9Unq+Ph8dqL/bCeA4y5Exx2J+PKcXcXXe884jpmmKWk9y8UiWc8ph3PuG/eSa3M+/F9+4o4A8MV68ZPy0cuvp65adsoiw1wyZsjqaJ1xHM/76jjj7GT0cZw2z3NaSRbDmD7PSZLd/b2sS8+V6Sj3PflIdr/zmCKA35ECAAAA4Dd2+fuvlU9fezfDlVUuZSfLqWaYSxalZprmW/43/RZbAgAA3NITY3/8u8/nWlYZ9oeUsaSeWcV/dnK/9pqyPZLN3vNzvXE7IiHI7++Gyf5bfKCl94x1yNCSMSXtYEpd9Ty4e1+WRz0f/Oy1r+5iAbinrP7mnXLw9sfZaSX7Zcww9dQkrZk3vl2Umwo3W5K5JGU55mi9SmrJcrlM7z3TNKUMNcv7djPv1jz07NeSZy+4mb8DBQAAAMBv5cO/fLlcfeej7LUhwzopq57FsEzbtts7djZB3xIZeADg13rsD7+Tcmk/bW9IKy01/YaV/sd7yPaTIoDTvenncnq0W/7u/K5+VTFnKSVtmlJLybIMyWrKfhmzmJNf/vilHPz4LVEgAF+aD176ZdbvX05tNaX3LBe7m6403BaGfnok21xRTfpi2MRtffNGm6bM6ym99xxO66yXJReefDhP/oMXkqd2FAH8lhQAAAAAv7X3Xnk9H7z5dnKwypiSOvfsDLdusdezTRpL/QIAv8L9/+ybfe9rD6TtDZnHnrlNaespQ9+EESft/VPTUnNzeNFTM5eaqW4Sy8lpcQBfhH7Lo5SStJ60njIlF8bdDHPJe798M4c/eFMECMCX69Xr5cNfvJ7VlWtJramlbKMEzldP2RZyDm1TzFmzieXmkhzNUzIOaemZ5zmLxSL7u7tZjovM85yr00Gulznj/XsZH33gvP8wdxwFAFutlPRNTfENrx8PLDarl2rS65n/JlFPDOetZPNz2E6+uuHdm/YDBAC+GO316+X9n76e4eqUi9OYdn2dZR2SzfBu+3Sumz16t6/ebWrfTDQcjxH68cxEymZg2+tJa+La63bwdTyecP5dzuXMKs8acR7AXeUbD/a9xx7IerfmMOus25RFLdkZ6vbf/+MRf/0NthbaPj8+U4F4ezzP7rTzZpuF5PizPO7ydNLpaZqzHHeyLIvMqzmLMma6fJBPX3s7APBVOPzxh+Xww8vZ7cusr6+yqIskv7qDzdn5v03IcFOv+hO3x/P4zjvfvB/kcXy2zaH0kmEYMs89c+8pGbJerzOvW8ZxzLizzDwk87LksW98PeU7D8kA/BZuvUTnHtNS00rNXMb0TDkuBahpmUu/5bTiyT5iPbJOcF56OXPalO/UkzYym5/kJBmbQh0A+DL0tw/KK2//MC/8H/603//4hVxb9wzLmlWb0+aW2nsWpWboLeWu3H+vJilp5Xhn4paknk5Sbwe1m7KI44RC3SbLz3sgfqee2zbmq2m9pr99zbIOgLvBMxf71/7wuYxfu5Qr/SC19tSezPOUxbibubTNiv+enD4TclJe2M7k6Y7byx6n8m7MCJz3c+zOPR/nW1pp6aXfkBsdelLmlr4qubDcS456Xv3pL5KXPacB+Op8+D+/VC498FDf2R/Sd5ZZzVcz7Ixp05yh1mQ70bxc7uZwdZQ6lu0o/iSi2K5Sb0mvaRmyeRae/3P4TjwfT532bFb8bz/ZlF4ztmxyJr1lrIskLVNPUoaU1JTWMqSlpeWwluw/uJ8nv/V83jyYe177VHzxG6i//lvufqetwEqOE3Y3F/j07Q956Wd/2LdDiJvLhBwOx5d+1F7O/IzePKFQcrZbBwDw5Xrp//vDsrw2ZecoaQdThpTUxbhp4zZNKdPdWYzXSstxHNLK8Z7ELb20zde1nayOm09+3dJLkjj/tudjxw2Hk2R47OLdWFkCcM955DvPZXzoQg6Hlqm2ZKzJUDcrwdq8fcZuvreXTZ+hzfO2Za6nX5e05Pic4+8/ez7/59mddk6S2jfndiY1c9oBoGRnuZf5cMqYknI459UfvZj85KPy+XccAL4cr/zHH2a/76RdX2Ucl+m9J72nracshjHLcczh4WGGcTyJFkovqds29cPNc4O3yfP4zjyfxgxzPc2HHMdpp/HareOP1XS0mcBd9KyHnvrgfpaP3ner284t6ACQzV+y2luG3tNyvKJku4dV76nlNNCtyUm2qWWz8gQ4Ly2l95Ty2RKA5Gy9wN056QAAt5Of/r/+Q3n+X/5xv/T4pVxrLauS1GGRWmrSWjInn92s5852PPHQy7zNEGxWCczbAe1xHNKSkwxC/Uwiwfm3PveW0ltabZkH8/8Ad7r6vUf6/U8+ksNFsp7X6TWZ0jf79w5D5tLT0zfP29wGz6F77LxRTl47fr2e2RLg6OgovbVcXO7k6MPLmX741t0V9AFw53hrVQ4+/KTvP3ZfLs+r9NKzu9jJ0cFh5tU6484ypfTU0tPbpsPN0JOhtZQMm9Xqddh2rxd/nNd5Lsk8zmk12dnZy7VPr2Z/by+P/4Pn8tqnV3p+rgvAr6MAIEmyaU9aekstmwKAlLaNYU//DtUc5+02ba5O+gCUzT8SrcTZ2fkrOh9XgfWSz0z+95LkTKXecStAAODL9cp/97fl6f/TP+yLh/dTL+5k1ef0XtLLkFL6phDgrrKZ9C/pm4Lh47FEkl42+xO3sm11V9omhumnwwzn3+68UU8mH1o5+zoAd6THh/7cP/xO1nXKus0pQ8tQktZaenpqramlZu7TuT+H7tlzT7ZZ0ZPbdnZlZOnJYhyzP+ylfXqYt37y88/eZwD4Cr353/9N+c7/9V/05bKmTXPq0LMcxkzTlNY2+8u31lK2W9wMLdvu30m2W/zNpSWZs+kudBs8j++x8/E92N3Zy7Vr1zLuLLIuLcPemG/8oz/IL67/oOet9WlwwmcoAEiymezfHtuGkq0kpbScBLdlszfFzev9N62v2kkrMmdn56/mfKKcJoCHdrZAYPt2P60aAwC+fG/89Yt5+HvP5uKzTyQ1mducsdSUUpJ37q6ncsnxSoGklqSVerLvcE/LfCYu6dnEJbXfGKvwm9sUdW5GZZtWgT03RYYA3GH+6P/4L/JJu5apbqr4d4YxU2lZz1OSbIoB5pah3FUhxB1nk1vpmyxpP8mWJj0Zek2Zk+naYd792WuZXvrEzQLg3P3s//4X5en/25/13f1FpsMpw2LMzv4iR+tVprTUWpPt5P5ctzN/vWYuNdO27m3sc6oh57loJVkud3P52tVcWO5nqDWHB0dZDMl6v+TpP/lu3njrR+d9mbc1BQBJ5tK3aaO2qWAtx2UANT09rdSUbXB7dnKxlU1iLzmdYHR2dv7qzqVv075nXi+tJL0mqSdVe/0zpTsAwJfmncPy4fB6T6/Zf/zBLO7bT5vWaW1Onhp63pzvnqRwL0lqau+Zt13BjpMDrZyujju7jdjxWOK4SNH5Nz9vcjAt2RZddIUUAHe0b/yrP+0fTVdSLi4yt9X22blKtl06h2GR9KT3krrtAHM7PI/uxfOxG4sYS9J6autZTiWfvPV+Dn749t0T5wFwx3vvxdfz7B9/O0eLMdenwwyLnUzp6b1vFin0npayXRxc0+rxIsSymVvYjvnP+zl8L55TkmlqWe7spc0tfe4ZxzHreU6GMZceeygP/OHX+id/957Y43MoAEjSyuaHeHNsVv23kpTUtLOR7jbIPS346acvAl+5XsrmuOHVelKRPiSxLSwAnIM3r5cP3/xJhn/2Qt//+uNpi6S1ngxDkvm8r+4L00pNT81cSjY9wVpSTwettW9HF2fjkV43RcU5Xsvu/Juek5ahbZIvw/HH3fQAALgT7f/JY3186EKuDoc5aqvUsaS16aTCvwxjWt+O92tN7/Xcn0P37rmllp6kbXIs21im957SShZTSb22ztF7n/7Kew4AX7XVD94pnz7wQL/wzNeS1BysjpKxZMyQobf0vlkAvNm6r6Vtn3xDq9sx6JjTV8/7eXxvnXuSPvTMrWW5WOTgykEWi0X2L+zn4PL1XK9DHvje87l8dNTbS58qArgFBQBJWtn8KA9tu59k76m9pKekttP3Nom80/RS6S29mF2E89R6zXHvuc1+PZvWu8cVemU7MAUAvnrv/fuXyjD3ft+Tj2S5t5Os7p7J/42aTSDSk2zGEm27Uv1kxX82q9Zrz7aD2ObV4/jE+Tc/b1Ydzp/Z7gmAO8vyO4/0J154Plf7nCyXKXXa7LM7jKkpqaWkpGaap7ResliMadt03O3wPLoXz3OZT7Y+SpLaS9JKhrlkuS559e9eSvvpx7IvANx23vsff1oe+ZeLfuHrj2TdDlJqz1BK5tVRhlpTe8tUj+cJp9ReM/acjOGN38/n3JPU3lNSsjpc5+LFi1mv17l8+XIu7u3n+uEqO/ct88x/8Q/y6pUf9rxzIA65iQKAJKUM6auevQu7Wa9XmcqcYaiZppK6bf8/bjuV9m2xQLmhHgU4D3U7uX/8YCg5LdapvWQcx/RpSh2Hc71OALiXvf39n5f+x+v+8DeeSfrd1QFg0Xv63LLYHXP16CDDoialnIwQak9SWmrbTFQPPZnqjUXF/OZaNn97Sq3pc8nc+g1tiQG4/ZXnHuxPfPeF9It76XVK63OyXZCTJD01bVvcP5ZlUmranKQ0z89z0zL3luVyzGo1p8wtO+NO6pzUwymfvvWhyX8AbmsfvPJ29h+6Pzt7Yw5XRymLIYtxTOaWubYzBW41tc9Zzpv5v6kmcz3XS79nlSRpmzxKrSXz+ihDT/YWY6Z5lSxqDjOn7ZU89O2v56N3fnbel3zbUQCQpFxZ5dobH2ReJNO0Sh9rylCy7i2ljJsCgG0ngLYdbJQ0q03gnB1X4W1/PFP6dn/YvnlhmqYMc8nBp9fO8SoBgHf+9tVyeLTueXd1VyWHe5syDsk0b5Lifehp7WT6P3PZJBJa6RmyXf1fTt/PuTfVu7POvbT0lLRaMtQx83JMsyUbwB3l/qe+llxc5npZZ11aflVgcMOunKXlvJ9D9+q5l6SOY64dHmVIzYXlXuajObttkf3UvPEXL99V8R0Ad6GfflDee+CN/ui3n86wu5PVdJTVtMpyOZ4W8GcTl2wK+WvSW04DlfN/Ht+L56HXlH58h/pJN8C5bGLDuSTZW2Tn8Uu5/8+e65/+1S/FJGcoAEhy5aV3y5WX3j3vywAAgLvWJz99664biI2lZCjJ0XSY7NSsVussFotNcWLvSdnM95f0k+LhXso2h3A8ce38m59Lpt6ynubUXjLPLSl33V8rgLvWfX/yZH/g6w9l3k/WfcpcW2pJ0ucMNWnltLC/1ONnZ9/8U19uh+fQvXkuSdJL9nf2sppa1q1np9Us55K//4vvBwDuBIevvJ0r+zu575lH0ncXGS4ss2rrtO2YcjPxn7QhWdVystmf8fv5nDfFGH1zT0pSes+2QXt6TVJ6xlKyno7SL5Zc+tZjufrRR31++bIkwZYCAAAAgN9F70mbUkvPIkOGsWSa+2YyYztw7duUQe8ttSc1Pb2U3A4D6jvxvDPsZGotNUPWdUgGWz0B3Anu+97D/aGnH86wv8hBP0orU2odMvSetDm91AxJat9sFVS3rf562az2Ot3y5fZ4Ht1L59KTebVOypDdYZFFG7Lfkhf/ww+Tt44k2QG4M7x7VK5c+rA/+MjDabXmcLVKdmpKSWpLso05jvef78nJ1gC3w/P4XjtvOj1vvy49SUm23QDq5qv0Pqf0krJcpM0tDz33RN5/+XLYUAAAAADwO6jDmLGW7NSW+WjOmGTYlqS349L0bcv/2su2XV09l2u9G5Rek+vr7NRF6rDIwVyT16+aeAC4Azz1/LMpD+5k1abUPmVnMaS0krnNWWZMps33Hbd2Pf7HvWeTiD/e6o+vXunJzrBMn5LVwZTdnkwfHyYvXfEMBuCOMr/0Qbn68AP94Reeye7OMgerVebaMrTT72nbFedJUnszgj9P2/ivpKWXnHRr2Ftv3p5LTUuyyJCk5NITj+bgH1/rV3/whhglCgAAAAB+N9fmLBY9O8sx87wZX5btgHSudVO7flIAsJnRqM049HdVks0egKlZtTnzYc/uYw/0w3c/8aEC3MYeeubRfnH/vqxaTzmYM5RlMg9p6WktWYxj+nxj8j3JNtGbHBfPeYaek9LTpjm1Jw8t9lLWPT/47/7SzQDgjvT+f/h52Vnu9d2HLubS/jKtDKn9xiBkPq7n78NJcSJfrVaSXmqSlqFtOv/NdbM1wGLeftM45mhaJ2lZ1EXGPuQ73/h2Xvu09fdfuvu2oQQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADg/98eHBIAAAAACPr/2hsGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADgL9wlRieXgztwAAAABJRU5ErkJggg==" alt="INFX">
    </div>

    <div class="header-right">
        <div class="status-dot" id="statusDot"></div>
        <div class="status-text" id="statusText">
            Connecting...
        </div>
    </div>

</header>

<main class="container">

    <div id="errorBox" class="error-box"></div>

    <div class="toolbar">

        <select
            id="symbolSelect"
            class="control"
        ></select>

        <select
            id="timeframeSelect"
            class="control"
        ></select>

        <button
            class="button"
            onclick="applyMarket()"
        >
            APPLY
        </button>

        <button
            class="button secondary"
            onclick="refreshBrain(true)"
        >
            REFRESH BRAIN
        </button>

        <button
            class="button secondary"
            onclick="openTradingView()"
        >
            OPEN TRADINGVIEW
        </button>

    </div>

    <section class="grid">

        <div>

            <div class="card chart-card">

                <div class="card-header">

                    <div class="card-title">
                        TRADINGVIEW LIVE CHART
                    </div>

                    <div id="chartLabel">
                        XAUUSD · M5
                    </div>

                </div>

                <div class="chart-wrapper">

                    <div
                        id="tvChart"
                        aria-label="INFX live market chart"
                    ></div>

                    <div
                        id="chartLoading"
                        class="chart-loading"
                    >
                        Loading TradingView...
                    </div>

                    <div
                        id="chartError"
                        class="chart-error"
                    >
                        Chart could not be loaded.
                        The chart library could not be loaded.
                        Check your internet connection.
                    </div>

                </div>

            </div>

            <div class="stats">

                <div class="stat">
                    <div class="stat-label">
                        LIVE PRICE
                    </div>
                    <div
                        class="stat-value live-price"
                        id="livePrice"
                    >
                        --
                    </div>
                </div>

                <div class="stat">
                    <div class="stat-label">
                        BID
                    </div>
                    <div
                        class="stat-value"
                        id="bidPrice"
                    >
                        --
                    </div>
                </div>

                <div class="stat">
                    <div class="stat-label">
                        ASK
                    </div>
                    <div
                        class="stat-value"
                        id="askPrice"
                    >
                        --
                    </div>
                </div>

                <div class="stat">
                    <div class="stat-label">
                        LAST CLOSED
                    </div>
                    <div
                        class="stat-value"
                        id="closedPrice"
                    >
                        --
                    </div>
                </div>

            </div>

        </div>

        <aside>

            <div class="card">

                <div class="card-header">

                    <div class="card-title">
                        TRADING BRAIN
                    </div>

                    <div id="brainUpdated">
                        --
                    </div>

                </div>

                <div id="signalContainer">

                    <div class="info">
                        Waiting for Brain...
                    </div>

                </div>

            </div>

            <div class="card" style="margin-top:16px;">

                <div class="card-header">

                    <div class="card-title">
                        ENGINE STATUS
                    </div>

                </div>

                <div class="count-grid">

                    <div class="count">
                        <div class="count-name">
                            CANDLES
                        </div>
                        <div
                            class="count-number"
                            id="countCandles"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            SWINGS
                        </div>
                        <div
                            class="count-number"
                            id="countSwings"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            BOS / BREAKS
                        </div>
                        <div
                            class="count-number"
                            id="countBreaks"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            LIQUIDITY
                        </div>
                        <div
                            class="count-number"
                            id="countLiquidity"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            DISPLACEMENT
                        </div>
                        <div
                            class="count-number"
                            id="countDisplacement"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            ORDER BLOCKS
                        </div>
                        <div
                            class="count-number"
                            id="countOB"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            FVG
                        </div>
                        <div
                            class="count-number"
                            id="countFVG"
                        >
                            --
                        </div>
                    </div>

                    <div class="count">
                        <div class="count-name">
                            CONFLUENCE
                        </div>
                        <div
                            class="count-number"
                            id="countConfluence"
                        >
                            --
                        </div>
                    </div>

                </div>

            </div>

        </aside>

    </section>

</main>

<script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>

<script>

let currentConfig = null;
let brainTimer = null;
let liveTimer = null;
let chartTimer = null;
let qxChart = null;
let qxCandleSeries = null;
let qxVolumeSeries = null;
let qxChartCandles = [];
let qxSignalLines = [];
let qxSignalLevels = [];
let qxSignalTime = null;
let qxSignalEndTime = null;
let qxSignalLabelNodes = [];
let qxLastClosedCandleTime = null;
let qxMarketGeneration = 0;

// DISPLAY-ONLY SIGNAL LOCK.
// Once an actionable setup is displayed, keep that same setup selected
// across brain refreshes while it remains ACTIVE. This prevents the card
// from flipping BUY <-> SELL merely because priority scores changed on a
// later candle. It does not alter Signal Engine output.
let qxLockedSignalKey = null;
let qxLockedSignalSnapshot = null;
// DISPLAY-ONLY POST-STOP GUARD.
// After the locked setup is proven to have hit its SL, do not immediately
// recycle an older setup. A new signal is allowed only when its zone became
// ready after the observed stop candle.
let qxStoppedSignalKey = null;
let qxStoppedAt = null;


function escapeHtml(value) {

    if (value === null || value === undefined) {
        return "";
    }

    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}


function showError(message) {

    const box = document.getElementById(
        "errorBox"
    );

    box.textContent = message;
    box.classList.add("show");
}


function clearError() {

    const box = document.getElementById(
        "errorBox"
    );

    box.textContent = "";
    box.classList.remove("show");
}


function setStatus(online, text) {

    const dot = document.getElementById(
        "statusDot"
    );

    const status = document.getElementById(
        "statusText"
    );

    if (online) {

        dot.style.background = "#4ade80";
        dot.style.boxShadow =
            "0 0 12px #4ade80";

    } else {

        dot.style.background = "#fb7185";
        dot.style.boxShadow =
            "0 0 12px #fb7185";
    }

    status.textContent = text;
}


function tvInterval(timeframe) {

    const map = {
        "M1": "1",
        "M2": "2",
        "M3": "3",
        "M4": "4",
        "M5": "5",
        "M10": "10",
        "M15": "15",
        "M30": "30",
        "H1": "60",
        "H4": "240",
        "D1": "D"
    };

    return map[timeframe] || "5";
}


function tvSymbol(symbol) {

    const upper = String(symbol || "").trim().toUpperCase();

    const map = {
        "XAUUSD": "OANDA:XAUUSD",
        "EURUSD": "OANDA:EURUSD",
        "GBPUSD": "OANDA:GBPUSD",
        "USDJPY": "OANDA:USDJPY",
        "USDCHF": "OANDA:USDCHF",
        "USDCAD": "OANDA:USDCAD",
        "AUDUSD": "OANDA:AUDUSD",
        "NZDUSD": "OANDA:NZDUSD",
        "XAGUSD": "OANDA:XAGUSD"
    };

    for (const key of Object.keys(map)) {
        if (upper.includes(key)) {
            return map[key];
        }
    }

    return upper;
}


function buildTradingViewUrl(symbol, timeframe) {

    const params = new URLSearchParams();

    params.set(
        "symbol",
        tvSymbol(symbol)
    );

    params.set(
        "interval",
        tvInterval(timeframe)
    );

    params.set(
        "hidesidetoolbar",
        "1"
    );

    params.set(
        "symboledit",
        "1"
    );

    params.set(
        "saveimage",
        "0"
    );

    params.set(
        "toolbarbg",
        "080b12"
    );

    params.set(
        "theme",
        "dark"
    );

    params.set(
        "style",
        "1"
    );

    params.set(
        "timezone",
        "Etc/UTC"
    );

    params.set(
        "withdateranges",
        "1"
    );

    params.set(
        "hideideas",
        "1"
    );

    return (
        "https://www.tradingview.com/widgetembed/?"
        + params.toString()
    );
}


function loadTradingView() {

    if (!currentConfig) {
        return;
    }

    const loading = document.getElementById("chartLoading");
    const error = document.getElementById("chartError");

    if (!loading || !error) {
        return;
    }

    loading.classList.remove("hidden");
    error.classList.remove("show");
    clearTimeout(chartTimer);

    try {
        initINFXChart();
        if (qxCandleSeries) qxCandleSeries.setData([]);
        if (qxVolumeSeries) qxVolumeSeries.setData([]);
        clearINFXSignalLines();
        qxChartCandles = [];
        if (qxChart) qxChart.__qxInitialFitDone = false;
        loading.classList.add("hidden");
    } catch (chartError) {
        loading.classList.add("hidden");
        error.classList.add("show");
        console.error("Chart initialization error:", chartError);
    }
}

function openTradingView() {

    if (!currentConfig) {
        return;
    }

    const symbol = tvSymbol(
        currentConfig.symbol
    );

    const interval = tvInterval(
        currentConfig.timeframe
    );

    const url =
        "https://www.tradingview.com/chart/?symbol="
        + encodeURIComponent(symbol)
        + "&interval="
        + encodeURIComponent(interval);

    window.open(
        url,
        "_blank",
        "noopener,noreferrer"
    );
}


async function loadConfig() {

    const fallbackSymbols = [
        "XAUUSD",
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "USDCHF",
        "USDCAD",
        "AUDUSD",
        "NZDUSD"
    ];

    const fallbackTimeframes = [
        "M1",
        "M2",
        "M3",
        "M4",
        "M5",
        "M10",
        "M15",
        "M30",
        "H1",
        "H4",
        "D1"
    ];

    try {

        const response = await fetch(
            "/api/config",
            {
                cache: "no-store"
            }
        );

        const data = await response.json();

        if (!data.ok) {
            throw new Error(
                data.error || "Config error"
            );
        }

        currentConfig = data.config;

        populateSelectors(
            data.symbols && data.symbols.length
                ? data.symbols
                : fallbackSymbols,
            data.timeframes && data.timeframes.length
                ? data.timeframes
                : fallbackTimeframes
        );

        updateChartLabel();
        loadTradingView();

    } catch (error) {

        /*
         * The chart must not disappear just because server configuration
         * is temporarily unavailable. Keep the default market alive.
         */
        currentConfig = {
            symbol: "XAUUSD",
            timeframe: "M5",
            timeframe_value: 5
        };

        populateSelectors(
            fallbackSymbols,
            fallbackTimeframes
        );

        updateChartLabel();
        loadTradingView();

        showError(
            "TradingView config unavailable: "
            + error.message
            + " | TradingView is still available."
        );

        setStatus(
            false,
            "TRADINGVIEW LIVE"
        );
    }
}


function populateSelectors(symbols, timeframes) {

    const symbolSelect =
        document.getElementById(
            "symbolSelect"
        );

    const timeframeSelect =
        document.getElementById(
            "timeframeSelect"
        );

    const currentSymbol =
        currentConfig
            ? currentConfig.symbol
            : "XAUUSD";

    const currentTimeframe =
        currentConfig
            ? currentConfig.timeframe
            : "M5";

    symbolSelect.innerHTML = "";

    for (const symbol of symbols) {

        const option =
            document.createElement(
                "option"
            );

        option.value = symbol;
        option.textContent = symbol;

        if (String(symbol).toUpperCase()
            === String(currentSymbol).toUpperCase()) {
            option.selected = true;
        }

        symbolSelect.appendChild(
            option
        );
    }

    /*
     * Always keep the current symbol visible even when the broker
     * uses a suffix such as XAUUSDm.
     */
    if (
        currentSymbol
        && !Array.from(symbolSelect.options).some(
            option => option.value === currentSymbol
        )
    ) {

        const option =
            document.createElement(
                "option"
            );

        option.value = currentSymbol;
        option.textContent = currentSymbol;
        option.selected = true;

        symbolSelect.insertBefore(
            option,
            symbolSelect.firstChild
        );
    }

    timeframeSelect.innerHTML = "";

    for (const timeframe of timeframes) {

        const option =
            document.createElement(
                "option"
            );

        option.value = timeframe;
        option.textContent = timeframe;

        if (
            String(timeframe).toUpperCase()
            === String(currentTimeframe).toUpperCase()
        ) {
            option.selected = true;
        }

        timeframeSelect.appendChild(
            option
        );
    }
}


function updateChartLabel() {

    if (!currentConfig) {
        return;
    }

    document.getElementById(
        "chartLabel"
    ).textContent =
        currentConfig.symbol
        + " · "
        + currentConfig.timeframe;
}


async function applyMarket() {

    const symbol =
        document.getElementById(
            "symbolSelect"
        ).value;

    const timeframe =
        document.getElementById(
            "timeframeSelect"
        ).value;

    clearError();

    // New market selection invalidates every in-flight old-market response.
    // This is display/data synchronization only; Signal Engine logic is untouched.
    const generation = ++qxMarketGeneration;

    // A symbol/timeframe change starts a completely new display context.
    // Release the previous setup lock so a signal from the old market cannot
    // survive into the new market/timeframe. Display-only state; no engine
    // calculation is changed.
    resetDisplaySignalState();

    // Stop the 1-second live poll and 15-second brain poll while the new
    // market/timeframe is being installed. Otherwise an older M5 response can
    // arrive after the user selects M1/M15 and put the old candles back.
    if (liveTimer) {
        clearInterval(liveTimer);
        liveTimer = null;
    }
    if (brainTimer) {
        clearInterval(brainTimer);
        brainTimer = null;
    }

    currentConfig = {
        ...(currentConfig || {}),
        symbol: symbol,
        timeframe: timeframe
    };

    updateChartLabel();

    try {

        const response = await fetch(
            "/api/config",
            {
                method: "POST",
                headers: {
                    "Content-Type":
                        "application/json"
                },
                body: JSON.stringify({
                    symbol: symbol,
                    timeframe: timeframe
                })
            }
        );

        const data =
            await response.json();

        if (!data.ok) {
            throw new Error(
                data.error || "Market configuration failed"
            );
        }

        // Ignore a response if the user has already selected another market.
        if (generation !== qxMarketGeneration) return;

        currentConfig = data.config;
        updateChartLabel();

        // Clear the old timeframe's candles/levels only after the server has
        // accepted the new configuration.
        loadTradingView();

        await refreshBrain(true, generation);
        await refreshLive(generation);

        if (generation === qxMarketGeneration) {
            liveTimer = setInterval(
                function() {
                    refreshLive(qxMarketGeneration);
                },
                1000
            );

            brainTimer = setInterval(
                function() {
                    refreshBrain(false, qxMarketGeneration);
                },
                15000
            );
        }

    } catch (error) {

        if (generation !== qxMarketGeneration) return;

        showError(
            "Configuration: "
            + error.message
            + " | TradingView chart remains on "
            + symbol
            + " · "
            + timeframe
        );

        setStatus(
            false,
            "TRADINGVIEW LIVE"
        );

        // Restore polling for the still-active configuration.
        liveTimer = setInterval(
            function() {
                refreshLive(qxMarketGeneration);
            },
            1000
        );
        brainTimer = setInterval(
            function() {
                refreshBrain(false, qxMarketGeneration);
            },
            15000
        );
    }
}

function formatPrice(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "--";
    }

    const number = Number(value);

    if (!Number.isFinite(number)) {
        return "--";
    }

    return number.toFixed(5);
}


function renderLive(live) {

    if (!live || !live.ok) {

        setStatus(
            false,
            "Live data unavailable"
        );

        return;
    }

    setStatus(
        true,
        live.stale ? "TRADINGVIEW LIVE · LAST KNOWN GOOD" : "TRADINGVIEW LIVE"
    );

    document.getElementById(
        "livePrice"
    ).textContent =
        formatPrice(
            live.price
        );

    document.getElementById(
        "bidPrice"
    ).textContent =
        formatPrice(
            live.bid
        );

    document.getElementById(
        "askPrice"
    ).textContent =
        formatPrice(
            live.ask
        );

    updateINFXChartLive(live);
}


function renderBrain(state) {

    const brain = state.brain || {};
    const counts =
        brain.counts || {};

    const latest =
        brain.latest || {};

    document.getElementById(
        "closedPrice"
    ).textContent =
        formatPrice(
            latest.close
        );

    document.getElementById(
        "brainUpdated"
    ).textContent =
        state.updated_at || "--";

    document.getElementById(
        "countCandles"
    ).textContent =
        counts.candles || 0;

    document.getElementById(
        "countSwings"
    ).textContent =
        counts.swings || 0;

    document.getElementById(
        "countBreaks"
    ).textContent =
        counts.structure_breaks || 0;

    document.getElementById(
        "countLiquidity"
    ).textContent =
        counts.liquidity || 0;

    document.getElementById(
        "countDisplacement"
    ).textContent =
        counts.displacement || 0;

    document.getElementById(
        "countOB"
    ).textContent =
        counts.order_blocks || 0;

    document.getElementById(
        "countFVG"
    ).textContent =
        counts.fvg || 0;

    document.getElementById(
        "countConfluence"
    ).textContent =
        counts.confluence || 0;

    renderSignals(
        brain.signals || [],
        brain.risk || [],
        brain.rejected_signals || []
    );
}


function selectBestDisplaySignal(signals, risk) {
    if (!Array.isArray(signals) || !signals.length) {
        return { signal: null, risk: null, index: -1 };
    }

    let bestIndex = 0;

    const scoreOf = (row) => {
        const value = Number(
            row?.priority_score ??
            row?.score ??
            row?.signal_score ??
            row?.confluence_score
        );
        return Number.isFinite(value) ? value : -Infinity;
    };

    const confluenceOf = (row) => {
        const value = Number(row?.confluence_score);
        return Number.isFinite(value) ? value : -Infinity;
    };

    const confirmationsOf = (row) => {
        const value = Number(row?.confirmation_count);
        return Number.isFinite(value) ? value : -Infinity;
    };

    for (let index = 1; index < signals.length; index++) {
        const current = signals[index] || {};
        const best = signals[bestIndex] || {};

        const currentKey = [
            scoreOf(current),
            confluenceOf(current),
            confirmationsOf(current)
        ];
        const bestKey = [
            scoreOf(best),
            confluenceOf(best),
            confirmationsOf(best)
        ];

        let replace = false;
        for (let k = 0; k < currentKey.length; k++) {
            if (currentKey[k] > bestKey[k]) {
                replace = true;
                break;
            }
            if (currentKey[k] < bestKey[k]) {
                break;
            }
        }

        if (replace) bestIndex = index;
    }

    return {
        signal: signals[bestIndex] || null,
        risk: Array.isArray(risk) ? (risk[bestIndex] || {}) : {},
        index: bestIndex
    };
}


// Risk Engine output is independently sorted and may skip a setup that
// fails validation. Therefore risk[index] is NOT guaranteed to belong to
// signals[index]. Match the risk row to the selected signal by the stable
// signal identity fields instead. This is display synchronization only; the
// Signal Engine and Risk Engine calculations are not changed.
function findMatchingRiskRow(signal, risk) {
    if (!signal || !Array.isArray(risk) || !risk.length) return {};

    const signalTime = tvChartTime(
        signal.signal_time ?? signal.time
    );
    const zoneTime = tvChartTime(
        signal.zone_time
    );
    const direction = String(
        signal.direction || ""
    ).trim().toLowerCase();
    const signalName = String(
        signal.signal || ""
    ).trim().toUpperCase();

    const matches = (row, requireZoneTime) => {
        if (!row) return false;

        const rowSignalTime = tvChartTime(
            row.signal_time ?? row.time
        );
        const rowZoneTime = tvChartTime(
            row.zone_time
        );
        const rowDirection = String(
            row.direction || ""
        ).trim().toLowerCase();
        const rowSignalName = String(
            row.signal || ""
        ).trim().toUpperCase();

        if (direction && rowDirection && direction !== rowDirection) return false;
        if (signalName && rowSignalName && signalName !== rowSignalName) return false;
        if (signalTime !== null && rowSignalTime !== null && signalTime !== rowSignalTime) return false;
        if (requireZoneTime && zoneTime !== null && rowZoneTime !== null && zoneTime !== rowZoneTime) return false;

        return (
            (signalTime !== null && rowSignalTime === signalTime)
            ||
            (zoneTime !== null && rowZoneTime === zoneTime)
        );
    };

    // Strongest identity: signal time + zone time + direction/signal.
    let row = risk.find((item) => matches(item, true));
    if (row) return row;

    // Fallback identity: signal time + direction/signal.
    row = risk.find((item) => matches(item, false));
    if (row) return row;

    // Do not pair an unrelated risk row by array position. If no matching
    // risk row exists, leave levels empty rather than showing levels from a
    // different BUY/SELL setup.
    return {};
}


function signalIdentityKey(signal) {
    if (!signal) return null;

    const zoneTime = tvChartTime(signal.zone_time);
    const readyTime = tvChartTime(signal.zone_ready_time);
    const signalTime = tvChartTime(signal.signal_time ?? signal.time);
    const direction = String(
        signal.signal || signal.direction || ""
    ).trim().toLowerCase();
    const signalName = String(signal.signal || "").trim().toUpperCase();

    // Prefer the stable setup identity. If zone_time is unavailable, fall
    // back to the signal/ready time so separate signals do not collapse into
    // one stored event.
    const primaryTime = zoneTime ?? readyTime ?? signalTime ?? "";

    return [
        primaryTime,
        readyTime ?? "",
        direction,
        signalName
    ].join("|");
}

function isActionableSignal(row) {
    if (!row || typeof row !== "object") return false;

    // V9 has already produced a confirmed signal on the latest CLOSED
    // candle. Do not hide it merely because the current price has moved out
    // of the original zone. That was a setup condition, not an order type.
    const direction = String(
        row.signal || row.direction || ""
    ).trim().toUpperCase();

    const validDirection =
        direction === "BUY" ||
        direction === "SELL" ||
        direction === "BULLISH" ||
        direction === "BEARISH" ||
        direction.includes("LONG") ||
        direction.includes("SHORT");

    if (!validDirection) return false;

    const status = String(
        row.setup_status || ""
    ).trim().toUpperCase();

    if (status && ["STOPPED", "TP3_HIT", "CANCELLED", "EXPIRED"].includes(status)) {
        return false;
    }

    return true;
}

function resetDisplaySignalState() {
    qxLockedSignalKey = null;
    qxLockedSignalSnapshot = null;
    qxStoppedSignalKey = null;
    qxStoppedAt = null;
}

function detectLockedSignalStop() {
    if (!qxLockedSignalSnapshot || !qxLockedSignalSnapshot.risk) {
        return null;
    }

    const snapshotSignal = qxLockedSignalSnapshot.signal || {};
    const riskRow = qxLockedSignalSnapshot.risk || {};
    const stop = Number(
        riskRow.stop_loss
        ?? riskRow.sl
        ?? riskRow.SL
    );

    if (!Number.isFinite(stop)) return null;

    const direction = String(
        snapshotSignal.signal
        || (String(snapshotSignal.direction || "").toLowerCase() === "bullish" ? "BUY" : "SELL")
    ).toUpperCase();

    const signalTime = tvChartTime(
        snapshotSignal.signal_time
    );

    if (!Number.isFinite(signalTime)) return null;

    const candles = Array.isArray(qxChartCandles)
        ? qxChartCandles
        : [];

    let hitTime = null;

    for (const candle of candles) {
        const candleTime = tvChartTime(candle && candle.time);
        if (!Number.isFinite(candleTime) || candleTime <= signalTime) continue;

        const high = Number(candle.high);
        const low = Number(candle.low);

        if (!Number.isFinite(high) || !Number.isFinite(low)) continue;

        const hit = direction === "BUY"
            ? low <= stop
            : high >= stop;

        if (hit && (hitTime === null || candleTime < hitTime)) {
            hitTime = candleTime;
        }
    }

    return hitTime;
}

function isAllowedAfterStop(row) {
    if (!row) return false;

    if (qxStoppedSignalKey === null) return true;

    const key = signalIdentityKey(row);
    if (key === qxStoppedSignalKey) return false;

    const readyTime = tvChartTime(row.zone_ready_time);

    // If the setup has no reliable ready time, do not guess that it is new.
    if (!Number.isFinite(readyTime) || !Number.isFinite(qxStoppedAt)) {
        return false;
    }

    return readyTime > qxStoppedAt;
}

function signalEventStorageKey() {
    const symbol = String(currentConfig?.symbol || "UNKNOWN").trim().toUpperCase();
    const timeframe = String(currentConfig?.timeframe || "UNKNOWN").trim().toUpperCase();
    return `infx.signal_events.v2.${symbol}.${timeframe}`;
}

function loadSignalEvents() {
    try {
        const raw = localStorage.getItem(signalEventStorageKey());
        const parsed = raw ? JSON.parse(raw) : [];
        return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
        console.warn("Signal event storage read failed:", error);
        return [];
    }
}

function saveSignalEvents(events) {
    try {
        // Keep a bounded history. This is display/history memory only.
        localStorage.setItem(
            signalEventStorageKey(),
            JSON.stringify(Array.isArray(events) ? events.slice(-200) : [])
        );
    } catch (error) {
        console.warn("Signal event storage write failed:", error);
    }
}

function eventRiskLevel(event, names) {
    const risk = event?.risk || {};
    const signal = event?.signal || {};
    for (const name of names) {
        const value = risk[name] ?? signal[name];
        const number = Number(value);
        if (Number.isFinite(number)) return number;
    }
    return null;
}

function evaluateSignalEvent(event) {
    if (!event || !event.signal) {
        return { status: "ACTIVE", hitTime: null, reachedTp: 0 };
    }

    const signal = event.signal || {};
    const risk = event.risk || {};
    const direction = String(
        signal.signal || signal.direction || ""
    ).trim().toUpperCase();
    const isBuy = direction === "BUY" || direction === "BULLISH" || direction.includes("LONG");
    const isSell = direction === "SELL" || direction === "BEARISH" || direction.includes("SHORT");

    if (!isBuy && !isSell) {
        return { status: "ACTIVE", hitTime: null, reachedTp: 0 };
    }

    const signalTime = tvChartTime(signal.signal_time ?? signal.time);
    if (!Number.isFinite(signalTime)) {
        return { status: "ACTIVE", hitTime: null, reachedTp: 0 };
    }

    const sl = eventRiskLevel(event, ["stop_loss", "sl", "SL"]);
    const tp1 = eventRiskLevel(event, ["take_profit_1", "tp1", "TP1"]);
    const tp2 = eventRiskLevel(event, ["take_profit_2", "tp2", "TP2"]);
    const tp3 = eventRiskLevel(event, ["take_profit_3", "tp3", "TP3"]);

    const candles = Array.isArray(qxChartCandles) ? qxChartCandles : [];
    const lastClosedTime = tvChartTime(
        window.__quantumBrainState?.brain?.latest?.time
    );
    let reachedTp = 0;
    let hitTime = null;
    let terminal = false;
    let terminalStatus = "ACTIVE";
    let postSignalClosedCandles = 0;

    for (const candle of candles) {
        const candleTime = tvChartTime(candle?.time);
        if (!Number.isFinite(candleTime) || candleTime <= signalTime) continue;
        // Never use the currently-forming candle for STOP/TP status.
        if (Number.isFinite(lastClosedTime) && candleTime > lastClosedTime) continue;
        postSignalClosedCandles += 1;

        const high = Number(candle.high);
        const low = Number(candle.low);
        if (!Number.isFinite(high) || !Number.isFinite(low)) continue;

        // With OHLC alone, an intra-candle TP/SL order is unknowable when
        // both levels are touched. Use the existing conservative SL-first
        // convention rather than inventing an intrabar sequence.
        const slHit = Number.isFinite(sl)
            && (isBuy ? low <= sl : high >= sl);

        if (slHit) {
            hitTime = candleTime;
            terminal = true;
            terminalStatus = reachedTp > 0
                ? `STOPPED_AFTER_TP${reachedTp}`
                : "STOPPED";
            break;
        }

        if (isBuy) {
            if (Number.isFinite(tp3) && high >= tp3) {
                reachedTp = 3;
                hitTime = hitTime ?? candleTime;
                terminal = true;
                terminalStatus = "TP3_HIT";
                break;
            }
            if (Number.isFinite(tp2) && high >= tp2) {
                reachedTp = Math.max(reachedTp, 2);
            }
            if (Number.isFinite(tp1) && high >= tp1) {
                reachedTp = Math.max(reachedTp, 1);
            }
        } else {
            if (Number.isFinite(tp3) && low <= tp3) {
                reachedTp = 3;
                hitTime = hitTime ?? candleTime;
                terminal = true;
                terminalStatus = "TP3_HIT";
                break;
            }
            if (Number.isFinite(tp2) && low <= tp2) {
                reachedTp = Math.max(reachedTp, 2);
            }
            if (Number.isFinite(tp1) && low <= tp1) {
                reachedTp = Math.max(reachedTp, 1);
            }
        }
    }

    if (terminal) {
        return { status: terminalStatus, hitTime, reachedTp };
    }

    if (reachedTp > 0) {
        return { status: `TP${reachedTp}_HIT`, hitTime, reachedTp };
    }

    if (postSignalClosedCandles === 0) {
        return { status: "NEW", hitTime: null, reachedTp: 0 };
    }

    return { status: "ACTIVE", hitTime: null, reachedTp: 0 };
}

function addOrRefreshSignalEvents(signals, risk) {
    const events = loadSignalEvents();
    const byKey = new Map(
        events.map(event => [event.key, event])
    );

    const actionable = (Array.isArray(signals) ? signals : [])
        .filter(isActionableSignal);

    for (const signal of actionable) {
        const key = signalIdentityKey(signal);
        if (!key) continue;

        const riskRow = findMatchingRiskRow(signal, risk);

        if (byKey.has(key)) {
            // IMMUTABLE TRADE SNAPSHOT: keep the Entry/SL/TP captured when
            // this signal was first issued. A later Brain refresh has a new
            // live price and must NOT rewrite an existing trade.
            continue;
        }

        byKey.set(key, {
            key,
            created_at: signal.signal_time ?? signal.time ?? null,
            symbol: currentConfig?.symbol || "",
            timeframe: currentConfig?.timeframe || "",
            signal: { ...signal },
            risk: { ...(riskRow || {}) },
            status: "ACTIVE",
            status_time: null,
        });
    }

    const updated = Array.from(byKey.values());

    // Re-evaluate every stored event only from candles that are actually
    // available. No signal-engine calculation is modified here.
    for (const event of updated) {
        const outcome = evaluateSignalEvent(event);
        event.status = outcome.status;
        event.status_time = outcome.hitTime ?? event.status_time ?? null;
        event.reached_tp = outcome.reachedTp;
    }

    updated.sort((a, b) => {
        const ta = tvChartTime(a.created_at) ?? 0;
        const tb = tvChartTime(b.created_at) ?? 0;
        return ta - tb;
    });

    saveSignalEvents(updated);
    return updated;
}

function eventIsTerminal(event) {
    const status = String(event?.status || "ACTIVE").toUpperCase();
    return status === "STOPPED"
        || status === "TP3_HIT"
        || status.startsWith("STOPPED_AFTER_TP");
}

function renderPersistentEvent(event) {
    if (!event || !event.signal) return false;

    const container = document.getElementById("signalContainer");
    if (!container) return false;

    const signal = event.signal || {};
    const riskRow = event.risk || {};
    const direction = String(
        signal.signal
        || (String(signal.direction || "").toLowerCase() === "bullish" ? "BUY" : "SELL")
    ).toUpperCase();
    const isBuy = direction === "BUY"
        || String(signal.direction || "").toUpperCase() === "BULLISH"
        || direction.includes("LONG");
    const sideClass = isBuy ? "buy" : "sell";

    const score = signal.priority_score ?? signal.score ?? signal.signal_score ?? signal.confluence_score;
    const confluenceScore = signal.confluence_score ?? score;
    const confirmations = signal.confirmation_count;
    const strength = signal.signal_strength || signal.quality || "";

    // The immutable Risk Engine entry is the single source of truth.
    // market_entry is metadata and must never override the calculated entry.
    const entry = riskRow.entry ?? riskRow.entry_price ?? riskRow.market_entry ?? signal.entry ?? signal.entry_price ?? signal.market_entry ?? signal.Entry;
    const sl = riskRow.stop_loss ?? riskRow.sl ?? riskRow.SL ?? signal.stop_loss ?? signal.sl ?? signal.SL;
    const tp1 = riskRow.take_profit_1 ?? riskRow.tp1 ?? riskRow.TP1 ?? signal.take_profit_1 ?? signal.tp1 ?? signal.TP1;
    const tp2 = riskRow.take_profit_2 ?? riskRow.tp2 ?? riskRow.TP2 ?? signal.take_profit_2 ?? signal.tp2 ?? signal.TP2;
    const tp3 = riskRow.take_profit_3 ?? riskRow.tp3 ?? riskRow.TP3 ?? signal.take_profit_3 ?? signal.tp3 ?? signal.TP3;

    let html = '<div class="signal ' + sideClass + '">';
    html += '<div class="signal-direction">' + escapeHtml(direction) + '</div>';
    html += '<div class="signal-meta">'
        + 'SCORE: <span class="signal-score">' + escapeHtml(formatScore(score)) + '</span>'
        + (strength ? '<span class="signal-strength">' + escapeHtml(strength) + '</span>' : '')
        + '<br>CONFLUENCE: ' + escapeHtml(formatScore(confluenceScore))
        + ' · CONFIRMATIONS: ' + escapeHtml(confirmations ?? "--")
        + (riskRow.rr_tp2 != null ? '<br>RR TP2: <span class="signal-score">1:' + escapeHtml(Number(riskRow.rr_tp2).toFixed(2)) + '</span>' : '')
        + (riskRow.stop_distance_atr != null ? ' · STOP: <span class="signal-score">' + escapeHtml(Number(riskRow.stop_distance_atr).toFixed(2)) + ' ATR</span>' : '')
        + '<br>STATUS: <span class="signal-strength">' + escapeHtml(event.status || "ACTIVE") + '</span>'
        + '</div>';
    html += '<div class="levels">';
    html += levelHtml("MARKET ENTRY", entry);
    html += levelHtml("SL", sl);
    html += levelHtml("TP1", tp1);
    html += levelHtml("TP2", tp2);
    html += levelHtml("TP3", tp3);
    html += '</div></div>';

    container.innerHTML = html;
    renderSignalOverlay([signal], [riskRow]);
    return true;
}

function hydrateServerSignalEvents(rows) {
    if (!Array.isArray(rows) || !rows.length) return;

    const existing = loadSignalEvents();
    const byKey = new Map(existing.map(event => [event.key, event]));

    for (const row of rows) {
        if (!row || String(row.event_type || '').toLowerCase() !== 'signal') continue;

        let payload = row.payload;
        if (typeof payload === 'string') {
            try { payload = JSON.parse(payload); } catch (_) { payload = null; }
        }
        if (!payload || typeof payload !== 'object') continue;
        // Never hydrate legacy V11 snapshots whose Entry/SL/TP may have been
        // created before the immutable MARKET execution snapshot existed.
        if (Number(payload.snapshot_version) !== 2) continue;

        const signal = payload.signal && typeof payload.signal === 'object'
            ? payload.signal
            : payload;
        const risk = payload.risk && typeof payload.risk === 'object'
            ? payload.risk
            : {};

        // Use the same stable browser identity for server hydration.
        // Server event_key intentionally excludes live market price.
        const key = signalIdentityKey(signal);
        if (!key) continue;

        const previous = byKey.get(key);
        byKey.set(key, {
            key,
            created_at: previous?.created_at || (signal.signal_time ?? signal.time ?? row.event_time ?? row.created_at ?? null),
            symbol: row.symbol || currentConfig?.symbol || '',
            timeframe: row.timeframe || currentConfig?.timeframe || '',
            signal: previous?.signal ? { ...previous.signal } : { ...signal },
            risk: previous?.risk ? { ...previous.risk } : { ...risk },
            status: previous?.status || row.status || 'ACTIVE',
            status_time: previous?.status_time || null,
            reached_tp: previous?.reached_tp || 0
        });
    }

    saveSignalEvents(Array.from(byKey.values()));
}

function renderSignals(signals, risk, rejectedSignals) {
    const container = document.getElementById("signalContainer");
    if (!container) return;

    const events = addOrRefreshSignalEvents(signals, risk);

    // The Signal Engine intentionally evaluates only the latest CLOSED candle.
    // Therefore a signal disappears from brain.signals on the next candle.
    // Event Memory must keep showing that signal until its outcome is known,
    // and a genuinely newer signal must immediately replace it.
    const currentSignals = (Array.isArray(signals) ? signals : [])
        .slice()
        .sort((a, b) => {
            const ta = tvChartTime(a?.signal_time ?? a?.time) ?? 0;
            const tb = tvChartTime(b?.signal_time ?? b?.time) ?? 0;
            if (tb !== ta) return tb - ta;
            return (Number(b?.priority_score ?? b?.score ?? 0) || 0)
                - (Number(a?.priority_score ?? a?.score ?? 0) || 0);
        });

    // FIRST PRIORITY — V9 signal is authoritative.
    // Never hide a signal that the V9 Signal Engine actually returned.
    // Event Memory is only a history/status layer; it is not allowed to
    // veto or filter the live V9 signal.
    if (currentSignals.length) {
        const freshSignal = currentSignals[0];
        const freshRisk = findMatchingRiskRow(freshSignal, Array.isArray(risk) ? risk : []);
        const freshKey = signalIdentityKey(freshSignal);
        const rememberedFresh = freshKey
            ? events.find(event => event.key === freshKey)
            : null;

        // A live V9 signal is authoritative, but if the same immutable signal
        // already exists in Event Memory, keep its evaluated status instead of
        // resetting it to NEW on every refresh.
        const directEvent = rememberedFresh
            ? {
                // IMPORTANT: an existing event is immutable. Do not merge
                // the current Brain refresh into its snapshot because the
                // Risk Engine may have a newer live execution price.
                ...rememberedFresh,
                signal: { ...(rememberedFresh.signal || {}) },
                risk: { ...(rememberedFresh.risk || {}) }
              }
            : {
                key: freshKey || `live|${Date.now()}`,
                created_at: freshSignal.signal_time ?? freshSignal.time ?? null,
                symbol: currentConfig?.symbol || "",
                timeframe: currentConfig?.timeframe || "",
                signal: { ...freshSignal },
                risk: { ...freshRisk },
                status: "NEW",
                status_time: null,
                reached_tp: 0
              };

        qxLockedSignalKey = directEvent.key;
        qxLockedSignalSnapshot = {
            signal: { ...directEvent.signal },
            risk: { ...directEvent.risk }
        };
        try {
            localStorage.setItem(signalEventStorageKey() + ".current", directEvent.key);
        } catch (_) {}
        renderPersistentEvent(directEvent);
        return;
    }

    // No V9 signal on this closed candle: now use Event Memory to continue
    // showing the most recent previously detected signal.
    let currentEvent = null;

    // Second priority: continue displaying the user's currently tracked event
    // when the current V9 candle has no new signal.
    let currentKey = null;
    if (!currentEvent) {
        try {
            currentKey = localStorage.getItem(signalEventStorageKey() + ".current") || null;
        } catch (_) {}
        currentEvent = currentKey
            ? events.find(event => event.key === currentKey) || null
            : null;
    }

    // Third priority: recover the newest remembered event if localStorage was
    // cleared or the previous key no longer exists.
    if (!currentEvent && events.length) {
        currentEvent = events
            .slice()
            .sort((a, b) => {
                const ta = tvChartTime(a?.created_at ?? a?.signal?.signal_time) ?? 0;
                const tb = tvChartTime(b?.created_at ?? b?.signal?.signal_time) ?? 0;
                return tb - ta;
            })[0] || null;
    }

    if (currentEvent) {
        currentKey = currentEvent.key;
        try {
            localStorage.setItem(signalEventStorageKey() + ".current", currentKey);
        } catch (_) {}

        qxLockedSignalKey = currentEvent.key;
        qxLockedSignalSnapshot = {
            signal: { ...(currentEvent.signal || {}) },
            risk: { ...(currentEvent.risk || {}) }
        };
        renderPersistentEvent(currentEvent);
        return;
    }

    const lastRemembered = events
        .slice()
        .sort((a, b) => {
            const ta = tvChartTime(a?.created_at ?? a?.signal?.signal_time) ?? 0;
            const tb = tvChartTime(b?.created_at ?? b?.signal?.signal_time) ?? 0;
            return tb - ta;
        })[0] || null;

    if (lastRemembered) {
        lastRemembered.status = lastRemembered.status || 'ACTIVE';
        container.innerHTML = '<div class="info">NO NEW SIGNAL ON THE LATEST CLOSED CANDLE<br>Showing the most recent remembered signal below.</div>';
        renderPersistentEvent(lastRemembered);
        return;
    }

    if (Array.isArray(rejectedSignals) && rejectedSignals.length) {
        const rejected = rejectedSignals[0] || {};
        const reason = String(rejected.risk_status || "REJECTED_RISK").replaceAll("_", " ");
        container.innerHTML = '<div class="info"><strong>NO EXECUTABLE SIGNAL</strong><br>V9 found a setup, but Risk Guard rejected it: ' + escapeHtml(reason) + '<br>Nothing is being presented as a MARKET trade until the risk geometry is valid.</div>';
        renderSignalOverlay([], []);
        return;
    }

    container.innerHTML = '<div class="info">NO CURRENT SIGNAL<br>V9 Signal Engine has not confirmed a BUY/SELL setup on the latest CLOSED candle.</div>';
    renderSignalOverlay([], []);
}


function formatScore(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "--";
    }

    const number = Number(value);

    if (!Number.isFinite(number)) {
        return "--";
    }

    return number.toFixed(1);
}


function levelHtml(
    label,
    value
) {

    return (
        '<div class="level">'
        + '<div class="level-label">'
        + escapeHtml(label)
        + '</div>'
        + '<div class="level-value">'
        + escapeHtml(
            formatPrice(value)
        )
        + '</div>'
        + '</div>'
    );
}



function tvChartTime(value) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "number" && Number.isFinite(value)) return Math.floor(value);
    const text = String(value).trim();
    if (/^\d+(\.\d+)?$/.test(text)) {
        const n = Number(text);
        return Number.isFinite(n) ? Math.floor(n) : null;
    }
    const parsed = Date.parse(text);
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : null;
}

function initINFXChart() {
    if (qxChart) return qxChart;
    const container = document.getElementById("tvChart");
    if (!container) return null;
    if (typeof LightweightCharts === "undefined" || typeof LightweightCharts.createChart !== "function") {
        throw new Error("Chart library could not be loaded.");
    }
    qxChart = LightweightCharts.createChart(container, {
        width: container.clientWidth || 900,
        height: container.clientHeight || 620,
        layout: { background: { type: "solid", color: "#080b12" }, textColor: "#aeb9ca" },
        grid: { vertLines: { color: "#151c28" }, horzLines: { color: "#151c28" } },
        // Keep the native chart crosshair enabled. Signal overlays do not
        // modify crosshair behavior.
        crosshair: {
            mode: 0,
            vertLine: { visible: true, labelVisible: true },
            horzLine: { visible: true, labelVisible: true },
        },
        rightPriceScale: { borderColor: "#273244", scaleMargins: { top: 0.08, bottom: 0.10 } },
        timeScale: { borderColor: "#273244", timeVisible: true, secondsVisible: false, rightOffset: 8, barSpacing: 8, minBarSpacing: 2 },
        handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: true },
        handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    });
    qxCandleSeries = qxChart.addCandlestickSeries({
        upColor: "#26a69a", downColor: "#ef5350", borderUpColor: "#26a69a", borderDownColor: "#ef5350",
        wickUpColor: "#26a69a", wickDownColor: "#ef5350", priceLineVisible: true, lastValueVisible: true,
    });
    qxVolumeSeries = qxChart.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "", lastValueVisible: false });
    qxVolumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    if (typeof ResizeObserver !== "undefined") {
        const ro = new ResizeObserver(() => {
            if (qxChart) {
                qxChart.resize(container.clientWidth, container.clientHeight);
                positionINFXSignalLabels();
            }
        });
        ro.observe(container);
        container.__qxResizeObserver = ro;
    }
    qxChart.timeScale().subscribeVisibleLogicalRangeChange(() => {
        positionINFXSignalLabels();
    });
    return qxChart;
}

function clearINFXSignalLines() {
    if (qxCandleSeries && typeof qxCandleSeries.setMarkers === "function") {
        qxCandleSeries.setMarkers([]);
    }

    if (qxChart) {
        for (const line of qxSignalLines) {
            try { qxChart.removeSeries(line); } catch (e) { console.warn("Could not remove signal line:", e); }
        }
    }
    for (const node of qxSignalLabelNodes) {
        try { node.remove(); } catch (_) {}
    }
    qxSignalLines = [];
    qxSignalLevels = [];
    qxSignalTime = null;
    qxSignalEndTime = null;
    qxSignalLabelNodes = [];
}

function quantumTimeframeSeconds() {
    const tf = String(currentConfig?.timeframe || "M5").toUpperCase().trim();
    const m = tf.match(/^(\d+)M$/);
    if (m) return Number(m[1]) * 60;
    const h = tf.match(/^(\d+)H$/);
    if (h) return Number(h[1]) * 3600;
    const d = tf.match(/^(\d+)D$/);
    if (d) return Number(d[1]) * 86400;
    return 300;
}

function signalLineLabelText(name, value) {
    return name + "  " + Number(value).toFixed(4);
}

function positionINFXSignalLabels() {
    if (!qxChart || !qxCandleSeries || !qxSignalLabelNodes.length || qxSignalEndTime === null) return;
    const x = qxChart.timeScale().timeToCoordinate(qxSignalEndTime);
    if (x === null || !Number.isFinite(x)) return;

    for (let i = 0; i < qxSignalLabelNodes.length; i++) {
        const node = qxSignalLabelNodes[i];
        const level = qxSignalLevels[i];
        if (!node || !level) continue;
        const y = qxCandleSeries.priceToCoordinate(level.value);
        if (y === null || !Number.isFinite(y)) {
            node.style.display = "none";
            continue;
        }
        node.style.display = "block";
        node.style.left = Math.round(x + 8) + "px";
        node.style.top = Math.round(y - 11) + "px";
    }
}

function createINFXSignalLabel(name, value, color) {
    const container = document.getElementById("tvChart");
    if (!container) return null;
    if (getComputedStyle(container).position === "static") container.style.position = "relative";

    const node = document.createElement("div");
    node.textContent = signalLineLabelText(name, value);
    node.style.position = "absolute";
    node.style.zIndex = "20";
    node.style.padding = "3px 7px";
    node.style.borderRadius = "4px";
    node.style.background = color;
    node.style.color = "#081018";
    node.style.font = "700 11px Arial, sans-serif";
    node.style.lineHeight = "16px";
    node.style.whiteSpace = "nowrap";
    node.style.pointerEvents = "none";
    node.style.boxShadow = "0 0 0 1px rgba(255,255,255,.12)";
    container.appendChild(node);
    return node;
}

function renderINFXCandles(candles, fitContent = false) {
    if (!Array.isArray(candles) || !candles.length) return;
    initINFXChart();
    if (!qxCandleSeries) return;
    const map = new Map();
    const volumes = new Map();
    for (const c of candles) {
        const time = tvChartTime(c.time), open = Number(c.open), high = Number(c.high), low = Number(c.low), close = Number(c.close);
        if (time === null || ![open,high,low,close].every(Number.isFinite)) continue;
        map.set(time, {time,open,high,low,close});
        const v = Number(c.volume ?? c.tick_volume);
        volumes.set(time, Number.isFinite(v) ? Math.max(0,v) : 0);
    }
    const data = Array.from(map.values()).sort((a,b)=>a.time-b.time);
    if (!data.length) return;
    qxChartCandles = data;
    // Brain.chart_candles contains CLOSED candles only.
    // The Signal Engine is evaluated on that same closed set, so this is
    // the exact candle on which the displayed signal is anchored.
    qxLastClosedCandleTime = data[data.length - 1].time;
    qxCandleSeries.setData(data);
    if (qxVolumeSeries) qxVolumeSeries.setData(data.map(c=>({time:c.time,value:volumes.get(c.time)||0,color:c.close>=c.open?"rgba(38,166,154,0.28)":"rgba(239,83,80,0.28)"})));
    if (fitContent || !qxChart.__qxInitialFitDone) {
        qxChart.timeScale().fitContent();
        qxChart.__qxInitialFitDone = true;
    }
}

function updateSignalLineGeometry() {
    if (!qxChart || !qxSignalLines.length || !qxSignalTime || !qxChartCandles.length) return;
    const lastTime = qxChartCandles[qxChartCandles.length - 1].time;
    if (!Number.isFinite(lastTime) || lastTime < qxSignalTime) return;

    qxSignalEndTime = Math.max(
        qxSignalTime,
        lastTime + quantumTimeframeSeconds()
    );

    for (let i = 0; i < qxSignalLines.length; i++) {
        const line = qxSignalLines[i];
        const level = qxSignalLevels[i];
        if (!line || !level || !Number.isFinite(level.value)) continue;
        line.setData([
            { time: qxSignalTime, value: level.value },
            { time: qxSignalEndTime, value: level.value },
        ]);
    }
    positionINFXSignalLabels();
}

function updateINFXChartLive(live) {
    if (!live || !live.ok || !Array.isArray(live.candles) || !live.candles.length) return;
    initINFXChart();
    if (!qxCandleSeries) return;

    const incoming = [];
    for (const c of live.candles) {
        const time = tvChartTime(c.time);
        const open = Number(c.open);
        const high = Number(c.high);
        const low = Number(c.low);
        const close = Number(c.close);
        if (time === null || ![open, high, low, close].every(Number.isFinite)) continue;
        incoming.push({
            time, open, high, low, close,
            volume: Number(c.volume ?? c.tick_volume)
        });
    }
    incoming.sort((a, b) => a.time - b.time);
    if (!incoming.length) return;

    // TradingView /api/live returns a small rolling window (not only the
    // forming candle). Lightweight Charts update() is append/update-only: it
    // rejects a candle older than the current last bar. Therefore NEVER feed
    // older candles back through update(); merge them into our local state and
    // update only the current/newest bar.
    const known = new Map(qxChartCandles.map(c => [c.time, c]));
    const currentLastTime = qxChartCandles.length
        ? qxChartCandles[qxChartCandles.length - 1].time
        : null;

    for (const c of incoming) {
        const candle = {time:c.time, open:c.open, high:c.high, low:c.low, close:c.close};
        known.set(c.time, candle);

        // Skip stale bars. Equal = update the existing live candle; greater =
        // append the newly opened TradingView candle.
        if (currentLastTime === null || c.time >= currentLastTime) {
            qxCandleSeries.update(candle);
            if (qxVolumeSeries && Number.isFinite(c.volume)) {
                qxVolumeSeries.update({
                    time:c.time,
                    value:Math.max(0, c.volume),
                    color:c.close >= c.open ? "rgba(38,166,154,0.28)" : "rgba(239,83,80,0.28)"
                });
            }
        }
    }

    qxChartCandles = Array.from(known.values())
        .sort((a,b)=>a.time-b.time)
        .slice(-300);

    updateSignalLineGeometry();
}

function renderSignalOverlay(signals, risk) {
    clearINFXSignalLines();
    qxSignalLevels = [];
    qxSignalTime = null;

    if (!signals || !signals.length || !qxCandleSeries) return;

    const first = signals[0] || {};
    const riskRow = risk[0] || {};

    // DISPLAY ONLY:
    // The signal must be anchored to the exact candle on which the
    // Signal Engine confirms it. zone_ready_time is historical setup
    // information and must NOT be used as the visual signal origin.
    // This changes only the chart drawing; Signal Engine/Risk Engine
    // calculations remain untouched.
    const requestedAnchor = tvChartTime(
        first.signal_time ||
        first.time
    );

    // Snap to the exact candle already rendered on this chart. This makes
    // the line start on a real candle time, rather than between candles.
    if (Number.isFinite(requestedAnchor) && qxChartCandles.length) {
        let bestTime = null;
        let bestDistance = Infinity;
        for (const candle of qxChartCandles) {
            const t = Number(candle.time);
            if (!Number.isFinite(t)) continue;
            const distance = Math.abs(t - requestedAnchor);
            if (distance < bestDistance) {
                bestDistance = distance;
                bestTime = t;
            }
        }
        if (bestTime !== null && bestDistance <= quantumTimeframeSeconds()) {
            qxSignalTime = bestTime;
        }
    }

    if (qxSignalTime === null) return;

    const levels = [
        {name:"MARKET ENTRY",value:riskRow.entry ?? riskRow.entry_price ?? riskRow.market_entry ?? first.entry ?? first.entry_price ?? first.market_entry,color:"#60a5fa"},
        {name:"SL",value:riskRow.stop_loss ?? first.stop_loss,color:"#fb7185"},
        {name:"TP1",value:riskRow.take_profit_1 ?? first.take_profit_1,color:"#4ade80"},
        {name:"TP2",value:riskRow.take_profit_2 ?? first.take_profit_2,color:"#4ade80"},
        {name:"TP3",value:riskRow.take_profit_3 ?? first.take_profit_3,color:"#4ade80"},
    ];

    const lastTime = qxChartCandles.length
        ? Number(qxChartCandles[qxChartCandles.length - 1].time)
        : qxSignalTime;

    if (!Number.isFinite(lastTime)) return;

    qxSignalEndTime = Math.max(
        qxSignalTime,
        lastTime + quantumTimeframeSeconds()
    );

    for (const item of levels) {
        const value = Number(item.value);
        if (!Number.isFinite(value)) continue;

        const series = qxChart.addLineSeries({
            color:item.color,
            lineWidth:2,
            lineStyle:0,
            priceLineVisible:false,
            lastValueVisible:false,
            crosshairMarkerVisible:false,
        });

        // The first point is the actual signal/setup candle. The second
        // point is the current chart edge. No vertical connector and no
        // extension to the left of the signal candle.
        series.setData([
            {time:qxSignalTime, value},
            {time:qxSignalEndTime, value},
        ]);

        // One visible origin marker only: ENTRY identifies where the signal
        // graphic begins without adding any extra vertical line.
        if (item.name === "MARKET ENTRY" && qxCandleSeries && typeof qxCandleSeries.setMarkers === "function") {
            qxCandleSeries.setMarkers([{
                time: qxSignalTime,
                position: "inBar",
                color: item.color,
                shape: "circle",
                text: "",
            }]);
        }

        qxSignalLines.push(series);
        qxSignalLevels.push({name:item.name,value,color:item.color});
        const label = createINFXSignalLabel(item.name, value, item.color);
        if (label) qxSignalLabelNodes.push(label);
    }

    positionINFXSignalLabels();
}

async function refreshLive(expectedGeneration = qxMarketGeneration) {

    try {

        const response = await fetch(
            "/api/live",
            {
                cache: "no-store"
            }
        );

        const data =
            await response.json();

        if (expectedGeneration !== qxMarketGeneration) return;

        if (!data.ok) {

            // Keep the last visible market snapshot during a transient
            // TradingView websocket failure. Never blank the live feed.
            setStatus(
                false,
                "MARKET DATA UNAVAILABLE"
            );

            return;
        }

        renderLive(
            data.live
        );

    } catch (error) {

        setStatus(
            false,
            "Connection error"
        );
    }
}


async function refreshBrain(
    force = false,
    expectedGeneration = qxMarketGeneration
) {

    try {

        const endpoint =
            force
                ? "/api/state?force=1"
                : "/api/state";

        const response =
            await fetch(
                endpoint,
                {
                    cache: "no-store"
                }
            );

        const state =
            await response.json();

        if (expectedGeneration !== qxMarketGeneration) return;

        if (!state.ok) {
            throw new Error(
                state.error
            );
        }

        if (state.config) {

            currentConfig =
                state.config;

            updateChartLabel();

        }

        window.__quantumBrainState =
            state;

        if (state.data_status === "STALE") {
            showError("DATA FEED STALE: " + String(state.data_error || "TradingView data temporarily unavailable"));
        }

        // SQLite Event Memory is the persistent source for old signals.
        // Seed browser memory before rendering so refreshes do not erase the
        // signal history/status view.
        hydrateServerSignalEvents(state.event_memory || []);

        const chartCandles =
            state.brain
            && Array.isArray(state.brain.chart_candles)
                ? state.brain.chart_candles
                : [];

        renderINFXCandles(
            chartCandles,
            false
        );

        // Put the current/forming TradingView candle into the chart BEFORE
        // creating the signal lines. The lines can therefore run from the
        // exact closed signal candle to the actual live candle.
        if (state.live) {
            renderLive(
                state.live
            );
        }

        renderBrain(
            state
        );

        // Journal UI has been removed; do not call the deleted loadJournal() function.
        clearError();

    } catch (error) {

        showError(
            "Brain error: "
            + error.message
        );

        setStatus(
            false,
            "Brain error"
        );
    }
}


async function startDashboard() {

    await loadConfig();

    await refreshBrain(
        true
    );

    await refreshLive();

    /*
     * LIVE:
     * Every 1 second.
     *
     * This does NOT reload TradingView.
     * This does NOT run Brain.
     */

    liveTimer =
        setInterval(
            refreshLive,
            1000
        );

    /*
     * BRAIN:
     * Every 15 seconds.
     */

    brainTimer =
        setInterval(
            function() {
                refreshBrain(false, qxMarketGeneration);
            },
            15000
        );
}


window.addEventListener(
    "load",
    startDashboard
);

</script>

</body>
</html>
"""


# ============================================================
# HTTP SERVER
# ============================================================

class INFXHandler(
    BaseHTTPRequestHandler
):
    """
    HTTP request handler.
    """

    def log_message(
        self,
        format_string,
        *args
    ):
        """
        Keep terminal output clean.
        """

        return

    # --------------------------------------------------------
    # RESPONSE
    # --------------------------------------------------------

    def send_json(
        self,
        data,
        status=200,
    ):
        payload = json_response(
            data
        )

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )

        self.send_header(
            "Cache-Control",
            "no-store, no-cache, must-revalidate",
        )

        self.send_header(
            "Pragma",
            "no-cache",
        )

        self.send_header(
            "Content-Length",
            str(len(payload)),
        )

        self.end_headers()

        self.wfile.write(
            payload
        )

    # --------------------------------------------------------
    # BODY
    # --------------------------------------------------------

    def read_json_body(self):
        length = int(
            self.headers.get(
                "Content-Length",
                "0",
            )
        )

        if length <= 0:
            return {}

        raw = self.rfile.read(
            length
        )

        if not raw:
            return {}

        try:

            return json.loads(
                raw.decode("utf-8")
            )

        except Exception:

            return {}

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def do_GET(self):

        parsed = urlparse(
            self.path
        )

        path = parsed.path

        # ----------------------------------------------------
        # ROOT
        # ----------------------------------------------------

        if path == "/":

            payload = HTML_PAGE.encode(
                "utf-8"
            )

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8",
            )

            self.send_header(
                "Cache-Control",
                "no-store",
            )

            self.send_header(
                "Content-Length",
                str(len(payload)),
            )

            self.end_headers()

            self.wfile.write(
                payload
            )

            return

        # ----------------------------------------------------
        # CONFIG
        # ----------------------------------------------------

        if path == "/api/config":
            try:
                config = get_current_config()
                merged_symbols = list(
                    dict.fromkeys(
                        [config["symbol"]]
                        + DEFAULT_SYMBOL_CHOICES
                    )
                )

                self.send_json(
                    {
                        "ok": True,
                        "config": config,
                        "symbols": merged_symbols,
                        "timeframes": DEFAULT_TIMEFRAME_CHOICES,
                        "mode": "TRADINGVIEW_ANALYSIS_ONLY",
                    }
                )

            except Exception as exc:
                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=500,
                )

            return

        # ----------------------------------------------------
        # LIVE
        # ----------------------------------------------------

        if path == "/api/live":

            try:

                live = get_live_market()

                self.send_json(
                    {
                        "ok": live.get(
                            "ok",
                            False,
                        ),
                        "live": live,
                    }
                )

            except Exception as exc:

                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=500,
                )

            return

        # ----------------------------------------------------
        # STATE
        # ----------------------------------------------------

        if path == "/api/state":

            query = parse_qs(
                parsed.query
            )

            force = (
                query.get(
                    "force",
                    ["0"],
                )[0]
                == "1"
            )

            try:

                state = build_state(
                    force=force
                )

                self.send_json(
                    state
                )

            except Exception as exc:

                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=500,
                )

            return

        # ----------------------------------------------------
        # EVENT MEMORY
        # ----------------------------------------------------

        if path == "/api/events":
            try:
                config = get_current_config()
                rows = get_event_memory(
                    symbol=config["symbol"],
                    timeframe=config["timeframe"],
                    limit=100,
                )
                self.send_json({
                    "ok": True,
                    "symbol": config["symbol"],
                    "timeframe": config["timeframe"],
                    "rows": rows,
                })
            except Exception as exc:
                self.send_json({
                    "ok": False,
                    "error": str(exc),
                }, status=500)
            return

        # ----------------------------------------------------
        # JOURNAL
        # ----------------------------------------------------

        if path == "/api/journal":

            try:

                rows = get_journal()

                self.send_json(
                    {
                        "ok": True,
                        "rows": rows,
                    }
                )

            except Exception as exc:

                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=500,
                )

            return

        # ----------------------------------------------------
        # HEALTH
        # ----------------------------------------------------

        if path == "/api/health":
            self.send_json({
                "ok": True,
                "analysis_only": True,
                "source": "TradingView LIVE",
                "mt5_connected": False,
                "broker_connected": False,
                "config": get_current_config(),
            })
            return

        # ----------------------------------------------------
        # 404
        # ----------------------------------------------------

        self.send_json(
            {
                "ok": False,
                "error": "Not found.",
            },
            status=404,
        )


    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    def do_POST(self):

        parsed = urlparse(
            self.path
        )

        path = parsed.path

        if path == "/api/config":

            data = self.read_json_body()

            try:

                config = set_market_config(
                    symbol=data.get(
                        "symbol"
                    ),
                    timeframe_name=data.get(
                        "timeframe"
                    ),
                )

                self.send_json(
                    {
                        "ok": True,
                        "config": config,
                    }
                )

            except Exception as exc:

                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=400,
                )

            return

        if path == "/api/chat":

            data = self.read_json_body()
            message = data.get(
                "message",
                "",
            )

            try:
                answer = chatbot_answer(
                    message
                )
                self.send_json(
                    {
                        "ok": True,
                        "answer": answer,
                    }
                )
            except Exception as exc:
                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=500,
                )
            return

        if path == "/api/journal":

            data = self.read_json_body()

            try:
                add_journal_entry(
                    data
                )
                self.send_json(
                    {
                        "ok": True,
                    }
                )
            except Exception as exc:
                self.send_json(
                    {
                        "ok": False,
                        "error": str(exc),
                    },
                    status=500,
                )
            return

        self.send_json(
            {
                "ok": False,
                "error": "Not found.",
            },
            status=404,
        )



def find_free_port(start_port, attempts=20):
    """Return the first available localhost TCP port."""
    for candidate in range(int(start_port), int(start_port) + int(attempts)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((HOST, candidate))
            return candidate
        except OSError:
            pass
        finally:
            sock.close()
    raise OSError(
        f"No free localhost port found in range "
        f"{start_port}-{start_port + attempts - 1}"
    )

# ============================================================
# MAIN
# ============================================================

def main():
    """Start the TradingView analysis-only dashboard. MT5 is disabled."""
    global _server, PORT

    print()
    print("=" * 60)
    print("              INFX")
    print("       TRADINGVIEW ANALYSIS ENGINE")
    print("=" * 60)
    print()
    print("[1] Initializing journal...")

    try:
        init_journal()
        init_event_memory()
        print("[OK] Journal initialized.")
        print("[OK] Event Memory initialized.")
    except Exception as exc:
        print("[WARNING] Journal:", exc)

    print("[2] TradingView mode...")
    print("[OK] MetaTrader: NOT USED")
    print("[OK] Auto trading: DISABLED")
    print("[OK] Analysis mode: SIGNAL ONLY")
    print("[OK] Chart: TradingView LIVE")
    print("[OK] Brain data: TradingView LIVE")
    print("[OK] Event Memory: ENABLED (SQLite)")

    print("[3] Default market...")
    print("[OK] Symbol:", CURRENT_SYMBOL)
    print("[OK] Timeframe:", CURRENT_TIMEFRAME_NAME)

    print("[4] Starting HTTP server...")

    try:
        requested_port = PORT
        PORT = find_free_port(
            requested_port,
            attempts=20,
        )

        if PORT != requested_port:
            print(
                f"[OK] Port {requested_port} is busy; "
                f"using {PORT}."
            )

        _server = ThreadingHTTPServer(
            (HOST, PORT),
            INFXHandler,
        )

        dashboard_url = f"http://{HOST}:{PORT}"

        print()
        print("=" * 60)
        print("Dashboard:")
        print(dashboard_url)
        print()
        print("TradingView: LIVE")
        print("Symbol selector: ENABLED")
        print("Timeframe selector: ENABLED")
        print("MT5: DISABLED")
        print("Auto trading: DISABLED")
        print("=" * 60)
        print()
        print("[READY] INFX is running.")
        print("[READY] Opening TradingView dashboard...")

        threading.Timer(
            0.8,
            lambda: webbrowser.open(
                dashboard_url,
                new=2,
            ),
        ).start()

        _server.serve_forever()

    except KeyboardInterrupt:
        print()
        print("Stopping INFX...")

    except OSError as exc:
        print()
        print("[SERVER ERROR]", exc)

    except Exception as exc:
        print()
        print("[ERROR]", exc)

    finally:
        if _server is not None:
            try:
                _server.server_close()
            except Exception:
                pass
            _server = None

        print("[OK] INFX stopped.")


# ============================================================
# DIRECT EXECUTION
# ============================================================

if __name__ == "__main__":
    main()
