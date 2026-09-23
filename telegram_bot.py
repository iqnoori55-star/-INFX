"""
INFX Telegram Bot + Mini App launcher.

- Runs the existing app.py engine in the same process.
- Registers users who send /start.
- Sends a Mini App button when INFX_WEBAPP_URL is configured.
- Polls INFX /api/events and sends each new signal event once.
- No trading logic is implemented here; app.py remains the source of truth.

Environment variables:
    TELEGRAM_BOT_TOKEN  = BotFather token
    INFX_WEBAPP_URL     = public HTTPS URL of the INFX Mini App
    INFX_HOST           = 127.0.0.1 (default)
    INFX_PORT           = 8877 (default; app.py may move to a free port)
"""

import json
import os
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
import app

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBAPP_URL = os.getenv("INFX_WEBAPP_URL", "").strip()
POLL_SECONDS = 2.0
EVENT_POLL_SECONDS = 3.0
SUBSCRIBERS_FILE = Path("telegram_subscribers.json")
SEEN_FILE = Path("telegram_seen_events.json")
EVENT_STATE_FILE = Path("telegram_event_state.json")
CURRENT_SIGNAL_FILE = Path("telegram_current_signal.json")

_subscribers_lock = threading.RLock()
_subscribers = set()
_seen_lock = threading.RLock()
_seen = set()
_event_state = {}


def _load_json_set(path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return set(str(x) for x in data)
    except Exception:
        pass
    return set()


def _save_json_set(path, values):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(sorted(values), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_state():
    global _subscribers, _seen, _event_state
    with _subscribers_lock:
        _subscribers = _load_json_set(SUBSCRIBERS_FILE)
    with _seen_lock:
        _seen = _load_json_set(SEEN_FILE)
    try:
        data = json.loads(EVENT_STATE_FILE.read_text(encoding="utf-8"))
        _event_state = data if isinstance(data, dict) else {}
    except Exception:
        _event_state = {}


def api(method, payload=None):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    with urllib.request.urlopen(request, timeout=35) as response:
        return json.loads(response.read().decode("utf-8"))


def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": int(chat_id), "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return api("sendMessage", payload)


def mini_app_markup():
    if not WEBAPP_URL:
        return None
    return {
        "keyboard": [[{
            "text": "🚀 Open INFX",
            "web_app": {"url": WEBAPP_URL},
        }]],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def welcome_text():
    if WEBAPP_URL:
        return (
            "<b>INFX</b>\n\n"
            "TradingView LIVE Analysis\n"
            "Signal notifications are enabled.\n\n"
            "Tap <b>🚀 Open INFX</b> to open the INFX Mini App."
        )
    return (
        "<b>INFX</b>\n\n"
        "TradingView LIVE Analysis\n\n"
        "The bot is connected. The Mini App URL is not configured yet."
    )


def register(chat_id):
    with _subscribers_lock:
        _subscribers.add(str(chat_id))
        _save_json_set(SUBSCRIBERS_FILE, _subscribers)


def remove_subscriber(chat_id):
    with _subscribers_lock:
        _subscribers.discard(str(chat_id))
        _save_json_set(SUBSCRIBERS_FILE, _subscribers)


def handle_update(update):
    message = update.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return

    text = str(message.get("text") or "").strip()
    command = text.split()[0].lower() if text else ""

    if command.startswith("/start"):
        register(chat_id)
        send_message(chat_id, welcome_text(), mini_app_markup())
        return

    if command == "/stop":
        remove_subscriber(chat_id)
        send_message(chat_id, "🔕 INFX signal notifications are disabled.\nSend /start to enable them again.")
        return

    if command == "/signals":
        try:
            cfg = app.get_current_config()
            rows = app.get_event_memory(cfg["symbol"], cfg["timeframe"], limit=100)
            row, key, _ = _select_tracked_event(cfg, rows)
            if row is None:
                send_message(chat_id, "No current INFX signal yet.")
                return
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            send_message(
                chat_id,
                format_signal(payload, row.get("status")),
            )
        except Exception as exc:
            send_message(chat_id, f"INFX is temporarily unavailable.\n<code>{escape(str(exc))}</code>")


def escape(value):
    return (str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def num(value):
    try:
        x = float(value)
        if x != x:
            return "-"
        return f"{x:.2f}"
    except Exception:
        return "-"


def format_signal(payload, status=None, event_time=None):
    risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
    direction = str(payload.get("signal") or payload.get("direction") or "").upper()
    if direction in ("BULLISH", "LONG"):
        direction = "BUY"
    if direction in ("BEARISH", "SHORT"):
        direction = "SELL"
    icon = "🟢" if direction == "BUY" else "🔴" if direction == "SELL" else "⚪"

    entry = risk.get("entry") or risk.get("entry_price") or risk.get("market_entry") or payload.get("entry") or payload.get("entry_price")
    sl = risk.get("stop_loss") or risk.get("sl") or payload.get("stop_loss")
    tp1 = risk.get("take_profit_1") or risk.get("tp1") or payload.get("take_profit_1")
    tp2 = risk.get("take_profit_2") or risk.get("tp2") or payload.get("take_profit_2")
    tp3 = risk.get("take_profit_3") or risk.get("tp3") or payload.get("take_profit_3")

    symbol = escape(payload.get("symbol") or app.CURRENT_SYMBOL)
    timeframe = escape(payload.get("timeframe") or app.CURRENT_TIMEFRAME_NAME)
    status_text = escape(status or payload.get("status") or "NEW")

    return (
        f"{icon} <b>INFX {direction or 'SIGNAL'}</b>\n"
        f"<b>{symbol} • {timeframe}</b>\n\n"
        f"Entry: <code>{num(entry)}</code>\n"
        f"SL: <code>{num(sl)}</code>\n"
        f"TP1: <code>{num(tp1)}</code>\n"
        f"TP2: <code>{num(tp2)}</code>\n"
        f"TP3: <code>{num(tp3)}</code>\n\n"
        f"Status: <b>{status_text}</b>"
    )

def event_key(row):
    value = row.get("event_key") or row.get("id")
    return str(value)


def _signal_rank(row):
    """Match INFX's live signal ordering: newest signal_time, then priority."""
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    signal_time = (
        payload.get("signal_time")
        or payload.get("time")
        or row.get("event_time")
        or row.get("created_at")
    )
    try:
        parsed = app.pd.to_datetime(signal_time, errors="coerce")
        timestamp = parsed.timestamp() if not app.pd.isna(parsed) else 0.0
    except Exception:
        timestamp = 0.0

    try:
        priority = float(
            payload.get("priority_score")
            or payload.get("score")
            or 0.0
        )
    except Exception:
        priority = 0.0

    try:
        row_id = int(row.get("id") or 0)
    except Exception:
        row_id = 0

    return (timestamp, priority, row_id)


def _brain_signal_rank(signal):
    """Same ordering used by the INFX dashboard for current brain signals."""
    signal_time = signal.get("signal_time") or signal.get("time")
    try:
        parsed = app.pd.to_datetime(signal_time, errors="coerce")
        timestamp = parsed.timestamp() if not app.pd.isna(parsed) else 0.0
    except Exception:
        timestamp = 0.0
    try:
        priority = float(signal.get("priority_score") or signal.get("score") or 0.0)
    except Exception:
        priority = 0.0
    return (timestamp, priority)


def _save_current_key(key):
    tmp = CURRENT_SIGNAL_FILE.with_suffix(CURRENT_SIGNAL_FILE.suffix + ".tmp")
    tmp.write_text(
        json.dumps({"event_key": str(key)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(CURRENT_SIGNAL_FILE)


def _load_current_key():
    try:
        data = json.loads(CURRENT_SIGNAL_FILE.read_text(encoding="utf-8"))
        key = data.get("event_key") if isinstance(data, dict) else None
        return str(key) if key else None
    except Exception:
        return None


def _brain_signal_key(signal, symbol, timeframe):
    """Build the same persistent signal identity used by app.py Event Memory."""
    parts = [
        str(symbol).upper(),
        str(timeframe).upper(),
        "signal",
        str(signal.get("zone_time", "")),
        str(signal.get("zone_ready_time", "")),
        str(signal.get("direction", "") or "").upper(),
        str(signal.get("signal", "") or "").upper(),
    ]
    return "|".join(parts)


def _current_brain_signal(cfg):
    """Read the actual current V9 brain signal, not the historical Event Memory."""
    try:
        state = app.build_state()
        brain = state.get("brain") if isinstance(state, dict) else None
        signals = brain.get("signals", []) if isinstance(brain, dict) else []
    except Exception:
        return None

    if hasattr(signals, "to_dict"):
        signals = signals.to_dict(orient="records")
    if not isinstance(signals, list):
        return None

    signals = [s for s in signals if isinstance(s, dict)]
    if not signals:
        return None
    return max(signals, key=_brain_signal_rank)


def _find_event(rows, key):
    if not key:
        return None
    for row in rows:
        if event_key(row) == key and row.get("event_type") == "signal":
            return row
    return None


def _select_tracked_event(cfg, rows):
    """Keep the same signal until INFX produces a genuinely newer signal."""
    current_key = _load_current_key()
    current = _find_event(rows, current_key)

    # A real signal from the current V9 brain is authoritative. If it has a
    # different stable identity, it is a genuinely newer setup and replaces
    # the tracked event. If there is no current brain signal, KEEP the existing
    # tracked event instead of selecting another historical Event Memory row.
    brain_signal = _current_brain_signal(cfg)
    if brain_signal is not None:
        brain_key = _brain_signal_key(
            brain_signal,
            cfg["symbol"],
            cfg["timeframe"],
        )
        brain_event = _find_event(rows, brain_key)
        if brain_event is not None:
            if current_key != brain_key:
                _save_current_key(brain_key)
            return brain_event, brain_key, current_key != brain_key

    if current is not None:
        return current, current_key, False

    # First startup / lost state: use the dashboard's remembered fallback.
    # This happens only when there is no tracked key available.
    if rows:
        signals = [r for r in rows if r.get("event_type") == "signal"]
        if signals:
            fallback = max(signals, key=_signal_rank)
            fallback_key = event_key(fallback)
            if fallback_key:
                _save_current_key(fallback_key)
            return fallback, fallback_key, True

    return None, None, False


def notify_new_events():
    global _event_state
    try:
        cfg = app.get_current_config()
        rows = app.get_event_memory(
            cfg["symbol"],
            cfg["timeframe"],
            limit=100,
        )
    except Exception:
        return

    row, key, became_new_current = _select_tracked_event(cfg, rows)
    if row is None or not key:
        return

    status = str(row.get("status") or "NEW").upper()

    with _seen_lock:
        previous = _event_state.get(key)

        if previous is None:
            # If this is the first time this tracked event is seen by the
            # current bot state, send it once. Existing legacy seen state is
            # respected so deployment does not duplicate an old alert.
            if key in _seen and not became_new_current:
                _event_state[key] = status
                return
            _event_state[key] = status
            _seen.add(key)
            _save_json_set(SEEN_FILE, _seen)
            EVENT_STATE_FILE.write_text(
                json.dumps(_event_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            should_send = True
        elif previous != status:
            _event_state[key] = status
            EVENT_STATE_FILE.write_text(
                json.dumps(_event_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            should_send = True
        elif became_new_current:
            # The tracked event changed to a genuinely new V9 signal. Even if
            # its key was seen in an older run, do not recycle that old state
            # into a new alert; only a fresh current signal should trigger it.
            should_send = True
        else:
            should_send = False

    if not should_send:
        return

    payload = row.get("payload")
    if not isinstance(payload, dict):
        return
    message = format_signal(payload, status)

    with _subscribers_lock:
        subscribers = list(_subscribers)
    for chat_id in subscribers:
        try:
            send_message(chat_id, message)
        except Exception as exc:
            if "bot was blocked" in str(exc).lower() or "chat not found" in str(exc).lower():
                remove_subscriber(chat_id)

def event_loop():
    while True:
        try:
            notify_new_events()
        except Exception:
            pass
        time.sleep(EVENT_POLL_SECONDS)


def bot_loop():
    offset = None
    while True:
        try:
            payload = {"timeout": 25, "allowed_updates": ["message"]}
            if offset is not None:
                payload["offset"] = offset
            result = api("getUpdates", payload)
            for update in result.get("result", []):
                offset = int(update["update_id"]) + 1
                try:
                    handle_update(update)
                except Exception as exc:
                    print("[Telegram update error]", exc)
        except Exception as exc:
            print("[Telegram polling error]", exc)
            time.sleep(3)


def main():
    if not TOKEN:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN before starting telegram_bot.py")

    load_state()

    # Start the existing INFX engine unchanged.
    server_thread = threading.Thread(target=app.main, daemon=True)
    server_thread.start()

    # Wait until app.py has selected its actual port.
    deadline = time.time() + 20
    while time.time() < deadline:
        if getattr(app, "_server", None) is not None:
            break
        time.sleep(0.2)

    try:
        api("setMyCommands", {
            "commands": [
                {"command": "start", "description": "Open INFX and enable signal notifications"},
                {"command": "signals", "description": "Show recent INFX signals"},
                {"command": "stop", "description": "Disable signal notifications"},
            ]
        })
    except Exception as exc:
        print("[Telegram setup warning]", exc)

    threading.Thread(target=event_loop, daemon=True).start()
    bot_loop()


if __name__ == "__main__":
    main()
