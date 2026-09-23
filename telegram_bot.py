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
            rows = app.get_event_memory(cfg["symbol"], cfg["timeframe"], limit=5)
            signals = [r for r in rows if r.get("event_type") == "signal"][:5]
            if not signals:
                send_message(chat_id, "No remembered INFX signals yet.")
                return
            lines = ["<b>Recent INFX Signals</b>"]
            for row in signals:
                p = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                lines.append(format_signal(p, row.get("status")))
            send_message(chat_id, "\n\n".join(lines))
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


def notify_new_events():
    global _event_state
    try:
        cfg = app.get_current_config()
        rows = app.get_event_memory(cfg["symbol"], cfg["timeframe"], limit=100)
    except Exception:
        return

    signals = [r for r in rows if r.get("event_type") == "signal"]
    for row in reversed(signals):
        key = event_key(row)
        if not key:
            continue
        status = str(row.get("status") or "NEW").upper()

        with _seen_lock:
            previous = _event_state.get(key)
            if previous is None:
                # Backward compatibility: events already present in the old
                # seen set must not be sent again as NEW after deployment.
                if key in _seen:
                    _event_state[key] = status
                    continue
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
            else:
                should_send = False

        if not should_send:
            continue

        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
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
