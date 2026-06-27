# =============================================================================
# app.py
# Flask Web Dashboard — Start/Stop controls + live metrics
# Import and run this file; it imports state and start_bot_thread from
# delta_bot_ethusd.py
# =============================================================================

import logging
import threading

from flask import Flask, jsonify, render_template_string

from delta_bot_ethusd import state, start_bot_thread

log = logging.getLogger(__name__)

app = Flask(__name__)
app = Flask(__name__)

# Auto-start bot on Gunicorn startup
if not getattr(app, "_bot_started", False):
    app._bot_started = True

    with state.lock:
        state.running = True
        state.bot_status = "Starting"

    threading.Thread(
        target=start_bot_thread,
        daemon=True,
        name="BotThread"
    ).start()

    log.info("===== BOT AUTO STARTED =====")
﻿# =============================================================================
# delta_bot_ethusd.py
# ETHUSD Volume Delta Trading Bot — Full Bot Logic
# =============================================================================

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests
import websockets

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
REST_BASE_URL    = "https://api.india.delta.exchange"
WS_PUBLIC_URL    = "wss://public-socket.india.delta.exchange"
WS_PRIVATE_URL   = "wss://socket.india.delta.exchange"

SYMBOL               = "ETHUSD"
PRODUCT_ID           = 3136
CONTRACT_VALUE       = 0.01        # 1 contract = 0.01 ETH

TARGET_LEVERAGE      = 150
CAPITAL_USAGE_PERCENT = 80
MAX_CONTRACTS        = 162683

LOSS_LIMIT_PERCENT   = 20.0        # Emergency close if unrealized loss > 20% of margin
DAILY_DRAWDOWN_LIMIT = 10.0        # Halt trading if daily drawdown > 10%

WS_RECONNECT_DELAY   = 5           # Seconds between reconnect attempts
WS_MAX_RECONNECT     = 20          # Max reconnect attempts before critical shutdown

API_KEY    = os.environ.get("DELTA_API_KEY", "")
API_SECRET = os.environ.get("DELTA_API_SECRET", "")

LOG_FILE = "delta_bot_ethusd.log"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────
class BotState:
    def __init__(self):
        self.lock = threading.Lock()

        # Control flags
        self.running         = False
        self.trading_enabled = True

        # Market data
        self.mark_price      = 0.0
        self.current_delta   = 0.0
        self.previous_delta  = 0.0

        # 1-minute bar accumulators
        self.bar_open_time   = 0
        self.bar_buy_volume  = 0.0
        self.bar_sell_volume = 0.0

        # Account
        self.available_balance = 0.0
        self.used_margin       = 0.0
        self.current_leverage  = 0
        self.calculated_size   = 0

        # Open position
        self.position_side   = None    # "long" | "short" | None
        self.position_size   = 0
        self.entry_price     = 0.0
        self.position_margin = 0.0
        self.unrealized_pnl  = 0.0
        self.loss_vs_margin_pct = 0.0

        # Daily drawdown tracking
        self.day_start_balance  = 0.0
        self.daily_drawdown_pct = 0.0
        self.current_utc_day    = ""

        # Trade history (last 100 trades)
        self.trade_history = deque(maxlen=100)

        # Dashboard display
        self.bot_status  = "Stopped"
        self.last_error  = ""


state = BotState()


# ─────────────────────────────────────────────
# AUTHENTICATION HELPERS
# ─────────────────────────────────────────────
def generate_signature(secret: str, message: str) -> str:
    return hmac.new(
        bytes(secret, "utf-8"),
        bytes(message, "utf-8"),
        hashlib.sha256
    ).hexdigest()


def get_auth_headers(method: str, path: str, query: str = "", body: str = "") -> dict:
    timestamp = str(int(time.time()))
    message   = method + timestamp + path + query + body
    signature = generate_signature(API_SECRET, message)
    return {
        "api-key":      API_KEY,
        "timestamp":    timestamp,
        "signature":    signature,
        "Content-Type": "application/json",
        "User-Agent":   "delta-bot-ethusd"
    }


# ─────────────────────────────────────────────
# REST API HELPERS
# ─────────────────────────────────────────────
def rest_get(path: str, params: dict = None) -> dict:
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    headers = get_auth_headers("GET", path, query)
    url     = REST_BASE_URL + path + query
    resp    = requests.get(url, headers=headers, timeout=(5, 30))
    resp.raise_for_status()
    return resp.json()


def rest_post(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    headers  = get_auth_headers("POST", path, "", body_str)
    url      = REST_BASE_URL + path
    resp     = requests.post(url, headers=headers, data=body_str, timeout=(5, 30))
    resp.raise_for_status()
    return resp.json()


def rest_delete(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    headers  = get_auth_headers("DELETE", path, "", body_str)
    url      = REST_BASE_URL + path
    resp     = requests.delete(url, headers=headers, data=body_str, timeout=(5, 30))
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────
# LEVERAGE MANAGEMENT
# ─────────────────────────────────────────────
def get_current_leverage() -> int:
    try:
        path = f"/v2/products/{PRODUCT_ID}/orders/leverage"
        data = rest_get(path)
        lev  = int(float(data["result"]["leverage"]))
        log.info(f"[Leverage] Current leverage: {lev}x")
        return lev
    except Exception as e:
        log.error(f"[Leverage] Failed to fetch leverage: {e}")
        return 0


def set_leverage(target: int) -> bool:
    try:
        path = f"/v2/products/{PRODUCT_ID}/orders/leverage"
        body = {"leverage": str(target)}
        data = rest_post(path, body)
        new_lev = int(float(data["result"]["leverage"]))
        log.info(f"[Leverage] Set leverage to {new_lev}x")
        return new_lev == target
    except Exception as e:
        log.error(f"[Leverage] Failed to set leverage: {e}")
        return False


def verify_and_set_leverage() -> int:
    current = get_current_leverage()
    with state.lock:
        state.current_leverage = current
    if current != TARGET_LEVERAGE:
        log.info(f"[Leverage] Adjusting {current}x → {TARGET_LEVERAGE}x")
        success = set_leverage(TARGET_LEVERAGE)
        if success:
            with state.lock:
                state.current_leverage = TARGET_LEVERAGE
            return TARGET_LEVERAGE
        else:
            log.error("[Leverage] Failed to set target leverage — aborting trade")
            return current
    return current


# ─────────────────────────────────────────────
# WALLET BALANCE
# ─────────────────────────────────────────────
def fetch_wallet_balance() -> tuple:
    """Returns (available_balance, used_margin) as floats."""
    try:
        data = rest_get("/v2/wallet/balances")
        for wallet in data.get("result", []):
            if wallet.get("asset_symbol") == "USD":
                available = float(wallet.get("available_balance", 0))
                margin    = float(wallet.get("blocked_margin", 0))
                with state.lock:
                    state.available_balance = available
                    state.used_margin       = margin
                log.info(f"[Balance] Available: ${available:.2f}  |  Margin: ${margin:.2f}")
                return available, margin
    except Exception as e:
        log.error(f"[Balance] Failed to fetch wallet: {e}")
    return 0.0, 0.0


# ─────────────────────────────────────────────
# DAILY DRAWDOWN TRACKING
# ─────────────────────────────────────────────
def update_daily_drawdown(available: float, margin: float):
    today         = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_balance = available + margin

    with state.lock:
        # New UTC day — reset baseline and re-enable trading
        if state.current_utc_day != today:
            state.current_utc_day   = today
            state.day_start_balance = total_balance
            if not state.trading_enabled:
                state.trading_enabled = True
                log.info("[Drawdown] New UTC day — trading re-enabled")
            log.info(f"[Drawdown] Day start balance: ${total_balance:.2f}")

        if state.day_start_balance > 0:
            drawdown = (state.day_start_balance - total_balance) / state.day_start_balance * 100
            state.daily_drawdown_pct = drawdown
            if drawdown >= DAILY_DRAWDOWN_LIMIT and state.trading_enabled:
                state.trading_enabled = False
                log.warning(
                    f"[Drawdown] Daily drawdown {drawdown:.2f}% exceeded "
                    f"{DAILY_DRAWDOWN_LIMIT}% limit — trading DISABLED"
                )


# ─────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────
def calculate_position_size(available_balance: float, leverage: int, mark_price: float) -> int:
    """
    Formula:
        position_value = available_balance * leverage * (CAPITAL_USAGE_PERCENT / 100)
        contracts      = floor(position_value / (mark_price * CONTRACT_VALUE))
    Capped at MAX_CONTRACTS.
    """
    if mark_price <= 0 or available_balance <= 0 or leverage <= 0:
        log.warning("[Sizing] Invalid inputs — returning 0 contracts")
        return 0

    position_value = available_balance * leverage * (CAPITAL_USAGE_PERCENT / 100)
    contracts      = math.floor(position_value / (mark_price * CONTRACT_VALUE))
    contracts      = min(contracts, MAX_CONTRACTS)

    with state.lock:
        state.calculated_size = contracts

    log.info(
        f"[Sizing] Balance=${available_balance:.2f}  Lev={leverage}x  "
        f"Price={mark_price:.2f}  →  {contracts} contracts"
    )
    return contracts


# ─────────────────────────────────────────────
# POSITION SYNC (startup)
# ─────────────────────────────────────────────
def sync_position():
    """Fetch current open position from REST API and populate state."""
    try:
        data   = rest_get("/v2/positions", {"product_id": PRODUCT_ID})
        result = data.get("result", {})
        size   = int(result.get("size", 0))

        if size != 0:
            entry  = float(result.get("entry_price", 0))
            margin = float(result.get("margin", 0))
            side   = "long" if size > 0 else "short"
            with state.lock:
                state.position_side   = side
                state.position_size   = abs(size)
                state.entry_price     = entry
                state.position_margin = margin
            log.info(f"[Sync] Existing position: {side.upper()} {abs(size)} contracts @ {entry}")
        else:
            with state.lock:
                state.position_side   = None
                state.position_size   = 0
                state.entry_price     = 0.0
                state.position_margin = 0.0
            log.info("[Sync] No open position found")

    except Exception as e:
        log.error(f"[Sync] Failed to sync position: {e}")


# ─────────────────────────────────────────────
# ORDER EXECUTION
# ─────────────────────────────────────────────
def place_market_order(side: str, size: int) -> bool:
    if size <= 0:
        log.warning("[Order] Size is 0 — skipping order placement")
        return False
    try:
        body = {
            "product_id": PRODUCT_ID,
            "size":       size,
            "side":       side,
            "order_type": "market_order"
        }
        data     = rest_post("/v2/orders", body)
        order_id = data.get("result", {}).get("id", "N/A")
        log.info(f"[Order] {side.upper()} {size} contracts placed — Order ID: {order_id}")
        return True
    except Exception as e:
        log.error(f"[Order] Failed to place {side} market order: {e}")
        return False


def close_position_market(reason: str = "signal"):
    with state.lock:
        side = state.position_side
        size = state.position_size

    if not side or size == 0:
        log.info("[Close] No open position to close")
        return

    close_side = "sell" if side == "long" else "buy"
    log.info(f"[Close] Closing {side.upper()} {size} contracts — Reason: {reason}")
    success = place_market_order(close_side, size)

    if success:
        with state.lock:
            trade = {
                "time":   datetime.now(timezone.utc).isoformat(),
                "action": f"CLOSE {side.upper()}",
                "size":   size,
                "price":  state.mark_price,
                "pnl":    state.unrealized_pnl,
                "reason": reason
            }
            state.trade_history.appendleft(trade)
            state.position_side      = None
            state.position_size      = 0
            state.entry_price        = 0.0
            state.position_margin    = 0.0
            state.unrealized_pnl     = 0.0
            state.loss_vs_margin_pct = 0.0


def open_position(side: str):
    """Verify leverage, size position, and place entry order."""
    leverage = verify_and_set_leverage()
    if leverage != TARGET_LEVERAGE:
        log.error("[Trade] Leverage mismatch — aborting entry")
        return

    available, margin = fetch_wallet_balance()
    update_daily_drawdown(available, margin)

    with state.lock:
        trading_ok = state.trading_enabled
        price      = state.mark_price

    if not trading_ok:
        log.warning("[Trade] Trading disabled (drawdown limit) — skipping entry")
        return

    size = calculate_position_size(available, leverage, price)
    if size <= 0:
        log.warning("[Trade] Calculated size is 0 — insufficient balance or invalid price")
        return

    log.info(f"[Trade] Opening {side.upper()} {size} contracts @ ~{price:.2f}")
    success = place_market_order(side, size)

    if success:
        with state.lock:
            state.position_side   = side
            state.position_size   = size
            state.entry_price     = price
            state.position_margin = (size * price * CONTRACT_VALUE) / leverage
            trade = {
                "time":   datetime.now(timezone.utc).isoformat(),
                "action": f"OPEN {side.upper()}",
                "size":   size,
                "price":  price,
                "pnl":    0.0,
                "reason": "signal"
            }
            state.trade_history.appendleft(trade)


# ─────────────────────────────────────────────
# EMERGENCY GUARD
# ─────────────────────────────────────────────
class EmergencyGuard:
    """
    Three independent emergency protections:
      1. Unrealized loss > LOSS_LIMIT_PERCENT of position margin → close immediately
      2. WebSocket disconnect with open position → close immediately
      3. Daily drawdown > DAILY_DRAWDOWN_LIMIT → disable trading (handled in update_daily_drawdown)
    """

    def __init__(self):
        self._task = None

    async def start(self):
        self._task = asyncio.create_task(self._monitor())
        log.info("[Emergency] Guard started")

    async def stop(self):
        if self._task:
            self._task.cancel()
            log.info("[Emergency] Guard stopped")

    async def _monitor(self):
        while True:
            await asyncio.sleep(5)
            try:
                self._check_loss_limit()
            except Exception as e:
                log.error(f"[Emergency] Monitor error: {e}")

    def _check_loss_limit(self):
        with state.lock:
            side   = state.position_side
            size   = state.position_size
            entry  = state.entry_price
            price  = state.mark_price
            margin = state.position_margin

        if not side or size == 0 or margin <= 0:
            return

        # Calculate unrealized PnL
        if side == "long":
            pnl = (price - entry) * size * CONTRACT_VALUE
        else:
            pnl = (entry - price) * size * CONTRACT_VALUE

        loss_pct = 0.0
        if pnl < 0:
            loss_pct = abs(pnl) / margin * 100

        with state.lock:
            state.unrealized_pnl     = pnl
            state.loss_vs_margin_pct = loss_pct

        if loss_pct >= LOSS_LIMIT_PERCENT:
            log.warning(
                f"[Emergency] Unrealized loss {loss_pct:.2f}% >= "
                f"{LOSS_LIMIT_PERCENT}% limit — closing position NOW"
            )
            close_position_market("emergency_loss_limit")

    def on_ws_disconnect(self):
        """Called immediately when a WebSocket exception is caught."""
        with state.lock:
            has_position = state.position_side is not None

        if has_position:
            log.warning("[Emergency] WebSocket disconnected with open position — closing NOW")
            close_position_market("ws_disconnect")


emergency_guard = EmergencyGuard()


# ─────────────────────────────────────────────
# STRATEGY ENGINE
# ─────────────────────────────────────────────
def process_trade_message(msg: dict):
    """
    Process a single recent_trade WebSocket message.
    Accumulates buy/sell volume into 1-minute bars.
    On bar close, evaluates entry/exit signals.

    Trade message fields:
      p  = price (string)
      s  = size in contracts (string)
      r  = buyer_role: "t" = taker (buy aggressor), "m" = maker (sell aggressor)
      t  = timestamp in microseconds (integer)
      sy = symbol
    """
    trades = msg.get("trades", [])
    if not trades:
        return

    for trade in trades:
        try:
            price    = float(trade.get("p", 0))
            size     = float(trade.get("s", 0))
            role     = trade.get("r", "")
            ts_us    = int(trade.get("t", 0))
            ts_sec   = ts_us / 1_000_000
            trade_bar = math.floor(ts_sec / 60) * 60
        except (ValueError, TypeError) as e:
            log.warning(f"[Strategy] Bad trade message: {e}")
            continue

        with state.lock:
            # Update mark price on every tick
            state.mark_price = price

            # Initialise first bar
            if state.bar_open_time == 0:
                state.bar_open_time   = trade_bar
                state.bar_buy_volume  = 0.0
                state.bar_sell_volume = 0.0

            if trade_bar > state.bar_open_time:
                # ── Bar closed ──────────────────────────────────────────
                bar_delta = state.bar_buy_volume - state.bar_sell_volume

                state.previous_delta = state.current_delta
                state.current_delta  = bar_delta

                curr      = state.current_delta
                prev      = state.previous_delta
                pos_side  = state.position_side
                running   = state.running
                trade_ok  = state.trading_enabled

                log.info(
                    f"[Bar] Closed @ {datetime.utcfromtimestamp(state.bar_open_time).strftime('%H:%M:%S')} "
                    f"| Delta={bar_delta:.2f}  Prev={prev:.2f}"
                )

                # Open new bar, accumulate current tick
                state.bar_open_time   = trade_bar
                state.bar_buy_volume  = size if role == "t" else 0.0
                state.bar_sell_volume = 0.0  if role == "t" else size

            else:
                # ── Accumulate into current bar ──────────────────────────
                if role == "t":
                    state.bar_buy_volume  += size
                else:
                    state.bar_sell_volume += size
                continue   # No bar close — nothing more to do

        # ── Strategy evaluation (outside lock, after bar close) ──────────
        if not running or not trade_ok:
            continue

        # Exit logic — check if hold condition still valid
        if pos_side == "long" and curr <= prev:
            close_position_market("exit_signal")
        elif pos_side == "short" and curr >= prev:
            close_position_market("exit_signal")

        # Re-read position after potential close
        with state.lock:
            pos_side_now = state.position_side

        # Entry logic — only enter if flat
        if pos_side_now is None:
            if curr > prev and curr > 0:
                open_position("buy")
            elif curr < prev and curr < 0:
                open_position("sell")


# ─────────────────────────────────────────────
# PUBLIC WEBSOCKET — TRADE FEED
# ─────────────────────────────────────────────
async def run_public_ws():
    reconnect_count = 0

    while True:
        with state.lock:
            if not state.running:
                break

        try:
            log.info("[WS-Public] Connecting to trade feed...")
            async with websockets.connect(
                WS_PUBLIC_URL,
                ping_interval=20,
                ping_timeout=30
            ) as ws:
                reconnect_count = 0

                sub_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "recent_trade", "symbols": [SYMBOL]}
                        ]
                    }
                }
                await ws.send(json.dumps(sub_msg))
                log.info(f"[WS-Public] Subscribed to {SYMBOL} recent_trade channel")

                async for raw in ws:
                    with state.lock:
                        if not state.running:
                            break
                    try:
                        msg = json.loads(raw)
                        if msg.get("type") == "recent_trade":
                            process_trade_message(msg)
                    except json.JSONDecodeError as e:
                        log.warning(f"[WS-Public] JSON decode error: {e}")

        except Exception as e:
            emergency_guard.on_ws_disconnect()
            reconnect_count += 1
            log.error(
                f"[WS-Public] Disconnected: {e} "
                f"(attempt {reconnect_count}/{WS_MAX_RECONNECT})"
            )
            if reconnect_count >= WS_MAX_RECONNECT:
                log.critical("[WS-Public] Max reconnects reached — shutting down bot")
                with state.lock:
                    state.running    = False
                    state.bot_status = "Crashed"
                break
            await asyncio.sleep(WS_RECONNECT_DELAY)


# ─────────────────────────────────────────────
# PRIVATE WEBSOCKET — AUTH + POSITION UPDATES
# ─────────────────────────────────────────────
async def run_private_ws():
    reconnect_count = 0

    while True:
        with state.lock:
            if not state.running:
                break

        try:
            log.info("[WS-Private] Connecting to private channel...")
            async with websockets.connect(
                WS_PRIVATE_URL,
                ping_interval=20,
                ping_timeout=30
            ) as ws:
                reconnect_count = 0

                # ── Authenticate ─────────────────────────────────────────
                timestamp      = str(int(time.time()))
                sig_message    = "GET" + timestamp + "/live"
                signature      = generate_signature(API_SECRET, sig_message)
                auth_payload   = {
                    "type": "key-auth",
                    "payload": {
                        "api-key":   API_KEY,
                        "signature": signature,
                        "timestamp": timestamp
                    }
                }
                await ws.send(json.dumps(auth_payload))
                log.info("[WS-Private] Authentication payload sent")

                # ── Subscribe to private channels ─────────────────────────
                sub_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "positions", "symbols": [SYMBOL]},
                            {"name": "orders",    "symbols": [SYMBOL]}
                        ]
                    }
                }
                await ws.send(json.dumps(sub_msg))
                log.info("[WS-Private] Subscribed to positions and orders channels")

                async for raw in ws:
                    with state.lock:
                        if not state.running:
                            break
                    try:
                        msg      = json.loads(raw)
                        msg_type = msg.get("type", "")

                        if msg_type == "key-auth":
                            if msg.get("success"):
                                log.info("[WS-Private] Authentication successful")
                            else:
                                log.error(f"[WS-Private] Authentication failed: {msg}")

                        elif msg_type == "positions":
                            _handle_position_update(msg)

                    except json.JSONDecodeError as e:
                        log.warning(f"[WS-Private] JSON decode error: {e}")

        except Exception as e:
            reconnect_count += 1
            log.error(
                f"[WS-Private] Disconnected: {e} "
                f"(attempt {reconnect_count}/{WS_MAX_RECONNECT})"
            )
            if reconnect_count >= WS_MAX_RECONNECT:
                log.critical("[WS-Private] Max reconnects reached — private feed offline")
                break
            await asyncio.sleep(WS_RECONNECT_DELAY)


def _handle_position_update(msg: dict):
    """Parse a positions WebSocket message and update shared state."""
    # Handle both snapshot (result is list) and incremental update (result is dict)
    result = msg.get("result", {})

    if isinstance(result, list):
        # Snapshot — find our product
        for pos in result:
            if pos.get("product_id") == PRODUCT_ID:
                result = pos
                break
        else:
            return

    size   = int(result.get("size", 0))
    entry  = float(result.get("entry_price", 0) or 0)
    margin = float(result.get("margin", 0) or 0)
    side   = "long" if size > 0 else ("short" if size < 0 else None)

    with state.lock:
        state.position_side   = side
        state.position_size   = abs(size)
        state.entry_price     = entry
        state.position_margin = margin

    log.info(f"[WS-Private] Position update: {side} {abs(size)} contracts @ {entry}")


# ─────────────────────────────────────────────
# MAIN BOT COROUTINE
# ─────────────────────────────────────────────
async def bot_main():
    log.info("=" * 60)
    log.info("[Bot] ETHUSD Volume Delta Bot starting up")
    log.info(f"[Bot] Target leverage: {TARGET_LEVERAGE}x")
    log.info(f"[Bot] Capital usage:   {CAPITAL_USAGE_PERCENT}%")
    log.info(f"[Bot] Loss limit:      {LOSS_LIMIT_PERCENT}%")
    log.info(f"[Bot] Drawdown limit:  {DAILY_DRAWDOWN_LIMIT}%")
    log.info("=" * 60)

    with state.lock:
        state.bot_status = "Running"

    # Initial REST sync
    sync_position()
    fetch_wallet_balance()

    # Start emergency guard background task
    await emergency_guard.start()

    # Run both WebSocket feeds concurrently
    try:
        await asyncio.gather(
            run_public_ws(),
            run_private_ws()
        )
    except asyncio.CancelledError:
        log.info("[Bot] Coroutines cancelled — shutting down")
    finally:
        await emergency_guard.stop()
        log.info("[Bot] Shutdown complete")
        with state.lock:
            state.bot_status = "Stopped"


def start_bot_thread():
    """Entry point for the background bot thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(bot_main())
    except Exception as e:
        log.critical(f"[Bot] Fatal error in bot thread: {e}")
        with state.lock:
            state.running    = False
            state.bot_status = "Crashed"
            state.last_error = str(e)
    finally:
        loop.close()
# ─────────────────────────────────────────────
# DASHBOARD HTML
# ─────────────────────────────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>ETHUSD Delta Bot</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #0d1117;
      color: #c9d1d9;
      font-family: 'Segoe UI', 'Helvetica Neue', monospace;
      padding: 24px;
      min-height: 100vh;
    }

    h1 {
      color: #58a6ff;
      font-size: 1.6rem;
      margin-bottom: 6px;
      letter-spacing: 0.5px;
    }

    .subtitle {
      color: #8b949e;
      font-size: 0.82rem;
      margin-bottom: 24px;
    }

    /* ── Controls ── */
    .controls {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 24px;
      flex-wrap: wrap;
    }

    .btn {
      padding: 10px 28px;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.9rem;
      font-weight: 700;
      transition: background 0.15s;
    }

    .btn-start  { background: #238636; color: #fff; }
    .btn-start:hover { background: #2ea043; }
    .btn-stop   { background: #b62324; color: #fff; }
    .btn-stop:hover  { background: #da3633; }

    #last-update {
      font-size: 0.75rem;
      color: #8b949e;
      margin-left: auto;
    }

    /* ── Grid ── */
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
      margin-bottom: 20px;
    }

    .card {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 18px 20px;
    }

    .card h3 {
      color: #8b949e;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin-bottom: 10px;
    }

    .metric {
      font-size: 1.5rem;
      font-weight: 700;
      color: #f0f6fc;
      line-height: 1.2;
    }

    .metric.green  { color: #3fb950; }
    .metric.red    { color: #f85149; }
    .metric.yellow { color: #d29922; }

    .sub-label {
      font-size: 0.78rem;
      color: #8b949e;
      margin-top: 5px;
    }

    /* ── Status badge ── */
    .badge {
      display: inline-block;
      padding: 3px 10px;
      border-radius: 10px;
      font-size: 0.75rem;
      font-weight: 700;
    }

    .badge-running  { background: #1f4a1f; color: #3fb950; }
    .badge-stopped  { background: #3d1f1f; color: #f85149; }
    .badge-crashed  { background: #4a3d1f; color: #d29922; }
    .badge-starting { background: #1a2a4a; color: #58a6ff; }

    /* ── Trade history table ── */
    .table-card {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 18px 20px;
      overflow-x: auto;
    }

    .table-card h3 {
      color: #8b949e;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin-bottom: 14px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.82rem;
    }

    th {
      color: #8b949e;
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid #30363d;
      white-space: nowrap;
    }

    td {
      padding: 8px 10px;
      border-bottom: 1px solid #21262d;
      white-space: nowrap;
    }

    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #1c2128; }

    .no-trades {
      color: #8b949e;
      font-size: 0.82rem;
      padding: 12px 0;
    }

    /* ── Divider ── */
    .section-title {
      color: #8b949e;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin: 4px 0 12px 2px;
    }
  </style>
</head>
<body>

  <h1>ETHUSD Volume Delta Bot</h1>
  <p class="subtitle">1-minute delta bars &nbsp;|&nbsp; 150x leverage &nbsp;|&nbsp; Dynamic sizing &nbsp;|&nbsp; Emergency guard active</p>

  <!-- Controls -->
  <div class="controls">
    <button class="btn btn-start" onclick="controlBot('start')">&#9654; Start Bot</button>
    <button class="btn btn-stop"  onclick="controlBot('stop')">&#9632; Stop Bot</button>
    <span id="last-update"></span>
  </div>

  <!-- Row 1: Status / Price / Delta / Balance -->
  <p class="section-title">Overview</p>
  <div class="grid">

    <div class="card">
      <h3>Bot Status</h3>
      <div id="bot-status" class="metric">--</div>
      <div id="trading-enabled" class="sub-label">--</div>
    </div>

    <div class="card">
      <h3>Mark Price</h3>
      <div id="mark-price" class="metric">--</div>
      <div class="sub-label">ETHUSD Perpetual</div>
    </div>

    <div class="card">
      <h3>Volume Delta (1m)</h3>
      <div id="current-delta" class="metric">--</div>
      <div id="prev-delta" class="sub-label">--</div>
    </div>

    <div class="card">
      <h3>Available Balance</h3>
      <div id="available-balance" class="metric">--</div>
      <div id="used-margin" class="sub-label">--</div>
    </div>

  </div>

  <!-- Row 2: Leverage / Drawdown / Position / PnL -->
  <p class="section-title">Risk &amp; Position</p>
  <div class="grid">

    <div class="card">
      <h3>Leverage / Sizing</h3>
      <div id="leverage" class="metric">--</div>
      <div id="calc-size" class="sub-label">--</div>
    </div>

    <div class="card">
      <h3>Daily Drawdown</h3>
      <div id="daily-drawdown" class="metric">--</div>
      <div id="day-start-bal" class="sub-label">--</div>
    </div>

    <div class="card">
      <h3>Open Position</h3>
      <div id="position-side" class="metric">--</div>
      <div id="position-details" class="sub-label">--</div>
    </div>

    <div class="card">
      <h3>Unrealized PnL</h3>
      <div id="unrealized-pnl" class="metric">--</div>
      <div id="loss-vs-margin" class="sub-label">--</div>
    </div>

  </div>

  <!-- Trade History -->
  <div class="table-card">
    <h3>Trade History (last 100)</h3>
    <div id="trade-history-container">
      <p class="no-trades">No trades yet.</p>
    </div>
  </div>

  <script>
    // ── Helpers ──────────────────────────────────────────────────────────────
    function colorClass(val) {
      if (val > 0) return 'green';
      if (val < 0) return 'red';
      return '';
    }

    function fmt2(n)  { return parseFloat(n).toFixed(2); }
    function fmtUSD(n){ return '$' + fmt2(n); }

    function badgeClass(status) {
      const map = {
        'Running':  'badge-running',
        'Stopped':  'badge-stopped',
        'Crashed':  'badge-crashed',
        'Starting': 'badge-starting',
        'Stopping': 'badge-stopped'
      };
      return map[status] || 'badge-stopped';
    }

    // ── Control ───────────────────────────────────────────────────────────────
    function controlBot(action) {
      fetch('/control/' + action, { method: 'POST' })
        .then(r => r.json())
        .then(d => console.log('[Control]', d))
        .catch(e => console.error('[Control] Error:', e));
    }

    // ── Dashboard update ──────────────────────────────────────────────────────
    function updateDashboard() {
      fetch('/api/status')
        .then(r => r.json())
        .then(d => {
          // ── Status ──
          const statusEl = document.getElementById('bot-status');
          statusEl.innerHTML =
            '<span class="badge ' + badgeClass(d.bot_status) + '">' + d.bot_status + '</span>';
          statusEl.className = 'metric';

          document.getElementById('trading-enabled').textContent =
            'Trading: ' + (d.trading_enabled ? 'ENABLED' : 'DISABLED');

          // ── Price ──
          document.getElementById('mark-price').textContent = fmtUSD(d.mark_price);

          // ── Delta ──
          const deltaEl = document.getElementById('current-delta');
          deltaEl.textContent  = fmt2(d.current_delta);
          deltaEl.className    = 'metric ' + colorClass(d.current_delta);
          document.getElementById('prev-delta').textContent =
            'Previous bar: ' + fmt2(d.previous_delta);

          // ── Balance ──
          document.getElementById('available-balance').textContent = fmtUSD(d.available_balance);
          document.getElementById('used-margin').textContent =
            'Used margin: ' + fmtUSD(d.used_margin);

          // ── Leverage ──
          document.getElementById('leverage').textContent = d.current_leverage + 'x';
          document.getElementById('calc-size').textContent =
            'Calculated size: ' + d.calculated_size + ' contracts';

          // ── Drawdown ──
          const ddEl = document.getElementById('daily-drawdown');
          ddEl.textContent  = fmt2(d.daily_drawdown_pct) + '%';
          ddEl.className    = 'metric ' + (d.daily_drawdown_pct >= 5 ? 'red' : 'green');
          document.getElementById('day-start-bal').textContent =
            'Day start: ' + fmtUSD(d.day_start_balance);

          // ── Position ──
          const posEl = document.getElementById('position-side');
          if (d.position_side) {
            posEl.textContent = d.position_side.toUpperCase();
            posEl.className   = 'metric ' + (d.position_side === 'long' ? 'green' : 'red');
            document.getElementById('position-details').textContent =
              d.position_size + ' contracts @ ' + fmtUSD(d.entry_price) +
              '  |  Margin: ' + fmtUSD(d.position_margin);
          } else {
            posEl.textContent = 'FLAT';
            posEl.className   = 'metric';
            document.getElementById('position-details').textContent = 'No open position';
          }

          // ── PnL ──
          const pnlEl = document.getElementById('unrealized-pnl');
          pnlEl.textContent = fmtUSD(d.unrealized_pnl);
          pnlEl.className   = 'metric ' + colorClass(d.unrealized_pnl);
          document.getElementById('loss-vs-margin').textContent =
            'Loss vs margin: ' + fmt2(d.loss_vs_margin_pct) + '%';

          // ── Trade history ──
          const container = document.getElementById('trade-history-container');
          if (!d.trade_history || d.trade_history.length === 0) {
            container.innerHTML = '<p class="no-trades">No trades yet.</p>';
          } else {
            let rows = '';
            d.trade_history.forEach(t => {
              const pnlColor = parseFloat(t.pnl) >= 0 ? '#3fb950' : '#f85149';
              rows += '<tr>' +
                '<td>' + t.time.substring(0, 19).replace('T', ' ') + '</td>' +
                '<td>' + t.action + '</td>' +
                '<td>' + t.size + '</td>' +
                '<td>' + fmtUSD(t.price) + '</td>' +
                '<td style="color:' + pnlColor + '">' + fmtUSD(t.pnl) + '</td>' +
                '<td>' + t.reason + '</td>' +
                '</tr>';
            });
            container.innerHTML =
              '<table>' +
              '<thead><tr>' +
              '<th>Time (UTC)</th><th>Action</th><th>Size</th>' +
              '<th>Price</th><th>PnL</th><th>Reason</th>' +
              '</tr></thead>' +
              '<tbody>' + rows + '</tbody>' +
              '</table>';
          }

          // ── Timestamp ──
          document.getElementById('last-update').textContent =
            'Last update: ' + new Date().toLocaleTimeString();
        })
        .catch(e => console.error('[Dashboard] Fetch error:', e));
    }

    // Poll every 3 seconds
    updateDashboard();
    setInterval(updateDashboard, 3000);
  </script>

</body>
</html>
"""


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/status")
def api_status():
    with state.lock:
        return jsonify({
            "bot_status":         state.bot_status,
            "trading_enabled":    state.trading_enabled,
            "mark_price":         state.mark_price,
            "current_delta":      state.current_delta,
            "previous_delta":     state.previous_delta,
            "available_balance":  state.available_balance,
            "used_margin":        state.used_margin,
            "current_leverage":   state.current_leverage,
            "calculated_size":    state.calculated_size,
            "position_side":      state.position_side,
            "position_size":      state.position_size,
            "entry_price":        state.entry_price,
            "position_margin":    state.position_margin,
            "unrealized_pnl":     state.unrealized_pnl,
            "loss_vs_margin_pct": state.loss_vs_margin_pct,
            "day_start_balance":  state.day_start_balance,
            "daily_drawdown_pct": state.daily_drawdown_pct,
            "trade_history":      list(state.trade_history),
            "last_error":         state.last_error
        })


@app.route("/control/start", methods=["POST"])
def control_start():
    with state.lock:
        if state.running:
            return jsonify({"status": "already_running"})
        state.running    = True
        state.bot_status = "Starting"

    t = threading.Thread(target=start_bot_thread, daemon=True, name="BotThread")
    t.start()
    log.info("[Control] Bot started via dashboard")
    return jsonify({"status": "started"})


@app.route("/control/stop", methods=["POST"])
def control_stop():
    with state.lock:
        state.running    = False
        state.bot_status = "Stopping"
    log.info("[Control] Bot stop requested via dashboard")
    return jsonify({"status": "stopping"})


@app.route("/health")
def health():
    """Health check endpoint for Railway / Render uptime monitors."""
    with state.lock:
        status = state.bot_status
    return jsonify({"status": "ok", "bot": status}), 200


# ─────────────────────────────────────────────
# ENTRY POINT (direct run)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    log.info(f"[App] Flask dashboard starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
