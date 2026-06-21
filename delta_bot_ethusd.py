# =============================================================================
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
