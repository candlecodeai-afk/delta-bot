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
