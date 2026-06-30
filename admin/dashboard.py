"""
CRYPTO MONITOR PRO — Admin Web Dashboard
FastAPI-based admin panel
"""
import asyncio
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, Depends, HTTPException, WebSocket, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import orjson
from loguru import logger

from config.settings import settings
from core.state_manager import state_manager
from db.models import AsyncSessionFactory, User, AlertLog
from sqlalchemy import select, func


app = FastAPI(title="ALPHATRADERAI Admin", docs_url="/api/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ADMIN_HTML = """
<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ALPHATRADERAI — Admin</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; }
  .header { background: #161b22; border-bottom: 1px solid #30363d; padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
  .header h1 { font-size: 20px; font-weight: 700; color: #58a6ff; }
  .header .status { width: 10px; height: 10px; border-radius: 50%; background: #2ea043; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }
  .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; }
  .card .label { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
  .card .value { font-size: 28px; font-weight: 700; color: #58a6ff; }
  .card .sub { font-size: 12px; color: #8b949e; margin-top: 4px; }
  .section { background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 16px; }
  .section-header { padding: 16px 20px; border-bottom: 1px solid #30363d; font-weight: 600; font-size: 14px; display: flex; justify-content: space-between; align-items: center; }
  .section-body { padding: 16px 20px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 12px; color: #8b949e; font-weight: 500; border-bottom: 1px solid #30363d; }
  td { padding: 8px 12px; border-bottom: 1px solid #21262d; }
  tr:last-child td { border-bottom: none; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
  .badge-extreme { background: #3d1f1f; color: #ff7b72; }
  .badge-strong { background: #1f3d2e; color: #3fb950; }
  .badge-notice { background: #3d3d1f; color: #d29922; }
  .log-entry { font-size: 12px; padding: 6px 0; border-bottom: 1px solid #21262d; display: flex; gap: 12px; }
  .log-entry .time { color: #8b949e; min-width: 80px; }
  .log-entry .sym { color: #58a6ff; font-weight: 600; min-width: 100px; }
  #live-log { max-height: 300px; overflow-y: auto; }
  .refresh-btn { background: #21262d; border: 1px solid #30363d; color: #e6edf3; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 12px; }
  .refresh-btn:hover { background: #30363d; }
</style>
</head>
<body>

<div class="header">
  <div class="status" id="status-dot"></div>
  <h1>🚀 ALPHATRADERAI</h1>
  <span style="color:#8b949e;font-size:13px;margin-left:auto" id="last-update">—</span>
</div>

<div class="container">

  <!-- Stats Grid -->
  <div class="grid" id="stats-grid">
    <div class="card">
      <div class="label">Kuzatilayotgan Coinlar</div>
      <div class="value" id="stat-symbols">—</div>
      <div class="sub">Binance Futures</div>
    </div>
    <div class="card">
      <div class="label">Yuborilgan Alertlar</div>
      <div class="value" id="stat-alerts">—</div>
      <div class="sub">Jami</div>
    </div>
    <div class="card">
      <div class="label">WS Xabarlari</div>
      <div class="value" id="stat-ws">—</div>
      <div class="sub">Real-time</div>
    </div>
    <div class="card">
      <div class="label">Foydalanuvchilar</div>
      <div class="value" id="stat-users">—</div>
      <div class="sub">Faol</div>
    </div>
    <div class="card">
      <div class="label">OI Eventlar</div>
      <div class="value" id="stat-oi">—</div>
      <div class="sub">Bugun</div>
    </div>
    <div class="card">
      <div class="label">Likvidatsiyalar</div>
      <div class="value" id="stat-liq">—</div>
      <div class="sub">Bugun</div>
    </div>
  </div>

  <!-- Live Alerts Log -->
  <div class="section">
    <div class="section-header">
      ⚡ Live Alertlar
      <button class="refresh-btn" onclick="loadAlerts()">🔄 Yangilash</button>
    </div>
    <div class="section-body">
      <div id="live-log">
        <div style="color:#8b949e;font-size:13px">Yuklanmoqda...</div>
      </div>
    </div>
  </div>

  <!-- System Health -->
  <div class="section">
    <div class="section-header">🏥 Tizim Holati</div>
    <div class="section-body">
      <table>
        <thead>
          <tr>
            <th>Komponent</th>
            <th>Holat</th>
            <th>Tafsilot</th>
          </tr>
        </thead>
        <tbody id="health-table">
          <tr><td colspan="3" style="color:#8b949e">Yuklanmoqda...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</div>

<script>
async function loadStats() {
  try {
    const r = await fetch('/api/stats');
    const data = await r.json();
    document.getElementById('stat-symbols').textContent = data.symbols_count?.toLocaleString() || '—';
    document.getElementById('stat-alerts').textContent = data.alerts_sent?.toLocaleString() || '—';
    document.getElementById('stat-ws').textContent = data.ws_messages?.toLocaleString() || '—';
    document.getElementById('stat-users').textContent = data.active_users?.toLocaleString() || '—';
    document.getElementById('stat-oi').textContent = data.oi_events?.toLocaleString() || '—';
    document.getElementById('stat-liq').textContent = data.liq_events?.toLocaleString() || '—';
    document.getElementById('last-update').textContent = 'Yangilandi: ' + new Date().toLocaleTimeString();
  } catch(e) { console.error(e); }
}

async function loadAlerts() {
  try {
    const r = await fetch('/api/alerts/recent');
    const data = await r.json();
    const log = document.getElementById('live-log');
    if (!data.alerts?.length) {
      log.innerHTML = '<div style="color:#8b949e;font-size:13px">Alertlar yo\\'q</div>';
      return;
    }
    log.innerHTML = data.alerts.map(a => `
      <div class="log-entry">
        <span class="time">${new Date(a.sent_at).toLocaleTimeString()}</span>
        <span class="sym">${a.symbol}</span>
        <span class="badge badge-${a.alert_level}">${a.alert_level.toUpperCase()}</span>
        <span style="color:#8b949e">${a.score?.toFixed(0)} ball</span>
        <span style="color:#e6edf3">$${a.price?.toLocaleString() || '—'}</span>
      </div>
    `).join('');
  } catch(e) {}
}

async function loadHealth() {
  try {
    const r = await fetch('/api/health');
    const data = await r.json();
    const tbody = document.getElementById('health-table');
    tbody.innerHTML = data.components.map(c => `
      <tr>
        <td>${c.name}</td>
        <td><span class="badge ${c.status === 'ok' ? 'badge-strong' : 'badge-extreme'}">${c.status === 'ok' ? '✅ OK' : '❌ ERROR'}</span></td>
        <td style="color:#8b949e">${c.detail || '—'}</td>
      </tr>
    `).join('');
  } catch(e) {}
}

// Auto-refresh every 5s
setInterval(() => { loadStats(); loadAlerts(); }, 5000);
setInterval(loadHealth, 30000);

// Initial load
loadStats();
loadAlerts();
loadHealth();
</script>
</body>
</html>
"""


@app.get("/")
@app.head("/")
async def root():
    return Response(content=ADMIN_HTML, media_type="text/html")

@app.head("/api/health")
async def health_head():
    return Response()




@app.get("/api/stats")
async def get_stats():
    stats = await state_manager.get_all_stats()
    symbols = await state_manager.get_symbols("binance", "futures")

    async with AsyncSessionFactory() as db:
        result = await db.execute(
            select(func.count(User.id)).where(User.is_active == True)
        )
        user_count = result.scalar() or 0

    return {
        "symbols_count": len(symbols),
        "alerts_sent": stats.get("alerts_sent", 0),
        "ws_messages": stats.get("ws_messages", 0),
        "active_users": user_count,
        "oi_events": stats.get("oi_events", 0),
        "liq_events": stats.get("liq_events", 0),
        "volume_events": stats.get("volume_events", 0),
        "whale_events": stats.get("whale_events", 0),
        "errors": stats.get("errors", 0),
    }


@app.get("/api/alerts/recent")
async def get_recent_alerts(limit: int = 50):
    async with AsyncSessionFactory() as db:
        result = await db.execute(
            select(AlertLog)
            .order_by(AlertLog.sent_at.desc())
            .limit(limit)
        )
        alerts = result.scalars().all()

    return {
        "alerts": [
            {
                "symbol": a.symbol,
                "exchange": a.exchange,
                "alert_level": a.alert_level,
                "score": a.score,
                "price": a.price,
                "sent_at": a.sent_at.isoformat() if a.sent_at else None,
            }
            for a in alerts
        ]
    }


@app.get("/api/health")
async def get_health():
    components = []

    # Memory State Manager (Redis emas)
    try:
        symbols = await state_manager.get_symbols("binance", "futures")
        components.append({"name": "StateManager", "status": "ok", "detail": f"{len(symbols)} symbols"})
    except Exception as e:
        components.append({"name": "StateManager", "status": "error", "detail": str(e)})

    # Database
    try:
        async with AsyncSessionFactory() as db:
            from sqlalchemy import text
            await db.execute(text("SELECT 1"))
        components.append({"name": "SQLite", "status": "ok", "detail": "Connected"})
    except Exception as e:
        components.append({"name": "SQLite", "status": "error", "detail": str(e)})

    return {"components": components, "timestamp": datetime.utcnow().isoformat()}
