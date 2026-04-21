// FluxTrader Dashboard – Real-time multi-bot monitoring
// Reads from /api/* endpoints (read-only from PersistentState)

const API_BASE = '/api';
const REFRESH_INTERVAL = 3000; // 3s

let equityChart = null;
let tradeData = [];
let strategies = [];
// Persistent UI-State: welche Bot-Cards sind ausgeklappt?
const expandedBots = new Set();

// ─────────────────────────────────────────────────────────────────────────
// Symbol-Status Rendering
// ─────────────────────────────────────────────────────────────────────────

// Code -> { label, cssClass }
const STATUS_META = {
  SIGNAL:             { label: 'Signal',          cls: 'ok' },
  IN_POSITION:        { label: 'In Position',     cls: 'info' },
  WAIT_BREAKOUT:      { label: 'Wartet Breakout', cls: 'muted' },
  WAIT_ORB:           { label: 'ORB-Periode',     cls: 'muted' },
  WAIT_SETUP:         { label: 'Wartet Setup',    cls: 'muted' },
  WAIT_WARMUP:        { label: 'Warmup',          cls: 'muted' },
  WAIT_Z:             { label: 'Wartet Z-Score',  cls: 'muted' },
  WEAK_SIGNAL:        { label: 'Zu schwach',      cls: 'muted' },
  WEAK_CONFLUENCE:    { label: 'Schwache Conf.',  cls: 'muted' },
  WEAK_TREND:         { label: 'Schwacher Trend', cls: 'muted' },
  NO_UPTREND:         { label: 'Kein Aufwärts',   cls: 'muted' },
  OUTSIDE_HOURS:      { label: 'Außer Handel',    cls: 'muted' },
  ENTRY_CUTOFF:       { label: 'Nach Cutoff',     cls: 'muted' },
  GAP_BLOCK:          { label: 'Gap zu hoch',     cls: 'warning' },
  TREND_BLOCK:        { label: 'Trend-Block',     cls: 'warning' },
  MTF_BLOCK:          { label: 'MTF-Block',       cls: 'warning' },
  MIT_BLOCK:          { label: 'MIT-Block',       cls: 'warning' },
  MIT_OVERLAY_REJECT: { label: 'MIT-Reject',      cls: 'warning' },
  SECTOR_BLOCK:       { label: 'Sektor-Block',    cls: 'warning' },
  DD_BREAKER:         { label: 'DD-Breaker',      cls: 'warning' },
  RSI_BLOCK:          { label: 'RSI-Block',       cls: 'warning' },
  MACD_BLOCK:         { label: 'MACD-Block',      cls: 'warning' },
  VOLUME_BLOCK:       { label: 'Volume-Block',    cls: 'warning' },
  SHORTS_DISABLED:    { label: 'Shorts aus',      cls: 'warning' },
  NO_ORB:             { label: 'Keine ORB',       cls: 'warning' },
  NO_OB:              { label: 'Keine OB',        cls: 'warning' },
  NO_VALID_OB:        { label: 'Keine valide OB', cls: 'warning' },
  NO_DATA:            { label: 'Keine Daten',     cls: 'muted' },
};

function statusMeta(code) {
  return STATUS_META[code] || { label: code, cls: 'muted' };
}

function getStatusCounts(symbolStatus) {
  const counts = {};
  for (const [, value] of Object.entries(symbolStatus || {})) {
    const code = (value && value.code) || 'UNKNOWN';
    counts[code] = (counts[code] || 0) + 1;
  }
  return counts;
}

function formatStatusBadgeSummary(symbolStatus) {
  const entries = Object.entries(symbolStatus || {});
  if (entries.length === 0) return '';
  const counts = getStatusCounts(symbolStatus);
  const parts = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .map(([code, n]) => `${statusMeta(code).label}: ${n}`);
  return `${entries.length} Symbols\n` + parts.join('\n');
}

function renderCompactStatusSummary(symbolStatus) {
  const counts = Object.entries(getStatusCounts(symbolStatus))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3);
  if (!counts.length) return '<span class="muted">Keine Statusdaten</span>';
  return counts.map(([code, count]) => {
    const meta = statusMeta(code);
    return `<span class="status-chip ${meta.cls}">${meta.label}: ${count}</span>`;
  }).join(' ');
}

function formatLagMs(value) {
  if (value === null || value === undefined) return '—';
  const ms = Number(value);
  if (!Number.isFinite(ms)) return '—';
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatBarAge(ts) {
  if (!ts) return '—';
  const ageSeconds = (Date.now() - new Date(ts).getTime()) / 1000;
  if (!Number.isFinite(ageSeconds)) return '—';
  if (ageSeconds < 60) return `${ageSeconds.toFixed(0)}s`;
  if (ageSeconds < 3600) return `${(ageSeconds / 60).toFixed(1)}m`;
  return `${(ageSeconds / 3600).toFixed(1)}h`;
}

function renderSymbolStatusTable(symbolStatus) {
  const entries = Object.entries(symbolStatus || {});
  if (entries.length === 0) {
    return '<div class="muted" style="padding: 8px 0;">Keine Symbol-Status-Daten verfügbar. Bot nicht live oder noch keine Bars verarbeitet.</div>';
  }
  // Sortierung: Signal / In Position zuerst, dann nach Symbol
  const priority = (code) => {
    if (code === 'SIGNAL') return 0;
    if (code === 'IN_POSITION') return 1;
    if (code && code.startsWith('WAIT')) return 2;
    return 3;
  };
  entries.sort((a, b) => {
    const pa = priority(a[1] && a[1].code);
    const pb = priority(b[1] && b[1].code);
    if (pa !== pb) return pa - pb;
    return a[0].localeCompare(b[0]);
  });
  const rows = entries.map(([sym, info]) => {
    const code = (info && info.code) || '—';
    const meta = statusMeta(code);
    const reason = (info && info.reason) || '';
    const ts = info && info.ts ? new Date(info.ts).toLocaleTimeString() : '—';
    return `
      <tr>
        <td><strong>${sym}</strong></td>
        <td><span class="status-chip ${meta.cls}">${meta.label}</span></td>
        <td class="muted">${reason || '—'}</td>
        <td class="muted">${ts}</td>
      </tr>`;
  }).join('');
  return `
    <table class="symbol-status-table">
      <thead>
        <tr><th>Symbol</th><th>Status</th><th>Detail</th><th>Zeit</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

// ─────────────────────────────────────────────────────────────────────────
// Main refresh loop
// ─────────────────────────────────────────────────────────────────────────

async function refreshDashboard() {
  try {
    await Promise.all([
      updatePortfolio(),
      updateStrategies(),
      updateHealth(),
      updatePositions(),
      updateTrades(),
      updateAnomalies(),
    ]);
    document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
  } catch (err) {
    console.error('Dashboard refresh failed:', err);
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Portfolio
// ─────────────────────────────────────────────────────────────────────────

async function updatePortfolio() {
  try {
    const resp = await fetch(`${API_BASE}/portfolio`);
    const data = await resp.json();

    document.getElementById('total-equity').textContent = formatCurrency(data.latest_equity);
    document.getElementById('total-drawdown').textContent = formatPercent(data.drawdown_pct);
    document.getElementById('total-positions').textContent = data.open_positions || 0;

    document.getElementById('portfolio-equity').textContent = formatCurrency(data.latest_equity);
    document.getElementById('portfolio-peak').textContent = formatCurrency(data.peak_equity);
    document.getElementById('portfolio-cash').textContent = formatCurrency(data.cash);

    // Update equity chart
    await updateEquityChart();
  } catch (err) {
    console.error('Portfolio update failed:', err);
  }
}

async function updateEquityChart() {
  try {
    const resp = await fetch(`${API_BASE}/equity?limit=50`);
    const curve = await resp.json();

    if (!curve || curve.length === 0) return;

    const labels = curve.map(s => new Date(s.ts).toLocaleDateString());
    const equities = curve.map(s => s.equity);

    if (!equityChart) {
      const ctx = document.getElementById('equity-chart').getContext('2d');
      equityChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels,
          datasets: [{
            label: 'Equity',
            data: equities,
            borderColor: '#4a8fef',
            backgroundColor: 'rgba(74, 143, 239, 0.1)',
            borderWidth: 2,
            fill: true,
            tension: 0.3,
          }],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            y: {
              ticks: { color: '#8a93a1' },
              grid: { color: '#262b33' },
            },
            x: {
              ticks: { color: '#8a93a1' },
              grid: { display: false },
            },
          },
        },
      });
    } else {
      equityChart.data.labels = labels;
      equityChart.data.datasets[0].data = equities;
      equityChart.update();
    }
  } catch (err) {
    console.error('Equity chart update failed:', err);
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Health Status (Live-Mode only)
// ─────────────────────────────────────────────────────────────────────────

async function updateHealth() {
  try {
    const resp = await fetch(`${API_BASE}/strategies/health`);
    const data = await resp.json();

    const panel = document.getElementById('health-panel');
    const container = document.getElementById('health-status');

    if (!data.available) {
      panel.style.display = 'none';
      return;
    }

    panel.style.display = 'block';

    // Broker Health
    const broker = data.broker || {};
    const brokerConnected = broker.connected;
    const brokerCard = `
      <div class="health-card">
        <h4>🔌 Broker Connection</h4>
        <div class="health-item ${brokerConnected ? 'ok' : 'error'}">
          <span>Status:</span>
          <span class="health-badge ${brokerConnected ? 'connected' : 'disconnected'}">
            ${brokerConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
        <div class="health-item">
          <span>Adapter:</span>
          <span>${broker.adapter || '—'}</span>
        </div>
        <div class="health-item">
          <span>Last Order:</span>
          <span>${broker.last_order_ms ? broker.last_order_ms.toFixed(0) + 'ms' : '—'}</span>
        </div>
      </div>
    `;

    // Circuit Breaker
    const cbActive = data.circuit_breaker || false;
    const cbCard = `
      <div class="health-card">
        <h4>🚨 Circuit Breaker</h4>
        <div class="health-item ${!cbActive ? 'ok' : 'error'}">
          <span>Status:</span>
          <span>${cbActive ? '🚨 ACTIVE' : '✅ OFF'}</span>
        </div>
      </div>
    `;

    // Portfolio
    const portfolio = data.portfolio || {};
    const portfolioCard = `
      <div class="health-card">
        <h4>📊 Portfolio</h4>
        <div class="health-item">
          <span>Equity:</span>
          <span>${formatCurrency(portfolio.equity)}</span>
        </div>
        <div class="health-item ${portfolio.drawdown_pct > -10 ? 'ok' : (portfolio.drawdown_pct > -15 ? 'warning' : 'error')}">
          <span>Drawdown:</span>
          <span>${formatPercent(portfolio.drawdown_pct)}</span>
        </div>
        <div class="health-item">
          <span>Open Pos:</span>
          <span>${portfolio.open_positions || 0}</span>
        </div>
      </div>
    `;

    // Strategies Health
    const strategiesHtml = (data.strategies || []).map(s => {
      const lagMs = s.last_bar_lag_ms;
      const lagClass = lagMs === null || lagMs === undefined
        ? 'warning'
        : (lagMs < 1000 ? 'ok' : 'warning');
      return `
      <div class="health-card">
        <h4>📡 ${s.name}</h4>
        <div class="health-item ${lagClass}">
          <span>Bar Lag:</span>
          <span>${formatLagMs(lagMs)}</span>
        </div>
        <div class="health-item">
          <span>Bar Age:</span>
          <span>${formatBarAge(s.last_bar_ts)}</span>
        </div>
        <div class="health-item">
          <span>Signals:</span>
          <span>${s.signals_today || 0}</span>
        </div>
        <div class="health-item">
          <span>Filtered:</span>
          <span>${s.signals_filtered_today || 0}</span>
        </div>
        <div class="health-item">
          <span>Last Bar:</span>
          <span class="muted">${s.last_bar_ts ? new Date(s.last_bar_ts).toLocaleTimeString() : '—'}</span>
        </div>
      </div>`;
    }).join('');

    container.innerHTML = brokerCard + cbCard + portfolioCard + strategiesHtml;
  } catch (err) {
    // Health endpoint may not be available in standalone mode
    document.getElementById('health-panel').style.display = 'none';
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Strategies / Bots
// ─────────────────────────────────────────────────────────────────────────

async function updateStrategies() {
  try {
    const resp = await fetch(`${API_BASE}/strategies/list?active_only=true`);
    const data = await resp.json();

    strategies = data.strategies || [];

    // Update bot cards
    const container = document.getElementById('bots-container');
    if (!strategies.length) {
      container.innerHTML = '<div class="muted">No active bots detected from live health telemetry.</div>';
      return;
    }
    container.innerHTML = strategies.map(bot => {
      const symStatus = bot.symbol_status || {};
      const symCount = Object.keys(symStatus).length;
      const expanded = expandedBots.has(bot.strategy);
      const toggleIcon = expanded ? '▾' : '▸';
      const summaryTooltip = formatStatusBadgeSummary(symStatus);
      const symbolsBadge = symCount > 0
        ? `<span class="symbols-badge" title="${escapeAttr(summaryTooltip)}">${symCount} Symbols</span>`
        : '';
      return `
      <div class="bot-card" data-bot="${bot.strategy}">
        <div class="bot-card-header" data-toggle="bot" data-bot="${bot.strategy}">
          <span class="bot-toggle">${toggleIcon}</span>
          <h3>${bot.strategy}</h3>
          ${symbolsBadge}
          <span class="bot-status-badge ${bot.running ? 'running' : 'stopped'}">
            ${bot.running ? 'RUNNING' : 'STOPPED'}
          </span>
        </div>
        <div class="bot-card-stats">
          <div class="bot-card-stat">
            <span>Equity:</span>
            <strong>${formatCurrency(bot.equity)}</strong>
          </div>
          <div class="bot-card-stat">
            <span>Peak:</span>
            <strong>${formatCurrency(bot.peak_equity)}</strong>
          </div>
          <div class="bot-card-stat">
            <span>Drawdown:</span>
            <strong class="${bot.drawdown_pct < -10 ? 'negative' : ''}">${formatPercent(bot.drawdown_pct)}</strong>
          </div>
          <div class="bot-card-stat">
            <span>Open Positions:</span>
            <strong>${bot.open_positions}</strong>
          </div>
          <div class="bot-card-stat">
            <span>Today PnL:</span>
            <strong class="bot-card-pnl ${bot.pnl_today >= 0 ? 'positive' : 'negative'}">
              ${formatCurrency(bot.pnl_today)}
            </strong>
          </div>
          <div class="bot-card-stat">
            <span>Trades Today:</span>
            <strong>${bot.trades_today}</strong>
          </div>
          <div class="bot-telemetry-grid">
            <div class="telemetry-pill">
              <span>Last Bar</span>
              <strong>${bot.last_bar_ts ? new Date(bot.last_bar_ts).toLocaleTimeString() : '—'}</strong>
            </div>
            <div class="telemetry-pill">
              <span>Bar Age</span>
              <strong>${formatBarAge(bot.last_bar_ts)}</strong>
            </div>
            <div class="telemetry-pill">
              <span>Bar Lag</span>
              <strong>${formatLagMs(bot.last_bar_lag_ms)}</strong>
            </div>
            <div class="telemetry-pill">
              <span>Signals</span>
              <strong>${bot.signals_today || 0} / ${bot.signals_filtered_today || 0} filtered</strong>
            </div>
          </div>
          <div class="bot-status-summary">
            ${renderCompactStatusSummary(symStatus)}
          </div>
          <div class="muted" style="margin-top: 4px;">
            Updated: ${bot.last_equity_ts ? new Date(bot.last_equity_ts).toLocaleTimeString() : '—'}
          </div>
        </div>
        ${expanded ? `<div class="bot-symbol-status">${renderSymbolStatusTable(symStatus)}</div>` : ''}
      </div>
    `;
    }).join('');

    // Update strategy filter
    const strategyFilter = document.getElementById('trade-filter-strategy');
    const currentValue = strategyFilter.value;
    strategyFilter.innerHTML = '<option value="">All Strategies</option>' +
      strategies.map(s => `<option value="${s.strategy}">${s.strategy}</option>`).join('');
    strategyFilter.value = currentValue;
  } catch (err) {
    console.error('Strategies update failed:', err);
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Open Positions
// ─────────────────────────────────────────────────────────────────────────

async function updatePositions() {
  try {
    const resp = await fetch(`${API_BASE}/positions`);
    const positions = await resp.json();

    const tbody = document.getElementById('positions-body');
    if (!positions || positions.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="muted">No open positions</td></tr>';
      return;
    }

    tbody.innerHTML = positions.map(pos => `
      <tr>
        <td><strong>${pos.symbol}</strong></td>
        <td>${pos.qty.toFixed(1)}</td>
        <td>${formatCurrency(pos.entry_price)}</td>
        <td>${formatCurrency(pos.current_price)}</td>
        <td class="${pos.unrealized_pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">
          ${formatCurrency(pos.unrealized_pnl)} (${formatPercent(pos.unrealized_pnl_pct)})
        </td>
        <td>${pos.held_minutes || 0}m</td>
      </tr>
    `).join('');
  } catch (err) {
    console.error('Positions update failed:', err);
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Trades
// ─────────────────────────────────────────────────────────────────────────

async function updateTrades() {
  try {
    const strategy = document.getElementById('trade-filter-strategy').value;
    const days = document.getElementById('trade-filter-days').value || 30;

    const url = new URL(`${API_BASE}/trades`, window.location);
    url.searchParams.append('only_closed', 'true');
    url.searchParams.append('limit', 100);
    if (strategy) url.searchParams.append('strategy', strategy);

    const resp = await fetch(url);
    tradeData = await resp.json();

    updateTradesTable();
    updateSummaryStats();
  } catch (err) {
    console.error('Trades update failed:', err);
  }
}

function updateTradesTable() {
  const tbody = document.getElementById('trades-body');

  if (!tradeData || tradeData.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" class="muted">No trades found</td></tr>';
    return;
  }

  tbody.innerHTML = tradeData.map(trade => `
    <tr>
      <td>${new Date(trade.entry_ts).toLocaleDateString()}</td>
      <td><strong>${trade.strategy}</strong></td>
      <td><strong>${trade.symbol}</strong></td>
      <td>${trade.side.toUpperCase()}</td>
      <td>${formatCurrency(trade.entry_price)}</td>
      <td>${formatCurrency(trade.exit_price)}</td>
      <td class="${trade.pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">
        ${formatCurrency(trade.pnl)}
      </td>
      <td>${formatPercent(trade.pnl_pct)}</td>
      <td>${trade.mit_qty_factor ? trade.mit_qty_factor.toFixed(2) : '—'}</td>
      <td>${trade.ev_estimate ? trade.ev_estimate.toFixed(3) : '—'}</td>
      <td class="muted">${trade.reason || '—'}</td>
    </tr>
  `).join('');
}

function updateSummaryStats() {
  if (!tradeData || tradeData.length === 0) {
    return;
  }

  const totalTrades = tradeData.length;
  const winningTrades = tradeData.filter(t => t.pnl > 0).length;
  const losingTrades = tradeData.filter(t => t.pnl < 0).length;
  const winRate = totalTrades > 0 ? (winningTrades / totalTrades * 100).toFixed(1) : 0;
  const avgPnL = tradeData.reduce((sum, t) => sum + (t.pnl || 0), 0) / totalTrades;
  const totalPnL = tradeData.reduce((sum, t) => sum + (t.pnl || 0), 0);

  document.getElementById('stat-total-trades').textContent = totalTrades;
  document.getElementById('stat-winning').textContent = winningTrades;
  document.getElementById('stat-losing').textContent = losingTrades;
  document.getElementById('stat-winrate').textContent = winRate + '%';
  document.getElementById('stat-avg-pnl').textContent = formatCurrency(avgPnL);
  document.getElementById('stat-total-pnl').textContent = formatCurrency(totalPnL);
}

// ─────────────────────────────────────────────────────────────────────────
// Anomalies
// ─────────────────────────────────────────────────────────────────────────

async function updateAnomalies() {
  // Future: fetch anomaly_events table and display recent ones
  // For now, just show "No anomalies" placeholder
}

// ─────────────────────────────────────────────────────────────────────────
// Formatting utilities
// ─────────────────────────────────────────────────────────────────────────

function formatCurrency(value) {
  if (!value && value !== 0) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatPercent(value) {
  if (!value && value !== 0) return '—';
  const sign = value > 0 ? '+' : '';
  return sign + parseFloat(value).toFixed(2) + '%';
}

function escapeAttr(s) {
  return String(s || '')
    .replaceAll('&', '&amp;')
    .replaceAll('"', '&quot;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

// ─────────────────────────────────────────────────────────────────────────
// Event listeners
// ─────────────────────────────────────────────────────────────────────────

document.getElementById('trade-filter-strategy').addEventListener('change', () => {
  updateTrades();
});

document.getElementById('trade-filter-days').addEventListener('change', () => {
  updateTrades();
});

// Bot-Card Toggle: Symbol-Status-Tabelle ein-/ausklappen
document.getElementById('bots-container').addEventListener('click', (ev) => {
  const header = ev.target.closest('[data-toggle="bot"]');
  if (!header) return;
  const bot = header.dataset.bot;
  if (!bot) return;
  if (expandedBots.has(bot)) {
    expandedBots.delete(bot);
  } else {
    expandedBots.add(bot);
  }
  updateStrategies();
});

// ─────────────────────────────────────────────────────────────────────────
// Startup
// ─────────────────────────────────────────────────────────────────────────

(async () => {
  await refreshDashboard();
  setInterval(refreshDashboard, REFRESH_INTERVAL);
})();
