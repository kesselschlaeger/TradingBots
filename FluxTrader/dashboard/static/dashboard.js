// FluxTrader Dashboard – Real-time multi-bot monitoring
// Reads from /api/* endpoints (read-only from PersistentState)

const API_BASE = '/api';
const REFRESH_INTERVAL = 3000; // 3s

let equityChart = null;
let tradeData = [];
let strategies = [];

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
    const strategiesHtml = (data.strategies || []).map(s => `
      <div class="health-card">
        <h4>📡 ${s.name}</h4>
        <div class="health-item ${s.bar_lag_ms && s.bar_lag_ms < 1000 ? 'ok' : 'warning'}">
          <span>Bar Lag:</span>
          <span>${s.bar_lag_ms ? s.bar_lag_ms.toFixed(0) + 'ms' : '—'}</span>
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
      </div>
    `).join('');

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
    const resp = await fetch(`${API_BASE}/strategies/list`);
    const data = await resp.json();

    strategies = data.strategies || [];

    // Update bot cards
    const container = document.getElementById('bots-container');
    container.innerHTML = strategies.map(bot => `
      <div class="bot-card">
        <div class="bot-card-header">
          <h3>${bot.strategy}</h3>
          <span class="bot-status-badge running">RUNNING</span>
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
          <div class="muted" style="margin-top: 4px;">
            Updated: ${new Date(bot.last_equity_ts).toLocaleTimeString()}
          </div>
        </div>
      </div>
    `).join('');

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

// ─────────────────────────────────────────────────────────────────────────
// Event listeners
// ─────────────────────────────────────────────────────────────────────────

document.getElementById('trade-filter-strategy').addEventListener('change', () => {
  updateTrades();
});

document.getElementById('trade-filter-days').addEventListener('change', () => {
  updateTrades();
});

// ─────────────────────────────────────────────────────────────────────────
// Startup
// ─────────────────────────────────────────────────────────────────────────

(async () => {
  await refreshDashboard();
  setInterval(refreshDashboard, REFRESH_INTERVAL);
})();
