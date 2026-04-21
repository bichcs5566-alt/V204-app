const DATA_DIR = './data';

const state = {
  meta: {},
  tradePlan: [],
  positionMonitor: [],
  watchlistMonitor: [],
  fullSummary: [],
  uiBusy: false,
};

const VALUE_MAP = {
  BUY: '買進',
  SELL: '賣出',
  ADD: '加碼',
  REDUCE: '減碼',
  HOLD: '持有',
  HOLD_MONITOR: '持有監控',
  WATCH: '觀察',
  BUY_READY: '可買候選',
  CANDIDATE: '候選',
  STOP_LOSS: '停損',
  NONE: '未進策略',
  LOCAL: '本地資料',
  ok: '✅ 最新資料',
  fresh: '✅ 最新資料',
  stale: '⚠️ 主資料未完整刷新',
  loading: '⏳ 讀取中',
  submitted: '已送出，等待資料同步與策略重算',
  synced: '已同步',
  idle: '待命',
  failed: '失敗，請重試',
  lt_50: '50以下',
  p50_100: '50-100',
  p100_300: '100-300',
  p300_500: '300-500',
  p500_1000: '500-1000',
  gt_1000: '1000以上',
  unknown: '等待新資料',
};

function byId(id) { return document.getElementById(id); }
function setText(id, value) { const el = byId(id); if (el) el.textContent = value; }

function toNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function fmtNum(v, digits = 3) {
  const n = toNum(v);
  if (n === null) return '—';
  return n.toLocaleString('zh-TW', {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function fmtPct(v) {
  const n = toNum(v);
  if (n === null) return '目前沒有資料';
  return `${(n * 100).toFixed(2)}%`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function tryDecodeMojibake(text) {
  if (text === null || text === undefined) return '';
  const s = String(text);
  if (!/[ÃÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]/.test(s)) {
    return s;
  }
  try { return decodeURIComponent(escape(s)); } catch { return s; }
}

function normalizeValue(value) {
  let s = tryDecodeMojibake(value).trim();
  if (VALUE_MAP[s] !== undefined) return VALUE_MAP[s];

  s = s
    .replaceAll('æªææ', '未持有')
    .replaceAll('å·²ææ', '已持有')
    .replaceAll('è²·é²', '買進')
    .replaceAll('è³£åº', '賣出')
    .replaceAll('è§å¯', '觀察')
    .replaceAll('æ¸ç¢¼', '減碼')
    .replaceAll('å ç¢¼', '加碼')
    .replaceAll('ææ', '持有')
    .replaceAll('æªé²ç­ç¥', '未進策略')
    .replaceAll('ç­å¾æ°è³æ', '等待新資料')
    .replaceAll('æ°é²å ´', '新進場');

  return VALUE_MAP[s] ?? s;
}

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function parseCsv(text) {
  const raw = text.replace(/\r/g, '').split('\n').filter(line => line.trim() !== '');
  if (!raw.length) return [];
  const rows = raw.map(parseCsvLine);
  const headers = rows[0].map(h => String(h).trim());
  return rows.slice(1).map(cols => {
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = tryDecodeMojibake(String(cols[i] ?? '').trim());
    });
    return obj;
  });
}

async function fetchCsv(name) {
  const res = await fetch(`${DATA_DIR}/${name}?t=${Date.now()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${name} 讀取失敗`);
  return parseCsv(await res.text());
}

async function fetchJson(name) {
  const res = await fetch(`${DATA_DIR}/${name}?t=${Date.now()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${name} 讀取失敗`);
  const data = await res.json();
  const fixed = {};
  Object.keys(data || {}).forEach(k => {
    fixed[k] = typeof data[k] === 'string' ? tryDecodeMojibake(data[k]) : data[k];
  });
  return fixed;
}

function setMiniStatus(text, kind = 'normal') {
  const el = byId('ui_status');
  if (!el) return;
  el.textContent = text;
  el.className = `mini-status ${kind}`;
}

function setBusy(isBusy, label = '') {
  state.uiBusy = isBusy;
  const refreshBtn = byId('refresh_btn');
  const pipelineBtn = byId('pipeline_btn');

  if (refreshBtn) {
    refreshBtn.disabled = isBusy;
    refreshBtn.textContent = isBusy ? (label || '⏳ 讀取資料中...') : '🔄 重新整理頁面';
  }
  if (pipelineBtn) {
    pipelineBtn.disabled = isBusy;
  }
}

function metaReady(meta) {
  return ['ok', 'fresh'].includes(String(meta?.data_state || '').toLowerCase());
}

function getCfg() {
  return window.GITHUB_CONFIG || {};
}

async function dispatchWorkflow(workflowFile, inputs = {}) {
  const cfg = getCfg();
  if (!cfg.owner || !cfg.repo || !cfg.token || !cfg.branch) {
    throw new Error('github_config.js 尚未填入 owner / repo / token / branch');
  }

  const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${workflowFile}/dispatches`;

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Accept': 'application/vnd.github+json',
      'Authorization': `Bearer ${cfg.token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      ref: cfg.branch,
      inputs,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`workflow dispatch 失敗: ${res.status} ${text}`);
  }
}

async function runPipeline() {
  try {
    setBusy(true, '⏳ 送出 pipeline 中...');
    const cfg = getCfg();
    await dispatchWorkflow(cfg.workflows?.pipeline || 'v2_pipeline.yml', {});
    setMiniStatus('已送出 pipeline，請稍候 1-2 分鐘後再重新整理', 'warn');
  } catch (err) {
    console.error(err);
    setMiniStatus(`送出 pipeline 失敗：${err.message}`, 'error');
  } finally {
    setBusy(false);
  }
}

function renderMeta() {
  const meta = state.meta || {};
  setText('now_time', meta.now_time || new Date().toLocaleString('zh-TW'));
  setText('last_update', meta.generated_at || '—');
  setText('signal_date', meta.signal_date || '—');
  setText('trade_date', meta.trade_date || '—');
  setText('data_state', normalizeValue(String(meta.data_state || 'loading')));

  const batch = meta.trade_plan_batch || meta.generated_at || '';
  setText('trade_plan_batch', batch ? `🟢 已更新（${batch}）` : '—');
  setText('position_writeback_state', normalizeValue(meta.position_writeback_state || 'idle'));

  if (metaReady(meta)) {
    setMiniStatus('頁面資料已同步', 'ok');
  } else if (String(meta.data_state || '').toLowerCase() === 'stale') {
    setMiniStatus('主資料尚未完整刷新，但頁面可正常操作', 'warn');
  } else {
    setMiniStatus('資料讀取中，但不鎖定頁面', 'loading');
  }
}

function renderTradePlan() {
  const tbody = byId('trade_plan_tbody');
  if (!tbody) return;
  tbody.innerHTML = '';

  const rows = Array.isArray(state.tradePlan) ? state.tradePlan : [];
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7">目前沒有資料</td></tr>';
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(normalizeValue(r.action || '—'))}</td>
      <td>${escapeHtml(r.stock_id || '—')}</td>
      <td>${escapeHtml(normalizeValue(r.price_tier || 'unknown'))}</td>
      <td>${escapeHtml(fmtNum(r.ref_price, 3))}</td>
      <td>${escapeHtml(fmtNum(r.target_weight, 3))}</td>
      <td>${escapeHtml(fmtNum(r.suggested_amount, 0))}</td>
      <td>${escapeHtml(normalizeValue(r.note || ''))}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderPositions() {
  const tbody = byId('positions_tbody');
  if (!tbody) return;
  tbody.innerHTML = '';

  const rows = Array.isArray(state.positionMonitor) ? state.positionMonitor : [];
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="8">目前沒有持倉</td></tr>';
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(r.stock_id || '')}</td>
      <td>${escapeHtml(normalizeValue(r.price_tier || 'unknown'))}</td>
      <td>${escapeHtml(r.ref_price ? fmtNum(r.ref_price, 3) : '等待新資料')}</td>
      <td>${escapeHtml(fmtNum(r.shares, 0))}</td>
      <td>${escapeHtml(fmtNum(r.avg_cost, 3))}</td>
      <td>${escapeHtml(r.pnl_pct === '' ? '目前沒有資料' : fmtPct(r.pnl_pct))}</td>
      <td>${escapeHtml(normalizeValue(r.action || 'HOLD'))}</td>
      <td><button class="danger-btn" data-remove-position="${escapeHtml(r.stock_id || '')}">移除</button></td>
    `;
    tbody.appendChild(tr);
  });

  tbody.querySelectorAll('[data-remove-position]').forEach(btn => {
    btn.addEventListener('click', () => removePosition(btn.dataset.removePosition));
  });
}

function renderWatchlist() {
  const tbody = byId('watchlist_tbody');
  if (!tbody) return;
  tbody.innerHTML = '';

  const rows = Array.isArray(state.watchlistMonitor) ? state.watchlistMonitor : [];
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7">目前沒有自選股</td></tr>';
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(r.stock_id || '')}</td>
      <td>${escapeHtml(normalizeValue(r.price_tier || 'unknown'))}</td>
      <td>${escapeHtml(r.ref_price ? fmtNum(r.ref_price, 3) : '等待新資料')}</td>
      <td>${escapeHtml(normalizeValue(r.holding_status || '未持有'))}</td>
      <td>${escapeHtml(normalizeValue(r.strategy_bucket || 'NONE'))}</td>
      <td>${escapeHtml(normalizeValue(r.action || 'WATCH'))}</td>
      <td>${escapeHtml(r.pnl_pct === '' ? '目前沒有資料' : fmtPct(r.pnl_pct))}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderSummary() {
  const box = byId('summary_box');
  if (!box) return;
  const s = Array.isArray(state.fullSummary) && state.fullSummary.length ? state.fullSummary[0] : {};
  box.innerHTML = `
    <div class="summary-item"><strong>報酬</strong><span>${escapeHtml(fmtPct(s.return || 0))}</span></div>
    <div class="summary-item"><strong>MDD</strong><span>${escapeHtml(fmtPct(s.mdd || 0))}</span></div>
    <div class="summary-item"><strong>Sharpe</strong><span>${escapeHtml(fmtNum(s.sharpe_daily || 0, 3))}</span></div>
  `;
}

async function addPosition() {
  const stock = byId('pos_stock')?.value.trim();
  const shares = byId('pos_shares')?.value.trim();
  const cost = byId('pos_cost')?.value.trim();

  if (!stock) {
    setMiniStatus('請輸入股票代號', 'warn');
    return;
  }

  try {
    setBusy(true, '⏳ 送出持倉中...');
    const cfg = getCfg();
    await dispatchWorkflow(cfg.workflows?.positionWriteback || 'v2_position_writeback.yml', {
      action_type: 'upsert',
      stock_id: stock,
      shares: shares || '1000',
      avg_cost: cost || '',
      note: '前端送出'
    });
    setMiniStatus(`已送出持倉 ${stock}，請再按一次「更新資料與策略」`, 'warn');

    if (byId('pos_stock')) byId('pos_stock').value = '';
    if (byId('pos_shares')) byId('pos_shares').value = '';
    if (byId('pos_cost')) byId('pos_cost').value = '';
  } catch (err) {
    console.error(err);
    setMiniStatus(`持倉送出失敗：${err.message}`, 'error');
  } finally {
    setBusy(false);
  }
}

async function removePosition(stock) {
  if (!window.confirm(`確定要移除持倉 ${stock} 嗎？`)) return;

  try {
    setBusy(true, '⏳ 送出刪除中...');
    const cfg = getCfg();
    await dispatchWorkflow(cfg.workflows?.positionWriteback || 'v2_position_writeback.yml', {
      action_type: 'delete',
      stock_id: stock,
      shares: '',
      avg_cost: '',
      note: '前端刪除'
    });
    setMiniStatus(`已送出移除持倉 ${stock}，請再按一次「更新資料與策略」`, 'warn');
  } catch (err) {
    console.error(err);
    setMiniStatus(`移除持倉失敗：${err.message}`, 'error');
  } finally {
    setBusy(false);
  }
}

async function refreshAll() {
  setBusy(true, '⏳ 讀取資料中...');
  setMiniStatus('讀取中...', 'loading');

  try {
    const [meta, tradePlan, positionMonitor, watchlistMonitor, fullSummary] = await Promise.all([
      fetchJson('meta.json'),
      fetchCsv('trade_plan.csv'),
      fetchCsv('position_monitor.csv'),
      fetchCsv('watchlist_monitor.csv'),
      fetchCsv('full_summary.csv'),
    ]);

    state.meta = meta || {};
    state.tradePlan = tradePlan || [];
    state.positionMonitor = positionMonitor || [];
    state.watchlistMonitor = watchlistMonitor || [];
    state.fullSummary = fullSummary || [];

    renderMeta();
    renderTradePlan();
    renderPositions();
    renderWatchlist();
    renderSummary();
  } catch (err) {
    console.error(err);
    setMiniStatus(`讀取失敗：${err.message}`, 'error');
  } finally {
    setBusy(false);
  }
}

function bindEvents() {
  byId('refresh_btn')?.addEventListener('click', refreshAll);
  byId('pipeline_btn')?.addEventListener('click', runPipeline);
  byId('pos_add_btn')?.addEventListener('click', addPosition);
}

document.addEventListener('DOMContentLoaded', () => {
  bindEvents();
  renderTradePlan();
  renderPositions();
  renderWatchlist();
  renderSummary();
  refreshAll();
});
