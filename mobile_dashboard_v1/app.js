const DATA_DIR = './data';

const state = {
  meta: {},
  tradePlan: [],
  positionMonitor: [],
  watchlistMonitor: [],
  fullSummary: [],
  localPositions: [],
  localWatchlist: [],
  uiBusy: false,
};

const STORAGE_KEYS = {
  positions: 'v1_stable_local_positions',
  watchlist: 'v1_stable_local_watchlist',
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
  lt_50: '50以下',
  p50_100: '50-100',
  p100_300: '100-300',
  p300_500: '300-500',
  p500_1000: '500-1000',
  gt_1000: '1000以上',
  unknown: '等待新資料',
};

function byId(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  const el = byId(id);
  if (el) el.textContent = value;
}

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
  try {
    return decodeURIComponent(escape(s));
  } catch {
    return s;
  }
}

function normalizeValue(value) {
  let s = tryDecodeMojibake(value).trim();

  if (VALUE_MAP[s] !== undefined) return VALUE_MAP[s];

  if (s === '未持有' || s === '已持有' || s === '目前沒有資料' || s === '等待新資料') return s;

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
    .replaceAll('ç­å¾æ°è³æ', '等待新資料');

  if (VALUE_MAP[s] !== undefined) return VALUE_MAP[s];
  return s;
}

function displayTier(value) {
  return normalizeValue(value || 'unknown');
}

function displayAction(value) {
  return normalizeValue(value || '');
}

function displayBucket(value) {
  return normalizeValue(value || '');
}

function displayHolding(value) {
  return normalizeValue(value || '');
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
  const text = await res.text();
  return parseCsv(text);
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

function loadLocalArray(key) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveLocalArray(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function syncLocalState() {
  state.localPositions = loadLocalArray(STORAGE_KEYS.positions);
  state.localWatchlist = loadLocalArray(STORAGE_KEYS.watchlist);
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
  if (refreshBtn) {
    refreshBtn.disabled = isBusy;
    refreshBtn.textContent = isBusy ? (label || '⏳ 讀取資料中...') : '🔄 重新整理頁面';
  }
}

function metaReady(meta) {
  return ['ok', 'fresh'].includes(String(meta?.data_state || '').toLowerCase());
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

  if (metaReady(meta)) {
    setMiniStatus('頁面資料已同步', 'ok');
    setBusy(false);
  } else if (String(meta.data_state || '').toLowerCase() === 'stale') {
    setMiniStatus('主資料尚未完整刷新，但頁面可正常操作', 'warn');
    setBusy(false);
  } else {
    setMiniStatus('資料讀取中，但不鎖定頁面', 'loading');
    setBusy(false);
  }
}

function getMergedPositions() {
  const map = new Map();

  for (const row of state.positionMonitor || []) {
    const id = String(row.stock_id || '').trim();
    if (!id) continue;
    map.set(id, {
      stock_id: id,
      price_tier: row.price_tier || 'unknown',
      ref_price: row.ref_price || '',
      shares: row.shares || '',
      avg_cost: row.avg_cost || '',
      pnl_pct: row.pnl_pct || '',
      action: row.action || 'HOLD',
      note: row.note || '',
    });
  }

  for (const row of state.localPositions || []) {
    const id = String(row.stock_id || '').trim();
    if (!id) continue;
    const old = map.get(id) || {};
    map.set(id, {
      stock_id: id,
      price_tier: old.price_tier || 'unknown',
      ref_price: old.ref_price || '',
      shares: row.shares || old.shares || '1000',
      avg_cost: row.avg_cost || old.avg_cost || '',
      pnl_pct: old.pnl_pct || '',
      action: old.action || 'LOCAL',
      note: row.note || old.note || '本地持倉',
    });
  }

  return Array.from(map.values()).sort((a, b) => a.stock_id.localeCompare(b.stock_id));
}

function getMergedWatchlist() {
  const map = new Map();

  for (const row of state.watchlistMonitor || []) {
    const id = String(row.stock_id || '').trim();
    if (!id) continue;
    map.set(id, {
      stock_id: id,
      price_tier: row.price_tier || 'unknown',
      ref_price: row.ref_price || '',
      holding_status: row.holding_status || '未持有',
      strategy_bucket: row.strategy_bucket || 'NONE',
      action: row.action || 'WATCH',
      pnl_pct: row.pnl_pct || '',
    });
  }

  for (const row of state.localWatchlist || []) {
    const id = String(row.stock_id || '').trim();
    if (!id) continue;
    const old = map.get(id) || {};
    map.set(id, {
      stock_id: id,
      price_tier: old.price_tier || 'unknown',
      ref_price: old.ref_price || '',
      holding_status: old.holding_status || '未持有',
      strategy_bucket: old.strategy_bucket || 'NONE',
      action: old.action || 'WATCH',
      pnl_pct: old.pnl_pct || '',
    });
  }

  return Array.from(map.values()).sort((a, b) => a.stock_id.localeCompare(b.stock_id));
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
      <td>${escapeHtml(displayAction(r.action || '—'))}</td>
      <td>${escapeHtml(r.stock_id || '—')}</td>
      <td>${escapeHtml(displayTier(r.price_tier || '—'))}</td>
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

  const rows = getMergedPositions();
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7">目前沒有持倉</td></tr>';
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(r.stock_id)}</td>
      <td>${escapeHtml(displayTier(r.price_tier || 'unknown'))}</td>
      <td>${escapeHtml(r.ref_price ? fmtNum(r.ref_price, 3) : '等待新資料')}</td>
      <td>${escapeHtml(fmtNum(r.shares, 0))}</td>
      <td>${escapeHtml(fmtNum(r.avg_cost, 3))}</td>
      <td>${escapeHtml(r.pnl_pct === '' ? '目前沒有資料' : fmtPct(r.pnl_pct))}</td>
      <td><button class="danger-btn" data-remove-position="${escapeHtml(r.stock_id)}">移除</button></td>
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

  const rows = getMergedWatchlist();
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="8">目前沒有自選股</td></tr>';
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(r.stock_id)}</td>
      <td>${escapeHtml(displayTier(r.price_tier || 'unknown'))}</td>
      <td>${escapeHtml(r.ref_price ? fmtNum(r.ref_price, 3) : '等待新資料')}</td>
      <td>${escapeHtml(displayHolding(r.holding_status || '未持有'))}</td>
      <td>${escapeHtml(displayBucket(r.strategy_bucket || 'NONE'))}</td>
      <td>${escapeHtml(displayAction(r.action || 'WATCH'))}</td>
      <td>${escapeHtml(r.pnl_pct === '' ? '目前沒有資料' : fmtPct(r.pnl_pct))}</td>
      <td><button class="danger-btn" data-remove-watch="${escapeHtml(r.stock_id)}">移除</button></td>
    `;
    tbody.appendChild(tr);
  });

  tbody.querySelectorAll('[data-remove-watch]').forEach(btn => {
    btn.addEventListener('click', () => removeWatch(btn.dataset.removeWatch));
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

function addPosition() {
  const stock = byId('pos_stock')?.value.trim();
  const shares = byId('pos_shares')?.value.trim();
  const cost = byId('pos_cost')?.value.trim();

  if (!stock) {
    setMiniStatus('請輸入股票代號', 'warn');
    return;
  }

  const rows = loadLocalArray(STORAGE_KEYS.positions).filter(r => String(r.stock_id) !== stock);
  rows.push({
    stock_id: stock,
    shares: shares || '1000',
    avg_cost: cost || '',
    note: '本地持倉',
  });
  saveLocalArray(STORAGE_KEYS.positions, rows);
  syncLocalState();
  renderPositions();

  if (byId('pos_stock')) byId('pos_stock').value = '';
  if (byId('pos_shares')) byId('pos_shares').value = '';
  if (byId('pos_cost')) byId('pos_cost').value = '';

  setMiniStatus(`已加入持倉 ${stock}`, 'ok');
}

function removePosition(stock) {
  if (!window.confirm(`確定要移除持倉 ${stock} 嗎？`)) return;
  const rows = loadLocalArray(STORAGE_KEYS.positions).filter(r => String(r.stock_id) !== stock);
  saveLocalArray(STORAGE_KEYS.positions, rows);
  syncLocalState();
  renderPositions();
  setMiniStatus(`已移除持倉 ${stock}`, 'ok');
}

function addWatch() {
  const stock = byId('watch_stock')?.value.trim();
  if (!stock) {
    setMiniStatus('請輸入自選股代號', 'warn');
    return;
  }

  const rows = loadLocalArray(STORAGE_KEYS.watchlist);
  if (!rows.some(r => String(r.stock_id) === stock)) rows.push({ stock_id: stock });
  saveLocalArray(STORAGE_KEYS.watchlist, rows);
  syncLocalState();
  renderWatchlist();

  if (byId('watch_stock')) byId('watch_stock').value = '';
  setMiniStatus(`已加入自選股 ${stock}`, 'ok');
}

function removeWatch(stock) {
  if (!window.confirm(`確定要移除自選股 ${stock} 嗎？`)) return;
  const rows = loadLocalArray(STORAGE_KEYS.watchlist).filter(r => String(r.stock_id) !== stock);
  saveLocalArray(STORAGE_KEYS.watchlist, rows);
  syncLocalState();
  renderWatchlist();
  setMiniStatus(`已移除自選股 ${stock}`, 'ok');
}

async function refreshAll() {
  setBusy(true, '⏳ 讀取資料中...');
  setMiniStatus('讀取中...', 'loading');

  try {
    syncLocalState();
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
    setBusy(false);
    renderTradePlan();
    renderPositions();
    renderWatchlist();
    renderSummary();
  } finally {
    if (!metaReady(state.meta)) {
      setBusy(false);
    }
  }
}

function bindEvents() {
  const refreshBtn = byId('refresh_btn');
  const posAddBtn = byId('pos_add_btn');
  const watchAddBtn = byId('watch_add_btn');

  if (refreshBtn) refreshBtn.addEventListener('click', refreshAll);
  if (posAddBtn) posAddBtn.addEventListener('click', addPosition);
  if (watchAddBtn) watchAddBtn.addEventListener('click', addWatch);
}

document.addEventListener('DOMContentLoaded', () => {
  syncLocalState();
  bindEvents();
  renderTradePlan();
  renderPositions();
  renderWatchlist();
  renderSummary();
  refreshAll();
});
