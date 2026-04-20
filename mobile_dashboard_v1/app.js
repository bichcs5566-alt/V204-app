
const DATA_BASE = "./data";

const TIER_LABELS = {
  lt_50: "50以下",
  p50_100: "50-100",
  p100_300: "100-300",
  p300_500: "300-500",
  p500_1000: "500-1000",
  gt_1000: "1000以上",
  unknown: "未知"
};

const ACTION_LABELS = {
  BUY: "買進",
  SELL: "賣出",
  HOLD: "續抱",
  REDUCE: "減碼",
  ADD: "加碼",
  STOP_LOSS: "停損",
  WATCH: "觀察",
  CANDIDATE: "候選",
  BUY_READY: "可買",
  HOLD_MONITOR: "持有監控",
  NONE: "未進策略",
  IGNORE: "忽略"
};

let tradeRows = [];
let positionRows = [];
let watchRows = [];
let summaryRows = [];
let debugRows = [];
let localPositions = [];
let localWatchlist = [];

function mapActionLabel(v) {
  const key = String(v || "").trim().toUpperCase();
  return ACTION_LABELS[key] || v || "";
}
function actionClass(v) { return String(v || "").trim().toLowerCase(); }
function tierLabel(v) { return TIER_LABELS[String(v || "").trim()] || v || ""; }
function formatNumber(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return v ?? "";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 2 });
}
function nowText() { return new Date().toLocaleString("zh-TW"); }
function saveLocal(name, value) { localStorage.setItem(name, JSON.stringify(value)); }
function loadLocal(name, fallback = []) {
  try { return JSON.parse(localStorage.getItem(name) || JSON.stringify(fallback)); }
  catch { return fallback; }
}

async function fetchText(path) {
  const res = await fetch(path + "?t=" + Date.now(), { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} 讀取失敗 (${res.status})`);
  return await res.text();
}

function parseCSV(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    const next = text[i + 1];
    if (ch === '"') {
      if (inQuotes && next === '"') { cell += '"'; i++; }
      else { inQuotes = !inQuotes; }
    } else if (ch === "," && !inQuotes) {
      row.push(cell); cell = "";
    } else if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i++;
      row.push(cell);
      if (row.some(v => String(v).trim() !== "")) rows.push(row);
      row = []; cell = "";
    } else {
      cell += ch;
    }
  }
  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    if (row.some(v => String(v).trim() !== "")) rows.push(row);
  }
  if (!rows.length) return [];
  const headers = rows[0].map(h => String(h).trim());
  return rows.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, idx) => obj[h] = (r[idx] ?? "").trim());
    return obj;
  });
}

async function loadCSV(file, optional = false) {
  try {
    const text = await fetchText(`${DATA_BASE}/${file}`);
    return parseCSV(text);
  } catch (err) {
    if (optional) return [];
    throw err;
  }
}

function updateStatus(lastUpdate, signalDate, tradeDate, stateText, stateClass) {
  document.getElementById("lastUpdate").textContent = lastUpdate || "--";
  document.getElementById("signalDate").textContent = signalDate || "--";
  document.getElementById("tradeDate").textContent = tradeDate || "--";
  const el = document.getElementById("dataState");
  el.textContent = stateText || "--";
  el.className = stateClass || "";
}

function deriveDates() {
  let signalDate = "";
  let tradeDate = "";
  if (tradeRows.length) {
    signalDate = tradeRows[0].signal_date || "";
    tradeDate = tradeRows[0].trade_date || "";
  } else if (positionRows.length) {
    signalDate = positionRows[0].signal_date || "";
    tradeDate = positionRows[0].trade_date || "";
  } else if (watchRows.length) {
    signalDate = watchRows[0].signal_date || "";
    tradeDate = watchRows[0].trade_date || "";
  }
  return { signalDate, tradeDate };
}

function renderTradeTable() {
  const tbody = document.querySelector("#trade-table tbody");
  const selectedTier = document.getElementById("tierFilter").value;
  let rows = [...tradeRows];
  if (selectedTier !== "全部") rows = rows.filter(r => String(r.price_tier || "") === selectedTier);
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted">目前沒有符合條件的資料</td></tr>';
    return;
  }
  rows.forEach(r => {
    const stockId = String(r.stock_id || "");
    tbody.innerHTML += `
      <tr>
        <td class="${actionClass(r.action)}">${mapActionLabel(r.action)}</td>
        <td>${stockId}</td>
        <td>${tierLabel(r.price_tier)}</td>
        <td>${formatNumber(r.ref_price)}</td>
        <td>${r.target_weight || ""}</td>
        <td>${formatNumber(r.suggested_amount)}</td>
        <td>${r.note || ""}</td>
        <td><button class="sync-btn" data-stock="${stockId}" data-price="${r.ref_price || ''}" type="button">加入持倉</button></td>
      </tr>`;
  });
  tbody.querySelectorAll(".sync-btn").forEach(btn => {
    btn.addEventListener("click", () => addTradeToPosition(btn.dataset.stock, btn.dataset.price));
  });
}

function renderPositionTable() {
  const tbody = document.querySelector("#position-table tbody");
  tbody.innerHTML = "";
  const merged = [...positionRows];
  localPositions.forEach(p => {
    if (!merged.find(x => String(x.stock_id) === String(p.stock_id))) {
      merged.push({
        stock_id: p.stock_id, price_tier: "unknown", ref_price: "",
        shares: p.shares, avg_cost: p.avg_cost, pnl_pct: "",
        target_weight: "", current_weight_est: "", action: "HOLD",
        note: "前端新增，待後端同步"
      });
    }
  });
  if (!merged.length) {
    tbody.innerHTML = '<tr><td colspan="11" class="muted">目前沒有持倉監控資料</td></tr>';
    return;
  }
  merged.forEach(r => {
    const stockId = String(r.stock_id || "");
    tbody.innerHTML += `
      <tr>
        <td>${stockId}</td>
        <td>${tierLabel(r.price_tier)}</td>
        <td>${formatNumber(r.ref_price)}</td>
        <td>${formatNumber(r.shares)}</td>
        <td>${formatNumber(r.avg_cost)}</td>
        <td>${r.pnl_pct || ""}</td>
        <td>${r.target_weight || ""}</td>
        <td>${r.current_weight_est || ""}</td>
        <td class="${actionClass(r.action)}">${mapActionLabel(r.action)}</td>
        <td>${r.note || ""}</td>
        <td><button class="remove-btn" data-stock="${stockId}" type="button">移除</button></td>
      </tr>`;
  });
  tbody.querySelectorAll(".remove-btn").forEach(btn => {
    btn.addEventListener("click", () => removePosition(btn.dataset.stock));
  });
}

function renderWatchTable() {
  const tbody = document.querySelector("#watch-table tbody");
  tbody.innerHTML = "";
  const merged = [...watchRows];
  localWatchlist.forEach(code => {
    if (!merged.find(x => String(x.stock_id) === String(code))) {
      merged.push({
        stock_id: code, price_tier: "unknown", ref_price: "",
        holding_status: "未持有", strategy_bucket: "NONE",
        action: "WATCH", pnl_pct: ""
      });
    }
  });
  if (!merged.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="muted">目前沒有自選股監控資料</td></tr>';
    return;
  }
  merged.forEach(r => {
    const stockId = String(r.stock_id || "");
    tbody.innerHTML += `
      <tr>
        <td>${stockId}</td>
        <td>${tierLabel(r.price_tier)}</td>
        <td>${formatNumber(r.ref_price)}</td>
        <td>${r.holding_status || ""}</td>
        <td>${mapActionLabel(r.strategy_bucket)}</td>
        <td class="${actionClass(r.action)}">${mapActionLabel(r.action)}</td>
        <td>${r.pnl_pct || ""}</td>
        <td><button class="remove-btn" data-stock="${stockId}" type="button">移除</button></td>
      </tr>`;
  });
  tbody.querySelectorAll(".remove-btn").forEach(btn => {
    btn.addEventListener("click", () => removeWatch(btn.dataset.stock));
  });
}

function renderSummaryTable() {
  const tbody = document.querySelector("#summary-table tbody");
  tbody.innerHTML = "";
  if (!summaryRows.length) {
    tbody.innerHTML = '<tr><td colspan="3" class="muted">目前沒有績效摘要資料</td></tr>';
    return;
  }
  summaryRows.forEach(r => {
    tbody.innerHTML += `<tr><td>${r.return || ""}</td><td>${r.mdd || ""}</td><td>${r.sharpe_daily || ""}</td></tr>`;
  });
}

function renderDebugTable() {
  const tbody = document.querySelector("#debug-table tbody");
  tbody.innerHTML = "";
  if (!debugRows.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="muted">目前沒有篩選除錯資料</td></tr>';
    return;
  }
  debugRows.forEach(r => {
    tbody.innerHTML += `
      <tr>
        <td>${r.total_input || ""}</td>
        <td>${r.valid_after_na || ""}</td>
        <td>${r.core_primary_count || ""}</td>
        <td>${r.alpha_primary_count || ""}</td>
        <td>${r.core_final_count || ""}</td>
        <td>${r.alpha_final_count || ""}</td>
      </tr>`;
  });
}

function addTradeToPosition(stockId, refPrice) {
  const shares = prompt(`請輸入 ${stockId} 的持有股數`, "100");
  if (shares === null) return;
  const cost = prompt(`請輸入 ${stockId} 的平均成本`, refPrice || "");
  if (cost === null) return;
  const row = {
    stock_id: String(stockId),
    shares: String(shares).trim(),
    avg_cost: String(cost).trim(),
    last_action_date: new Date().toISOString().slice(0, 10),
    note: "由今日操作加入持倉"
  };
  const idx = localPositions.findIndex(x => String(x.stock_id) === String(stockId));
  if (idx >= 0) localPositions[idx] = row;
  else localPositions.push(row);
  saveLocal("current_positions_v26652", localPositions);
  renderPositionTable();
}

function addPositionManual() {
  const stockId = document.getElementById("posCode").value.trim();
  const shares = document.getElementById("posShares").value.trim();
  const avgCost = document.getElementById("posCost").value.trim();
  if (!stockId || !shares || !avgCost) return;
  const row = {
    stock_id: stockId, shares, avg_cost: avgCost,
    last_action_date: new Date().toISOString().slice(0, 10),
    note: "由持倉監控新增"
  };
  const idx = localPositions.findIndex(x => String(x.stock_id) === stockId);
  if (idx >= 0) localPositions[idx] = row;
  else localPositions.push(row);
  saveLocal("current_positions_v26652", localPositions);
  document.getElementById("posCode").value = "";
  document.getElementById("posShares").value = "";
  document.getElementById("posCost").value = "";
  renderPositionTable();
}

function removePosition(stockId) {
  localPositions = localPositions.filter(x => String(x.stock_id) !== String(stockId));
  saveLocal("current_positions_v26652", localPositions);
  renderPositionTable();
}

function addWatchManual() {
  const code = document.getElementById("watchCode").value.trim();
  if (!code) return;
  if (!localWatchlist.includes(code)) localWatchlist.push(code);
  saveLocal("watchlist_v26652", localWatchlist);
  document.getElementById("watchCode").value = "";
  renderWatchTable();
}

function removeWatch(stockId) {
  localWatchlist = localWatchlist.filter(x => String(x) !== String(stockId));
  saveLocal("watchlist_v26652", localWatchlist);
  renderWatchTable();
}

async function init() {
  try {
    updateStatus(nowText(), "--", "--", "讀取中...", "");
    const [trade, posMon, watchMon, summary, debug, posCsv, watchCsv] = await Promise.all([
      loadCSV("trade_plan.csv"),
      loadCSV("position_monitor.csv", true),
      loadCSV("watchlist_monitor.csv", true),
      loadCSV("full_summary.csv", true),
      loadCSV("selection_debug.csv", true),
      loadCSV("current_positions.csv", true),
      loadCSV("watchlist.csv", true),
    ]);
    tradeRows = trade;
    positionRows = posMon;
    watchRows = watchMon;
    summaryRows = summary;
    debugRows = debug;
    const localPos = loadLocal("current_positions_v26652", []);
    localPositions = localPos.length ? localPos : posCsv;
    const localWatch = loadLocal("watchlist_v26652", []);
    localWatchlist = localWatch.length ? localWatch : watchCsv.map(x => String(x.stock_id || "").trim()).filter(Boolean);
    const dates = deriveDates();
    updateStatus(nowText(), dates.signalDate || "--", dates.tradeDate || "--", "✅ 最新資料", "state-fresh");
    renderTradeTable();
    renderPositionTable();
    renderWatchTable();
    renderSummaryTable();
    renderDebugTable();
  } catch (err) {
    console.error(err);
    updateStatus(nowText(), "--", "--", "❌ 讀取失敗", "state-fail");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("refreshBtn").addEventListener("click", init);
  document.getElementById("tierFilter").addEventListener("change", renderTradeTable);
  document.getElementById("addPositionBtn").addEventListener("click", addPositionManual);
  document.getElementById("addWatchBtn").addEventListener("click", addWatchManual);
  init();
});
