const DATA_BASE = "./data";

let tradeRows = [];
let watchlistMonitor = [];
let positionMonitor = [];
let currentPositions = [];
let currentWatchlist = [];

const TIER_LABELS = {
  "lt_50": "50以下",
  "p50_100": "50-100",
  "p100_300": "100-300",
  "p300_500": "300-500",
  "p500_1000": "500-1000",
  "gt_1000": "1000以上",
  "unknown": "未知"
};

async function fetchText(path) {
  const res = await fetch(path + "?t=" + Date.now(), { cache: "no-store" });
  if (!res.ok) throw new Error(`讀取失敗: ${path} (${res.status})`);
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
      if (inQuotes && next === '"') {
        cell += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      row.push(cell);
      cell = "";
    } else if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i++;
      row.push(cell);
      if (row.some(v => String(v).trim() !== "")) rows.push(row);
      row = [];
      cell = "";
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

function toCSV(rows, headers) {
  const escapeCell = (v) => {
    const s = String(v ?? "");
    if (s.includes(",") || s.includes('"') || s.includes("\n")) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };
  const lines = [headers.join(",")];
  rows.forEach(r => {
    lines.push(headers.map(h => escapeCell(r[h])).join(","));
  });
  return lines.join("\n");
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

function formatTW(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return v ?? "";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 2 });
}

function tierLabel(key) {
  return TIER_LABELS[key] || key || "";
}

function saveLocal(name, value) {
  localStorage.setItem(name, JSON.stringify(value));
}

function loadLocal(name, fallback = []) {
  try {
    return JSON.parse(localStorage.getItem(name) || JSON.stringify(fallback));
  } catch {
    return fallback;
  }
}

function syncTradeToPosition(stockId, refPrice) {
  const shares = prompt(`請輸入 ${stockId} 的持有股數`, "100");
  if (shares === null) return;
  const cost = prompt(`請輸入 ${stockId} 的平均成本`, refPrice || "");
  if (cost === null) return;

  const idx = currentPositions.findIndex(x => String(x.stock_id) === String(stockId));
  const row = {
    stock_id: stockId,
    shares: String(shares).trim(),
    avg_cost: String(cost).trim(),
    last_action_date: new Date().toISOString().slice(0, 10),
    note: "由今日操作加入持倉"
  };

  if (idx >= 0) currentPositions[idx] = row;
  else currentPositions.push(row);

  saveLocal("current_positions_v2665", currentPositions);
  alert(`已加入前端持倉：${stockId}\n提醒：這是前端同步版，正式納入後端仍需回寫 current_positions.csv`);
  renderPositionMonitor();
}

function addPosition() {
  const code = document.getElementById("pos_code").value.trim();
  const shares = document.getElementById("pos_shares").value.trim();
  const cost = document.getElementById("pos_cost").value.trim();
  if (!code || !shares || !cost) return;

  const idx = currentPositions.findIndex(x => String(x.stock_id) === code);
  const row = {
    stock_id: code,
    shares,
    avg_cost: cost,
    last_action_date: new Date().toISOString().slice(0, 10),
    note: "由持倉監控新增"
  };

  if (idx >= 0) currentPositions[idx] = row;
  else currentPositions.push(row);

  saveLocal("current_positions_v2665", currentPositions);
  document.getElementById("pos_code").value = "";
  document.getElementById("pos_shares").value = "";
  document.getElementById("pos_cost").value = "";
  renderPositionMonitor();
}

function removePosition(stockId) {
  currentPositions = currentPositions.filter(x => String(x.stock_id) !== String(stockId));
  saveLocal("current_positions_v2665", currentPositions);
  renderPositionMonitor();
}

function addWatch() {
  const code = document.getElementById("watch_code").value.trim();
  if (!code) return;
  if (!currentWatchlist.includes(code)) currentWatchlist.push(code);
  saveLocal("watchlist_v2665", currentWatchlist);
  document.getElementById("watch_code").value = "";
  renderWatchlistMonitor();
}

function removeWatch(stockId) {
  currentWatchlist = currentWatchlist.filter(x => String(x) !== String(stockId));
  saveLocal("watchlist_v2665", currentWatchlist);
  renderWatchlistMonitor();
}

function renderTradeTable() {
  const tbody = document.querySelector("#trade-table tbody");
  const tierSelect = document.getElementById("tierFilter");
  if (!tbody || !tierSelect) return;

  let rows = [...tradeRows];
  const selectedTier = tierSelect.value;
  if (selectedTier !== "全部") {
    rows = rows.filter(r => String(r.price_tier || "") === selectedTier);
  }

  tbody.innerHTML = "";

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="muted">目前沒有符合條件的資料</td></tr>`;
    return;
  }

  rows.forEach(r => {
    const action = String(r.action || "").toUpperCase();
    const stockId = String(r.stock_id || "");
    tbody.innerHTML += `
      <tr>
        <td class="${action.toLowerCase()}">${action}</td>
        <td>${stockId}</td>
        <td>${tierLabel(r.price_tier)}</td>
        <td>${formatTW(r.ref_price)}</td>
        <td>${r.target_weight || ""}</td>
        <td>${formatTW(r.suggested_amount)}</td>
        <td>${r.note || ""}</td>
        <td><button class="sync-btn" data-stock="${stockId}" data-price="${r.ref_price || ''}" type="button">加入持倉</button></td>
      </tr>`;
  });

  tbody.querySelectorAll(".sync-btn").forEach(btn => {
    btn.addEventListener("click", () => syncTradeToPosition(btn.dataset.stock, btn.dataset.price));
  });
}

function renderPositionMonitor() {
  const tbody = document.querySelector("#position-monitor-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const merged = [...positionMonitor];
  currentPositions.forEach(p => {
    if (!merged.find(x => String(x.stock_id) === String(p.stock_id))) {
      merged.push({
        stock_id: p.stock_id,
        price_tier: "unknown",
        ref_price: "",
        shares: p.shares,
        avg_cost: p.avg_cost,
        pnl_pct: "",
        target_weight: "",
        current_weight_est: "",
        action: "HOLD",
        note: "前端新增，待後端同步"
      });
    }
  });

  if (!merged.length) {
    tbody.innerHTML = `<tr><td colspan="11" class="muted">目前沒有持倉監控資料</td></tr>`;
    return;
  }

  merged.forEach(r => {
    const action = String(r.action || "").toUpperCase();
    tbody.innerHTML += `
      <tr>
        <td>${r.stock_id || ""}</td>
        <td>${tierLabel(r.price_tier)}</td>
        <td>${formatTW(r.ref_price)}</td>
        <td>${formatTW(r.shares)}</td>
        <td>${formatTW(r.avg_cost)}</td>
        <td>${r.pnl_pct || ""}</td>
        <td>${r.target_weight || ""}</td>
        <td>${r.current_weight_est || ""}</td>
        <td class="${action.toLowerCase()}">${action}</td>
        <td>${r.note || ""}</td>
        <td><button class="remove-btn" data-stock="${r.stock_id}" type="button">移除</button></td>
      </tr>`;
  });

  tbody.querySelectorAll(".remove-btn").forEach(btn => {
    btn.addEventListener("click", () => removePosition(btn.dataset.stock));
  });
}

function renderWatchlistMonitor() {
  const tbody = document.querySelector("#watchlist-monitor-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const merged = [...watchlistMonitor];
  currentWatchlist.forEach(code => {
    if (!merged.find(x => String(x.stock_id) === String(code))) {
      merged.push({
        stock_id: code,
        price_tier: "unknown",
        ref_price: "",
        holding_status: "未持有",
        strategy_bucket: "NONE",
        action: "WATCH",
        pnl_pct: ""
      });
    }
  });

  if (!merged.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="muted">目前沒有自選股監控資料</td></tr>`;
    return;
  }

  merged.forEach(r => {
    const action = String(r.action || "").toLowerCase();
    tbody.innerHTML += `
      <tr>
        <td>${r.stock_id || ""}</td>
        <td>${tierLabel(r.price_tier)}</td>
        <td>${formatTW(r.ref_price)}</td>
        <td>${r.holding_status || ""}</td>
        <td>${r.strategy_bucket || ""}</td>
        <td class="${action}">${r.action || ""}</td>
        <td>${r.pnl_pct || ""}</td>
        <td><button class="remove-btn" data-stock="${r.stock_id}" type="button">移除</button></td>
      </tr>`;
  });

  tbody.querySelectorAll(".remove-btn").forEach(btn => {
    btn.addEventListener("click", () => removeWatch(btn.dataset.stock));
  });
}

function renderSummaryTable(rows) {
  const tbody = document.querySelector("#summary-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="3" class="muted">目前沒有績效摘要資料</td></tr>`;
    return;
  }

  rows.forEach(r => {
    tbody.innerHTML += `
      <tr>
        <td>${r.return || ""}</td>
        <td>${r.mdd || ""}</td>
        <td>${r.sharpe_daily || ""}</td>
      </tr>`;
  });
}

function renderDebugTable(rows) {
  const tbody = document.querySelector("#debug-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="muted">目前沒有篩選除錯資料</td></tr>`;
    return;
  }

  rows.forEach(r => {
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

async function init() {
  const updateEl = document.getElementById("update-time");
  if (updateEl) updateEl.textContent = "資料載入中...";

  try {
    const [trade, summary, debug, watchMon, posMon, posCsv, watchCsv] = await Promise.all([
      loadCSV("trade_plan.csv"),
      loadCSV("full_summary.csv", true),
      loadCSV("selection_debug.csv", true),
      loadCSV("watchlist_monitor.csv", true),
      loadCSV("position_monitor.csv", true),
      loadCSV("current_positions.csv", true),
      loadCSV("watchlist.csv", true),
    ]);

    tradeRows = trade;
    watchlistMonitor = watchMon;
    positionMonitor = posMon;

    const localPos = loadLocal("current_positions_v2665", []);
    currentPositions = localPos.length ? localPos : posCsv;

    const localWatch = loadLocal("watchlist_v2665", []);
    currentWatchlist = localWatch.length ? localWatch : watchCsv.map(x => String(x.stock_id || "").trim()).filter(Boolean);

    renderTradeTable();
    renderPositionMonitor();
    renderWatchlistMonitor();
    renderSummaryTable(summary);
    renderDebugTable(debug);

    if (updateEl) updateEl.textContent = "最後更新：" + new Date().toLocaleString("zh-TW");
  } catch (err) {
    console.error(err);
    if (updateEl) updateEl.textContent = "讀取資料失敗，請確認 data 資料夾內 CSV 是否存在";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("refreshBtn")?.addEventListener("click", init);
  document.getElementById("tierFilter")?.addEventListener("change", renderTradeTable);
  document.getElementById("addPositionBtn")?.addEventListener("click", addPosition);
  document.getElementById("addWatchBtn")?.addEventListener("click", addWatch);
  init();
});
