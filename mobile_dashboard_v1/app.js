const DATA_BASE = "./data";

let tradeRows = [];
let watchlist = [];
let watchlistMonitor = [];
let positionMonitor = [];

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

async function loadCSV(file, optional = false) {
  try {
    const text = await fetchText(`${DATA_BASE}/${file}`);
    return parseCSV(text);
  } catch (err) {
    if (optional) return [];
    throw err;
  }
}

function getLocalWatchlist() {
  try {
    return JSON.parse(localStorage.getItem("watchlist_v2664") || "[]");
  } catch {
    return [];
  }
}

function saveLocalWatchlist(list) {
  localStorage.setItem("watchlist_v2664", JSON.stringify(list));
}

function addWatch() {
  const input = document.getElementById("watchInput");
  const code = String(input.value || "").trim();
  if (!code) return;
  if (!watchlist.includes(code)) {
    watchlist.push(code);
    saveLocalWatchlist(watchlist);
  }
  input.value = "";
  renderWatchlist();
  renderWatchlistMonitor();
}

function renderWatchlist() {
  const box = document.getElementById("watchList");
  if (!box) return;
  box.innerHTML = "";

  if (!watchlist.length) {
    box.innerHTML = '<div class="muted">目前沒有自選股</div>';
    return;
  }

  watchlist.forEach(code => {
    const tag = document.createElement("div");
    tag.className = "watch-tag";
    tag.innerHTML = `<span>${code}</span><button data-code="${code}" type="button">✕</button>`;
    box.appendChild(tag);
  });

  box.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      watchlist = watchlist.filter(x => x !== btn.dataset.code);
      saveLocalWatchlist(watchlist);
      renderWatchlist();
      renderWatchlistMonitor();
    });
  });
}

function formatTW(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return v ?? "";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 2 });
}

function renderTradeTable() {
  const tbody = document.querySelector("#trade-table tbody");
  const tierSelect = document.getElementById("tierFilter");
  if (!tbody || !tierSelect) return;

  const selectedTier = tierSelect.value || "全部";
  let rows = [...tradeRows];
  if (selectedTier !== "全部") {
    rows = rows.filter(r => String(r.price_tier || "") === selectedTier);
  }

  tbody.innerHTML = "";

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7" class="muted">目前沒有符合條件的資料</td>`;
    tbody.appendChild(tr);
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");
    const action = String(r.action || "").toUpperCase();
    const actionClass = action.toLowerCase();
    const stockId = String(r.stock_id || "");
    const isWatch = watchlist.includes(stockId);

    tr.innerHTML = `
      <td class="${actionClass}">${action}</td>
      <td>${stockId} ${isWatch ? '<span class="star">★</span>' : ''}</td>
      <td>${r.price_tier || ""}</td>
      <td>${formatTW(r.ref_price)}</td>
      <td>${r.target_weight || ""}</td>
      <td>${formatTW(r.suggested_amount)}</td>
      <td>${r.note || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderPositionMonitor() {
  const tbody = document.querySelector("#position-monitor-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!positionMonitor.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="10" class="muted">目前沒有持倉動作資料</td>`;
    tbody.appendChild(tr);
    return;
  }

  positionMonitor.forEach(r => {
    const tr = document.createElement("tr");
    const action = String(r.action || "").toUpperCase();
    tr.innerHTML = `
      <td>${r.stock_id || ""}</td>
      <td>${r.price_tier || ""}</td>
      <td>${formatTW(r.ref_price)}</td>
      <td>${formatTW(r.shares)}</td>
      <td>${formatTW(r.avg_cost)}</td>
      <td>${r.pnl_pct || ""}</td>
      <td>${r.target_weight || ""}</td>
      <td>${r.current_weight_est || ""}</td>
      <td class="${action.toLowerCase()}">${action}</td>
      <td>${r.note || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderWatchlistMonitor() {
  const tbody = document.querySelector("#watchlist-monitor-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  let rows = [...watchlistMonitor];
  if (watchlist.length) {
    rows = rows.filter(r => watchlist.includes(String(r.stock_id || "")));
  }

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7" class="muted">目前沒有自選股監控資料</td>`;
    tbody.appendChild(tr);
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.stock_id || ""}</td>
      <td>${r.price_tier || ""}</td>
      <td>${formatTW(r.ref_price)}</td>
      <td>${r.holding_status || ""}</td>
      <td>${r.strategy_bucket || ""}</td>
      <td>${r.action || ""}</td>
      <td>${r.pnl_pct || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderSummaryTable(rows) {
  const tbody = document.querySelector("#summary-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="3" class="muted">目前沒有績效摘要資料</td>`;
    tbody.appendChild(tr);
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.return || ""}</td>
      <td>${r.mdd || ""}</td>
      <td>${r.sharpe_daily || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderDebugTable(rows) {
  const tbody = document.querySelector("#debug-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6" class="muted">目前沒有篩選除錯資料</td>`;
    tbody.appendChild(tr);
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.total_input || ""}</td>
      <td>${r.valid_after_na || ""}</td>
      <td>${r.core_primary_count || ""}</td>
      <td>${r.alpha_primary_count || ""}</td>
      <td>${r.core_final_count || ""}</td>
      <td>${r.alpha_final_count || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

async function init() {
  const updateEl = document.getElementById("update-time");
  if (updateEl) updateEl.textContent = "資料載入中...";

  try {
    watchlist = getLocalWatchlist();
    renderWatchlist();

    const [trade, summary, debug, watchMon, posMon] = await Promise.all([
      loadCSV("trade_plan.csv"),
      loadCSV("full_summary.csv", true),
      loadCSV("selection_debug.csv", true),
      loadCSV("watchlist_monitor.csv", true),
      loadCSV("position_monitor.csv", true),
    ]);

    tradeRows = trade;
    watchlistMonitor = watchMon;
    positionMonitor = posMon;

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
  document.getElementById("addWatchBtn")?.addEventListener("click", addWatch);
  document.getElementById("refreshBtn")?.addEventListener("click", init);
  document.getElementById("tierFilter")?.addEventListener("change", renderTradeTable);
  init();
});
