const DATA_BASE = "./mobile_dashboard_v1/data";
let tradeRows = [];
let watchlist = [];

async function loadCSV(path) {
  const res = await fetch(path + "?t=" + Date.now());
  const text = await res.text();
  return parseCSV(text);
}

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length || !lines[0]) return [];
  const headers = splitCSVLine(lines[0]);
  return lines.slice(1).filter(Boolean).map(line => {
    const values = splitCSVLine(line);
    const row = {};
    headers.forEach((h, i) => row[h] = values[i] ?? "");
    return row;
  });
}

function splitCSVLine(line) {
  const result = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) {
      result.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  result.push(cur);
  return result;
}

function getLocalWatchlist() {
  try {
    return JSON.parse(localStorage.getItem("watchlist_v2662") || "[]");
  } catch {
    return [];
  }
}

function saveLocalWatchlist(list) {
  localStorage.setItem("watchlist_v2662", JSON.stringify(list));
}

function renderWatchlist() {
  const box = document.getElementById("watchList");
  box.innerHTML = "";
  watchlist.forEach(code => {
    const tag = document.createElement("div");
    tag.className = "watch-tag";
    tag.innerHTML = `<span>${code}</span><button data-code="${code}">✕</button>`;
    box.appendChild(tag);
  });
  box.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      watchlist = watchlist.filter(x => x !== btn.dataset.code);
      saveLocalWatchlist(watchlist);
      renderWatchlist();
      renderTradeTable();
    });
  });
}

function addWatch() {
  const input = document.getElementById("watchInput");
  const code = input.value.trim();
  if (!code) return;
  if (!watchlist.includes(code)) watchlist.push(code);
  saveLocalWatchlist(watchlist);
  input.value = "";
  renderWatchlist();
  renderTradeTable();
}

function renderTradeTable() {
  const tbody = document.querySelector("#trade-table tbody");
  const tier = document.getElementById("tierFilter").value;
  tbody.innerHTML = "";

  let rows = [...tradeRows];
  if (tier !== "全部") {
    rows = rows.filter(r => (r.price_tier || "") === tier);
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");
    const actionClass = (r.action || "").toLowerCase();
    const isWatch = watchlist.includes(String(r.stock_id).trim());
    tr.innerHTML = `
      <td class="${actionClass}">${r.action || ""}</td>
      <td>${r.stock_id || ""} ${isWatch ? '<span class="star">★</span>' : ''}</td>
      <td>${r.price_tier || ""}</td>
      <td>${r.ref_price || ""}</td>
      <td>${r.target_weight || ""}</td>
      <td>${r.suggested_amount || ""}</td>
      <td>${r.note || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderPositionTable(rows) {
  const tbody = document.querySelector("#position-table tbody");
  tbody.innerHTML = "";
  rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.stock_id || ""}</td>
      <td>${r.shares || ""}</td>
      <td>${r.avg_cost || ""}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderSummaryTable(rows) {
  const tbody = document.querySelector("#summary-table tbody");
  tbody.innerHTML = "";
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
  tbody.innerHTML = "";
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
  watchlist = getLocalWatchlist();
  renderWatchlist();

  const trade = await loadCSV(`${DATA_BASE}/trade_plan.csv`);
  const positions = await loadCSV(`${DATA_BASE}/current_positions.csv`);
  const summary = await loadCSV(`${DATA_BASE}/full_summary.csv`);
  const debug = await loadCSV(`${DATA_BASE}/selection_debug.csv`).catch(() => []);

  tradeRows = trade;
  renderTradeTable();
  renderPositionTable(positions);
  renderSummaryTable(summary);
  renderDebugTable(debug);

  document.getElementById("update-time").textContent =
    "最後更新：" + new Date().toLocaleString("zh-TW");
}

document.getElementById("addWatchBtn").addEventListener("click", addWatch);
document.getElementById("refreshBtn").addEventListener("click", init);
document.getElementById("tierFilter").addEventListener("change", renderTradeTable);
init();
