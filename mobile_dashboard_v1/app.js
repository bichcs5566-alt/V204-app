const DATA_PATH = "./data/";

function parseCSV(text) {
  if (!text || !text.trim()) return [];
  const lines = text.trim().split(/\r?\n/);
  if (lines.length <= 1) return [];

  const headers = splitCSVLine(lines[0]);
  return lines.slice(1).map(line => {
    const values = splitCSVLine(line);
    const obj = {};
    headers.forEach((h, i) => {
      obj[h.trim()] = (values[i] ?? "").trim();
    });
    return obj;
  });
}

function splitCSVLine(line) {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"' && inQuotes && next === '"') {
      current += '"';
      i++;
    } else if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += ch;
    }
  }

  result.push(current);
  return result;
}

async function loadCSV(name) {
  try {
    const res = await fetch(`${DATA_PATH}${name}?t=${Date.now()}`);
    if (!res.ok) return [];
    const text = await res.text();
    return parseCSV(text);
  } catch (err) {
    console.warn("loadCSV failed", name, err);
    return [];
  }
}

async function loadJSON(name) {
  try {
    const res = await fetch(`${DATA_PATH}${name}?t=${Date.now()}`);
    if (!res.ok) return {};
    return await res.json();
  } catch (err) {
    console.warn("loadJSON failed", name, err);
    return {};
  }
}

function money(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
}

function num(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(digits).replace(/\.00$/, "");
}

function actionClass(action) {
  if (action === "BUY") return "buy";
  if (action === "TEST") return "test";
  if (action === "WATCH") return "watch";
  if (action === "NO_TRADE") return "none";
  return "skip";
}

function renderMeta(meta, trade) {
  const el = document.querySelector("#meta-card");
  if (!el) return;

  const source = meta.source || "v265_clean_strategy_core";
  const signalDate = meta.signal_date || "-";
  const tradeDate = meta.trade_date || "-";
  const generated = meta.generated_at || meta.bridge_generated_at || "-";
  const state = meta.data_state || "-";
  const count = meta.trade_plan_count ?? trade.length ?? 0;

  el.innerHTML = `
    <h1>ð æ¯æ¥æä½ä»é¢</h1>
    <div class="meta-grid">
      <div><b>ä¾æºçæ¬</b><span>${source}</span></div>
      <div><b>è³æçæ</b><span>${state}</span></div>
      <div><b>è¨èæ¥</b><span>${signalDate}</span></div>
      <div><b>äº¤ææ¥</b><span>${tradeDate}</span></div>
      <div><b>æå¾æ´æ°</b><span>${generated}</span></div>
      <div><b>åå®æ¸</b><span>${count}</span></div>
    </div>
  `;
}

function renderTodayAction(trade) {
  const box = document.querySelector("#today-action");
  if (!box) return;

  if (!trade.length) {
    box.innerHTML = `
      <div class="empty">
        <div class="empty-title">è§å¯</div>
        <div>ä»å¤©æ²ææ°çè²·é²åä½</div>
      </div>
    `;
    return;
  }

  box.innerHTML = trade.map(row => {
    const cls = actionClass(row.action);
    const amount = money(row.suggested_amount);
    const weight = num(Number(row.target_weight || 0) * 100, 2);
    const score = num(row.entry_score, 1);
    const ref = num(row.ref_price, 2);

    return `
      <div class="trade-row ${cls}">
        <div class="action-pill">${row.action_label || row.action || "è§å¯"}</div>
        <div class="stock-id">${row.stock_id || "-"}</div>
        <div class="price-tier">${row.price_tier || "-"}</div>
        <div class="ref-price">${ref}</div>
        <div class="score">${score}</div>
        <div class="amount">${amount}</div>
        <div class="sub">${row.action_sub || ""}</div>
        <div class="note">${row.note || ""}</div>
      </div>
    `;
  }).join("");
}

function renderStats(debug, trade) {
  const box = document.querySelector("#stats");
  if (!box) return;

  const d = debug[0] || {};
  const buy = trade.filter(x => x.action === "BUY").length;
  const test = trade.filter(x => x.action === "TEST").length;
  const watch = trade.filter(x => x.action === "WATCH").length;

  box.innerHTML = `
    <h2>ð§ª ç¯©é¸çæ</h2>
    <div class="stat-grid">
      <div><b>è¼¸å¥æªæ¸</b><span>${d.total_input_rows || "-"}</span></div>
      <div><b>ææ°è¡ç¥¨</b><span>${d.latest_stock_count || "-"}</span></div>
      <div><b>BUY è²·é²</b><span>${buy}</span></div>
      <div><b>TEST è©¦å®</b><span>${test}</span></div>
      <div><b>WATCH è§å¯</b><span>${watch}</span></div>
      <div><b>Core</b><span>${d.core_count || "-"}</span></div>
      <div><b>Alpha</b><span>${d.alpha_count || "-"}</span></div>
      <div><b>æé¤</b><span>${d.skip_count || "-"}</span></div>
    </div>
  `;
}

function renderCandidates(id, title, rows) {
  const box = document.querySelector(id);
  if (!box) return;

  const list = rows.slice(0, 12);
  box.innerHTML = `
    <h2>${title}</h2>
    ${list.length ? list.map(r => `
      <div class="candidate-row">
        <span>${r.stock_id || "-"}</span>
        <span>åæ¸ ${num(r.entry_score, 1)}</span>
        <span>${r.action_label || ""}</span>
      </div>
    `).join("") : `<div class="empty small">ç®åæ²æè³æ</div>`}
  `;
}

async function main() {
  const [meta, trade, debug, core, alpha] = await Promise.all([
    loadJSON("meta.json"),
    loadCSV("trade_plan.csv"),
    loadCSV("selection_debug.csv"),
    loadCSV("core_candidates.csv"),
    loadCSV("alpha_candidates.csv")
  ]);

  renderMeta(meta, trade);
  renderTodayAction(trade);
  renderStats(debug, trade);
  renderCandidates("#core-list", "Core åé¸", core);
  renderCandidates("#alpha-list", "Alpha åé¸", alpha);
}

main();
