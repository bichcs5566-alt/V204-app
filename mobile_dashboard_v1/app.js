/*
app.js - 掃描列表 + 展開詳情版

重點：
1. 預設一行一檔，降低高度
2. 保留原本所有資訊
3. 點擊該列可展開：來源、策略層、目標權重、原因、系統提示
4. 優先讀 final_action_plan.csv
*/

const DATA_DIR = "./data/";

const FILES = {
  final: DATA_DIR + "final_action_plan.csv",
  finalSummary: DATA_DIR + "final_action_summary.json",
  regime: DATA_DIR + "market_regime.json",
  tradePlan: DATA_DIR + "trade_plan.csv"
};

const ACTION_LABEL = {
  SELL: "賣出",
  REDUCE: "減碼",
  BUY: "買進",
  TEST: "試單",
  WATCH: "觀察",
  BLOCK: "禁止"
};

const ACTION_EMOJI = {
  SELL: "🔴",
  REDUCE: "🟠",
  BUY: "🟢",
  TEST: "🟡",
  WATCH: "⚪",
  BLOCK: "⛔"
};

const ACTION_CLASS = {
  SELL: "sell",
  REDUCE: "reduce",
  BUY: "buy",
  TEST: "test",
  WATCH: "watch",
  BLOCK: "block"
};

const ACTION_PRIORITY = {
  SELL: 1,
  REDUCE: 2,
  BUY: 3,
  TEST: 4,
  WATCH: 5,
  BLOCK: 6
};

function qs(id) {
  return document.getElementById(id);
}

function safeText(v, fallback = "--") {
  if (v === undefined || v === null || v === "") return fallback;
  return String(v);
}

async function fetchText(url) {
  const res = await fetch(url + "?t=" + Date.now(), { cache: "no-store" });
  if (!res.ok) throw new Error("fetch failed: " + url);
  return await res.text();
}

async function fetchJson(url, fallback = {}) {
  try {
    const txt = await fetchText(url);
    return JSON.parse(txt);
  } catch (e) {
    return fallback;
  }
}

function parseCsv(text) {
  const rows = [];
  const lines = text.replace(/\r/g, "").split("\n").filter(x => x.trim() !== "");
  if (lines.length <= 1) return rows;

  const headers = parseCsvLine(lines[0]).map(h => h.trim());

  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i]);
    const obj = {};
    headers.forEach((h, idx) => obj[h] = values[idx] ?? "");
    rows.push(obj);
  }
  return rows;
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
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
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function normalizeAction(a) {
  const s = String(a || "").trim().toUpperCase();
  if (s === "賣出") return "SELL";
  if (s === "減碼") return "REDUCE";
  if (s === "買進") return "BUY";
  if (s === "試單") return "TEST";
  if (s === "觀察") return "WATCH";
  if (s === "禁止") return "BLOCK";
  return s || "WATCH";
}

function money(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return Math.round(n).toLocaleString("en-US");
}

function pct(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  if (Math.abs(n) <= 1) return (n * 100).toFixed(2) + "%";
  return n.toFixed(2) + "%";
}

function num(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return n.toFixed(digits);
}

function groupCounts(rows) {
  const counts = { SELL: 0, REDUCE: 0, BUY: 0, TEST: 0, WATCH: 0, BLOCK: 0 };
  rows.forEach(r => {
    const a = normalizeAction(r.final_action || r.action);
    if (counts[a] !== undefined) counts[a]++;
  });
  return counts;
}

function sortRows(rows) {
  return rows.slice().sort((a, b) => {
    const aa = normalizeAction(a.final_action || a.action);
    const bb = normalizeAction(b.final_action || b.action);
    const pa = ACTION_PRIORITY[aa] || 99;
    const pb = ACTION_PRIORITY[bb] || 99;
    if (pa !== pb) return pa - pb;

    const sa = Number(a.score || 0);
    const sb = Number(b.score || 0);
    if (sb !== sa) return sb - sa;

    return String(a.stock_id || "").localeCompare(String(b.stock_id || ""));
  });
}

function classifyMainDecision(counts) {
  if (counts.SELL > 0) return {
    label: "先賣出",
    desc: `今日有 ${counts.SELL} 檔賣出訊號，先處理出場`,
    cls: "sell"
  };

  if (counts.REDUCE > 0) return {
    label: "先減碼",
    desc: `今日有 ${counts.REDUCE} 檔減碼訊號，先控風險`,
    cls: "reduce"
  };

  if (counts.BUY > 0) return {
    label: "買進",
    desc: `今日有 ${counts.BUY} 檔買進候選，請分批執行`,
    cls: "buy"
  };

  if (counts.TEST > 0) return {
    label: "試單",
    desc: `今日有 ${counts.TEST} 檔可小倉試單`,
    cls: "test"
  };

  return {
    label: "觀察",
    desc: "今日沒有主要操作",
    cls: "watch"
  };
}

function renderAppShell() {
  document.body.innerHTML = `
    <main class="page">
      <section class="card hero">
        <h1>📊 每日操作介面</h1>
        <div class="actions">
          <button id="refreshBtn">🔄 重新整理</button>
          <button id="updateBtn">🚀 更新資料</button>
        </div>
        <div id="syncStatus" class="sync">讀取中...</div>
        <div id="metaBox" class="meta-grid"></div>
      </section>

      <section id="mainDecision" class="card decision"></section>

      <section class="card">
        <div class="section-head">
          <h2>🔥 最終操作</h2>
          <span class="hint">點擊股票可展開詳情</span>
        </div>
        <div id="finalActionList"></div>
      </section>

      <section class="card">
        <h2>🧪 篩選狀態</h2>
        <div id="filterStats" class="meta-grid"></div>
      </section>

      <section class="card">
        <h2>Core 候選</h2>
        <div id="coreList"></div>
      </section>

      <section class="card">
        <h2>Alpha 候選</h2>
        <div id="alphaList"></div>
      </section>
    </main>
  `;

  qs("refreshBtn").addEventListener("click", () => location.reload());
  qs("updateBtn").addEventListener("click", () => {
    alert("請到 GitHub Actions 手動 Run data_pipeline，或等待 19:00 自動更新。");
  });
}

function renderMeta(regime, summary, rows) {
  qs("metaBox").innerHTML = `
    <div class="mini"><span>來源版本</span><b>C 完整交易系統</b></div>
    <div class="mini"><span>市場狀態</span><b>${safeText(regime.label || regime.regime)}</b></div>
    <div class="mini"><span>風險模式</span><b>${safeText(regime.risk_mode)}</b></div>
    <div class="mini"><span>訊號日</span><b>${safeText(regime.latest_date)}</b></div>
    <div class="mini"><span>最後更新</span><b>${safeText(summary.generated_at || regime.generated_at)}</b></div>
    <div class="mini"><span>操作筆數</span><b>${rows.length}</b></div>
  `;

  qs("syncStatus").innerHTML = "✅ 最終操作表已同步";
  qs("syncStatus").className = "sync ok";
}

function renderDecision(rows) {
  const counts = groupCounts(rows);
  const d = classifyMainDecision(counts);

  qs("mainDecision").className = `card decision ${d.cls}`;
  qs("mainDecision").innerHTML = `
    <div class="small-title">今日主判斷</div>
    <div class="big-decision">${d.label}</div>
    <p>${d.desc}</p>
    <div class="count-grid">
      <div><b>${counts.SELL}</b><span>賣出</span></div>
      <div><b>${counts.REDUCE}</b><span>減碼</span></div>
      <div><b>${counts.BUY}</b><span>買進</span></div>
      <div><b>${counts.TEST}</b><span>試單</span></div>
      <div><b>${counts.WATCH}</b><span>觀察</span></div>
    </div>
  `;
}

function renderFinalActions(rows) {
  const sorted = sortRows(rows);
  const container = qs("finalActionList");

  if (!sorted.length) {
    container.innerHTML = `<div class="empty">沒有最終操作資料</div>`;
    return;
  }

  container.innerHTML = sorted.map((row, idx) => {
    const action = normalizeAction(row.final_action || row.action);
    const cls = ACTION_CLASS[action] || "watch";
    const label = ACTION_LABEL[action] || action;
    const emoji = ACTION_EMOJI[action] || "⚪";

    const stock = safeText(row.stock_id);
    const score = safeText(row.score);
    const source = safeText(row.source);
    const bucket = safeText(row.bucket);
    const entry = safeText(row.entry_type);
    const close = num(row.close);
    const amount = row.suggested_amount ? money(row.suggested_amount) : "--";
    const weight = row.target_weight ? pct(row.target_weight) : "--";
    const reason = safeText(row.reason, "無");
    const note = safeText(row.system_note, "無");

    return `
      <article class="scan-item ${cls}" data-row="${idx}">
        <div class="scan-main">
          <div class="scan-action ${cls}">${emoji} ${label}</div>
          <div class="scan-stock">${stock}</div>
          <div class="scan-score">${score}</div>
          <div class="scan-entry">${entry}</div>
          <div class="scan-close">${close}</div>
          <div class="scan-amount">${amount}</div>
        </div>

        <div class="scan-detail">
          <div class="detail-grid">
            <div><span>來源</span><b>${source}</b></div>
            <div><span>策略層</span><b>${bucket}</b></div>
            <div><span>進場型態</span><b>${entry}</b></div>
            <div><span>參考價</span><b>${close}</b></div>
            <div><span>建議金額</span><b>${amount}</b></div>
            <div><span>目標權重</span><b>${weight}</b></div>
          </div>
          <div class="detail-text"><b>原因</b><p>${reason}</p></div>
          <div class="detail-text"><b>系統提示</b><p>${note}</p></div>
        </div>
      </article>
    `;
  }).join("");

  document.querySelectorAll(".scan-item").forEach(el => {
    el.addEventListener("click", () => {
      el.classList.toggle("open");
    });
  });
}

function renderStats(rows, summary) {
  const counts = groupCounts(rows);
  qs("filterStats").innerHTML = `
    <div class="mini"><span>總筆數</span><b>${rows.length}</b></div>
    <div class="mini"><span>SELL</span><b>${counts.SELL}</b></div>
    <div class="mini"><span>REDUCE</span><b>${counts.REDUCE}</b></div>
    <div class="mini"><span>BUY</span><b>${counts.BUY}</b></div>
    <div class="mini"><span>TEST</span><b>${counts.TEST}</b></div>
    <div class="mini"><span>WATCH</span><b>${counts.WATCH}</b></div>
    <div class="mini"><span>BLOCK</span><b>${counts.BLOCK}</b></div>
    <div class="mini"><span>資料來源</span><b>${safeText(summary.source)}</b></div>
  `;
}

function renderCandidateLists(rows) {
  const core = sortRows(rows).filter(r => String(r.bucket).toUpperCase() === "CORE").slice(0, 30);
  const alpha = sortRows(rows).filter(r => String(r.bucket).toUpperCase() === "ALPHA").slice(0, 30);

  qs("coreList").innerHTML = renderSimpleRows(core);
  qs("alphaList").innerHTML = renderSimpleRows(alpha);
}

function renderSimpleRows(rows) {
  if (!rows.length) return `<div class="empty">沒有資料</div>`;

  return rows.map(r => {
    const action = normalizeAction(r.final_action || r.action);
    const cls = ACTION_CLASS[action] || "watch";
    return `
      <div class="simple-row">
        <b>${safeText(r.stock_id)}</b>
        <span>${safeText(r.score)}</span>
        <em class="${cls}">${ACTION_LABEL[action] || action}</em>
      </div>
    `;
  }).join("");
}

async function loadFinalRows() {
  try {
    const txt = await fetchText(FILES.final);
    const rows = parseCsv(txt);
    if (rows.length) return rows;
  } catch (e) {
    console.warn("final_action_plan fallback", e);
  }

  try {
    const txt = await fetchText(FILES.tradePlan);
    const oldRows = parseCsv(txt);
    return oldRows.map(r => ({
      final_action: normalizeAction(r.action || "BUY"),
      stock_id: r.stock_id,
      source: "ENTRY",
      bucket: "CORE",
      score: r.score || r.rank_score || "",
      entry_type: "",
      execution_flag: "TOP",
      allowed: "True",
      close: r.ref_price || r.close || r.price || "",
      suggested_amount: r.suggested_amount || "",
      target_weight: r.target_weight || "",
      reason: r.reason || r.note || "",
      system_note: "fallback trade_plan"
    }));
  } catch (e) {
    console.error(e);
    return [];
  }
}

async function init() {
  renderAppShell();

  try {
    const [regime, summary, rows] = await Promise.all([
      fetchJson(FILES.regime, {}),
      fetchJson(FILES.finalSummary, {}),
      loadFinalRows()
    ]);

    renderMeta(regime, summary, rows);
    renderDecision(rows);
    renderFinalActions(rows);
    renderStats(rows, summary);
    renderCandidateLists(rows);
  } catch (e) {
    console.error(e);
    qs("syncStatus").innerHTML = "❌ 讀取失敗：" + e.message;
    qs("syncStatus").className = "sync error";
  }
}

document.addEventListener("DOMContentLoaded", init);
