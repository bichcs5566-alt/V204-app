/*
app.js - 最終穩定版 UI

設計原則：
1. 保留卡片質感
2. SELL / REDUCE / BUY 放在最終操作區
3. TEST / WATCH / BLOCK 預設收合
4. 點擊股票列展開詳情
5. 篩選狀態改成一行摘要
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

function isTop(row) {
  return String(row.execution_flag || "").toUpperCase() === "TOP";
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

    const at = isTop(a) ? 1 : 0;
    const bt = isTop(b) ? 1 : 0;
    if (bt !== at) return bt - at;

    const sa = Number(a.score || 0);
    const sb = Number(b.score || 0);
    if (sb !== sa) return sb - sa;

    return String(a.stock_id || "").localeCompare(String(b.stock_id || ""));
  });
}

function splitRows(rows) {
  const sorted = sortRows(rows);
  return {
    main: sorted.filter(r => ["SELL", "REDUCE", "BUY"].includes(normalizeAction(r.final_action || r.action))),
    test: sorted.filter(r => normalizeAction(r.final_action || r.action) === "TEST"),
    watch: sorted.filter(r => normalizeAction(r.final_action || r.action) === "WATCH"),
    block: sorted.filter(r => normalizeAction(r.final_action || r.action) === "BLOCK")
  };
}

function classifyMainDecision(counts) {
  if (counts.SELL > 0) {
    return {
      label: "先賣出",
      desc: `今日有 ${counts.SELL} 檔賣出訊號，先處理出場，再看買進。`,
      cls: "sell"
    };
  }

  if (counts.REDUCE > 0) {
    return {
      label: "先減碼",
      desc: `今日有 ${counts.REDUCE} 檔減碼訊號，先控風險。`,
      cls: "reduce"
    };
  }

  if (counts.BUY > 0) {
    return {
      label: "買進",
      desc: `今日有 ${counts.BUY} 檔買進候選，請分批執行。`,
      cls: "buy"
    };
  }

  if (counts.TEST > 0) {
    return {
      label: "試單",
      desc: `今日有 ${counts.TEST} 檔可小倉試單。`,
      cls: "test"
    };
  }

  return {
    label: "觀察",
    desc: "今日沒有主要操作。",
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

      <section class="card compact-card">
        <details>
          <summary>🟡 TEST 試單清單</summary>
          <div id="testList"></div>
        </details>
      </section>

      <section class="card compact-card">
        <details>
          <summary>⚪ WATCH 觀察清單</summary>
          <div id="watchList"></div>
        </details>
      </section>

      <section class="card compact-card">
        <details>
          <summary>⛔ BLOCK 禁止清單</summary>
          <div id="blockList"></div>
        </details>
      </section>

      <section class="card compact-stats-card">
        <h2>🧪 篩選狀態</h2>
        <div id="filterStats"></div>
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
  const container = qs("finalActionList");

  if (!rows.length) {
    container.innerHTML = `<div class="empty">目前沒有 SELL / REDUCE / BUY 主操作</div>`;
    return;
  }

  container.innerHTML = rows.map((row, idx) => renderScanRow(row, "main-" + idx)).join("");
  bindToggle();
}

function renderSectionList(targetId, rows, prefix, limit = 80) {
  const container = qs(targetId);

  if (!rows.length) {
    container.innerHTML = `<div class="empty">沒有資料</div>`;
    return;
  }

  const list = rows.slice(0, limit);
  let html = list.map((row, idx) => renderScanRow(row, prefix + "-" + idx)).join("");

  if (rows.length > limit) {
    html += `<div class="more-note">已顯示前 ${limit} 檔，其餘 ${rows.length - limit} 檔省略。</div>`;
  }

  container.innerHTML = html;
  bindToggle();
}


function zhSource(v) {
  const s = String(v || "").toUpperCase();
  if (s === "ENTRY") return "策略進場";
  if (s === "EXIT") return "策略出場";
  if (s === "POSITION") return "持倉風控";
  return safeText(v);
}

function zhBucket(v) {
  const s = String(v || "").toUpperCase();
  if (s.includes("POSITION")) return "持倉風控";
  if (s.includes("ALPHA")) return "主力延續";
  if (s.includes("CORE")) return "核心卡位";
  if (s.includes("PRE")) return "預備布局";
  return safeText(v);
}

function zhEntryType(v) {
  const s = String(v || "").toUpperCase();
  if (s.includes("BREAK")) return "突破確認";
  if (s.includes("PULLBACK")) return "回檔轉強";
  if (s.includes("WAIT")) return "等待確認";
  if (s.includes("SELL")) return "賣出";
  return safeText(v);
}

function getStockName(row) {
  return safeText(row.stock_name || row.name || row.stockName || row["股票名稱"], "--");
}

function getTopBadge(row) {
  const sectionTop = safeText(row.section_top_opportunity || "", "");
  const overallTop = safeText(row.top_opportunity || "", "");
  const sectionRank = safeText(row.section_opportunity_rank || "", "");
  const overallRank = safeText(row.opportunity_rank || "", "");

  if (sectionTop && sectionTop !== "--") return "🔥 " + sectionTop + "｜優先觀察";
  if (overallTop && overallTop !== "--") return "🔥 " + overallTop + "｜優先觀察";
  if (sectionRank && sectionRank !== "--") return "🔥 TOP" + sectionRank + "｜本區最可能發動";
  if (overallRank && overallRank !== "--") return "🔥 TOP" + overallRank + "｜全清單優先";
  return "";
}

function volumeLots(v) {
  if (v === undefined || v === null || v === "" || v === "--") return "--";
  const raw = Number(String(v).replace(/,/g, "").replace("張", "").replace("股", ""));
  if (!Number.isFinite(raw) || raw <= 0) return "--";
  const lots = raw >= 2000000 ? raw / 1000 : raw;
  return Math.round(lots).toLocaleString("en-US") + "張";
}

function turnoverYi(v) {
  if (v === undefined || v === null || v === "" || v === "--") return "--";
  let raw = Number(String(v).replace(/,/g, "").replace("億", ""));
  if (!Number.isFinite(raw) || raw <= 0) return "--";
  if (raw > 10000000) raw = raw / 100000000;
  return raw.toLocaleString("en-US", { maximumFractionDigits: 2 }) + "億";
}

function inferExitType(row) {
  const text = `${row.exit_type || ""} ${row.reason || ""} ${row.system_note || ""}`;
  if (row.exit_type) return safeText(row.exit_type);
  if (/停損|stop/i.test(text)) return "停損出場";
  if (/跌破\s*MA20|MA20|月線/i.test(text)) return "跌破均線出場";
  if (/獲利|停利|take profit/i.test(text)) return "停利出場";
  if (/減碼|風險/i.test(text)) return "風險減碼";
  if (/轉弱|動能轉弱|弱勢/i.test(text)) return "動能轉弱出場";
  return "持倉風控出場";
}

function inferExitKbar(row) {
  const v = row.exit_kbar_type || row.kbar_type || row.candle_type || row.candle_pattern || row.price_action;
  if (v) return safeText(v);

  const text = `${row.reason || ""} ${row.system_note || ""}`;
  if (/長黑|黑K/i.test(text)) return "長黑K轉弱";
  if (/跌破|破線|破位/i.test(text)) return "跌破型K棒";
  if (/吞噬/i.test(text)) return "空方吞噬";
  if (/上影|爆量上影/i.test(text)) return "上影線壓力";
  if (/量縮|無量/i.test(text)) return "量縮轉弱";
  if (/MA20|均線/i.test(text)) return "跌破均線K";
  return "尚未標註K棒";
}

function inferExitKbarReason(row) {
  const v = row.exit_kbar_reason || row.kbar_reason || row.candle_reason;
  if (v) return safeText(v);

  const text = `${row.reason || ""} ${row.system_note || ""}`;
  if (/停損/i.test(text)) return "價格已觸發停損條件，先保護本金。";
  if (/跌破\s*MA20|MA20|均線/i.test(text)) return "收盤或盤中結構跌破關鍵均線，趨勢防守失效。";
  if (/動能轉弱|5日動能轉弱|轉弱/i.test(text)) return "短線動能轉弱，續抱勝率下降。";
  if (/損益|虧損|負/i.test(text)) return "持倉損益惡化，需優先控風險。";
  return "後端尚未提供K棒原因，先以出場原因判斷。";
}

function inferRiskLevel(row) {
  const text = `${row.risk_level || ""} ${row.reason || ""} ${row.system_note || ""}`.toUpperCase();
  if (row.risk_level) return safeText(row.risk_level);
  if (/HIGH|高風險|停損|跌破/.test(text)) return "HIGH";
  if (/MEDIUM|中風險|轉弱|減碼/.test(text)) return "MEDIUM";
  if (/LOW|低風險/.test(text)) return "LOW";
  return "--";
}

function exitAdvice(row, action) {
  const v = row.exit_advice || row.action_advice || row.decision_note;
  if (v) return safeText(v);
  if (action === "SELL") return "優先處理賣出，不建議拖延。";
  if (action === "REDUCE") return "先減碼控風險，保留觀察彈性。";
  return safeText(row.system_note, "依系統提示執行。");
}

function renderScanRow(row, key) {
  const action = normalizeAction(row.final_action || row.action);
  const cls = ACTION_CLASS[action] || "watch";
  const label = ACTION_LABEL[action] || action;
  const emoji = ACTION_EMOJI[action] || "⚪";

  const stock = safeText(row.stock_id);
  const stockName = getStockName(row);
  const score = safeText(row.score);
  const source = zhSource(row.source);
  const bucket = zhBucket(row.bucket);
  const entry = zhEntryType(row.entry_type);
  const close = num(row.close);
  const amount = row.suggested_amount ? money(row.suggested_amount) : "--";
  const weight = row.target_weight ? pct(row.target_weight) : "--";
  const reason = safeText(row.reason, "無");
  const note = safeText(row.system_note, "無");
  const topBadge = getTopBadge(row);

  const liq = safeText(row.liquidity_tag || row.liquidity_level, "--");
  const liqScore = safeText(row.liquidity_score, "--");
  const vol = volumeLots(row.volume);
  const turn = turnoverYi(row.turnover);

  const isExit = ["SELL", "REDUCE"].includes(action);

  const exitType = inferExitType(row);
  const exitKbar = inferExitKbar(row);
  const exitKbarReason = inferExitKbarReason(row);
  const riskLevel = inferRiskLevel(row);
  const advice = exitAdvice(row, action);

  const detailGrid = isExit ? `
          <div><span>股票名稱</span><b>${stockName}</b></div>
          <div><span>來源</span><b>${source}</b></div>
          <div><span>策略層</span><b>${bucket}</b></div>
          <div><span>出場型態</span><b>${exitType}</b></div>
          <div><span>出場K棒型態</span><b>${exitKbar}</b></div>
          <div><span>參考價</span><b>${close}</b></div>
          <div><span>建議金額</span><b>${amount}</b></div>
          <div><span>目標權重</span><b>${weight}</b></div>
          <div><span>流動性</span><b>${liq}</b></div>
          <div><span>成交量</span><b>${vol}</b></div>
          <div><span>成交金額</span><b>${turn}</b></div>
          <div><span>風險等級</span><b>${riskLevel}</b></div>
  ` : `
          <div><span>股票名稱</span><b>${stockName}</b></div>
          <div><span>系統評測</span><b>${topBadge || "--"}</b></div>
          <div><span>來源</span><b>${source}</b></div>
          <div><span>策略層</span><b>${bucket}</b></div>
          <div><span>進場型態</span><b>${entry}</b></div>
          <div><span>參考價</span><b>${close}</b></div>
          <div><span>建議金額</span><b>${amount}</b></div>
          <div><span>目標權重</span><b>${weight}</b></div>
          <div><span>流動性</span><b>${liq}</b></div>
          <div><span>成交量</span><b>${vol}</b></div>
          <div><span>成交金額</span><b>${turn}</b></div>
          <div><span>流動性分數</span><b>${liqScore}</b></div>
  `;

  const detailText = isExit ? `
        <div class="detail-text"><b>出場原因</b><p>${reason}</p></div>
        <div class="detail-text"><b>K棒判斷原因</b><p>${exitKbarReason}</p></div>
        <div class="detail-text"><b>建議動作</b><p>${advice}</p></div>
        <div class="detail-text"><b>系統提示</b><p>${note}</p></div>
  ` : `
        <div class="detail-text"><b>原因</b><p>${reason}</p></div>
        <div class="detail-text"><b>中文決策提示</b><p>${safeText(row.zh_hint || row.chinese_hint || row.decision_hint, note)}</p></div>
        <div class="detail-text"><b>系統提示</b><p>${note}</p></div>
  `;

  return `
    <article class="scan-item ${cls}">
      <div class="scan-main" data-toggle="${key}">
        <div class="scan-action ${cls}">${emoji} ${label}</div>
        <div class="scan-stock">${stock}</div>
        <div class="scan-score">${score}</div>
        <div class="scan-top">${topBadge}</div>
        <div class="scan-entry">${isExit ? label : entry}</div>
        <div class="scan-close">${close}</div>
      </div>

      <div class="scan-detail" id="${key}">
        <div class="detail-grid">
          ${detailGrid}
        </div>
        ${detailText}
      </div>
    </article>
  `;
}

function bindToggle() {
  document.querySelectorAll("[data-toggle]").forEach(el => {
    if (el.dataset.bound === "1") return;
    el.dataset.bound = "1";
    el.addEventListener("click", () => {
      const id = el.getAttribute("data-toggle");
      const detail = document.getElementById(id);
      if (!detail) return;
      detail.classList.toggle("open");
    });
  });
}

function renderStats(rows, summary) {
  const c = groupCounts(rows);
  qs("filterStats").innerHTML = `
    <div class="stats-line">
      <span>總筆數 <b>${rows.length}</b></span>
      <span>SELL <b>${c.SELL}</b></span>
      <span>REDUCE <b>${c.REDUCE}</b></span>
      <span>BUY <b>${c.BUY}</b></span>
      <span>TEST <b>${c.TEST}</b></span>
      <span>WATCH <b>${c.WATCH}</b></span>
      <span>BLOCK <b>${c.BLOCK}</b></span>
    </div>
    <div class="source-line">資料來源：${safeText(summary.source)}</div>
  `;
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

    const groups = splitRows(rows);

    renderMeta(regime, summary, rows);
    renderDecision(rows);
    renderFinalActions(groups.main);
    renderSectionList("testList", groups.test, "test", 80);
    renderSectionList("watchList", groups.watch, "watch", 80);
    renderSectionList("blockList", groups.block, "block", 80);
    renderStats(rows, summary);
  } catch (e) {
    console.error(e);
    qs("syncStatus").innerHTML = "❌ 讀取失敗：" + e.message;
    qs("syncStatus").className = "sync error";
  }
}

document.addEventListener("DOMContentLoaded", init);
