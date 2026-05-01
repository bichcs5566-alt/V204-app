// ===== v266.15.2 Macro / TOP 說明補強 =====
function getTopBadgeV266152(row) {
  row = safeObj(row);
  const sectionTop = safeText(row.section_top_opportunity || "", "");
  const overallTop = safeText(row.top_opportunity || "", "");
  const sectionRank = safeText(row.section_opportunity_rank || "", "");
  const overallRank = safeText(row.opportunity_rank || "", "");

  if (sectionTop && sectionTop !== "--") return `🔥 ${sectionTop}`;
  if (overallTop && overallTop !== "--") return `🔥 ${overallTop}`;
  if (sectionRank && sectionRank !== "--") return `🔥 TOP${sectionRank}`;
  if (overallRank && overallRank !== "--") return `🔥 TOP${overallRank}`;
  return "";
}

function macroRuleTextV266152(data) {
  const valid = Number(data?.valid_indicator_count || 0);
  const total = Number(data?.total_indicator_count || 0);
  const unknown = Number(data?.unknown_count || 0);
  const raw = data?.macro_raw_label || data?.macro_label || "--";
  const label = data?.macro_label || "--";
  const score = Number(data?.macro_score || 0);
  const adj = Number(data?.macro_adjusted_score ?? data?.macro_score ?? 0);
  const confidence = data?.macro_confidence_label || "";

  const rule = "評分：每項指標 +1 / 0 / -1；分數越高越偏多，分數越低越保守。";
  const confidenceText = total
    ? `有效 ${valid}/${total}，未知 ${unknown}，${confidence || "信心未定"}，加權分數 ${adj.toFixed(2)}。`
    : "有效資料不足，暫以中性處理。";
  return `${rule}｜原始：${raw} ${score.toFixed(1)}｜目前：${label}｜${confidenceText}`;
}

function macroAdviceTextV266152(data) {
  const label = data?.macro_label || "--";
  const policy = data?.macro_policy || "--";
  const unknown = Number(data?.unknown_count || 0);

  let tip = `${label}：${policy}`;
  if (unknown >= 4) tip += "｜注意：總經資料仍不完整，不能單獨作為重倉依據。";
  return tip;
}

async function loadMacroExplainV266152() {
  try {
    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const ruleText = macroRuleTextV266152(data);
    const adviceText = macroAdviceTextV266152(data);

    const macroRuleEl =
      document.querySelector("[data-macro-rule]") ||
      document.querySelector("#macroRule") ||
      document.querySelector(".macro-rule");

    const macroAdviceEl =
      document.querySelector("[data-macro-advice]") ||
      document.querySelector("#macroAdvice") ||
      document.querySelector(".macro-advice");

    if (macroRuleEl) macroRuleEl.textContent = ruleText;
    if (macroAdviceEl) macroAdviceEl.textContent = adviceText;

    // 沒有專用欄位時，直接補在總經狀態下方
    const macroBox =
      document.querySelector("[data-macro]")?.closest(".stat-card, .info-card, .meta-card, .kv-card, .card") ||
      document.querySelector("[data-macro]")?.parentElement;

    if (macroBox && !document.querySelector(".macro-explain-v266152")) {
      const div = document.createElement("div");
      div.className = "macro-explain-v266152";
      div.innerHTML = `
        <div><b>評分標準</b><br>${ruleText}</div>
        <div style="margin-top:8px;"><b>總經提示</b><br>${adviceText}</div>
      `;
      macroBox.appendChild(div);
    }
  } catch (e) {
    console.log("macro explain load fail", e);
  }
}

var topBadge = "";

// ===== v266.15.1 Stable Helpers / 防炸工具 =====
function safeObj(row) {
  return row && typeof row === "object" ? row : {};
}

function getTopBadge(row) { return getTopBadgeV266152(row); }

function formatLotsFromShares(v) {
  if (v === undefined || v === null || v === "" || v === "--") return "--";
  const raw = Number(String(v).replace(/,/g, "").replace("張", "").replace("股", ""));
  if (!Number.isFinite(raw) || raw <= 0) return "--";

  // 後端若是股數，轉張；若本來就是張數（通常小於 2,000,000），保留。
  const lots = raw >= 2000000 ? raw / 1000 : raw;
  return Math.round(lots).toLocaleString("zh-TW") + "張";
}

function formatTurnoverTW(v) {
  if (v === undefined || v === null || v === "" || v === "--") return "--";
  let raw = Number(String(v).replace(/,/g, "").replace("億", ""));
  if (!Number.isFinite(raw) || raw <= 0) return "--";

  // 如果是元，轉億；如果已是億，保留。
  if (raw > 10000000) raw = raw / 100000000;
  return raw.toLocaleString("zh-TW", { maximumFractionDigits: 2 }) + "億";
}

function macroConfidenceText(data) {
  const label = data?.macro_label || "--";
  const score = Number(data?.macro_score || 0);
  const adj = Number(data?.macro_adjusted_score ?? data?.macro_score ?? 0);
  const conf = data?.macro_confidence_label || "";
  const valid = Number(data?.valid_indicator_count || 0);
  const total = Number(data?.total_indicator_count || 0);

  if (valid && total) {
    return `${label}｜分數 ${score.toFixed(1)}｜${conf} ${valid}/${total}｜加權 ${adj.toFixed(2)}`;
  }
  return `${label}｜分數 ${score.toFixed(1)}`;
}


// ===== v266.14 Macro Dashboard / 總經狀態讀取 =====
async function loadMacroDashboardV26614() {
  try {
    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const macroText = data ? macroConfidenceText(data) : "--｜分數 --";

    const policyText =
      (data && data.macro_policy)
        ? data.macro_policy
        : "--";

    const macroEl =
      document.querySelector("[data-macro]") ||
      document.querySelector("#macroStatus") ||
      document.querySelector(".macro-status");

    const riskEl =
      document.querySelector("[data-risk]") ||
      document.querySelector("#riskMode") ||
      document.querySelector(".risk-mode");

    if (macroEl) macroEl.textContent = macroText;
    if (riskEl) {
      const warn = Number(data?.unknown_count || 0) >= 4 ? "｜資料不完整" : "";
      riskEl.textContent = `${policyText}${warn}`;
    }

    window.__macroRegime = data || {};
  } catch (e) {
    console.log("macro dashboard load fail", e);
  }
}

/*
app.js - 最新 data_pipeline 系統：本機 Token 觸發版 + 時間校準版

保留：
1. 原本卡片 UI / 列表 / CSV 讀取 / 排序 / 展開邏輯
2. GitHub 本機設定區
3. 更新資料按鈕可觸發 data_pipeline.yml
4. 觸發後輪詢 GitHub Actions，完成後提示並重新整理
5. 頂部「現在時間」每秒自動跑
6. 「最後更新」自動校準 GitHub Actions UTC → 台灣時間
*/

const DATA_DIR = "./data/";

const FILES = {
  final: DATA_DIR + "final_action_plan.csv",
  finalSummary: DATA_DIR + "final_action_summary.json",
  regime: DATA_DIR + "market_regime.json",
  macro: DATA_DIR + "macro_regime.json",
  tradePlan: DATA_DIR + "trade_plan.csv"
};

const GH_STORAGE_KEY = "daily_dashboard_github_settings_v1";
const POS_STORAGE_KEY = "daily_dashboard_positions_v1";
const DEFAULT_WORKFLOW_ID = "data_pipeline.yml";

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

let liveClockTimer = null;
let positionClockTimer = null;
let pollingTimer = null;

function qs(id) {
  return document.getElementById(id);
}


// ===== v266.10.3 中文語意轉換層 =====
function zhSource(v) {
  const s = String(v || "").trim().toUpperCase();
  const map = {
    "ENTRY": "策略進場",
    "EXIT": "策略出場",
    "POSITION": "持倉管理",
    "V266_DUAL": "雙策略系統",
    "FINAL_DECISION_ENGINE": "最終決策",
    "TRADE_PLAN": "交易計畫",
    "CANDIDATES": "候選名單",
    "MANUAL": "手動持倉"
  };
  return map[s] || safeText(v, "--");
}

function zhStrategy(v) {
  const s = String(v || "").trim().toUpperCase();
  const map = {
    "PRE": "預備佈局",
    "CORE": "核心卡位",
    "CORE 卡位": "核心卡位",
    "ALPHA": "主力動能",
    "ALPHA 主力": "主力動能",
    "DUAL": "雙策略",
    "POSITION": "持倉風控",
    "TEST": "試單觀察",
    "WATCH": "觀察名單",
    "BLOCK": "禁止交易"
  };
  return map[s] || safeText(v, "--");
}

function zhEntry(v) {
  const s = String(v || "").trim().toUpperCase();
  const map = {
    "WAIT": "等待確認",
    "BREAK": "突破確認",
    "PULLBACK": "回檔接近",
    "BUY": "可進場",
    "TEST": "小量試單",
    "WATCH": "只觀察",
    "SELL": "賣出",
    "REDUCE": "減碼",
    "HOLD": "續抱觀察",
    "HIGH_LIQUIDITY_BUY": "高流動性買進",
    "高流動性強勢買進": "高流動性買進",
    "強勢試單": "強勢試單",
    "早期卡位": "早期卡位",
    "低量試單": "低量試單",
    "CORE卡位": "核心卡位",
    "CORE小倉試單": "核心小倉試單",
    "ALPHA主力買進": "主力買進",
    "ALPHA試單": "主力試單",
    "ALPHA觀察": "主力觀察"
  };
  return map[s] || safeText(v, "--");
}


function topOpportunityBadge(row) { return getTopBadgeV266152(row); }

function zhFinalAdvice(row) {
  const action = String(row.final_action || row.action || "").trim().toUpperCase();
  const strategy = String(row.strategy_type || row.bucket || "").trim().toUpperCase();
  const entry = String(row.entry_type || row.action_sub || "").trim().toUpperCase();
  const liq = String(row.liquidity_level || "").trim().toUpperCase();

  if (action === "SELL") return "持倉風控：優先處理賣出，不建議拖延。";
  if (action === "REDUCE") return "持倉風控：建議先減碼，降低部位風險。";
  if (action === "BUY" && strategy.includes("ALPHA")) return "主力動能：流動性充足，可分批進場。";
  if (action === "BUY" && strategy.includes("CORE")) return "核心卡位：可小倉進場，等待結構放大。";
  if (action === "TEST") return "試單模式：只適合小量測試，不要一次重倉。";
  if (action === "WATCH") return "觀察模式：條件尚未完整，不急著下單。";
  if (action === "BLOCK") return "禁止交易：條件不足，暫時不要碰。";
  if (entry === "WAIT") return "等待確認：訊號未完成，先觀察。";
  if (entry === "BREAK") return "突破型態：注意是否能站穩，不追高。";
  if (liq === "LOW" || liq === "BLOCK") return "流動性不足：不適合放大資金。";
  return "系統提示：依照分層操作，避免情緒下單。";
}

function safeText(v, fallback = "--") {
  if (v === undefined || v === null || v === "") return fallback;
  return String(v);
}

function setSyncStatus(message, cls = "sync") {
  const el = qs("syncStatus");
  if (!el) return;
  el.innerHTML = message;
  el.className = cls;
}

function compactErrorText(text) {
  const s = String(text || "");
  if (!s) return "未知錯誤";
  try {
    const obj = JSON.parse(s);
    if (obj.message) return obj.message;
  } catch (e) {}
  return s.length > 120 ? s.slice(0, 120) + "..." : s;
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

  const headers = parseCsvLine(lines[0].replace(/^\uFEFF/, "")).map(h => h.trim());

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

function formatTWClock(date = new Date()) {
  return new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(date);
}

function formatTWDateTime(input) {
  if (!input) return "--";

  const s = String(input).trim();
  let d;

  // GitHub Actions 常見輸出如果沒有時區，視為 UTC，再轉台灣時間。
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(s)) {
    d = new Date(s.replace(" ", "T") + "Z");
  } else if (/^\d{4}-\d{2}-\d{2}T/.test(s) && !/[zZ]|[+-]\d{2}:\d{2}$/.test(s)) {
    d = new Date(s + "Z");
  } else {
    d = new Date(s);
  }

  if (Number.isNaN(d.getTime())) return s;

  const parts = new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).formatToParts(d);

  const get = (type) => parts.find(p => p.type === type)?.value || "";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

function startLiveClock() {
  const el = qs("liveClock");
  if (!el) return;

  if (liveClockTimer) clearInterval(liveClockTimer);

  const tick = () => {
    el.textContent = formatTWClock(new Date());
  };

  tick();
  liveClockTimer = setInterval(tick, 1000);
}


function setPositionStatus(message, cls = "position-status") {
  const el = qs("positionStatus");
  if (!el) return;
  el.innerHTML = message;
  el.className = cls;
}

function startPositionClock() {
  const el = qs("positionLiveClock");
  if (!el) return;

  if (positionClockTimer) clearInterval(positionClockTimer);

  const tick = () => {
    el.textContent = formatTWClock(new Date());
  };

  tick();
  positionClockTimer = setInterval(tick, 1000);
}

function getLastPositionUpdateText() {
  const rows = loadPositions();
  if (!rows.length) return "尚無持倉";

  const times = rows
    .map(r => new Date(String(r.updated_at || "").replace(" ", "T")).getTime())
    .filter(t => Number.isFinite(t));

  if (!times.length) return "已建立持倉";
  return formatTWDateTime(new Date(Math.max(...times)).toISOString());
}

function refreshPositionStatus(prefix = "持倉已就緒") {
  setPositionStatus(
    `${prefix}｜最後更新 ${getLastPositionUpdateText()}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`,
    "position-status ok"
  );
  startPositionClock();
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

    const sa = Number(a.score || a.entry_score || 0);
    const sb = Number(b.score || b.entry_score || 0);
    if (sb !== sa) return sb - sa;

    const la = liquiditySortRank(a);
    const lb = liquiditySortRank(b);
    if (lb !== la) return lb - la;

    const va = Number(a.volume || 0);
    const vb = Number(b.volume || 0);
    if (vb !== va) return vb - va;

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

function loadGithubSettings() {
  try {
    const raw = localStorage.getItem(GH_STORAGE_KEY);
    if (!raw) return {
      owner: "bichcs5566-alt",
      repo: "bichcs5566-alt.github.io",
      branch: "main",
      token: "",
      workflow: DEFAULT_WORKFLOW_ID
    };

    const obj = JSON.parse(raw);
    return {
      owner: obj.owner || "bichcs5566-alt",
      repo: obj.repo || "bichcs5566-alt.github.io",
      branch: obj.branch || "main",
      token: obj.token || "",
      workflow: obj.workflow || DEFAULT_WORKFLOW_ID
    };
  } catch (e) {
    return {
      owner: "bichcs5566-alt",
      repo: "bichcs5566-alt.github.io",
      branch: "main",
      token: "",
      workflow: DEFAULT_WORKFLOW_ID
    };
  }
}

function saveGithubSettings() {
  const settings = {
    owner: qs("ghOwner")?.value.trim() || "",
    repo: qs("ghRepo")?.value.trim() || "",
    branch: qs("ghBranch")?.value.trim() || "main",
    token: qs("ghToken")?.value.trim() || "",
    workflow: DEFAULT_WORKFLOW_ID
  };

  localStorage.setItem(GH_STORAGE_KEY, JSON.stringify(settings));
  renderGithubSettingsStatus("已儲存", true);
}

function clearGithubSettings() {
  localStorage.removeItem(GH_STORAGE_KEY);
  const settings = loadGithubSettings();
  if (qs("ghOwner")) qs("ghOwner").value = settings.owner;
  if (qs("ghRepo")) qs("ghRepo").value = settings.repo;
  if (qs("ghBranch")) qs("ghBranch").value = settings.branch;
  if (qs("ghToken")) qs("ghToken").value = "";
  renderGithubSettingsStatus("已清除", false);
}

function renderGithubSettingsStatus(message, saved) {
  const el = qs("ghStatus");
  if (!el) return;
  el.textContent = `狀態：${message}｜Workflow：${DEFAULT_WORKFLOW_ID}`;
  el.className = saved ? "github-status saved" : "github-status";
}


function loadPositions() {
  try {
    const raw = localStorage.getItem(POS_STORAGE_KEY);
    const rows = raw ? JSON.parse(raw) : [];
    return Array.isArray(rows) ? rows.filter(p => p.stock_id) : [];
  } catch (e) {
    return [];
  }
}

function savePositions(rows) {
  localStorage.setItem(POS_STORAGE_KEY, JSON.stringify(rows || []));
}

function positionToCsv(rows) {
  const headers = ["stock_id", "avg_price", "shares", "lots", "note", "updated_at"];
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };

  const lines = [headers.join(",")];
  rows.forEach(r => {
    lines.push(headers.map(h => esc(r[h])).join(","));
  });
  return lines.join("\n") + "\n";
}

function positionCost(row) {
  const avg = Number(row.avg_price);
  const shares = Number(row.shares);
  if (!Number.isFinite(avg) || !Number.isFinite(shares)) return "--";
  return Math.round(avg * shares).toLocaleString("en-US");
}


function getPositionRiskMap() {
  return window.__positionRiskMap || {};
}

function setPositionRiskMap(rows) {
  const map = {};
  (rows || []).forEach(r => {
    const source = String(r.source || "").toUpperCase();
    const bucket = String(r.bucket || "").toUpperCase();
    if (source === "EXIT" || source === "POSITION" || bucket === "POSITION") {
      const sid = safeText(r.stock_id, "");
      if (sid) map[sid] = r;
    }
  });
  window.__positionRiskMap = map;
}

function renderPositionRiskInsideCard(stock) {
  const row = getPositionRiskMap()[String(stock)];
  if (!row) return "";

  const action = normalizeAction(row.final_action || row.action);
  const cls = ACTION_CLASS[action] || "watch";
  const label = ACTION_LABEL[action] || action;
  const close = num(row.close);
  const entry = safeText(row.entry_type || row.execution_flag, "--");
  const reason = safeText(row.reason, "無");
  const note = safeText(row.system_note, "無");
  const amount = row.suggested_amount ? money(row.suggested_amount) : "--";

  return `
    <div class="position-inline-risk ${cls}">
      <div class="position-inline-risk-head">
        <span class="scan-action ${cls}">${ACTION_EMOJI[action] || "⚪"} ${label}</span>
        <b>${topBadge ? `<span class="top-badge">${topBadge}</span>` : ""}${entry}</b>
        <strong>${close}</strong>
      </div>
      <div class="position-inline-risk-grid">
        <div><span>參考價</span><b>${close}</b></div>
        <div><span>部位金額</span><b>${amount}</b></div>
      </div>
      <div class="position-inline-risk-text"><b>原因</b><p>${reason}</p></div>
      <div class="position-inline-risk-text"><b>系統提示</b><p>${note}</p></div>
    </div>
  `;
}


function renderPositions() {
  const box = qs("positionList");
  if (!box) return;

  const rows = loadPositions();

  if (!rows.length) {
    box.innerHTML = `<div class="empty">尚未建立持倉。請輸入個股、均價、張數後按「新增 / 更新」。</div>`;
    return;
  }

  box.innerHTML = rows.map((row, idx) => {
    const key = `pos-${idx}`;
    let stock = safeText(row.stock_id);
  if (stock.endsWith(".0")) stock = stock.slice(0, -2);
  const stockName = safeText(row.stock_name, "");
  const topBadge = getTopBadge(row);
    const avg = num(row.avg_price);
    const lots = num(row.lots, 2);
    const shares = money(row.shares);
    const cost = positionCost(row);
    const note = safeText(row.note, "手動持倉");

    return `
      <article class="scan-item position">
        <div class="scan-main position-main" data-toggle="${key}">
          <div class="scan-action position">📦 持倉</div>
          <div class="scan-stock">${stock}</div>
          <div class="scan-score">${lots}</div>
          <div class="scan-top">張</div>
          <div class="scan-entry">均價 ${avg}</div>
          <div class="scan-close">${cost}</div>
        </div>

        <div class="scan-detail" id="${key}">
          <div class="detail-grid">
            <div><span>個股</span><b>${stock}</b></div>
            <div><span>均價</span><b>${avg}</b></div>
            <div><span>張數</span><b>${lots}</b></div>
            <div><span>股數</span><b>${shares}</b></div>
            <div><span>成本</span><b>${cost}</b></div>
            <div><span>更新時間</span><b>${safeText(row.updated_at)}</b></div>
          </div>
          <div class="detail-text"><b>備註</b><p>${note}</p></div>
          ${renderPositionRiskInsideCard(stock)}
          <div class="position-row-actions">
            <button type="button" data-edit-position="${stock}">編輯</button>
            <button type="button" class="danger" data-delete-position="${stock}">刪除</button>
          </div>
        </div>
      </article>
    `;
  }).join("");

  bindToggle();
  bindPositionRowActions();
}

function clearPositionForm() {
  if (qs("posStock")) qs("posStock").value = "";
  if (qs("posPrice")) qs("posPrice").value = "";
  if (qs("posLots")) qs("posLots").value = "";
  if (qs("posNote")) qs("posNote").value = "";
}

function addOrUpdatePosition() {
  const stock = (qs("posStock")?.value || "").trim();
  const price = Number(qs("posPrice")?.value || "");
  const lots = Number(qs("posLots")?.value || "");
  const note = (qs("posNote")?.value || "").trim();

  if (!stock) {
    setSyncStatus("❌ 請輸入個股代號", "sync error");
    return;
  }

  if (!Number.isFinite(price) || price <= 0) {
    setSyncStatus("❌ 請輸入正確均價", "sync error");
    return;
  }

  if (!Number.isFinite(lots) || lots <= 0) {
    setSyncStatus("❌ 請輸入正確張數", "sync error");
    return;
  }

  const rows = loadPositions();
  const idx = rows.findIndex(r => String(r.stock_id) === stock);
  const item = {
    stock_id: stock,
    avg_price: String(price),
    lots: String(lots),
    shares: String(Math.round(lots * 1000)),
    note: note || "手動持倉",
    updated_at: formatTWDateTime(new Date().toISOString())
  };

  if (idx >= 0) rows[idx] = item;
  else rows.push(item);

  rows.sort((a, b) => String(a.stock_id).localeCompare(String(b.stock_id)));
  savePositions(rows);
  renderPositions();
  refreshPositionStatus("持倉區已同步");
  clearPositionForm();
  setSyncStatus(`✅ 持倉已儲存於本機｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
  startLiveClock();
  refreshPositionStatus("持倉已儲存於本機");
}

function bindPositionRowActions() {
  document.querySelectorAll("[data-edit-position]").forEach(btn => {
    if (btn.dataset.bound === "1") return;
    btn.dataset.bound = "1";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const stock = btn.getAttribute("data-edit-position");
      const row = loadPositions().find(r => String(r.stock_id) === stock);
      if (!row) return;
      qs("posStock").value = row.stock_id || "";
      qs("posPrice").value = row.avg_price || "";
      qs("posLots").value = row.lots || "";
      qs("posNote").value = row.note || "";
      window.scrollTo({ top: qs("positionCard").offsetTop - 10, behavior: "smooth" });
    });
  });

  document.querySelectorAll("[data-delete-position]").forEach(btn => {
    if (btn.dataset.bound === "1") return;
    btn.dataset.bound = "1";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const stock = btn.getAttribute("data-delete-position");
      const rows = loadPositions().filter(r => String(r.stock_id) !== stock);
      savePositions(rows);
      renderPositions();
  refreshPositionStatus("持倉區已同步");
      setSyncStatus(`✅ 已刪除持倉 ${stock}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
      startLiveClock();
      refreshPositionStatus(`已刪除持倉 ${stock}`);
    });
  });
}

async function getFileSha(path) {
  try {
    const res = await githubApi(`/contents/${encodeURIComponent(path).replace(/%2F/g, "/")}`, {
      method: "GET"
    });
    const text = await res.text();
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`讀取 ${path} 失敗 ${res.status}：${compactErrorText(text)}`);
    const data = JSON.parse(text);
    return data.sha || null;
  } catch (e) {
    if (String(e.message || "").includes("404")) return null;
    throw e;
  }
}

function base64Utf8(str) {
  return btoa(unescape(encodeURIComponent(str)));
}

async function putRepoFile(path, content, message) {
  const gh = loadGithubSettings();
  const sha = await getFileSha(path);

  const body = {
    message,
    content: base64Utf8(content),
    branch: gh.branch
  };

  if (sha) body.sha = sha;

  const res = await githubApi(`/contents/${encodeURIComponent(path).replace(/%2F/g, "/")}`, {
    method: "PUT",
    body: JSON.stringify(body)
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`寫入 ${path} 失敗 ${res.status}：${compactErrorText(text)}`);
  }

  return JSON.parse(text);
}

async function syncPositionsToRepo() {
  const rows = loadPositions();
  const csv = positionToCsv(rows);

  if (!rows.length) {
    setSyncStatus("❌ 尚未建立持倉，請先新增持倉再同步。", "sync error");
    return false;
  }

  setSyncStatus(`📦 持倉同步到 GitHub 中｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
  startLiveClock();
  setPositionStatus(`📦 持倉同步到 GitHub 中｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
  startPositionClock();

  await putRepoFile("positions_manual.csv", csv, "update manual positions");
  await putRepoFile("mobile_dashboard_v1/data/positions_manual.csv", csv, "update dashboard manual positions");

  setSyncStatus(`✅ 持倉已同步到 GitHub｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
  startLiveClock();
  setPositionStatus(`✅ 持倉已同步到 GitHub｜同步時間 ${formatTWDateTime(new Date().toISOString())}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status ok");
  startPositionClock();
  return true;
}

async function rerunStrategyWithPositions() {
  try {
    setPositionStatus(`🚀 持倉重跑策略準備中｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
    startPositionClock();

    const ok = await syncPositionsToRepo();
    if (!ok) return;

    setPositionStatus(`🚀 已同步持倉，正在觸發重跑策略｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status ok");
    startPositionClock();

    await triggerDataPipeline();
  } catch (e) {
    setSyncStatus(`❌ 持倉同步 / 重跑失敗：${e.message}`, "sync error");
    setPositionStatus(`❌ 持倉同步 / 重跑失敗：${e.message}`, "position-status error");
  }
}

function renderAppShell() {
  const gh = loadGithubSettings();

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

      <section id="positionCard" class="card position-card">
        <div class="section-head">
          <h2>📦 持倉管理</h2>
          <span class="hint">輸入後可同步並重跑策略</span>
        </div>

        <div class="position-form">
          <input id="posStock" class="position-input" placeholder="個股，例如 2330" inputmode="numeric" />
          <input id="posPrice" class="position-input" placeholder="均價，例如 580" inputmode="decimal" />
          <input id="posLots" class="position-input" placeholder="張數，例如 1.5" inputmode="decimal" />
          <input id="posNote" class="position-input" placeholder="備註，可不填" />
        </div>

        <div class="position-actions">
          <button id="addPositionBtn" type="button">新增 / 更新</button>
          <button id="syncPositionBtn" type="button" class="secondary">同步持倉</button>
          <button id="rerunWithPositionBtn" type="button" class="danger">重跑策略</button>
        </div>

        <div id="positionStatus" class="position-status">持倉狀態讀取中...</div>
        <div id="positionList"></div>
      </section>

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

      <section class="card github-settings-card">
        <h2>🔐 GitHub 本機設定</h2>
        <input id="ghOwner" class="github-input" value="${gh.owner}" placeholder="owner，例如 bichcs5566-alt" autocomplete="off" />
        <input id="ghRepo" class="github-input" value="${gh.repo}" placeholder="repo，例如 bichcs5566-alt.github.io" autocomplete="off" />
        <input id="ghBranch" class="github-input" value="${gh.branch}" placeholder="branch，例如 main" autocomplete="off" />
        <input id="ghToken" class="github-input" value="${gh.token}" placeholder="token，只存在本機瀏覽器" type="password" autocomplete="off" />
        <div class="github-actions">
          <button id="saveGhBtn" type="button">儲存</button>
          <button id="clearGhBtn" type="button" class="secondary">清除</button>
        </div>
        <div id="ghStatus" class="github-status">狀態：${gh.token ? "已儲存" : "未儲存"}｜Workflow：${DEFAULT_WORKFLOW_ID}</div>
      </section>
    </main>
  `;

  qs("refreshBtn").addEventListener("click", () => location.reload());
  qs("updateBtn").addEventListener("click", triggerDataPipeline);
  qs("saveGhBtn").addEventListener("click", saveGithubSettings);
  qs("clearGhBtn").addEventListener("click", clearGithubSettings);
  qs("addPositionBtn").addEventListener("click", addOrUpdatePosition);
  qs("syncPositionBtn").addEventListener("click", syncPositionsToRepo);
  qs("rerunWithPositionBtn").addEventListener("click", rerunStrategyWithPositions);
  renderPositions();
  refreshPositionStatus("持倉區已同步");
}

async function githubApi(path, options = {}) {
  const gh = loadGithubSettings();
  if (!gh.owner || !gh.repo || !gh.branch || !gh.token) {
    throw new Error("請先完成 GitHub 本機設定：owner / repo / branch / token");
  }

  const res = await fetch(`https://api.github.com/repos/${encodeURIComponent(gh.owner)}/${encodeURIComponent(gh.repo)}${path}`, {
    ...options,
    headers: {
      "Authorization": `Bearer ${gh.token}`,
      "Accept": "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      ...(options.headers || {})
    }
  });

  return res;
}

async function getLatestWorkflowRun(createdAfterIso) {
  const gh = loadGithubSettings();
  const res = await githubApi(`/actions/workflows/${encodeURIComponent(gh.workflow)}/runs?branch=${encodeURIComponent(gh.branch)}&per_page=10`, {
    method: "GET"
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`讀取進度失敗 ${res.status}：${compactErrorText(text)}`);
  }

  const data = JSON.parse(text);
  const runs = Array.isArray(data.workflow_runs) ? data.workflow_runs : [];
  const after = new Date(createdAfterIso).getTime();

  const matched = runs.find(run => {
    const t = new Date(run.created_at).getTime();
    return Number.isFinite(t) && t >= after - 30000;
  });

  return matched || runs[0] || null;
}

async function pollWorkflowRun(createdAfterIso) {
  if (pollingTimer) clearTimeout(pollingTimer);

  const started = Date.now();
  const timeoutMs = 60 * 60 * 1000;

  const loop = async () => {
    try {
      const run = await getLatestWorkflowRun(createdAfterIso);

      if (!run) {
        setSyncStatus(`⏳ 已觸發，等待 GitHub 建立任務｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
        startLiveClock();
      } else {
        const status = run.status || "";
        const conclusion = run.conclusion || "";
        const elapsedSec = Math.floor((Date.now() - started) / 1000);

        if (status === "completed") {
          if (conclusion === "success") {
            setSyncStatus(`✅ 後端更新完成｜${formatTWClock(new Date())}｜重新整理中...`, "sync ok");
            setTimeout(() => location.reload(), 1800);
            return;
          }

          setSyncStatus(`❌ 後端更新失敗：${safeText(conclusion)}｜請到 Actions 查看`, "sync error");
          return;
        }

        setSyncStatus(`⏳ 後端更新中：${safeText(status)}｜已等 ${elapsedSec} 秒｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
        startLiveClock();
      }

      if (Date.now() - started > timeoutMs) {
        setSyncStatus("⚠️ 等待超過 60 分鐘，請到 GitHub Actions 查看狀態。", "sync error");
        return;
      }

      pollingTimer = setTimeout(loop, 8000);
    } catch (e) {
      setSyncStatus(`❌ 進度查詢失敗：${e.message}`, "sync error");
    }
  };

  loop();
}

async function triggerDataPipeline() {
  const gh = loadGithubSettings();

  try {
    if (!gh.owner || !gh.repo || !gh.branch || !gh.token) {
      setSyncStatus("❌ 尚未完成 GitHub 本機設定", "sync error");
      return;
    }

    setSyncStatus(`🚀 觸發中｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
    startLiveClock();

    const createdAfterIso = new Date().toISOString();

    const res = await githubApi(`/actions/workflows/${encodeURIComponent(gh.workflow)}/dispatches`, {
      method: "POST",
      body: JSON.stringify({ ref: gh.branch })
    });

    const text = await res.text();

    if (res.status !== 204) {
      setSyncStatus(`❌ 觸發失敗 ${res.status}：${compactErrorText(text)}`, "sync error");
      return;
    }

    setSyncStatus(`✅ 已觸發 data_pipeline｜等待後端完成｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
    startLiveClock();
    pollWorkflowRun(createdAfterIso);
  } catch (e) {
    setSyncStatus(`❌ 觸發失敗：${e.message}`, "sync error");
  }
}


function zhMarketRegime(regime) {
  const r = String(regime || "").toUpperCase();
  const map = {
    BULL: "大盤偏多",
    NEUTRAL: "大盤中性",
    BEAR: "大盤偏弱"
  };
  return map[r] || safeText(regime, "--");
}

function zhMacroRegime(regime) {
  const r = String(regime || "").toUpperCase();
  const map = {
    RISK_ON: "總經偏多",
    NEUTRAL: "總經中性",
    RISK_OFF: "總經偏空"
  };
  return map[r] || safeText(regime, "--");
}

function zhRiskMode(summary, regime, macro) {
  const macroLabel = safeText(macro.macro_label || summary.macro_label, "");
  const marketLabel = safeText(regime.market_label || summary.market_label || regime.label || regime.regime, "");
  const guardLabel = safeText(summary.market_guard_label || regime.action_policy || regime.risk_mode, "");
  const parts = [];
  if (macroLabel) parts.push(macroLabel);
  if (marketLabel) parts.push(marketLabel);
  if (guardLabel) parts.push(guardLabel);
  return parts.length ? parts.join("｜") : "--";
}

function renderMeta(regime, summary, macro, rows) {
  const backendUpdatedAt = formatTWDateTime(summary.generated_at || regime.generated_at);

  const marketText = `${safeText(regime.market_label || summary.market_label || regime.label || regime.regime, "--")} ${safeText(regime.index_change_pct_text || summary.index_change_pct_text, "")}`.trim();
  const macroText = `${safeText(macro.macro_label || summary.macro_label, "--")}｜分數 ${safeText(macro.macro_score ?? summary.macro_score, "--")}`;
  const signalDate = safeText(regime.date || regime.latest_date || summary.generated_at, "--");

  qs("metaBox").innerHTML = `
    <div class="mini"><span>來源版本</span><b>C 完整交易系統</b></div>
    <div class="mini"><span>市場狀態</span><b>${marketText}</b></div>
    <div class="mini"><span>總經狀態</span><b>${macroText}</b></div>
    <div class="mini"><span>風險模式</span><b>${zhRiskMode(summary, regime, macro)}</b></div>
    <div class="mini"><span>訊號日</span><b>${signalDate}</b></div>
    <div class="mini"><span>最後更新</span><b>${backendUpdatedAt}</b></div>
    <div class="mini"><span>操作筆數</span><b>${rows.length}</b></div>
  `;

  setSyncStatus(`✅ 最終操作表已同步｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
  startLiveClock();
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


function getPositionDecisionRows(rows) {
  return (rows || []).filter(r => {
    const source = String(r.source || "").toUpperCase();
    const bucket = String(r.bucket || "").toUpperCase();
    return source === "EXIT" || source === "POSITION" || bucket === "POSITION";
  });
}

function renderPositionRiskHints(rows) {
  // 已改為顯示在每一張持倉卡片內，不再產生額外區塊。
  setPositionRiskMap(rows);
}


function normalizeLiquidityLevel(v) {
  const s = String(v || "").trim().toUpperCase();
  if (["HIGH", "高", "高流動性"].includes(s)) return "HIGH";
  if (["MEDIUM", "MID", "中", "中流動性"].includes(s)) return "MEDIUM";
  if (["LOW", "低", "低流動性"].includes(s)) return "LOW";
  return "";
}

function liquidityLabel(v) {
  const level = normalizeLiquidityLevel(v);
  if (level === "HIGH") return "高流動性";
  if (level === "MEDIUM") return "中流動性";
  if (level === "LOW") return "低流動性";
  return "--";
}

function liquidityClass(v) {
  const level = normalizeLiquidityLevel(v);
  if (level === "HIGH") return "liq-high";
  if (level === "MEDIUM") return "liq-mid";
  if (level === "LOW") return "liq-low";
  return "liq-none";
}

function formatVolume(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return Math.round(n).toLocaleString("en-US") + "張";
}

function formatTurnover(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  if (n >= 100000000) return (n / 100000000).toFixed(2) + "億";
  if (n >= 10000) return Math.round(n / 10000).toLocaleString("en-US") + "萬";
  return Math.round(n).toLocaleString("en-US");
}

function strategyDisplay(row) {
  const st = String(row.strategy_type || row.strategy || row.bucket || "").toUpperCase();
  if (st === "ALPHA") return "ALPHA 主力";
  if (st === "CORE") return "CORE 卡位";
  if (st === "TEST") return "TEST 觀察";
  return safeText(row.bucket || row.strategy_type || row.strategy, "--");
}

function liquiditySortRank(row) {
  const level = normalizeLiquidityLevel(row.liquidity_level || row.liquidity_tag);
  if (level === "HIGH") return 3;
  if (level === "MEDIUM") return 2;
  if (level === "LOW") return 1;
  return 0;
}


function detailCell(label, value, extraClass = "") {
  let v = safeText(value, "--");
  if (v === "" || v === "undefined" || v === "null") v = "--";
  return `<div><span>${label}</span><b class="${extraClass}">${v}</b></div>`;
}

function renderScanRow(row, key) {
  const action = normalizeAction(row.final_action || row.action);
  const cls = ACTION_CLASS[action] || "watch";
  const label = ACTION_LABEL[action] || action;
  const emoji = ACTION_EMOJI[action] || "⚪";
  const top = isTop(row) ? "🔥TOP" : "";
  let stock = safeText(row.stock_id);
  if (stock.endsWith(".0")) stock = stock.slice(0, -2);
  const stockName = safeText(row.stock_name, "");
  const topBadge = getTopBadge(row);
  const score = safeText(row.score);
  const source = zhSource(row.source);
  const bucket = zhStrategy(row.bucket || row.strategy_type);
  const entry = zhEntry(row.entry_type || row.action_sub);
  const close = num(row.close || row.ref_price);
  const amount = row.suggested_amount ? money(row.suggested_amount) : "--";
  const weight = row.target_weight ? pct(row.target_weight) : "--";
  const volume = formatVolume(row.volume);
  const turnover = formatTurnover(row.turnover);
  const liqLabel = liquidityLabel(row.liquidity_level || row.liquidity_tag);
  const liqCls = liquidityClass(row.liquidity_level || row.liquidity_tag);
  const liqScore = row.liquidity_score ? num(row.liquidity_score) : "--";
  const strat = zhStrategy(strategyDisplay(row));
  const reason = safeText(row.reason || row.note, "無");
  const note = safeText(row.system_note || row.note, "無");
  const isPositionDecision = String(source).toUpperCase() === "EXIT" || String(bucket).toUpperCase() === "POSITION";

  const finalAdvice = zhFinalAdvice(row);
  const extraDetail = isPositionDecision
    ? `<div class="detail-text position-detail-note"><b>持倉提示</b><p>${finalAdvice}</p></div>`
    : `<div class="detail-text"><b>原因</b><p>${reason}</p></div>
       <div class="detail-text"><b>中文決策提示</b><p>${finalAdvice}</p></div>
       <div class="detail-text"><b>系統提示</b><p>${note}</p></div>`;

  return `
    <article class="scan-item ${cls}">
      <div class="scan-main scan-main-live" data-toggle="${key}">
        <div class="scan-action ${cls}">${emoji} ${label}</div>
        <div class="scan-stock">${stock}</div>
        <div class="scan-score">${score}</div>
        <div class="scan-top">${top}</div>
        <div class="scan-entry">${entry}</div>
        <div class="scan-liq ${liqCls}">${liqLabel}</div>
        <div class="scan-close">${close}</div>
      </div>

      <div class="scan-detail" id="${key}">
        <div class="detail-grid">
          ${detailCell("股票名稱", stockName)}
          ${topBadge ? detailCell("系統評測", topBadge + "｜優先觀察") : ""}
          ${detailCell("來源", source)}
          ${detailCell("策略層", strat)}
          ${detailCell("進場型態", entry)}
          ${detailCell("參考價", close)}
          ${detailCell("建議金額", amount)}
          ${detailCell("目標權重", weight)}
          ${detailCell("流動性", liqLabel, liqCls)}
          ${detailCell("成交量", formatLotsFromShares(row.volume))}
          ${detailCell("成交金額", formatTurnoverTW(row.turnover))}
          ${detailCell("流動性分數", liqScore)}
        </div>
        ${extraDetail}
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
  const high = rows.filter(r => normalizeLiquidityLevel(r.liquidity_level || r.liquidity_tag) === "HIGH").length;
  const mid = rows.filter(r => normalizeLiquidityLevel(r.liquidity_level || r.liquidity_tag) === "MEDIUM").length;
  const low = rows.filter(r => normalizeLiquidityLevel(r.liquidity_level || r.liquidity_tag) === "LOW").length;
  const alpha = rows.filter(r => String(r.strategy_type || r.strategy || r.bucket || "").toUpperCase() === "ALPHA").length;
  const core = rows.filter(r => String(r.strategy_type || r.strategy || r.bucket || "").toUpperCase() === "CORE").length;

  qs("filterStats").innerHTML = `
    <div class="stats-line">
      <span>總筆數 <b>${rows.length}</b></span>
      <span>SELL <b>${c.SELL}</b></span>
      <span>REDUCE <b>${c.REDUCE}</b></span>
      <span>BUY <b>${c.BUY}</b></span>
      <span>TEST <b>${c.TEST}</b></span>
      <span>WATCH <b>${c.WATCH}</b></span>
      <span>BLOCK <b>${c.BLOCK}</b></span>
      <span>ALPHA <b>${alpha}</b></span>
      <span>CORE <b>${core}</b></span>
      <span>高流動性 <b>${high}</b></span>
      <span>中流動性 <b>${mid}</b></span>
      <span>低流動性 <b>${low}</b></span>
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
      source: r.source || "ENTRY",
      bucket: r.strategy_type || r.bucket || "CORE",
      strategy_type: r.strategy_type || r.strategy || r.bucket || "",
      score: r.score || r.entry_score || r.rank_score || "",
      entry_type: r.action_sub || r.entry_type || "",
      execution_flag: r.action || "TOP",
      allowed: "True",
      close: r.ref_price || r.close || r.price || "",
      suggested_amount: r.suggested_amount || "",
      target_weight: r.target_weight || "",
      liquidity_level: r.liquidity_level || "",
      liquidity_tag: r.liquidity_tag || "",
      liquidity_score: r.liquidity_score || "",
      volume: r.volume || "",
      turnover: r.turnover || "",
      reason: r.reason || r.note || "",
      system_note: r.note || "fallback trade_plan"
    }));
  } catch (e) {
    console.error(e);
    return [];
  }
}

async function init() {
  renderAppShell();

  try {
    const [regime, summary, macro, rows] = await Promise.all([
      fetchJson(FILES.regime, {}),
      fetchJson(FILES.finalSummary, {}),
      fetchJson(FILES.macro, {}),
      loadFinalRows()
    ]);

    const groups = splitRows(rows);

    renderMeta(regime, summary, macro, rows);
    renderPositionRiskHints(rows);
    renderPositions();
    renderDecision(rows);
    renderFinalActions(groups.main);
    renderSectionList("testList", groups.test, "test", 80);
    renderSectionList("watchList", groups.watch, "watch", 80);
    renderSectionList("blockList", groups.block, "block", 80);
    renderStats(rows, summary);
  } catch (e) {
    console.error(e);
    setSyncStatus("❌ 讀取失敗：" + e.message, "sync error");
  }
}

document.addEventListener("DOMContentLoaded", init);


try { loadMacroDashboardV26614(); } catch(e) { console.log(e); }

try { loadMacroExplainV266152(); } catch(e) { console.log(e); }
