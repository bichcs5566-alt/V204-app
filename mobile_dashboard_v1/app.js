// ===== v266.36.2.2 Blank Screen Guard / 空白頁防護 =====
window.__APP_BOOT_ERROR__ = "";
window.addEventListener("error", function(e) {
  window.__APP_BOOT_ERROR__ = e && e.message ? e.message : String(e || "");
  try {
    if (document && document.body && !document.body.innerHTML.trim()) {
      document.body.innerHTML = '<main style="padding:28px;font-family:-apple-system,BlinkMacSystemFont,Noto Sans TC,sans-serif;color:#111827"><h1>⚠️ 前端載入失敗</h1><p>app.js 執行錯誤：' + window.__APP_BOOT_ERROR__ + '</p><p>請先回報這段錯誤，不要重跑策略。</p></main>';
    }
  } catch (_) {}
});

window.addEventListener("unhandledrejection", function(e) {
  window.__APP_BOOT_ERROR__ = e && e.reason ? (e.reason.message || String(e.reason)) : "Promise rejection";
});

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
    : "有效依策略判斷，暫以中性處理。";
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
app.js - v304.3 score欄改股價完整覆蓋版
來源：使用者上傳原檔 app 4.js，僅修改 renderScanRow 的 score 顯示來源。

app.js - v266.30E MA顯示修補版：保留原本功能 + 只補持倉 MA5/MA20 顯示

保留：
1. 原本卡片 UI / 列表 / CSV 讀取 / 排序 / 展開邏輯
2. GitHub 本機設定區
3. 更新資料按鈕可觸發 data_pipeline.yml
4. 觸發後輪詢 GitHub Actions，完成後提示並重新整理
5. 頂部「現在時間」每秒自動跑
6. 「最後更新」自動校準 GitHub Actions UTC → 台灣時間
*/

const DATA_DIR = "./data/";

const APP_PATCH_VERSION = "v329_priority_final_bridge";
const FORCE_REFRESH_NONCE_V26646 = Date.now();
function bustUrlV26647(url) {
  const sep = String(url).includes("?") ? "&" : "?";
  return `${url}${sep}v=${APP_PATCH_VERSION}&t=${Date.now()}`;
}



const FILES = {
  final: DATA_DIR + "final_action_plan.csv",
  finalSummary: DATA_DIR + "final_action_summary.json",
  meta: DATA_DIR + "meta.json",
  regime: DATA_DIR + "market_regime.json",
  macro: DATA_DIR + "macro_regime.json",
  tradePlan: DATA_DIR + "trade_plan.csv",
  candidates: DATA_DIR + "candidates.csv",
  core: DATA_DIR + "core_candidates.csv",
  alpha: DATA_DIR + "alpha_candidates.csv",
  positionOverlay: DATA_DIR + "position_overlay.csv",
  ignition: DATA_DIR + "ignition_candidates.csv",
  evolution: DATA_DIR + "strategy_evolution.csv"
};

// v266.57：前端強制刷新鎖。
// 目的：Safari / GitHub Pages 不可再沿用舊 final_action_plan。
let LAST_WORKFLOW_RUN_V26648 = "";
let LIVE_WORKFLOW_TIMER_V26648 = null;

function isManualPositionActiveV26648() {
  try {
    const rows = loadPositions?.() || [];
    return rows.some(r => Number(String(r.shares || r.qty || 0).replace(/,/g, "")) > 0);
  } catch (e) {
    return false;
  }
}

function purgeStaleFinalRowsV26648(rows) {
  const active = isManualPositionActiveV26648();
  return (rows || []).filter(row => {
    const txt = String(row.final_action || row.action || row.status || row.decision || "").toUpperCase()
      + " " + String(row.reason || row.system_note || "");
    const isSell = /SELL|賣|賣出|出場|停損|REDUCE|減碼/.test(txt);
    if (isSell && !active) return false;
    return true;
  });
}

function forceClearClientCacheV26648() {
  try {
    Object.keys(localStorage || {}).forEach(k => {
      // v332：本機持倉測試模式，持倉必須保留，才能重新整理後繼續重新評估。
      if (k === POS_STORAGE_KEY || k === "daily_dashboard_positions_v1") return;
      if (/position/i.test(k)) return;
      if (/final|workflow|dashboard|csv|cache/i.test(k)) localStorage.removeItem(k);
    });
  } catch (e) {}
}



const GH_STORAGE_KEY = "daily_dashboard_github_settings_v1";
const POS_STORAGE_KEY = "daily_dashboard_positions_v1";
const DEFAULT_WORKFLOW_ID = "data_pipeline.yml";

// v266.57.1：GitHub 後端連結鎖定。
// Pages 網址是 bichcs5566-alt.github.io/V204-app/，但 Actions 所在 repo 是 V204-app。
// 舊設定若誤存成 bichcs5566-alt.github.io，會打到錯 repo，造成前端與後端斷開。
function normalizeGithubRepoV266571(repo) {
  const r = String(repo || "").trim();
  if (!r || r === "bichcs5566-alt.github.io" || r === "github.io") return "V204-app";
  return r;
}


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

// ===== v266.31 真後端秒數同步層 =====
let workflowStatusTimerV26631 = null;
let workflowStatusFetchTimerV26631 = null;
let workflowStatusCacheV26631 = null;

// ===== v266.57.4 後端執行狀態穩定修補 =====
// 只修 UI 狀態閃爍：
// 1. 已觸發新 workflow 時，不吃舊 workflow_status.json 的 success。
// 2. 後端執行中時，renderMeta 不再把狀態洗回「最終操作表已同步」。
function isWorkflowStatusActiveV266574() {
  const s = String(workflowStatusCacheV26631?.status || "").toLowerCase();
  return ["queued", "running", "in_progress", "waiting", "requested"].includes(s);
}

function isWorkflowRunTrackingActiveV266574() {
  try {
    const active = getActiveBackendRunV26630N?.();
    return !!(active && active.created_after_iso && active.started_at_ms);
  } catch (e) {
    return false;
  }
}

function shouldIgnoreStaleWorkflowStatusV266574(data) {
  try {
    const active = getActiveBackendRunV26630N?.();
    if (!active || !active.started_at_ms) return false;

    const status = String(data?.status || "").toLowerCase();
    if (!["success", "failed", "failure", "cancelled", "canceled"].includes(status)) return false;

    const activeStart = Number(active.started_at_ms);
    const candidates = [
      data?.end_time,
      data?.completed_at,
      data?.updated_at,
      data?.start_time,
      data?.started_at,
      data?.created_at
    ];

    let newest = null;
    for (const v of candidates) {
      const t = parseTimeMsV26631(v);
      if (!t) continue;
      if (!newest || t > newest) newest = t;
    }

    return !!(newest && newest < activeStart - 3000);
  } catch (e) {
    return false;
  }
}

function setIdleSyncStatusV266574(message, cls = "sync ok") {
  if (isWorkflowRunTrackingActiveV266574() || isWorkflowStatusActiveV266574()) return;
  setSyncStatus(message, cls);
}


function parseTimeMsV26631(v) {
  const t = new Date(v || "").getTime();
  return Number.isFinite(t) ? t : null;
}

function fmtDurationV26631(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}分${String(s).padStart(2, "0")}秒`;
}

function statusPhaseTextV26631(status) {
  const s = String(status || "").toLowerCase();
  if (s === "running") return "執行中";
  if (s === "queued") return "排隊中";
  if (s === "success") return "完成";
  if (s === "failed" || s === "failure") return "失敗";
  if (s === "cancelled" || s === "canceled") return "已取消";
  return s || "等待中";
}

function applyWorkflowStatusV26631(data) {
  if (!data || typeof data !== "object") return false;
  if (shouldIgnoreStaleWorkflowStatusV266574(data)) return false;
  workflowStatusCacheV26631 = data;

  const status = String(data.status || "").toLowerCase();
  const runNumber = data.run_number ? `#${data.run_number}` : "";
  const runKey = String(data.run_id || data.run_number || "");
  if (runKey && LAST_WORKFLOW_RUN_V26648 && LAST_WORKFLOW_RUN_V26648 !== runKey) {
    forceClearClientCacheV26648();
  }
  if (runKey) LAST_WORKFLOW_RUN_V26648 = runKey;
  const startMs = parseTimeMsV26631(data.start_time || data.started_at || data.created_at);
  const endMs = parseTimeMsV26631(data.end_time || data.completed_at);

  let durationText = data.duration_text || "--";
  if (startMs) {
    const baseEnd = endMs || Date.now();
    durationText = fmtDurationV26631(baseEnd - startMs);
  }

  if (status === "running" || status === "queued" || status === "in_progress") {
    setSyncStatus(`⏳ 後端策略${statusPhaseTextV26631(status)} ${runNumber}｜已跑 ${durationText}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
    setPositionStatus?.(`⏳ 後端策略${statusPhaseTextV26631(status)} ${runNumber}｜已跑 ${durationText}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
    startLiveClock();
    startPositionClock?.();
    return true;
  }

  if (status === "success") {
    const completeClock = formatTWClock(endMs ? new Date(endMs) : new Date());
    setSyncStatus(`✅ 後端策略完成 ${runNumber}｜總耗時 ${durationText}｜完成時間 ${completeClock}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
    setPositionStatus?.(`✅ 後端策略完成 ${runNumber}｜總耗時 ${durationText}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status ok");
    startLiveClock();
    startPositionClock?.();
    return true;
  }

  if (status === "failed" || status === "failure" || status === "cancelled" || status === "canceled") {
    setSyncStatus(`❌ 後端策略${statusPhaseTextV26631(status)} ${runNumber}｜耗時 ${durationText}｜請到 Actions 查看`, "sync error");
    setPositionStatus?.(`❌ 後端策略${statusPhaseTextV26631(status)} ${runNumber}｜耗時 ${durationText}`, "position-status error");
    return true;
  }

  return false;
}

function tickWorkflowStatusV26631() {
  if (workflowStatusCacheV26631) applyWorkflowStatusV26631(workflowStatusCacheV26631);
}

async function fetchWorkflowStatusV26631() {
  const urls = [
    "./data/workflow_status.json",
    "./mobile_dashboard_v1/data/workflow_status.json",
    "./workflow_status.json"
  ];
  for (const url of urls) {
    try {
      const res = await fetch(url + (url.includes("?") ? "&" : "?") + "v=" + Date.now(), { cache: "no-store" });
      if (!res.ok) continue;
      const text = await res.text();
      if (!text || text.trim().startsWith("<")) continue;
      const data = JSON.parse(text);
      if (applyWorkflowStatusV26631(data)) return true;
    } catch (e) {}
  }
  return false;
}

function startWorkflowStatusWatchV26631() {
  if (workflowStatusTimerV26631) clearInterval(workflowStatusTimerV26631);
  if (workflowStatusFetchTimerV26631) clearInterval(workflowStatusFetchTimerV26631);

  fetchWorkflowStatusV26631();
  workflowStatusTimerV26631 = setInterval(tickWorkflowStatusV26631, 1000);
  workflowStatusFetchTimerV26631 = setInterval(fetchWorkflowStatusV26631, 3000);
}

function markWorkflowTriggeredLocalV26631() {
  const now = new Date().toISOString();
  workflowStatusCacheV26631 = {
    status: "queued",
    start_time: now,
    updated_at: now,
    run_number: ""
  };
  applyWorkflowStatusV26631(workflowStatusCacheV26631);
  startWorkflowStatusWatchV26631();
}

function formatRunDurationV26630K(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}分${String(s).padStart(2, "0")}秒`;
}

function rememberBackendRunV26630K(payload) {
  try {
    localStorage.setItem("v26630_backend_run_status", JSON.stringify({
      ...payload,
      saved_at: new Date().toISOString()
    }));
  } catch (e) {}
}

function consumeBackendRunV26630K() {
  try {
    const raw = localStorage.getItem("v26630_backend_run_status");
    if (!raw) return null;
    localStorage.removeItem("v26630_backend_run_status");
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function showBackendRunCompleteIfAnyV26630K() {
  const last = consumeBackendRunV26630K();
  if (!last || last.status !== "success") return false;
  const duration = last.duration_text || "--";
  const doneAt = last.done_at || formatTWClock(new Date());
  setSyncStatus(`✅ 後端策略已完成｜耗時 ${duration}｜完成時間 ${doneAt}`, "sync ok");
  return true;
}

function saveActiveBackendRunV26630N(createdAfterIso) {
  try {
    localStorage.setItem("v26630_active_backend_run", JSON.stringify({
      created_after_iso: createdAfterIso,
      started_at_ms: Date.now(),
      saved_at: new Date().toISOString()
    }));
  } catch (e) {}
}

function getActiveBackendRunV26630N() {
  try {
    const raw = localStorage.getItem("v26630_active_backend_run");
    if (!raw) return null;
    const obj = JSON.parse(raw);
    if (!obj || !obj.created_after_iso || !obj.started_at_ms) return null;

    // 超過 2 小時視為過期，避免永久卡住。
    if (Date.now() - Number(obj.started_at_ms) > 2 * 60 * 60 * 1000) {
      localStorage.removeItem("v26630_active_backend_run");
      return null;
    }
    return obj;
  } catch (e) {
    return null;
  }
}

function clearActiveBackendRunV26630N() {
  try { localStorage.removeItem("v26630_active_backend_run"); } catch (e) {}
}

function resumeBackendRunIfActiveV26630N() {
  const active = getActiveBackendRunV26630N();
  if (!active) return false;
  const elapsedText = formatRunDurationV26630K(Date.now() - Number(active.started_at_ms));
  setSyncStatus(`⏳ 後端策略仍在追蹤｜已跑 ${elapsedText}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
  setPositionStatus?.(`⏳ 後端策略仍在追蹤｜已跑 ${elapsedText}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
  startLiveClock();
  startPositionClock?.();
  pollWorkflowRun(active.created_after_iso, Number(active.started_at_ms));
  return true;
}

async function detectActiveWorkflowOnLoadV26630O() {
  // v266.30O：不只靠 localStorage。
  // 頁面重新整理後，直接查 GitHub Actions 是否有 data_pipeline 正在跑。
  // 有正在跑就立刻接回計時，避免被 renderMeta 的「最終操作表已同步」洗掉。
  try {
    const gh = loadGithubSettings();
    const res = await githubApi(`/actions/workflows/${encodeURIComponent(gh.workflow)}/runs?branch=${encodeURIComponent(gh.branch)}&per_page=20`, {
      method: "GET"
    });

    const text = await res.text();
    if (!res.ok) return false;

    const trimmed = String(text || "").trim();
    if (trimmed.startsWith("<")) return false;

    let data;
    try {
      data = JSON.parse(text);
    } catch (e) {
      return false;
    }

    const runs = Array.isArray(data.workflow_runs) ? data.workflow_runs : [];
    const active = runs
      .filter(run => String(run.head_branch || gh.branch) === String(gh.branch))
      .filter(run => ["queued", "in_progress", "waiting", "requested"].includes(String(run.status || "")))
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0];

    if (!active) return false;

    const createdAt = active.created_at || new Date().toISOString();
    const startedMs = new Date(createdAt).getTime();
    const safeStartedMs = Number.isFinite(startedMs) ? startedMs : Date.now();
    const elapsedText = formatRunDurationV26630K(Date.now() - safeStartedMs);
    const runNumber = active.run_number ? `#${active.run_number}` : "";

    saveActiveBackendRunV26630N(createdAt);

    setSyncStatus(`⏳ 後端策略執行中 ${runNumber}｜已跑 ${elapsedText}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
    setPositionStatus?.(`⏳ 後端策略執行中 ${runNumber}｜已跑 ${elapsedText}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
    startLiveClock();
    startPositionClock?.();

    pollWorkflowRun(createdAt, safeStartedMs);
    return true;
  } catch (e) {
    return false;
  }
}

async function resumeOrDetectBackendRunV26630O() {
  if (resumeBackendRunIfActiveV26630N()) return true;
  return await detectActiveWorkflowOnLoadV26630O();
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

function parseAsTaipeiDateV26643(v) {
  if (!v) return null;
  const raw = String(v).trim();
  if (!raw || raw === "--") return null;

  // 已帶 Z 或 +08:00 的時間，直接讓 Date 解析。
  if (/[zZ]$|[+-]\d{2}:\d{2}$/.test(raw)) {
    const d = new Date(raw);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  // GitHub / Python 常輸出 YYYY-MM-DD HH:mm:ss；若沒有時區，視為台灣時間顯示，不再硬加 8 小時。
  const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [, yy, mo, dd, hh, mi, ss = "00"] = m;
    return new Date(Number(yy), Number(mo) - 1, Number(dd), Number(hh), Number(mi), Number(ss));
  }

  const d = new Date(raw);
  return Number.isNaN(d.getTime()) ? null : d;
}

function formatTWDateTime(v) {
  const d = parseAsTaipeiDateV26643(v);
  if (!d) return v ? String(v) : "--";
  const pad = n => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
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


function getTopRankV26630(row) {
  const fields = [
    row.section_top_opportunity,
    row.top_opportunity,
    row.section_opportunity_rank,
    row.opportunity_rank,
    row.execution_flag,
    row.system_note,
    row.note,
    row.reason
  ];
  const text = fields.map(v => String(v ?? "")).join(" ");
  const m = text.match(/TOP\s*([1-9]\d*)/i);
  if (m) return Number(m[1]);
  if (String(row.execution_flag || "").toUpperCase() === "TOP") return 99;
  return 9999;
}

function rowScoreV26630(row) {
  const n = Number(row.score || row.opportunity_score || row.entry_score || row.rank_score || row.liquidity_score || 0);
  return Number.isFinite(n) ? n : 0;
}

function dedupeByStockV26630(rows) {
  const map = new Map();
  (rows || []).forEach(row => {
    const sid = String(row.stock_id || "").trim();
    if (!sid) return;
    if (!map.has(sid)) {
      map.set(sid, row);
      return;
    }
    const old = map.get(sid);
    const aTop = getTopRankV26630(row);
    const bTop = getTopRankV26630(old);
    if (aTop < bTop) map.set(sid, row);
    else if (aTop === bTop && rowScoreV26630(row) > rowScoreV26630(old)) map.set(sid, row);
  });
  return Array.from(map.values());
}

function sortRows(rows) {
  return rows.slice().sort((a, b) => {
    const aa = normalizeAction(a.final_action || a.action);
    const bb = normalizeAction(b.final_action || b.action);
    const pa = ACTION_PRIORITY[aa] || 99;
    const pb = ACTION_PRIORITY[bb] || 99;
    if (pa !== pb) return pa - pb;

    const ta = getTopRankV26630(a);
    const tb = getTopRankV26630(b);
    if (ta !== tb) return ta - tb;

    const sb = rowScoreV26630(b);
    const sa = rowScoreV26630(a);
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
  const byAction = (actions) => sorted.filter(r => actions.includes(normalizeAction(r.final_action || r.action)));
  return {
    main: dedupeByStockV26630(byAction(["SELL", "REDUCE", "BUY"])),
    test: dedupeByStockV26630(byAction(["TEST"])),
    watch: dedupeByStockV26630(byAction(["WATCH"])),
    block: dedupeByStockV26630(byAction(["BLOCK"]))
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
      repo: "V204-app",
      branch: "main",
      token: "",
      workflow: DEFAULT_WORKFLOW_ID
    };

    const obj = JSON.parse(raw);
    return {
      owner: obj.owner || "bichcs5566-alt",
      repo: normalizeGithubRepoV266571(obj.repo || "V204-app"),
      branch: obj.branch || "main",
      token: obj.token || "",
      workflow: obj.workflow || DEFAULT_WORKFLOW_ID
    };
  } catch (e) {
    return {
      owner: "bichcs5566-alt",
      repo: "V204-app",
      branch: "main",
      token: "",
      workflow: DEFAULT_WORKFLOW_ID
    };
  }
}

function saveGithubSettings() {
  const settings = {
    owner: qs("ghOwner")?.value.trim() || "",
    repo: normalizeGithubRepoV266571(qs("ghRepo")?.value.trim() || "V204-app"),
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
  // v266.30I：同步修復版。
  // 後端 position_overlay_engine 主要吃 manual_positions.csv，且格式需包含 stock_name。
  // 這裡統一輸出 stock_id,stock_name,avg_price,shares,lots,note,updated_at。
  const headers = ["stock_id", "stock_name", "avg_price", "shares", "lots", "note", "updated_at"];
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };

  const lines = [headers.join(",")];
  (rows || []).forEach(r => {
    const sid = typeof stockKeyV26630H === "function" ? stockKeyV26630H(r.stock_id) : sidV26630(r.stock_id);
    const item = {
      stock_id: sid,
      stock_name: r.stock_name || window.__stockNameMapV26630?.[sid] || "",
      avg_price: r.avg_price || "",
      shares: r.shares || "",
      lots: r.lots || "",
      note: r.note || "手動持倉",
      updated_at: r.updated_at || formatTWDateTime(new Date().toISOString())
    };
    lines.push(headers.map(h => esc(item[h])).join(","));
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
  const topBadge = typeof getTopBadge === "function" ? getTopBadge(row) : "";

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



// ===== v266.30 Clean Position UI Helpers / 持倉乾淨整合層 =====
window.__positionOverlayMapV26630 = window.__positionOverlayMapV26630 || {};
window.__stockNameMapV26630 = window.__stockNameMapV26630 || {};

function sidV26630(v) {
  const m = String(v || "").match(/(\d{4})/);
  return m ? m[1] : String(v || "").trim();
}

function cleanV26630(v, fallback = "--") {
  const s = String(v ?? "").trim();
  if (!s || s === "nan" || s === "NaN" || s === "undefined" || s === "null") return fallback;
  return s;
}

function nV26630(v) {
  const n = Number(String(v ?? "").replace(/,/g, "").replace("%", ""));
  return Number.isFinite(n) ? n : null;
}

function priceV26630(v) {
  const n = nV26630(v);
  if (n === null) return "--";
  return Number.isInteger(n) ? String(n) : n.toFixed(2).replace(/\.00$/, "");
}

function pctV26630(v) {
  const n = nV26630(v);
  if (n === null) return "--";
  return n.toFixed(2).replace(/\.00$/, "") + "%";
}

function moneyV26630(v) {
  const n = nV26630(v);
  if (n === null) return "--";
  return Math.round(n).toLocaleString("zh-TW");
}

function lotsV26630(v) {
  const n = nV26630(v);
  if (n === null) return "--";
  return String(n).replace(/\.00$/, "");
}

function sharesV26630(v) {
  const n = nV26630(v);
  if (n === null) return "--";
  return Math.round(n).toLocaleString("zh-TW");
}

function zhPositionActionV26630(v) {
  const s = String(v || "").toUpperCase();
  if (s.includes("SELL") || s.includes("STOP") || s.includes("出場") || s.includes("賣")) return { key: "SELL", cls: "sell", pill: "🔴 出場", text: "出場" };
  if (s.includes("REDUCE") || s.includes("減")) return { key: "REDUCE", cls: "reduce", pill: "🟠 減碼", text: "減碼" };
  if (s.includes("WATCH") || s.includes("觀察")) return { key: "WATCH", cls: "watch", pill: "🟡 觀察", text: "觀察" };
  return { key: "HOLD", cls: "hold", pill: "🟢 抱住", text: "抱住" };
}

function zhRiskV26630(v, actionLike = "") {
  const s = String(v || actionLike || "").toUpperCase();
  if (s.includes("STOP_LOSS") || s.includes("STOP") || s.includes("停損")) return "🔴 停損風控";
  if (s.includes("HIGH") || s.includes("高")) return "🔴 高風險";
  if (s.includes("MEDIUM") || s.includes("MID") || s.includes("中")) return "🟠 中風險";
  if (s.includes("LOW") || s.includes("低")) return "🟢 低風險";
  if (s.includes("HOLD_CHECK") || s.includes("HOLD") || s.includes("抱")) return "🟢 續抱觀察";
  if (s.includes("WATCH") || s.includes("觀察")) return "🟡 觀察確認";
  return "🟢 續抱觀察";
}


// ===== v266.30H 最終版：台股代號/欄位對接鎖定層 =====
function stockKeyV26630H(v) {
  const s = String(v ?? "").trim();
  const m = s.match(/\d{4}/);
  return m ? m[0] : s;
}

function validTextV26630H(v) {
  if (v === undefined || v === null) return false;
  const s = String(v).trim();
  if (!s) return false;
  return !["--", "nan", "NaN", "undefined", "null", "None"].includes(s);
}

function pickV26630H(row, keys, fallback = "") {
  if (!row || typeof row !== "object") return fallback;
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(row, k) && validTextV26630H(row[k])) return row[k];
  }
  return fallback;
}

function normalizeRowV26630H(row) {
  const out = {};
  Object.keys(row || {}).forEach(k => {
    const nk = String(k).replace(/^\uFEFF/, "").trim();
    out[nk] = row[k];
  });

  out.stock_id = pickV26630H(out, ["stock_id", "stockId", "symbol", "code", "個股", "股票代號"], out.stock_id || "");
  out.stock_id = stockKeyV26630H(out.stock_id);

  out.stock_name = pickV26630H(out, [
    "stock_name", "stockName", "name", "Name",
    "股票名稱", "證券名稱", "證券簡稱", "公司簡稱", "公司名稱",
    "security_name", "SecurityName", "short_name"
  ], out.stock_name || "");
  out.close = pickV26630H(out, ["close", "Close", "收盤價", "price", "ref_price", "參考價"], out.close || "");
  out.ma5 = pickV26630H(out, ["ma5", "MA5", "ma_5", "sma5", "五日線", "五日均線"], out.ma5 || "");
  out.ma20 = pickV26630H(out, ["ma20", "MA20", "ma_20", "sma20", "二十日線", "二十日均線"], out.ma20 || "");
  out.ma5_status = pickV26630H(out, ["ma5_status", "MA5_status", "五日線觀察", "MA5觀察"], out.ma5_status || "");
  out.ma20_status = pickV26630H(out, ["ma20_status", "MA20_status", "MA20觀察", "二十日線觀察"], out.ma20_status || "");

  return out;
}

function overlayLookupV26630H(stock) {
  const sid = stockKeyV26630H(stock);
  const map = window.__positionOverlayMapV26630 || {};
  if (map[sid]) return map[sid];

  for (const [k, v] of Object.entries(map)) {
    if (stockKeyV26630H(k) === sid) return v || {};
  }

  const rows = window.__positionOverlayRowsV26630 || [];
  return rows.find(r => stockKeyV26630H(r.stock_id) === sid) || {};
}


function maStatusV26630(label, close, ma, direct) {
  // v266.30E：MA 顯示修補。
  // 後端 position_overlay.csv 已有 ma5 / ma20，但 close 可能缺值。
  // 舊版在 close 缺值時直接回傳 "--"，導致 MA5 / MA20 明明有資料卻不顯示。
  const d = cleanV26630(direct, "");
  if (d && d !== "--") return d.startsWith(label) ? d : `${label}：${d}`;

  const m = nV26630(ma);
  if (m === null || m === undefined) return `${label}：--`;

  const c = nV26630(close);

  // 核心修補：允許沒有 close。至少把 MA 數值顯示出來。
  if (c === null || c === undefined) return `${label}：${priceV26630(m)}`;

  const diff = (c - m) / m;
  if (diff > 0.02) return `${label}：站上｜↑ 強勢`;
  if (diff < -0.02) return `${label}：跌破｜↓ 轉弱`;
  return `${label}：貼近｜→ 盤整`;
}

function chipTextV26630(row) {
  const score = cleanV26630(row.chip_score || row.chip_concentration_score, "--");
  const label = cleanV26630(row.chip_label || row.chip_display || row.chip_confidence, "--");
  if (score === "--" && label === "--") return "--｜籌碼資料有限";
  return `${score}｜${label}`;
}

function positionNameV26630(stock, posRow = {}, overlay = {}, riskRow = {}) {
  const sid = stockKeyV26630H(stock || posRow.stock_id || overlay.stock_id || riskRow.stock_id);
  const posH = normalizeRowV26630H(posRow);
  const overlayH = normalizeRowV26630H(overlay);
  const riskH = normalizeRowV26630H(riskRow);

  const name =
    posH.stock_name ||
    overlayH.stock_name ||
    riskH.stock_name ||
    window.__stockNameMapV26630?.[sid] ||
    "";

  const fallbackNamesV266311 = {
    "2317": "鴻海",
    "2330": "台積電",
    "2409": "友達",
    "3051": "力特",
    "3583": "辛耘"
  };

  return cleanV26630(name || fallbackNamesV266311[sid], "--");
}

function positionDetailCellV26630(label, value) {
  return `<div><span>${label}</span><b>${cleanV26630(value)}</b></div>`;
}

async function loadPositionOverlayV26630() {
  window.__positionOverlayMapV26630 = {};
  window.__positionOverlayRowsV26630 = [];
  window.__stockNameMapV26630 = {};

  // v266.31.1：優先讀後端產出的股票名稱對照表。
  // 解決 manual_positions.csv 的 stock_name 空白時，持倉卡片名稱顯示 "--"。
  try {
    const res = await fetch("./data/stock_name_map.json?v=" + Date.now(), { cache: "no-store" });
    if (res.ok) {
      const txt = await res.text();
      if (txt && !txt.trim().startsWith("<")) {
        const m = JSON.parse(txt);
        Object.entries(m || {}).forEach(([k, v]) => {
          const sid = stockKeyV26630H(k);
          const name = cleanV26630(v, "");
          if (sid && name && name !== "--") window.__stockNameMapV26630[sid] = name;
        });
      }
    }
  } catch (e) {}

  const files = [
    "./data/position_overlay.csv",
    "./data/positions_manual.csv",
    "./data/manual_positions.csv",
    "./data/final_action_plan.csv",
    "./data/trade_plan.csv",
    "./data/market_snapshot.csv",
    "./data/full_summary.csv",
    "./data/selection_debug.csv",
    "./data/watchlist_monitor.csv",
    "./data/chip_source_twse.csv",
    "./data/stock_name_map.csv"
  ];

  for (const url of files) {
    try {
      const bust = url + (url.includes("?") ? "&" : "?") + "v=" + Date.now();
      const txt = await fetchText(bust);
      const rows = parseCsv(txt).map(normalizeRowV26630H);

      rows.forEach(r => {
        const sid = stockKeyV26630H(r.stock_id);
        if (!sid) return;

        const name = cleanV26630(
          r.stock_name || r.stockName || r.name || r.Name ||
          r["股票名稱"] || r["證券名稱"] || r["證券簡稱"] || r["公司簡稱"] || r["公司名稱"],
          ""
        );
        if (name && name !== "--") window.__stockNameMapV26630[sid] = name;

        // 只有 position_overlay 才進 overlay map；其他檔只補股票名稱。
        if (url.includes("position_overlay")) {
          window.__positionOverlayMapV26630[sid] = r;
          window.__positionOverlayRowsV26630.push(r);
        }
      });
    } catch (e) {}
  }
}

function getPositionOverlayRowV26630(stock) {
  return overlayLookupV26630H(stock);
}

function getPositionRiskRowV26630(stock) {
  const sid = stockKeyV26630H(stock);
  const map = typeof getPositionRiskMap === "function" ? getPositionRiskMap() : {};
  if (map[sid]) return map[sid];
  for (const [k, v] of Object.entries(map || {})) {
    if (stockKeyV26630H(k) === sid) return v || {};
  }
  return {};
}

function renderMergedPositionHintV26630(stock, posRow) {
  const sid = sidV26630(stock);
  const overlay = { ...(window.__techMapV26637?.[sid] || {}), ...getPositionOverlayRowV26630(sid) };
  const riskRow = { ...(window.__techMapV26637?.[sid] || {}), ...getPositionRiskRowV26630(sid) };
  const actionRaw = overlay.position_action || riskRow.final_action || riskRow.action || "HOLD";
  const actionInfo = zhPositionActionV26630(actionRaw);

  const avg = priceV26630(posRow.avg_price);
  const lots = lotsV26630(posRow.lots);
  const shares = sharesV26630(posRow.shares);
  // v266.30E：close fallback 修補。
  // position_overlay.csv 若沒有 close，至少用手動持倉均價避免 MA 判斷短路。
  const close = priceV26630(overlay.close || riskRow.close || riskRow.ref_price || posRow.close || posRow.avg_price);
  const cost = moneyV26630(nV26630(posRow.avg_price) && nV26630(posRow.shares) ? nV26630(posRow.avg_price) * nV26630(posRow.shares) : positionCost(posRow));
  const pnlRaw = overlay.pnl_pct || riskRow.pnl_pct;
  const pnl = cleanV26630(pnlRaw, (nV26630(close) && nV26630(posRow.avg_price)) ? pctV26630((nV26630(close) - nV26630(posRow.avg_price)) / nV26630(posRow.avg_price) * 100) : "--");
  // v266.30H：MA 最終對接。先 normalize overlay/risk/pos，再抓標準欄位。
  const overlayH = normalizeRowV26630H(overlay);
  const riskH = normalizeRowV26630H(riskRow);
  const posH = normalizeRowV26630H(posRow);

  const ma5RawH = pickV26630H(overlayH, ["ma5"], "") || pickV26630H(riskH, ["ma5"], "") || pickV26630H(posH, ["ma5"], "");
  const ma10RawH = pickV26630H(overlayH, ["ma10"], "") || pickV26630H(riskH, ["ma10"], "") || pickV26630H(posH, ["ma10"], "");
  const ma20RawH = pickV26630H(overlayH, ["ma20"], "") || pickV26630H(riskH, ["ma20"], "") || pickV26630H(posH, ["ma20"], "");
  const ma5StatusH = pickV26630H(overlayH, ["ma5_status", "ma5_label"], "") || pickV26630H(riskH, ["ma5_status", "ma5_label"], "");
  const ma10StatusH = pickV26630H(overlayH, ["ma10_status", "ma10_label"], "") || pickV26630H(riskH, ["ma10_status", "ma10_label"], "");
  const ma20StatusH = pickV26630H(overlayH, ["ma20_status", "ma20_label"], "") || pickV26630H(riskH, ["ma20_status", "ma20_label"], "");

  const ma5 = maStatusV26630("MA5", close, ma5RawH, ma5StatusH);
  const ma10 = maStatusV26630("MA10", close, ma10RawH, ma10StatusH);
  const ma20 = maStatusV26630("MA20", close, ma20RawH, ma20StatusH);
  const positionKbarTypeV26635 = pickFieldV26635({...riskRow, ...overlay}, ["kbar_type", "k_bar_type", "exit_kbar_type"], "依策略判斷");
  const positionKStructureV26635 = pickFieldV26635({...riskRow, ...overlay}, ["k_structure", "kline_structure"], "依策略判斷");
  const riskZh = zhRiskV26630(overlay.risk_flag || riskRow.risk_flag || riskRow.risk_level || riskRow.exit_risk_level, actionRaw);
  const chip = chipTextV26630({ ...riskRow, ...overlay });
  const name = positionNameV26630(sid, posRow, overlay, riskRow);

  const reason = cleanV26630(
    overlay.position_reason || riskRow.position_reason || riskRow.reason,
    actionInfo.key === "SELL" ? "觸發停損或趨勢防守，優先保護本金。" : "尚未出現明顯下跌或系統賣出訊號，趨勢未完全破壞。"
  );
  const kbar = cleanV26630(overlay.kbar_hint || riskRow.kbar_reason || riskRow.exit_kbar_reason, `${ma5}；${ma20}。`);
  const takeProfit = cleanV26630(overlay.take_profit_hint || riskRow.take_profit_hint, actionInfo.key === "SELL" ? "目前不是停利情境，而是停損／風控優先。" : "尚未達明確停利條件，先依趨勢與籌碼續抱觀察。");
  const chipHint = cleanV26630(overlay.chip_hint || riskRow.chip_hint, "籌碼資料有限，需搭配技術面確認。");
  const chipReason = cleanV26630(overlay.chip_reason || riskRow.chip_reason, "籌碼資料有限");
  const advice = cleanV26630(overlay.position_hint || riskRow.position_hint || riskRow.system_note, actionInfo.key === "SELL" ? "優先處理出場，不建議拖延或凹單。" : "在還沒有明顯下跌、未觸發風控前，以續抱觀察為主。");
  const systemHint = actionInfo.key === "SELL"
    ? "持倉已有風險或停損訊號，先控制部位，不要情緒化加碼。"
    : "尚未跌破關鍵防守時續抱；若跌破五日線、MA20 或籌碼轉弱，再分批停利或出場。";

  return `
    <div class="position-merged-v26630 ${actionInfo.cls}">
      <div class="position-merged-head-v26630">
        <span class="position-merged-pill-v26630 ${actionInfo.cls}">${actionInfo.pill}</span>
        <b>持倉提示</b>
        <strong>${close}</strong>
      </div>
      <div class="detail-grid position-merged-grid-v26630">
        ${positionDetailCellV26630("股票代號", sid)}
        ${positionDetailCellV26630("股票名稱", name)}
        ${positionDetailCellV26630("持倉狀態", actionInfo.text)}
        ${positionDetailCellV26630("來源", actionInfo.key === "SELL" ? "策略出場" : "手動持倉")}
        ${positionDetailCellV26630("策略層", actionInfo.key === "SELL" ? "持倉風控" : "持倉管理")}
        ${positionDetailCellV26630("參考價", close)}
        ${positionDetailCellV26630("均價", avg)}
        ${positionDetailCellV26630("張數", lots)}
        ${positionDetailCellV26630("股數", shares)}
        ${positionDetailCellV26630("部位金額", cost)}
        ${positionDetailCellV26630("損益%", pnl)}
        ${positionDetailCellV26630("MA5觀察", ma5)}
        ${positionDetailCellV26630("MA10觀察", ma10)}
        ${positionDetailCellV26630("MA20觀察", ma20)}
        ${positionDetailCellV26630("K棒型態", positionKbarTypeV26635)}
        ${positionDetailCellV26630("K線結構", positionKStructureV26635)}
        ${positionDetailCellV26630("籌碼集中度", chip)}
        ${positionDetailCellV26630("風控提示", riskZh)}
        ${positionDetailCellV26630("更新時間", cleanV26630(posRow.updated_at))}
        ${positionDetailCellV26630("備註", cleanV26630(posRow.note, "手動持倉"))}
      </div>
      <div class="detail-text position-merged-text-v26630"><b>原因</b><p>${reason}</p></div>
      <div class="detail-text position-merged-text-v26630"><b>K線／原因提示</b><p>${kbar}｜${positionKbarTypeV26635}｜${positionKStructureV26635}</p></div>
      <div class="detail-text position-merged-text-v26630"><b>停利提示</b><p>${takeProfit}</p></div>
      <div class="detail-text position-merged-text-v26630"><b>籌碼提示</b><p>${chipReason}｜${chipHint}</p></div>
      <div class="detail-text position-merged-text-v26630"><b>建議動作</b><p>${advice}</p></div>
      <div class="detail-text position-merged-text-v26630"><b>系統提示</b><p>${systemHint}</p></div>
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
    const stock = sidV26630(row.stock_id);
    const avg = priceV26630(row.avg_price);
    const lots = lotsV26630(row.lots);
    const cost = moneyV26630(nV26630(row.avg_price) && nV26630(row.shares) ? nV26630(row.avg_price) * nV26630(row.shares) : positionCost(row));

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
          ${renderMergedPositionHintV26630(stock, row)}
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
  const sid = typeof stockKeyV26630H === "function" ? stockKeyV26630H(stock) : sidV26630(stock);
  const idx = rows.findIndex(r => (typeof stockKeyV26630H === "function" ? stockKeyV26630H(r.stock_id) : sidV26630(r.stock_id)) === sid);
  const item = {
    stock_id: sid,
    stock_name: window.__stockNameMapV26630?.[sid] || "",
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
  refreshPositionStatus("本機持倉已重新評估");
  clearPositionForm();
  setSyncStatus(`✅ 持倉已儲存於本機｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
  startLiveClock();
  refreshPositionStatus("本機持倉已重新評估");
}

function bindPositionRowActions() {
  document.querySelectorAll("[data-edit-position]").forEach(btn => {
    if (btn.dataset.bound === "1") return;
    btn.dataset.bound = "1";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const stock = btn.getAttribute("data-edit-position");
      const row = loadPositions().find(r => (typeof stockKeyV26630H === "function" ? stockKeyV26630H(r.stock_id) : sidV26630(r.stock_id)) === (typeof stockKeyV26630H === "function" ? stockKeyV26630H(stock) : sidV26630(stock)));
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
      const rows = loadPositions().filter(r => (typeof stockKeyV26630H === "function" ? stockKeyV26630H(r.stock_id) : sidV26630(r.stock_id)) !== (typeof stockKeyV26630H === "function" ? stockKeyV26630H(stock) : sidV26630(stock)));
      savePositions(rows);
      renderPositions();
  refreshPositionStatus("本機持倉已重新評估");
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

async function readRepoFileText(path) {
  const res = await githubApi(`/contents/${encodeURIComponent(path).replace(/%2F/g, "/")}?t=${Date.now()}`, {
    method: "GET",
    headers: { "Cache-Control": "no-cache" }
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`驗證讀取 ${path} 失敗 ${res.status}：${compactErrorText(text)}`);
  }

  const trimmed = String(text || "").trim();
  if (trimmed.startsWith("<")) {
    throw new Error(`驗證讀回遇到 HTML 回應，通常是 GitHub API 暫時回傳非 JSON；寫入不一定失敗。`);
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`驗證讀回 JSON 解析失敗：${e.message}`);
  }

  const content = String(data.content || "").replace(/\n/g, "");
  try {
    return decodeURIComponent(escape(atob(content)));
  } catch (e) {
    return atob(content);
  }
}

function assertPositionsSyncedV26630I(csvText, rows) {
  const syncedRows = parseCsv(csvText);
  const syncedIds = new Set((syncedRows || []).map(r => {
    const raw = r.stock_id || r.stockId || r.symbol || r.code || r["股票代號"];
    return typeof stockKeyV26630H === "function" ? stockKeyV26630H(raw) : sidV26630(raw);
  }).filter(Boolean));

  const localIds = (rows || []).map(r => {
    const raw = r.stock_id || r.stockId || r.symbol || r.code || r["股票代號"];
    return typeof stockKeyV26630H === "function" ? stockKeyV26630H(raw) : sidV26630(raw);
  }).filter(Boolean);

  const missing = localIds.filter(id => !syncedIds.has(id));
  if (missing.length) {
    throw new Error(`同步驗證失敗，GitHub 檔案缺少：${missing.join(", ")}`);
  }
  return true;
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

  // v266.30I：同步修復版。
  // 同時寫入後端真正吃的 manual_positions.csv，以及舊版相容的 positions_manual.csv。
  const targets = [
    "manual_positions.csv",
    "mobile_dashboard_v1/data/manual_positions.csv",
    "positions_manual.csv",
    "mobile_dashboard_v1/data/positions_manual.csv"
  ];

  for (const path of targets) {
    await putRepoFile(path, csv, `update manual positions sync ${formatTWDateTime(new Date().toISOString())}`);
  }

  // 寫完後立刻讀回 dashboard 的 manual_positions.csv 驗證。
  // v266.30J：若 GitHub 寫入已成功，但讀回驗證遇到 HTML / 暫時非 JSON，不中斷重跑策略。
  let verified = false;
  let verifyWarning = "";
  try {
    const verifyText = await readRepoFileText("mobile_dashboard_v1/data/manual_positions.csv");
    assertPositionsSyncedV26630I(verifyText, rows);
    verified = true;
  } catch (e) {
    verifyWarning = e.message || String(e);
    console.warn("[v266.30J] sync verify skipped:", verifyWarning);
  }

  if (verified) {
    setSyncStatus(`✅ 持倉已同步到 GitHub｜已驗證 ${rows.length} 檔｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
    setPositionStatus(`✅ 持倉已同步到 GitHub｜已驗證 ${rows.length} 檔｜同步時間 ${formatTWDateTime(new Date().toISOString())}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status ok");
  } else {
    setSyncStatus(`✅ 持倉已寫入 GitHub｜驗證略過：${compactErrorText(verifyWarning)}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
    setPositionStatus(`✅ 持倉已寫入 GitHub｜驗證略過但會繼續重跑｜同步時間 ${formatTWDateTime(new Date().toISOString())}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status ok");
  }
  startLiveClock();
  startPositionClock();
  return true;
}

async function rerunStrategyWithPositions() {
  // v330：本機持倉測試模式。
  // 只重跑後端策略，不同步 / 不回寫 manual_positions.csv 或 current_positions.csv。
  // 前端持倉保留在 localStorage，跑完後重新載入最新清單，再和本機持倉一起重新評估顯示。
  try {
    const rows = loadPositions();

    if (!rows.length) {
      setPositionStatus(`⚠️ 尚未建立本機持倉；仍可重跑策略更新前方清單｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
      startPositionClock();
    } else {
      setPositionStatus(`🚀 本機持倉 ${rows.length} 檔保留，不回寫後端；正在重跑策略｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status ok");
      startPositionClock();
    }

    setSyncStatus(`🚀 正在觸發後端策略｜本機持倉不回寫｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
    startLiveClock();

    await triggerDataPipeline();
  } catch (e) {
    setSyncStatus(`❌ 重跑策略失敗：${e.message}`, "sync error");
    setPositionStatus(`❌ 重跑策略失敗：${e.message}`, "position-status error");
  }
}


function safeBindClickV3091(id, handler) {
  try {
    const el = qs(id);
    if (el && typeof el.addEventListener === "function") {
      el.addEventListener("click", handler);
    }
  } catch (e) {
    console.warn("safeBindClickV3091 failed:", id, e);
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
          <span class="hint">輸入後只做本機重新評估，不回寫後端</span>
        </div>

        <div class="position-form">
          <input id="posStock" class="position-input" placeholder="個股，例如 2330" inputmode="numeric" />
          <input id="posPrice" class="position-input" placeholder="均價，例如 580" inputmode="decimal" />
          <input id="posLots" class="position-input" placeholder="張數，例如 1.5" inputmode="decimal" />
          <input id="posNote" class="position-input" placeholder="備註，可不填" />
        </div>

        <div class="position-actions">
          <button id="addPositionBtn" type="button">新增 / 更新</button>
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

      <section class="card compact-card ignition-card">
        <details open>
          <summary>🧪 IGNITION 起漲訊號（防假突破）</summary>
          <div class="hint">只顯示市場起漲雷達，不自動丟入 TEST / WATCH；已加入 FakeScore 假起漲過濾，TOP5 會排在最前面。</div>
          <div id="ignitionList"></div>
        </details>
      </section>

      <section class="card compact-card evolution-card">
        <details open>
          <summary>🧬 EVOLUTION 策略進化訊號</summary>
          <div class="hint">只顯示升級提示，不自動丟入原本清單；用來判斷可否加碼或提高優先級。</div>
          <div id="evolutionList"></div>
        </details>
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
        <input id="ghRepo" class="github-input" value="${gh.repo}" placeholder="repo，例如 V204-app" autocomplete="off" />
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

  safeBindClickV3091("refreshBtn", () => {
    const safePath = (location.pathname && location.pathname !== "/") ? location.pathname : "/V204-app/";
    location.href = safePath + "?v=" + Date.now() + location.hash;
  });
  safeBindClickV3091("updateBtn", triggerDataPipeline);
  safeBindClickV3091("saveGhBtn", saveGithubSettings);
  safeBindClickV3091("clearGhBtn", clearGithubSettings);
  safeBindClickV3091("addPositionBtn", addOrUpdatePosition);
  safeBindClickV3091("rerunWithPositionBtn", rerunStrategyWithPositions);
  renderPositions();
  refreshPositionStatus("本機持倉已重新評估");
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
  const res = await githubApi(`/actions/workflows/${encodeURIComponent(gh.workflow)}/runs?branch=${encodeURIComponent(gh.branch)}&per_page=30`, {
    method: "GET"
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`讀取進度失敗 ${res.status}：${compactErrorText(text)}`);
  }

  const trimmed = String(text || "").trim();
  if (trimmed.startsWith("<")) {
    throw new Error("GitHub Actions 進度查詢回傳 HTML，請稍後再看 Actions。");
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`GitHub Actions 進度 JSON 解析失敗：${e.message}`);
  }

  const runs = Array.isArray(data.workflow_runs) ? data.workflow_runs : [];
  const after = new Date(createdAfterIso).getTime();

  const sorted = runs
    .filter(run => String(run.head_branch || gh.branch) === String(gh.branch))
    .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

  // v266.30M：優先抓「目前正在跑」的 data_pipeline。
  // 避免剛剛被 cancel 的 #88 蓋過真正正在跑的 #89。
  const active = sorted.find(run =>
    ["queued", "in_progress", "waiting", "requested"].includes(String(run.status || ""))
  );
  if (active) return active;

  // 沒有 active 才看本次按下後的 completed。
  const candidates = sorted.filter(run => {
    const t = new Date(run.created_at).getTime();
    return Number.isFinite(t) && t >= after - 10000;
  });

  if (!candidates.length) return null;

  return candidates[0] || null;
}

async function pollWorkflowRun(createdAfterIso, startedAtMs = null) {
  if (pollingTimer) clearTimeout(pollingTimer);

  const started = Number(startedAtMs || Date.now());
  const timeoutMs = 60 * 60 * 1000;

  const loop = async () => {
    const elapsedText = formatRunDurationV26630K(Date.now() - started);

    try {
      const run = await getLatestWorkflowRun(createdAfterIso);

      if (!run) {
        setSyncStatus(`⏳ 後端策略已送出｜等待 GitHub 建立任務｜已等 ${elapsedText}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
        setPositionStatus?.(`⏳ 後端策略已送出｜等待建立任務｜已等 ${elapsedText}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
        startLiveClock();
        startPositionClock?.();
      } else {
        const status = run.status || "";
        const conclusion = run.conclusion || "";
        const runNumber = run.run_number ? `#${run.run_number}` : "";
        const phase =
          status === "queued" ? "排隊中" :
          status === "in_progress" ? "執行中" :
          status === "completed" ? "已完成" :
          safeText(status, "更新中");

        if (status === "completed") {
          if (conclusion === "success") {
            const doneClock = formatTWClock(new Date());
            clearActiveBackendRunV26630N();
            rememberBackendRunV26630K({
              status: "success",
              duration_text: elapsedText,
              done_at: doneClock,
              run_number: runNumber
            });
            setSyncStatus(`✅ 後端策略完成 ${runNumber}｜耗時 ${elapsedText}｜完成時間 ${doneClock}｜重新整理中...`, "sync ok");
            setPositionStatus?.(`✅ 後端策略完成 ${runNumber}｜耗時 ${elapsedText}｜完成時間 ${doneClock}｜重新整理中...`, "position-status ok");
            setTimeout(() => {
              const safePath = (location.pathname && location.pathname !== "/") ? location.pathname : "/V204-app/";
              location.href = safePath + '?v=' + Date.now() + location.hash;
            }, 1800);
            return;
          }

          if (String(conclusion).toLowerCase() === "cancelled" && Date.now() - started < 90 * 1000) {
            setSyncStatus(`⏳ 偵測到舊任務取消 ${runNumber}｜等待新的後端任務接手｜已跑 ${elapsedText}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
            setPositionStatus?.(`⏳ 偵測到舊任務取消 ${runNumber}｜等待新任務｜已跑 ${elapsedText}`, "position-status");
            startLiveClock();
            startPositionClock?.();
            pollingTimer = setTimeout(loop, 5000);
            return;
          }

          clearActiveBackendRunV26630N();
          setSyncStatus(`❌ 後端策略失敗 ${runNumber}：${safeText(conclusion)}｜耗時 ${elapsedText}｜請到 Actions 查看`, "sync error");
          setPositionStatus?.(`❌ 後端策略失敗 ${runNumber}：${safeText(conclusion)}｜耗時 ${elapsedText}`, "position-status error");
          return;
        }

        setSyncStatus(`⏳ 後端策略${phase} ${runNumber}｜已跑 ${elapsedText}｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
        setPositionStatus?.(`⏳ 後端策略${phase} ${runNumber}｜已跑 ${elapsedText}｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
        startLiveClock();
        startPositionClock?.();
      }

      if (Date.now() - started > timeoutMs) {
        clearActiveBackendRunV26630N();
        setSyncStatus("⚠️ 後端策略等待超過 60 分鐘，請到 GitHub Actions 查看狀態。", "sync error");
        setPositionStatus?.("⚠️ 後端策略等待超過 60 分鐘，請到 GitHub Actions 查看狀態。", "position-status error");
        return;
      }

      pollingTimer = setTimeout(loop, 5000);
    } catch (e) {
      setSyncStatus(`⚠️ 後端進度查詢暫時失敗｜已跑 ${elapsedText}｜${e.message}`, "sync");
      setPositionStatus?.(`⚠️ 後端進度查詢暫時失敗｜已跑 ${elapsedText}`, "position-status");
      pollingTimer = setTimeout(loop, 8000);
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

    setSyncStatus(`🚀 正在觸發後端策略｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync");
    setPositionStatus?.(`🚀 正在觸發後端策略｜現在時間 <span id="positionLiveClock">${formatTWClock(new Date())}</span>`, "position-status");
    startLiveClock();
    startPositionClock?.();

    const createdAfterIso = new Date().toISOString();
    saveActiveBackendRunV26630N(createdAfterIso);

    const res = await githubApi(`/actions/workflows/${encodeURIComponent(gh.workflow)}/dispatches`, {
      method: "POST",
      body: JSON.stringify({ ref: gh.branch })
    });

    const text = await res.text();

    if (res.status !== 204) {
      setSyncStatus(`❌ 觸發失敗 ${res.status}：${compactErrorText(text)}`, "sync error");
      return;
    }

    // v266.31：先用本機立即顯示，等後端 workflow_status.json 產出後自動接管。
    markWorkflowTriggeredLocalV26631();
    pollWorkflowRun(createdAfterIso, Date.now());
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


function resolveTradeDateV26630(regime, summary) {
  // v266.30D：交易日只吃後端交易日欄位；不再用「最後更新」或前端時間推算。
  // 優先順序：summary.trade_date / next_trade_date -> regime.trade_date / next_trade_date -> 訊號日備援。
  const raw =
    summary.trade_date ||
    summary.next_trade_date ||
    regime.trade_date ||
    regime.next_trade_date ||
    summary.signal_date ||
    regime.date ||
    regime.latest_date ||
    "--";
  const m = String(raw || "").match(/\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : safeText(raw, "--");
}


// ===== v266.57.3 最後更新時間同步修補 =====
// 只修 UI 顯示：最後更新優先吃後端 workflow 完成時間，避免顯示到舊 generated_at。
function resolveBackendUpdatedAtV266573(regime, summary) {
  const candidates = [
    summary.workflow_completed_at,
    summary.backend_completed_at,
    summary.completed_at,
    summary.updated_at,
    summary.generated_at,
    regime.workflow_completed_at,
    regime.backend_completed_at,
    regime.completed_at,
    regime.updated_at,
    regime.generated_at,
    workflowStatusCacheV26631?.end_time,
    workflowStatusCacheV26631?.completed_at,
    workflowStatusCacheV26631?.updated_at
  ];

  let best = null;
  for (const v of candidates) {
    const d = parseAsTaipeiDateV26643(v);
    if (!d) continue;
    if (!best || d.getTime() > best.getTime()) best = d;
  }

  return best ? formatTWDateTime(best.toISOString()) : "--";
}



function renderMeta(regime, summary, macro, rows) {
  regime = regime || {};
  summary = summary || {};
  macro = macro || {};
  rows = Array.isArray(rows) ? rows : [];

  const backendUpdatedAt = resolveBackendUpdatedAtV266573(regime, summary);

  const pickMetaV3332 = (keys, fallback = "") => {
    for (const k of keys) {
      const candidates = [summary[k], regime[k], macro[k]];
      for (const v of candidates) {
        if (v !== undefined && v !== null && String(v).trim() !== "") return v;
      }
    }
    return fallback;
  };

  const cleanMetaV3332 = (v, fallback = "--") => {
    if (v === undefined || v === null) return fallback;
    const s = String(v).trim();
    if (!s || ["nan", "NaN", "undefined", "null", "None", "--"].includes(s)) return fallback;
    return s;
  };

  const normalizeMacroScoreV3332 = (v) => {
    const s = cleanMetaV3332(v, "");
    if (!s) return "--";
    if (s.includes("/")) return s;
    const n = Number(String(s).replace(/[^0-9.-]/g, ""));
    if (Number.isFinite(n)) return `${n}/7`;
    return s;
  };

  // 1) 市場狀態：只講盤勢，不混總經與操作限制
  const marketLabelV3332 = cleanMetaV3332(
    pickMetaV3332([
      "market_state_v333",
      "market_status_v333",
      "market_status",
      "market_label",
      "label",
      "regime"
    ], ""),
    "--"
  );

  const riskScoreV3332 = cleanMetaV3332(
    pickMetaV3332(["market_risk_score_v333"], ""),
    ""
  );

  const marketText = riskScoreV3332
    ? `${marketLabelV3332}｜風險分 ${riskScoreV3332}`
    : marketLabelV3332;

  // 2) 總經狀態：只講總經方向 + 分數，不放操作限制
  const macroBiasV3332 = cleanMetaV3332(
    pickMetaV3332([
      "macro_bias_v333",
      "macro_label_v333",
      "macro_label",
      "macro_regime_label"
    ], ""),
    "總經中性"
  );

  const macroScoreV3332 = normalizeMacroScoreV3332(
    pickMetaV3332([
      "macro_score_v333",
      "macro_score"
    ], "")
  );

  const macroConfidenceV3332 = cleanMetaV3332(
    pickMetaV3332([
      "confidence_v333",
      "macro_confidence_label",
      "macro_confidence"
    ], ""),
    ""
  );

  const macroText = macroConfidenceV3332
    ? `${macroBiasV3332}｜分數 ${macroScoreV3332}｜${macroConfidenceV3332}`
    : `${macroBiasV3332}｜分數 ${macroScoreV3332}`;

  // 3) 風險模式：只講實際操作限制
  const riskModeRawV3332 = cleanMetaV3332(
    pickMetaV3332([
      "risk_mode_v333",
      "risk_mode",
      "test_pressure_v333",
      "macro_policy",
      "action_policy"
    ], ""),
    "--"
  );

  const testPressureV3332 = cleanMetaV3332(
    pickMetaV3332(["test_pressure_v333"], ""),
    ""
  );

  const riskPartsV3332 = [];
  riskPartsV3332.push(riskModeRawV3332);
  if (testPressureV3332 && !riskModeRawV3332.includes(testPressureV3332)) riskPartsV3332.push(testPressureV3332);
  if (macroConfidenceV3332) riskPartsV3332.push(`信心${macroConfidenceV3332.replace(/^信心/, "")}`);

  const riskText = riskPartsV3332.filter(Boolean).join("｜") || "--";

  const signalDate = safeText(
    summary.signal_date ||
    summary.latest_date ||
    regime.signal_date ||
    regime.latest_date ||
    regime.date ||
    summary.generated_at,
    "--"
  );
  const tradeDate = resolveTradeDateV26630(regime, summary);

  qs("metaBox").innerHTML = `
    <div class="mini"><span>來源版本</span><b>C 完整交易系統</b></div>
    <div class="mini"><span>市場狀態</span><b>${marketText}</b></div>
    <div class="mini"><span>總經狀態</span><b>${macroText}</b></div>
    <div class="mini"><span>風險模式</span><b>${riskText}</b></div>
    <div class="mini"><span>訊號日</span><b>${signalDate}</b></div>
    <div class="mini"><span>交易日</span><b>${tradeDate}</b></div>
    <div class="mini"><span>最後更新</span><b>${backendUpdatedAt}</b></div>
    <div class="mini"><span>操作筆數</span><b>${rows.length}</b></div>
  `;

  setIdleSyncStatusV266574(`✅ 最終操作表已同步｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
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


// v266.57：無同步持倉時，最終操作不可顯示舊 SELL / 出場資料。
function hasSyncedPositionRowsV26656() {
  try {
    const candidates = [
      window.positionRows,
      window.positions,
      window.currentPositions,
      window.manualPositions,
      window.positionOverlayRows
    ];
    for (const rows of candidates) {
      if (Array.isArray(rows) && rows.some(r => {
        const q = Number(String(r?.shares || r?.qty || r?.股數 || r?.lots || 0).replace(/,/g, ""));
        const sid = String(r?.stock_id || r?.code || r?.股票代號 || "").trim();
        return sid && q > 0;
      })) return true;
    }
  } catch (e) {}
  try {
    const box = document.body?.innerText || "";
    if (/尚未建立持倉/.test(box)) return false;
  } catch (e) {}
  return false;
}

function isSellLikeFinalRowV26656(row) {
  const txt = [
    row?.final_action, row?.action, row?.status, row?.decision,
    row?.reason, row?.system_note, row?.source
  ].map(v => String(v || "")).join(" ").toUpperCase();
  return /SELL|REDUCE|賣|賣出|出場|停損|減碼/.test(txt);
}

function filterFinalRowsBySyncedPositionsV26656(rows) {
  rows = Array.isArray(rows) ? rows : [];
  const hasPos = hasSyncedPositionRowsV26656();
  if (!hasPos) {
    return rows.filter(r => !isSellLikeFinalRowV26656(r));
  }
  return rows;
}

function clearStaleFinalCacheV26656() {
  try {
    Object.keys(localStorage || {}).forEach(k => {
      // v332：保護本機持倉。重新整理 / 渲染最終操作時不可清掉持倉。
      if (k === POS_STORAGE_KEY || k === "daily_dashboard_positions_v1") return;
      if (/position/i.test(k)) return;
      if (/final|action|sell|workflow|dashboard|csv/i.test(k)) {
        localStorage.removeItem(k);
      }
    });
  } catch (e) {}
}


function renderFinalActions(rows) {
  clearStaleFinalCacheV26656();
  rows = filterFinalRowsBySyncedPositionsV26656(rows || []);

  const container = qs("finalActionList");
  rows = purgeStaleFinalRowsV26648(rows || []);

  if (!rows.length) {
    container.innerHTML = `<div class="empty">本輪沒有最終操作，且沒有同步持倉，不顯示舊標的。</div>`;
    return;
  }

  container.innerHTML = rows.map((row, idx) => renderScanRow(row, "main-" + idx)).join("");
  bindToggle();
}

function renderSectionList(targetId, rows, prefix, limit = 80) {
  const container = qs(targetId);

  if (!rows.length) {
    container.innerHTML = sectionEmptyHintV26634(prefix);
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


/* =========================================================
   v326 CORE NATIVE SAFE MARK / CORE 原生安全標記
   - 不掃 DOM、不 replace 已渲染 HTML
   - 只在 renderScanRow 產生卡片時加 class / 欄位
   ========================================================= */

function isCoreRowV326(row) {
  try {
    row = row || {};
    const joined = [
      row.strategy_type,
      row.strategy_layer,
      row.strategy_bucket,
      row.lifecycle_stage,
      row.bucket,
      row.source,
      row.entry_type,
      row.strategy_name,
      row.system_note,
      row.reason,
      row.core_marker,
      row.is_core_v319,
      row.is_core,
      row.core_score_v319
    ].map(v => String(v ?? "")).join(" ").toUpperCase();

    if (String(row.is_core_v319 ?? "").trim() === "1") return true;
    if (String(row.is_core ?? "").trim() === "1") return true;
    return /CORE|核心主升|🟣/.test(joined);
  } catch (e) {
    return false;
  }
}

function coreStrategyTextV326(row) {
  return isCoreRowV326(row) ? "🟣 CORE｜核心主升" : "";
}

function coreDetailCellV326(row) {
  try {
    if (!isCoreRowV326(row)) return "";
    return detailCell("CORE標記", "🟣 核心主升", "core-text-v326");
  } catch (e) {
    return "";
  }
}

function injectCoreStyleV326() {
  try {
    if (document.getElementById("core-style-v326")) return;
    const style = document.createElement("style");
    style.id = "core-style-v326";
    style.textContent = `
      .scan-item.core-card-v326 {
        border-color: #a855f7 !important;
        box-shadow: 0 0 0 2px rgba(168,85,247,.12) !important;
      }
      .scan-main.core-main-v326,
      .scan-main-live.core-main-v326 {
        background: linear-gradient(90deg, rgba(250,245,255,.96), rgba(255,255,255,.98)) !important;
        border-color: rgba(168,85,247,.30) !important;
      }
      .core-text-v326 {
        color: #6d28d9 !important;
        font-weight: 950 !important;
      }
      .scan-detail .detail-grid div:has(.core-text-v326) {
        background: rgba(250,245,255,.92) !important;
        border-color: rgba(168,85,247,.30) !important;
      }
    `;
    document.head.appendChild(style);
  } catch (e) {}
}

try { injectCoreStyleV326(); } catch(e) {}
document.addEventListener("DOMContentLoaded", function() {
  try { injectCoreStyleV326(); } catch(e) {}
});


function strategyDisplay(row) {
  try {
    const coreText = typeof coreStrategyTextV326 === "function" ? coreStrategyTextV326(row) : "";
    if (coreText) return coreText;
  } catch (e) {}

  const st = String(row.strategy_type || row.strategy || row.bucket || "").toUpperCase();
  if (st === "ALPHA") return "ALPHA 主力";
  if (st === "CORE") return "🟣 CORE｜核心主升";
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



// ===== v266.16.1 SELL / REDUCE 出場詳情強化 =====
function inferExitTypeV26616(row) {
  const direct = row.exit_type || row.exit_pattern || row.exit_mode || row["出場型態"];
  if (direct) return safeText(direct);

  const text = `${row.reason || ""} ${row.system_note || ""} ${row.note || ""}`;
  if (/停損|stop/i.test(text)) return "停損出場";
  if (/跌破\s*MA20|MA20|月線|均線/i.test(text)) return "跌破均線出場";
  if (/停利|獲利|take profit/i.test(text)) return "停利出場";
  if (/減碼|風險|部位/i.test(text)) return "風險減碼";
  if (/動能轉弱|5日動能轉弱|轉弱|弱勢/i.test(text)) return "動能轉弱出場";
  return "持倉風控出場";
}

function inferExitReasonV26616(row) {
  const direct = row.exit_reason || row["出場原因"];
  if (direct) return safeText(direct);
  return safeText(row.reason || row.note || row.system_note, "依持倉風控條件出場。");
}

function inferExitKbarTypeV26616(row) {
  const direct =
    row.exit_kbar_type ||
    row.exit_candle_type ||
    row.kbar_type ||
    row.candle_type ||
    row.candle_pattern ||
    row.price_action ||
    row["出場K棒型態"];

  if (direct) return safeText(direct);

  const text = `${row.reason || ""} ${row.system_note || ""} ${row.note || ""}`;

  if (/長黑|黑K/i.test(text)) return "長黑K轉弱";
  if (/跌破|破線|破位|MA20|均線/i.test(text)) return "跌破型K棒";
  if (/吞噬/i.test(text)) return "空方吞噬";
  if (/上影|爆量上影|長上影/i.test(text)) return "上影線壓力";
  if (/量縮|無量/i.test(text)) return "量縮轉弱";
  if (/停損/i.test(text)) return "停損觸發K棒";
  return "尚未標註K棒";
}

function inferExitKbarReasonV26616(row) {
  const direct =
    row.exit_kbar_reason ||
    row.exit_candle_reason ||
    row.kbar_reason ||
    row.candle_reason ||
    row["K棒判斷原因"];

  if (direct) return safeText(direct);

  const text = `${row.reason || ""} ${row.system_note || ""} ${row.note || ""}`;

  if (/停損/i.test(text)) return "價格已觸發停損條件，先保護本金。";
  if (/跌破\s*MA20|MA20|均線|月線/i.test(text)) return "價格跌破關鍵均線，趨勢防守失效。";
  if (/動能轉弱|5日動能轉弱|轉弱/i.test(text)) return "短線動能轉弱，續抱勝率下降。";
  if (/損益|虧損|負/i.test(text)) return "持倉損益惡化，需要優先控制風險。";
  if (/上影|壓力/i.test(text)) return "上方賣壓增加，短線容易轉弱。";
  return "後端尚未提供K棒細節，先以出場原因判斷。";
}

function inferExitRiskLevelV26616(row) {
  const direct = row.risk_level || row.exit_risk_level || row["風險等級"];
  if (direct) return riskZhV26637(direct);

  const text = `${row.reason || ""} ${row.system_note || ""} ${row.note || ""}`.toUpperCase();
  if (/HIGH|高風險|停損|跌破/.test(text)) return "高風險";
  if (/MEDIUM|中風險|轉弱|減碼/.test(text)) return "中風險";
  if (/LOW|低風險/.test(text)) return "低風險";
  return "依策略判斷";
}

function inferExitAdviceV26616(row, action) {
  const direct = row.exit_advice || row.action_advice || row.decision_note || row.zh_hint || row.chinese_hint;
  if (direct) return safeText(direct);

  if (action === "SELL") return "優先處理賣出，不建議拖延。";
  if (action === "REDUCE") return "先減碼控制風險，保留觀察彈性。";
  return "依系統提示處理。";
}

function isExitActionV26616(action) {
  return ["SELL", "REDUCE"].includes(String(action || "").toUpperCase());
}


function detailCell(label, value, extraClass = "") {
  let v = safeText(value, "--");
  if (v === "" || v === "undefined" || v === "null") v = "--";
  return `<div><span>${label}</span><b class="${extraClass}">${v}</b></div>`;
}


// ===== v266.20 籌碼集中度 UI 輔助 =====
function chipDisplayV26621(row) {
  const display = row.chip_display || row["籌碼集中度"];
  if (display && String(display).trim() !== "--") return safeText(display);

  const scoreRaw = row.chip_score || row.chip_concentration_score || row["籌碼分數"];
  const score = Number(scoreRaw);
  if (!Number.isFinite(score)) return "--";

  let label = "🟡 普通";
  if (score >= 80) label = "🔥 高度集中";
  else if (score >= 60) label = "🟢 偏集中";
  else if (score >= 40) label = "🟡 普通";
  else if (score >= 20) label = "⚠️ 分散";
  else label = "❌ 極度分散";

  return `${Math.round(score)}（${label}）`;
}

function chipReasonV26621(row) {
  return safeText(
    row.chip_reason ||
    row.chip_concentration_reason ||
    row["籌碼原因"],
    "籌碼依策略判斷"
  );
}

function chipHintV26621(row) {
  return safeText(
    row.chip_hint ||
    row.chip_concentration_hint ||
    row["籌碼提示"],
    "籌碼依策略判斷，先以中性處理。"
  );
}



function sectionEmptyHintV26634(prefix) {
  if (prefix === "ignition") {
    return `
      <div class="empty signal-empty">
        <b>今日未偵測到乾淨起漲訊號</b>
        <p>市場狀態：目前沒有同時符合「收斂、放量、站穩均線、假突破痕跡低」的標的。</p>
        <p>操作建議：暫停開新倉，不要為了交易而交易；等待明確突破與延續K棒。</p>
      </div>`;
  }
  if (prefix === "evolution") {
    return `
      <div class="empty signal-empty">
        <b>目前無策略進化標的</b>
        <p>市場結構：暫時沒有從起漲、試單、強勢到核心的明確升級訊號。</p>
        <p>操作建議：不加碼，先維持原持倉風控。</p>
      </div>`;
  }
  return `<div class="empty">沒有資料</div>`;
}

function fakeScoreLabelV26634(row) {
  const fs = Number(row.fake_score);
  if (!Number.isFinite(fs)) return "--";
  if (fs <= 1) return `${fs}｜✅ 乾淨`;
  if (fs <= 2) return `${fs}｜⚠️ 需確認`;
  if (fs <= 3) return `${fs}｜⚠️ 偏疑慮`;
  return `${fs}｜❌ 疑似假起漲`;
}

function ignitionChinesePromptV26634(row) {
  const hint = safeText(row.ignition_hint_zh || row.reason, "起漲訊號：後端尚未提供完整中文提示。");
  const fake = safeText(row.fake_reason_zh || row.fake_flags, "假起漲檢查：依策略判斷。");
  const advice = safeText(row.operation_advice_zh || row.system_note, "操作建議：僅小量觀察，不建議重倉。");
  return `
    <div class="detail-text"><b>中文起漲提示</b><p>${hint}</p></div>
    <div class="detail-text"><b>假起漲 / 主力假K檢查</b><p>${fake}</p></div>
    <div class="detail-text"><b>操作建議</b><p>${advice}</p></div>
  `;
}

function evolutionChinesePromptV26634(row) {
  const phase = safeText(row.evolution_phase || row.entry_type, "策略進化");
  const note = safeText(row.system_note || row.reason || row.note, "策略進化提示：等待後續確認。");
  return `
    <div class="detail-text"><b>中文進化提示</b><p>${phase}｜${note}</p></div>
    <div class="detail-text"><b>操作建議</b><p>這是升級提示，不代表自動進場；若已持有，可評估是否續抱或分批加碼。</p></div>
  `;
}



function sidKeyV26637(v) {
  const s = String(v ?? "").trim();
  const m = s.match(/\d{4}/);
  return m ? m[0] : s;
}

window.__techMapV26637 = window.__techMapV26637 || {};

function usefulV26637(v) {
  if (v === undefined || v === null) return false;
  const s = String(v).trim();
  return !!s && !["--", "-", "nan", "NaN", "undefined", "null", "None", "依策略判斷", "資料有限", "N/A"].includes(s);
}

function cleanTechV26637(v, fallback = "依策略判斷") {
  if (!usefulV26637(v)) return fallback;
  let s = safeText(v, fallback);
  s = s.replace(/MA5\s*[:：]\s*MA5\s*[:：]/g, "MA5：");
  s = s.replace(/MA10\s*[:：]\s*MA10\s*[:：]/g, "MA10：");
  s = s.replace(/MA20\s*[:：]\s*MA20\s*[:：]/g, "MA20：");
  s = s.replace(/\s+\|/g, "｜").replace(/\|\s+/g, "｜").replace(/\|/g, "｜");
  s = s.replace(/｜\s*依策略判斷/g, "");
  return s.trim() || fallback;
}

function riskZhV26637(v) {
  const s = String(v || "").trim().toUpperCase();
  if (!s || s === "--") return "依策略判斷";
  if (s.includes("HIGH") || s.includes("高")) return "高風險";
  if (s.includes("MEDIUM") || s.includes("MID") || s.includes("中")) return "中風險";
  if (s.includes("LOW") || s.includes("低")) return "低風險";
  return safeText(v, "依策略判斷");
}

function mergeTechRowV26637(row) {
  const sid = sidKeyV26637(row?.stock_id || row?.symbol || row?.code);
  const m = sid ? (window.__techMapV26637?.[sid] || {}) : {};
  return { ...m, ...row };
}

function pickFieldV26635(row, keys, fallback = "依策略判斷") {
  const merged = mergeTechRowV26637(row || {});
  for (const k of keys) {
    const v = merged?.[k];
    if (usefulV26637(v)) return cleanTechV26637(v, fallback);
  }
  return fallback;
}

function inferMaLabelV26635(row, maKey, label) {
  const merged = mergeTechRowV26637(row || {});
  const existing = pickFieldV26635(merged, [`${maKey}_label`, `${maKey}_status`, `${label}觀察`, `${label}_status`], "");
  if (usefulV26637(existing)) return cleanTechV26637(existing);

  const close = Number(String(merged.close || merged.ref_price || merged.price || "").replace(/,/g, ""));
  const ma = Number(String(merged[maKey] || "").replace(/,/g, ""));
  if (Number.isFinite(close) && Number.isFinite(ma) && ma > 0) {
    if (close >= ma * 1.01) return `${label}：站上｜↑ 強勢`;
    if (close <= ma * 0.99) return `${label}：跌破｜↓ 轉弱`;
    return `${label}：貼近｜→ 盤整`;
  }

  const txt = String(merged.reason || "") + " " + String(merged.system_note || "") + " " + String(merged.final_action || merged.action || merged.status || "");
  if (/停損|跌破|出場|SELL|賣/.test(txt)) return `${label}：依出場風控判斷`;
  if (/試單|突破|強勢|BUY|買/.test(txt)) return `${label}：依試單節奏判斷`;
  if (/觀察|整理|收斂|WATCH/.test(txt)) return `${label}：依觀察節奏判斷`;
  return `${label}：依策略判斷`;
}



/* ===== v331.1 Safe Number Helper / 修補 numV26641 未定義 ===== */
function numV26641(v) {
  const n = Number(String(v ?? "").replace(/,/g, "").replace("%", "").replace("張", "").replace("股", ""));
  return Number.isFinite(n) ? n : NaN;
}

function inferKbarFromPriceV26641(row) {
  const merged = mergeTechRowV26637(row || {});
  const o = numV26641(merged.open);
  const h = numV26641(merged.high);
  const l = numV26641(merged.low);
  const c = numV26641(merged.close || merged.ref_price || merged.price);
  const ma20 = numV26641(merged.ma20);

  if (![o, h, l, c].every(Number.isFinite) || h <= l) {
    const action = normalizeAction(merged.final_action || merged.action || merged.status || "");
    const reason = String(merged.reason || merged.exit_reason || merged.system_note || "");
    if (/停損|跌破|出場|SELL|賣/.test(reason + action)) return "跌破型K棒";
    if (/突破|強勢|試單|BUY|買/.test(reason + action)) return "突破確認K";
    if (/觀察|整理|收斂|WATCH/.test(reason + action)) return "整理觀察K";
    return "依策略判斷K";
  }

  const range = Math.max(h - l, 0.0001);
  const body = Math.abs(c - o);
  const upper = h - Math.max(o, c);
  const lower = Math.min(o, c) - l;
  const bodyRatio = body / range;
  const upperRatio = upper / range;
  const lowerRatio = lower / range;

  if (bodyRatio <= 0.18) return "十字K／猶豫";
  if (upperRatio >= 0.45) return "上影壓力K";
  if (lowerRatio >= 0.45) return "下影支撐K";
  if (c > o && (!Number.isFinite(ma20) || c >= ma20)) return "突破長紅K";
  if (c < o && (!Number.isFinite(ma20) || c < ma20)) return "跌破型K棒";
  if (c > o) return "陽K續強";
  if (c < o) return "陰K轉弱";
  return "一般K棒";
}

function inferKStructureFromMaV26641(row) {
  const merged = mergeTechRowV26637(row || {});
  const c = numV26641(merged.close || merged.ref_price || merged.price);
  const ma5 = numV26641(merged.ma5);
  const ma10 = numV26641(merged.ma10);
  const ma20 = numV26641(merged.ma20);
  const reason = String(merged.reason || merged.system_note || merged.tech_reason || "");
  const action = normalizeAction(merged.final_action || merged.action || merged.status || "");

  if ([c, ma5, ma10, ma20].every(Number.isFinite)) {
    if (ma5 > ma10 && ma10 > ma20 && c > ma20) return "多頭排列";
    if (ma5 < ma10 && ma10 < ma20 && c < ma20) return "空頭排列";
    if (c > ma20 && ma5 >= ma10) return "整理後轉強";
    if (c < ma20) return "短線轉弱";
    return "震盪整理";
  }

  if (/停損|跌破|出場|SELL|賣/.test(reason + action)) return "短線轉弱";
  if (/突破|強勢|試單|BUY|買/.test(reason + action)) return "整理後轉強";
  if (/觀察|收斂|安靜|吸籌|WATCH/.test(reason + action)) return "整理收斂";
  return "依策略結構";
}

function inferKbarTypeV26635(row) {
  const direct = pickFieldV26635(row, [
    "kbar_type", "k_bar_type", "K棒型態", "k棒型態",
    "exit_kbar_type"
  ], "");
  if (usefulV26637(direct)) return cleanTechV26637(direct);
  return inferKbarFromPriceV26641(row);
}

function inferKStructureV26635(row) {
  const direct = pickFieldV26635(row, [
    "k_structure", "kline_structure", "K線結構", "k線結構",
    "tech_structure", "structure_label"
  ], "");
  if (usefulV26637(direct)) return cleanTechV26637(direct);
  return inferKStructureFromMaV26641(row);
}

function techGridCellsV26635(row) {
  return `
          ${detailCell("MA5觀察", inferMaLabelV26635(row, "ma5", "MA5"))}
          ${detailCell("MA10觀察", inferMaLabelV26635(row, "ma10", "MA10"))}
          ${detailCell("MA20觀察", inferMaLabelV26635(row, "ma20", "MA20"))}
          ${detailCell("K棒型態", inferKbarTypeV26635(row))}
          ${detailCell("K線結構", inferKStructureV26635(row))}
        `;
}


function inferBehaviorHintV26650(row) {
  const merged = mergeTechRowV26637(row || {});
  const direct = pickFieldV26635(merged, ["behavior_hint", "行為判讀"], "");
  if (usefulV26637(direct)) return cleanTechV26637(direct);

  const kbar = inferKbarTypeV26635(merged);
  const kstruct = inferKStructureV26635(merged);
  const txt = [
    kbar, kstruct,
    merged.final_action || merged.action || merged.status || "",
    merged.reason || "",
    merged.system_note || "",
    merged.tech_reason || ""
  ].join(" ");

  if (/SELL|REDUCE|賣|出場|停損|跌破|短線轉弱/.test(txt)) return "🔻 結構轉弱（優先控風險）";
  if (/高檔出貨|假突破|上影壓力|疑似假突破/.test(txt)) return "⚠️ 高檔出貨／誘多風險（不追高）";
  if (/突破長紅|突破確認|多頭排列|強勢/.test(txt)) return "🚀 主力拉升（趨勢延續）";
  if (/整理收斂|整理觀察|下影支撐|洗盤|吸籌/.test(txt)) return "🟡 洗盤吸籌／整理換手（結構未壞）";
  if (/WATCH|觀察|盤整|震盪|十字/.test(txt)) return "⚪ 盤整觀望（等待方向）";
  return "⚪ 行為中性（依策略判斷）";
}

function inferBehaviorConfidenceV26650(row) {
  const direct = pickFieldV26635(row, ["behavior_confidence", "行為信心"], "");
  if (usefulV26637(direct)) return cleanTechV26637(direct);
  const hint = inferBehaviorHintV26650(row);
  if (/主力拉升|結構轉弱/.test(hint)) return "高";
  if (/出貨|誘多|洗盤|吸籌/.test(hint)) return "中高";
  if (/盤整/.test(hint)) return "中低";
  return "中低";
}

function inferBehaviorActionHintV26650(row) {
  const direct = pickFieldV26635(row, ["behavior_action_hint", "行為操作提示"], "");
  if (usefulV26637(direct)) return cleanTechV26637(direct);
  const hint = inferBehaviorHintV26650(row);
  if (/主力拉升/.test(hint)) return "可續抱；若是試單，可觀察是否進入加碼條件。";
  if (/結構轉弱/.test(hint)) return "先控風險，不急著攤平；等站回關鍵均線再觀察。";
  if (/出貨|誘多/.test(hint)) return "避免追價；若已有部位，觀察是否跌破 MA5 / MA10。";
  if (/洗盤|吸籌/.test(hint)) return "不急追；等放量突破或站穩 MA10 / MA20 再提高權重。";
  return "先觀察，不急進場；等突破、量能或均線方向出現。";
}


function positionBehaviorBlockV26654(row) {
  return `<div class="detail-text position-behavior-v26654"><b>🧠 行為判讀</b><p>${inferBehaviorHintV26650(row)}｜信心：${inferBehaviorConfidenceV26650(row)}<br>${inferBehaviorActionHintV26650(row)}</p></div>`;
}


function techTextBlockV26635(row, isExit = false) {
  const merged = mergeTechRowV26637(row || {});
  const techReason = [
    inferMaLabelV26635(merged, "ma5", "MA5"),
    inferMaLabelV26635(merged, "ma10", "MA10"),
    inferMaLabelV26635(merged, "ma20", "MA20"),
    "K棒：" + inferKbarTypeV26635(merged),
    "K線：" + inferKStructureV26635(merged)
  ].map(x => cleanTechV26637(x)).join("｜");

  const kReason = "K棒型態：" + inferKbarTypeV26635(merged) + "｜K線結構：" + inferKStructureV26635(merged);
  const hint = pickFieldV26635(merged, ["tech_decision_hint", "technical_hint", "技術提示"], "");

  return `
        <div class="detail-text ${isExit ? "exit-detail-text" : ""}"><b>技術補充</b><p>${cleanTechV26637(techReason)}</p></div>
        <div class="detail-text ${isExit ? "exit-detail-text" : ""}"><b>K線／型態提示</b><p>${cleanTechV26637(kReason)}</p></div>
        <div class="detail-text ${isExit ? "exit-detail-text" : ""}"><b>🧠 行為判讀</b><p>${inferBehaviorHintV26650(merged)}｜信心：${inferBehaviorConfidenceV26650(merged)}<br>${inferBehaviorActionHintV26650(merged)}</p></div>
        <div class="detail-text ${isExit ? "exit-detail-text" : ""}"><b>技術決策提示</b><p>${cleanTechV26637(hint || (isExit ? "若 MA5/MA20 轉弱，優先控風險；若站回均線再觀察。" : "依原策略執行，技術欄位用於確認節奏與風險。"))}</p></div>
      `;
}

async function loadTechMapV26637() {
  const files = [
    FILES.candidates,
    FILES.tradePlan,
    FILES.core,
    FILES.alpha,
    FILES.ignition,
    FILES.evolution,
    FILES.positionOverlay,
    FILES.final
  ].filter(Boolean);

  const out = {};
  for (const file of files) {
    try {
      const txt = await fetchText(file);
      const rows = parseCsv(txt);
      for (const r of rows) {
        const sid = sidKeyV26637(r.stock_id || r.symbol || r.code);
        if (!sid) continue;
        out[sid] = { ...(out[sid] || {}), ...r };
      }
    } catch (e) {}
  }
  window.__techMapV26637 = out;
  return out;
}



/* ===== v306.5 DETAIL INDUSTRY + TOP ORDER RESTORE ===== */
function industryTagV3065(row) {
  row = row || {};
  const direct = safeText(
    row.industry_tag || row.industry || row.industry_name || row.sector || row.theme || row.group || "",
    ""
  ).trim();
  if (direct && direct !== "--") return direct;

  const sid = String(row.stock_id || row.code || row.symbol || "").trim().slice(0, 4);

  const exact = {
    "3231": "AI伺服器",
    "2382": "AI伺服器",
    "2317": "AI伺服器",
    "6669": "AI伺服器",
    "2330": "半導體",
    "2303": "半導體",
    "2344": "半導體",
    "3034": "IC設計",
    "3443": "IC設計",
    "2379": "IC設計",
    "6179": "通訊",
    "6189": "電子零組件",
    "2498": "電子",
    "1402": "紡織",
    "2009": "鋼鐵",
    "2014": "鋼鐵",
    "2603": "航運",
    "2609": "航運",
    "2615": "航運",
    "2618": "航運",
    "2636": "航運",
    "3706": "伺服器",
    "6585": "重電",
    "1513": "重電",
    "1514": "重電",
    "1605": "電纜",
    "2368": "PCB",
    "2367": "PCB",
    "3037": "PCB",
    "8046": "PCB",
    "2753": "觀光",
    "8936": "國防",
    "5484": "IC設計",
    "9935": "航運"
  };

  if (exact[sid]) return exact[sid];
  if (/^(28|58)/.test(sid)) return "金融";
  if (/^26/.test(sid)) return "航運";
  if (/^14/.test(sid)) return "紡織";
  if (/^20/.test(sid)) return "鋼鐵";
  if (/^(15|16)/.test(sid)) return "機電";
  if (/^(23|24|30|34|61|62|65)/.test(sid)) return "電子";
  if (/^27/.test(sid)) return "觀光";
  return "其他";
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
  // v304.3：主清單分數欄改顯示個股股價，不再顯示策略分數。
  const score = num(row.close || row.ref_price || row.price || row.last_price || row.stock_price || row.current_price);
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
  const finalAdvice = zhFinalAdvice(row);
  const isExit = isExitActionV26616(action);
  const isCoreCardV326 = typeof isCoreRowV326 === "function" ? isCoreRowV326(row) : false;
  const coreExtraClassV326 = isCoreCardV326 ? " core-card-v326" : "";
  const coreMainExtraClassV326 = isCoreCardV326 ? " core-main-v326" : "";

  const exitType = inferExitTypeV26616(row);
  const exitReason = inferExitReasonV26616(row);
  const exitKbarType = inferExitKbarTypeV26616(row);
  const exitKbarReason = inferExitKbarReasonV26616(row);
  const exitRisk = inferExitRiskLevelV26616(row);
  const exitAdvice = inferExitAdviceV26616(row, action);

  const detailGrid = isExit ? `
          ${detailCell("股票名稱", stockName)}
          ${detailCell("來源", source)}
          ${detailCell("策略層", strat)}
          ${detailCell("產業類別", industryTagV3065(row))}
          ${detailCell("出場型態", exitType)}
          ${detailCell("出場K棒型態", exitKbarType)}
          ${detailCell("參考價", close)}
          ${detailCell("建議金額", amount)}
          ${detailCell("目標權重", weight)}
          ${detailCell("流動性", liqLabel, liqCls)}
          ${detailCell("成交量", formatLotsFromShares(row.volume))}
          ${detailCell("成交金額", formatTurnoverTW(row.turnover))}
          ${detailCell("風險等級", exitRisk)}
          ${techGridCellsV26635(row)}
          ${detailCell("籌碼集中度", chipDisplayV26621(row))}
        ` : `
          ${detailCell("股票名稱", stockName)}
          ${topBadge ? detailCell("系統評測", topBadge + "｜優先觀察") : ""}
          ${detailCell("來源", source)}
          ${detailCell("策略層", strat)}
          ${detailCell("產業類別", industryTagV3065(row))}
          ${detailCell("進場型態", entry)}
          ${detailCell("參考價", close)}
          ${detailCell("建議金額", amount)}
          ${detailCell("目標權重", weight)}
          ${detailCell("流動性", liqLabel, liqCls)}
          ${detailCell("成交量", formatLotsFromShares(row.volume))}
          ${detailCell("成交金額", formatTurnoverTW(row.turnover))}
          ${detailCell("流動性分數", liqScore)}
          ${String(row.strategy_type || row.bucket || "").toUpperCase() === "IGNITION" ? detailCell("假起漲分數", fakeScoreLabelV26634(row)) : ""}
          ${String(row.strategy_type || row.bucket || "").toUpperCase() === "IGNITION" ? detailCell("假起漲風險", row.fake_risk_tag || row.fake_risk_level || "--") : ""}
          ${techGridCellsV26635(row)}
          ${detailCell("籌碼集中度", chipDisplayV26621(row))}
        `;

  const detailText = isExit ? `
        <div class="detail-text exit-detail-text"><b>出場原因</b><p>${exitReason}</p></div>
        <div class="detail-text exit-detail-text"><b>K棒判斷原因</b><p>${exitKbarReason}</p></div>
        <div class="detail-text exit-detail-text"><b>建議動作</b><p>${exitAdvice}</p></div>
        <div class="detail-text exit-detail-text"><b>籌碼原因</b><p>${chipReasonV26621(row)}</p></div>
        <div class="detail-text exit-detail-text"><b>中文籌碼提示</b><p>${chipHintV26621(row)}</p></div>
        <div class="detail-text exit-detail-text"><b>系統提示</b><p>${note}</p></div>
        ${techTextBlockV26635(row, true)}
      ` : `
        ${String(row.strategy_type || row.bucket || "").toUpperCase() === "IGNITION" ? ignitionChinesePromptV26634(row) : ""}
        ${String(row.strategy_type || row.bucket || "").toUpperCase() === "EVOLUTION" ? evolutionChinesePromptV26634(row) : ""}
        <div class="detail-text"><b>原因</b><p>${reason}</p></div>
        <div class="detail-text"><b>中文決策提示</b><p>${finalAdvice}</p></div>
        <div class="detail-text"><b>籌碼原因</b><p>${chipReasonV26621(row)}</p></div>
        <div class="detail-text"><b>中文籌碼提示</b><p>${chipHintV26621(row)}</p></div>
        <div class="detail-text"><b>系統提示</b><p>${note}</p></div>
        ${techTextBlockV26635(row, false)}
      `;

  return `
    <article class="scan-item ${cls}${coreExtraClassV326}">
      <div class="scan-main scan-main-live${coreMainExtraClassV326}" data-toggle="${key}">
        <div class="scan-action ${cls}">${emoji} ${label}</div>
        <div class="scan-stock">${stock}</div>
        <div class="scan-score">${score}</div>
        <div class="scan-top">${top}</div>
        <div class="scan-entry">${isExit ? label : entry}</div>
        <div class="scan-liq ${liqCls}">${liqLabel}</div>
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
      <span>IGNITION <b>${rows.filter(r => String(r.strategy_type || r.strategy || r.bucket || "").toUpperCase() === "IGNITION").length}</b></span>
      <span>EVOLUTION <b>${rows.filter(r => String(r.strategy_type || r.strategy || r.bucket || "").toUpperCase() === "EVOLUTION").length}</b></span>
      <span>高流動性 <b>${high}</b></span>
      <span>中流動性 <b>${mid}</b></span>
      <span>低流動性 <b>${low}</b></span>
    </div>
    <div class="source-line">資料來源：${safeText(summary.source)}</div>
  `;
}

// ===== v329 PRIORITY Final Action Bridge / 最終操作吃紫框主升池 =====
// 目的：
// 1) 不動原本 UI / 持倉 / Actions。
// 2) final_action_plan.csv 若沒有 BUY，仍從 trade_plan.csv 抓 v328 PRIORITY 主升操作池。
// 3) 讓「🔥 最終操作」顯示紫框 TOP 排序，不再因沒有同步持倉而空白。
function isPriorityOperationRowV329(row) {
  row = row || {};
  const joined = [
    row.source,
    row.bucket,
    row.strategy_type,
    row.strategy_layer,
    row.strategy_bucket,
    row.engine,
    row.priority_grade_v328,
    row.priority_rank_v328,
    row.is_core_v319,
    row.core_score_v319,
    row.system_note,
    row.reason
  ].map(v => String(v ?? "")).join(" ").toUpperCase();

  return (
    joined.includes("V328_PRIORITY_OPERATION_POOL") ||
    joined.includes("PRIORITY") ||
    joined.includes("CORE｜主升候選") ||
    joined.includes("主升操作池") ||
    String(row.is_core_v319 ?? "").trim() === "1" ||
    String(row.priority_rank_v328 ?? "").trim() !== ""
  );
}

function normalizePriorityOperationRowV329(r) {
  const rank = String(r.priority_rank_v328 || r.opportunity_rank || r.section_opportunity_rank || "").trim();
  const grade = String(r.priority_grade_v328 || "").trim();
  const actionRaw = normalizeAction(r.final_action || r.action || (rank && Number(rank) <= 6 ? "BUY" : "WATCH"));
  const action = actionRaw === "WATCH" && rank && Number(rank) <= 6 ? "BUY" : actionRaw;
  const note = r.system_note || r.reason || r.action_sub || "🟣 PRIORITY 主升操作池：由紫框名單匯總排序。";

  return {
    ...r,
    final_action: action,
    action: action,
    stock_id: r.stock_id,
    source: r.source || "v328_priority_operation_pool",
    bucket: r.bucket || "PRIORITY",
    strategy_type: r.strategy_type || "CORE",
    strategy_layer: r.strategy_layer || `🟣 PRIORITY｜主升操作池 ${grade}`,
    strategy_bucket: r.strategy_bucket || `🟣 CORE｜主升候選${rank ? " TOP" + rank : ""}`,
    score: r.core_score_v319 || r.score || r.entry_score || r.rank_score || r.liquidity_score || "",
    entry_type: r.entry_type || r.action_sub || "主升候選操作",
    execution_flag: r.execution_flag || (rank ? `TOP${rank}` : "TOP"),
    section_top_opportunity: r.section_top_opportunity || (rank ? `PRIORITY_TOP${rank}` : "PRIORITY"),
    top_opportunity: r.top_opportunity || (rank ? `🔥PRIORITY_TOP${rank}` : "🔥PRIORITY"),
    opportunity_rank: r.opportunity_rank || rank,
    close: r.close || r.ref_price || r.price || "",
    ref_price: r.ref_price || r.close || r.price || "",
    suggested_amount: r.suggested_amount || r.suggest_amount || "",
    target_weight: r.target_weight || "",
    liquidity_level: r.liquidity_level || r.liquidity_tag || "HIGH",
    liquidity_tag: r.liquidity_tag || r.liquidity_level || "HIGH",
    liquidity_score: r.liquidity_score || "",
    volume: r.volume || "",
    turnover: r.turnover || "",
    reason: r.reason || note,
    system_note: note,
    is_core_v319: r.is_core_v319 || "1",
    core_score_v319: r.core_score_v319 || r.score || ""
  };
}

function mergeFinalAndPriorityRowsV329(finalRows, priorityRows) {
  const out = [];
  const seen = new Set();

  const push = (row) => {
    if (!row || !row.stock_id) return;
    const sid = String(row.stock_id).match(/\d{4}/)?.[0] || String(row.stock_id).trim();
    const action = normalizeAction(row.final_action || row.action || "");
    const key = sid + "::" + action;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ ...row, stock_id: sid });
  };

  (priorityRows || []).forEach(push);
  (finalRows || []).forEach(push);
  return out;
}

async function loadPriorityOperationRowsV329() {
  try {
    const txt = await fetchText(FILES.tradePlan);
    const rows = parseCsv(txt);
    return rows
      .filter(isPriorityOperationRowV329)
      .map(normalizePriorityOperationRowV329);
  } catch (e) {
    console.warn("priority operation pool fallback fail", e);
    return [];
  }
}

async function loadFinalRows() {
  let finalRows = [];

  try {
    const txt = await fetchText(FILES.final);
    finalRows = parseCsv(txt);
  } catch (e) {
    console.warn("final_action_plan fallback", e);
  }

  const priorityRows = await loadPriorityOperationRowsV329();

  if (finalRows.length || priorityRows.length) {
    return mergeFinalAndPriorityRowsV329(finalRows, priorityRows);
  }

  try {
    const txt = await fetchText(FILES.tradePlan);
    const oldRows = parseCsv(txt);
    return oldRows.map(r => ({
      ...r,
      final_action: normalizeAction(r.final_action || r.action || "BUY"),
      action: normalizeAction(r.final_action || r.action || "BUY"),
      stock_id: r.stock_id,
      source: r.source || "ENTRY",
      bucket: r.strategy_type || r.bucket || "CORE",
      strategy_type: r.strategy_type || r.strategy || r.bucket || "",
      score: r.score || r.entry_score || r.rank_score || "",
      entry_type: r.action_sub || r.entry_type || "",
      execution_flag: r.action || "TOP",
      allowed: "True",
      close: r.ref_price || r.close || r.price || "",
      suggested_amount: r.suggested_amount || r.suggest_amount || "",
      target_weight: r.target_weight || "",
      liquidity_level: r.liquidity_level || "",
      liquidity_tag: r.liquidity_tag || "",
      liquidity_score: r.liquidity_score || "",
      volume: r.volume || "",
      turnover: r.turnover || "",
      reason: r.reason || r.note || "",
      system_note: r.system_note || r.note || "fallback trade_plan"
    }));
  } catch (e) {
    console.error(e);
    return [];
  }
}

async function loadIgnitionRows() {
  try {
    const txt = await fetchText(FILES.ignition);
    const rows = parseCsv(txt);
    return rows
      .filter(r => normalizeAction(r.action || r.final_action) !== "SKIP")
      .map(r => ({
        ...r,
        final_action: normalizeAction(r.action || r.final_action || "WATCH"),
        source: r.source || "IGNITION",
        bucket: r.strategy_type || "IGNITION",
        strategy_type: r.strategy_type || "IGNITION",
        strategy_name: r.strategy_name || "IGNITION 起漲啟動",
        score: r.entry_score || r.score || "",
        entry_type: r.ignition_phase || r.action_sub || "起漲觀察",
        execution_flag: r.section_top_opportunity || r.execution_flag || "",
        close: r.close || r.ref_price || "",
        reason: r.reason || r.note || "起漲啟動清單",
        system_note: r.system_note || r.operation_advice_zh || "起漲清單：小量試單 / 優先觀察，不建議一次重倉。",
        fake_score: r.fake_score || "",
        fake_risk_tag: r.fake_risk_tag || "",
        fake_risk_level: r.fake_risk_level || "",
        fake_flags: r.fake_flags || "",
        fake_reason_zh: r.fake_reason_zh || "",
        ignition_hint_zh: r.ignition_hint_zh || "",
        operation_advice_zh: r.operation_advice_zh || ""
      }));
  } catch (e) {
    console.warn("ignition_candidates load fail", e);
    return [];
  }
}

async function loadEvolutionRows() {
  try {
    const txt = await fetchText(FILES.evolution);
    const rows = parseCsv(txt);
    return rows
      .filter(r => normalizeAction(r.action || r.final_action) !== "SKIP")
      .map(r => ({
        ...r,
        final_action: normalizeAction(r.action || r.final_action || "TEST"),
        source: r.source || "EVOLUTION",
        bucket: r.bucket || "EVOLUTION",
        strategy_type: r.strategy_type || "EVOLUTION",
        strategy_name: r.strategy_name || "EVOLUTION 策略進化鏈",
        score: r.evolution_score || r.entry_score || r.score || "",
        entry_type: r.evolution_phase || r.entry_type || r.action_sub || "策略進化",
        execution_flag: r.section_top_opportunity || r.execution_flag || "",
        close: r.close || r.ref_price || "",
        reason: r.reason || r.note || "策略進化清單",
        system_note: r.system_note || "策略進化提示：由起漲、試單、強勢、核心狀態逐步升級。"
      }));
  } catch (e) {
    console.warn("strategy_evolution load fail", e);
    return [];
  }
}

async function init() {
  renderAppShell();
  showBackendRunCompleteIfAnyV26630K();

  try {
    const [regime, summaryRaw, metaRaw, macro, rows, ignitionRowsRaw, evolutionRowsRaw] = await Promise.all([
      fetchJson(FILES.regime, {}),
      fetchJson(FILES.finalSummary, {}),
      fetchJson(FILES.meta, {}),
      fetchJson(FILES.macro, {}),
      loadFinalRows(),
      loadIgnitionRows(),
      loadEvolutionRows()
    ]);

    const summary = { ...(metaRaw || {}), ...(summaryRaw || {}) };

    await loadPositionOverlayV26630();
    await loadTechMapV26637();
    const rowsWithTech = (rows || []).map(r => mergeTechRowV26637(r));
    const groups = splitRows(rowsWithTech);
    const ignitionRows = sortRows((ignitionRowsRaw || []).map(r => mergeTechRowV26637(r)));
    const evolutionRows = sortRows((evolutionRowsRaw || []).map(r => mergeTechRowV26637(r)));

    renderMeta(regime, summary, macro, rowsWithTech);
    // v266.31：頁面載入/重新整理後，直接讀後端 workflow_status.json 接回秒數。
    startWorkflowStatusWatchV26631();
    renderPositionRiskHints(rowsWithTech);
    renderPositions();
    renderDecision(rowsWithTech);
    renderFinalActions(groups.main);
    renderSectionList("ignitionList", ignitionRows, "ignition", 80);
    renderSectionList("evolutionList", evolutionRows, "evolution", 80);
    renderSectionList("testList", groups.test, "test", 80);
    renderSectionList("watchList", groups.watch, "watch", 80);
    renderSectionList("blockList", groups.block, "block", 80);
    renderStats(rowsWithTech, summary);
  } catch (e) {
    console.error(e);
    try {
      if (!document.body.innerHTML.trim()) renderAppShell();
      setSyncStatus("❌ 讀取失敗：" + e.message, "sync error");
    } catch (_) {}
  }
}

document.addEventListener("DOMContentLoaded", init);

// v266.36.2.2：如果 Safari / GitHub Pages 快取或早期錯誤導致畫面空白，至少強制渲染外殼，避免整頁空白。
document.addEventListener("DOMContentLoaded", function blankScreenGuardV266361() {
  setTimeout(function() {
    try {
      if (document.body && !document.body.innerHTML.trim() && typeof renderAppShell === "function") {
        renderAppShell();
        setSyncStatus("⚠️ 前端已啟動空白頁防護，請重新整理或回報錯誤：" + (window.__APP_BOOT_ERROR__ || "未知錯誤"), "sync error");
      }
    } catch (e) {
      try {
        document.body.innerHTML = '<main style="padding:28px;font-family:-apple-system,BlinkMacSystemFont,Noto Sans TC,sans-serif;color:#111827"><h1>⚠️ 前端防護啟動失敗</h1><p>' + e.message + '</p></main>';
      } catch (_) {}
    }
  }, 1200);
});


try { loadMacroDashboardV26614(); } catch(e) { console.log(e); }





// ===== v266.15.3 總經說明強制顯示 =====
function macroRuleTextV266153(data) {
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
    : "有效依策略判斷，暫以中性處理。";

  return `${rule}｜原始：${raw} ${score.toFixed(1)}｜目前：${label}｜${confidenceText}`;
}

function macroAdviceTextV266153(data) {
  const label = data?.macro_label || "--";
  const policy = data?.macro_policy || "--";
  const unknown = Number(data?.unknown_count || 0);

  let tip = `${label}：${policy}`;
  if (unknown >= 4) tip += "｜注意：總經資料仍不完整，不能單獨作為重倉依據。";
  return tip;
}

async function loadMacroExplainV266153() {
  try {
    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const html = `
      <div class="macro-explain-v266153">
        <div class="macro-explain-title">📘 總經評分標準</div>
        <div class="macro-explain-body">${macroRuleTextV266153(data)}</div>
        <div class="macro-explain-title" style="margin-top:12px;">🧭 總經操作提示</div>
        <div class="macro-explain-body">${macroAdviceTextV266153(data)}</div>
      </div>
    `;

    if (document.querySelector(".macro-explain-v266153")) {
      document.querySelector(".macro-explain-v266153").outerHTML = html;
      return;
    }

    const all = Array.from(document.querySelectorAll("body *"));
    let target = null;

    for (const el of all) {
      const t = (el.textContent || "").trim();
      if (
        t.includes("總經狀態") &&
        t.includes("總經偏") &&
        !t.includes("總經評分標準")
      ) {
        target = el;
        break;
      }
    }

    if (!target) {
      for (const el of all) {
        const t = (el.textContent || "").trim();
        if (t.includes("市場狀態") && !t.includes("總經評分標準")) {
          target = el;
          break;
        }
      }
    }

    const wrap = document.createElement("div");
    wrap.innerHTML = html;
    const node = wrap.firstElementChild;

    if (target && target.parentElement) {
      target.insertAdjacentElement("afterend", node);
    } else {
      const main = document.querySelector("main") || document.querySelector(".app") || document.body;
      main.prepend(node);
    }
  } catch (e) {
    console.log("macro explain force insert fail", e);
  }
}







// ===== v266.16.2 總經提示強制顯示：評分標準 + 操作提示 =====
function macroRuleTextV266162(data) {
  const valid = Number(data?.valid_indicator_count || 0);
  const total = Number(data?.total_indicator_count || 0);
  const unknown = Number(data?.unknown_count || 0);
  const rawLabel = data?.macro_raw_label || data?.macro_label || "--";
  const nowLabel = data?.macro_label || "--";
  const score = Number(data?.macro_score || 0);
  const adjusted = Number(data?.macro_adjusted_score ?? data?.macro_score ?? 0);
  const confidence = data?.macro_confidence_label || data?.macro_confidence || "信心未定";

  const validText = total > 0
    ? `有效指標 ${valid}/${total}｜未知 ${unknown}｜${confidence}｜加權分數 ${adjusted.toFixed(2)}`
    : `有效依策略判斷｜${confidence}`;

  return `每項總經指標以 +1 / 0 / -1 評分；分數越高代表環境越偏多，分數越低代表越保守。原始判斷：${rawLabel}｜目前判斷：${nowLabel}｜分數 ${score.toFixed(1)}｜${validText}`;
}

function macroAdviceTextV266162(data) {
  const label = data?.macro_label || "--";
  const policy = data?.macro_policy || "";
  const score = Number(data?.macro_score || 0);
  const confidence = data?.macro_confidence || "";
  const unknown = Number(data?.unknown_count || 0);

  let advice = "";

  if (score >= 3) {
    advice = "總經偏多：允許正常試單與分批買進，但仍需避開追高。";
  } else if (score >= 1) {
    advice = "總經偏多但強度普通：BUY 降級 TEST，適合小量測試，不適合一次重倉。";
  } else if (score <= -2) {
    advice = "總經偏弱：優先控風險，買進降級觀察，持倉需嚴格停損。";
  } else {
    advice = "總經中性：以個股訊號為主，但買進金額要保守。";
  }

  if (policy) advice += `｜系統政策：${policy}`;
  if (confidence === "LOW" || unknown >= 4) {
    advice += "｜注意：總經資料不完整，不能單獨作為重倉依據。";
  }

  return advice;
}

async function renderMacroExplainV266162() {
  try {
    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const html = `
      <div class="macro-explain-v266162">
        <div class="macro-explain-title">📘 總經評分標準</div>
        <div class="macro-explain-body">${macroRuleTextV266162(data)}</div>
        <div class="macro-explain-title">🧭 總經操作提示</div>
        <div class="macro-explain-body">${macroAdviceTextV266162(data)}</div>
      </div>
    `;

    const old = document.querySelector(".macro-explain-v266162");
    if (old) {
      old.outerHTML = html;
      return;
    }

    const all = Array.from(document.querySelectorAll("body *"));
    let target = null;

    // 優先插在「總經狀態」卡片後面
    for (const el of all) {
      const txt = (el.textContent || "").trim();
      if (txt.includes("總經狀態") && (txt.includes("總經偏") || txt.includes("分數"))) {
        target = el;
        break;
      }
    }

    // 找不到就插在「風險模式」前面，避免跑到很下面
    if (!target) {
      for (const el of all) {
        const txt = (el.textContent || "").trim();
        if (txt.includes("風險模式")) {
          target = el;
          break;
        }
      }
    }

    const wrap = document.createElement("div");
    wrap.innerHTML = html;
    const node = wrap.firstElementChild;

    if (target && target.parentElement) {
      target.insertAdjacentElement("afterend", node);
    } else {
      (document.querySelector("main") || document.body).prepend(node);
    }
  } catch (e) {
    console.log("v266.16.2 macro explain render failed", e);
  }
}

// 等首頁資料 render 完再插入，避免找不到卡片
// ===== v266.21 籌碼可用版 =====
function macroInlineDecisionV26617(data) {
  const score = Number(data?.macro_score || 0);
  const unknown = Number(data?.unknown_count || 0);
  const confidence = String(data?.macro_confidence || data?.macro_confidence_label || "").toUpperCase();

  let decision = "⚖️ 中性｜控倉操作";
  if (score >= 3) decision = "🔥 偏多｜可正常分批";
  else if (score >= 1) decision = "🧭 試單｜不可重倉";
  else if (score <= -2) decision = "⚠️ 防守｜停止新倉";

  let conf = "📘 信心未定";
  if (confidence.includes("HIGH") || confidence.includes("高")) conf = "📘 信心高";
  else if (confidence.includes("MEDIUM") || confidence.includes("中")) conf = "📘 信心中";
  else if (confidence.includes("LOW") || confidence.includes("低") || unknown >= 4) conf = "📘 信心低";

  return `${decision}｜${conf}`;
}

async function renderMacroInlineHintV26617() {
  try {
    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();
    const hint = macroInlineDecisionV26617(data);

    const all = Array.from(document.querySelectorAll("body *"));
    let target = null;

    for (const el of all) {
      const txt = (el.textContent || "").trim();
      if (
        txt.includes("總經狀態") &&
        (txt.includes("總經偏") || txt.includes("分數"))
      ) {
        target = el;
        break;
      }
    }

    if (!target) return;

    let hintNode = target.querySelector(".macro-inline-hint-v26617");
    if (!hintNode) {
      hintNode = document.createElement("div");
      hintNode.className = "macro-inline-hint-v26617";
      target.appendChild(hintNode);
    }

    hintNode.textContent = hint;
  } catch (e) {
    console.log("v266.17 macro inline hint failed", e);
  }
}




// ===== v266.21 籌碼可用版：只保留 inline，不顯示下方大卡 =====
function macroInlineDecisionV266171(data) {
  const score = Number(data?.macro_score || 0);
  const unknown = Number(data?.unknown_count || 0);
  const confidence = String(data?.macro_confidence || data?.macro_confidence_label || "").toUpperCase();

  let decision = "⚖️ 中性｜控倉";
  if (score >= 3) decision = "🔥 偏多｜可分批";
  else if (score >= 1) decision = "🧭 試單｜不可重倉";
  else if (score <= -2) decision = "⚠️ 防守｜停止新倉";

  let conf = "📘 信心未定";
  if (confidence.includes("HIGH") || confidence.includes("高")) conf = "📘 信心高";
  else if (confidence.includes("MEDIUM") || confidence.includes("中")) conf = "📘 信心中";
  else if (confidence.includes("LOW") || confidence.includes("低") || unknown >= 4) conf = "📘 信心低";

  return `${decision}｜${conf}`;
}

function macroScoreTextV266171(data) {
  const score = Number(data?.macro_score || 0);
  const total =
    Number(data?.total_indicator_count || 0) ||
    Number(data?.valid_indicator_count || 0) + Number(data?.unknown_count || 0) ||
    7;

  return `${score}/${total}`;
}

async function renderMacroInlineHintV266171() {
  try {
    // 清掉前幾版放在下面的大型說明卡
    document.querySelectorAll(
      ".macro-explain-v266162, .macro-explain-v266153, .macro-explain-v266152"
    ).forEach(el => el.remove());

    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const hint = macroInlineDecisionV266171(data);
    const scoreText = macroScoreTextV266171(data);
    const label = data?.macro_label || "總經狀態";

    const all = Array.from(document.querySelectorAll("body *"));
    let target = null;

    for (const el of all) {
      const txt = (el.textContent || "").trim();
      if (
        txt.includes("總經狀態") &&
        (txt.includes("總經偏") || txt.includes("分數"))
      ) {
        target = el;
        break;
      }
    }

    if (!target) return;

    // 將卡片內容改成：總經偏多｜分數 2/7 + inline 提示
    const labelNode = Array.from(target.querySelectorAll("*")).find(el =>
      (el.textContent || "").trim() === "總經狀態"
    );

    // 找實際數值區塊：通常是最大字那個，也可能就是 target 自身文字
    let valueNode = null;
    const nodes = Array.from(target.querySelectorAll("*")).reverse();
    for (const el of nodes) {
      const txt = (el.textContent || "").trim();
      if (
        txt.includes("總經偏") ||
        txt.includes("分數") ||
        txt.match(/總經.*\|\s*分數/)
      ) {
        valueNode = el;
        break;
      }
    }

    if (!valueNode || valueNode === labelNode) {
      valueNode = target;
    }

    // 避免重複嵌套
    const oldHint = target.querySelector(".macro-inline-hint-v266171");
    if (oldHint) oldHint.remove();

    const cleanValue = `${label}｜分數 ${scoreText}`;
    const inline = document.createElement("span");
    inline.className = "macro-inline-hint-v266171";
    inline.textContent = hint;

    if (valueNode === target) {
      // 保守模式：只附加在卡片內，不破壞標題
      const existing = Array.from(target.childNodes).find(n => 
        n.nodeType === Node.TEXT_NODE && String(n.textContent || "").includes("總經")
      );
      target.appendChild(inline);
    } else {
      valueNode.textContent = cleanValue + " ";
      valueNode.appendChild(inline);
    }
  } catch (e) {
    console.log("v266.17.1 macro inline hint failed", e);
  }
}




// ===== v266.21 籌碼可用版：分數/滿分/信心/變化/操作提示 inline 顯示 =====
function macroTotalV26618(data) {
  const total = Number(data?.total_indicator_count || 0);
  const valid = Number(data?.valid_indicator_count || 0);
  const unknown = Number(data?.unknown_count || 0);
  if (total > 0) return total;
  if (valid + unknown > 0) return valid + unknown;
  return 7;
}

function macroScoreV26618(data) {
  const raw = Number(data?.macro_score ?? data?.score ?? 0);
  return Number.isFinite(raw) ? raw : 0;
}

function macroConfidenceV26618(data) {
  const unknown = Number(data?.unknown_count || 0);
  const valid = Number(data?.valid_indicator_count || 0);
  const total = macroTotalV26618(data);
  const raw = String(data?.macro_confidence || data?.macro_confidence_label || "").toUpperCase();

  if (raw.includes("HIGH") || raw.includes("高")) return "📘 信心高";
  if (raw.includes("MEDIUM") || raw.includes("中")) return "📘 信心中";
  if (raw.includes("LOW") || raw.includes("低")) return "📘 信心低";

  if (total > 0 && valid / total >= 0.75) return "📘 信心高";
  if (total > 0 && valid / total >= 0.45) return "📘 信心中";
  if (unknown >= 4) return "📘 信心低";
  return "📘 信心未定";
}

function macroLabelV26618(data) {
  const label = data?.macro_label || data?.macro_regime_label || "";
  if (label) return String(label);

  const score = macroScoreV26618(data);
  if (score >= 2) return "總經偏多";
  if (score <= -2) return "總經偏空";
  return "總經中性";
}

function macroDecisionV26618(data) {
  const score = macroScoreV26618(data);
  const total = macroTotalV26618(data);
  const ratio = total ? score / total : 0;

  if (score >= 3 || ratio >= 0.45) return "🔥 可分批｜勿追高";
  if (score >= 1) return "🧭 試單｜不可重倉";
  if (score <= -2) return "⚠️ 防守｜停止新倉";
  return "⚖️ 中性｜控倉";
}

function macroChangeTextV26618(data) {
  const now = macroScoreV26618(data);
  const prevFields = [
    data?.prev_macro_score,
    data?.previous_macro_score,
    data?.yesterday_macro_score,
    data?.last_macro_score
  ];
  const found = prevFields.find(v => v !== undefined && v !== null && v !== "");
  if (found === undefined) return "";

  const prev = Number(found);
  if (!Number.isFinite(prev)) return "";

  const diff = now - prev;
  const sign = diff > 0 ? "+" : "";
  const word = diff > 0 ? "轉強" : diff < 0 ? "轉弱" : "持平";
  return `📈 分數變化 ${sign}${diff.toFixed(1)}｜${word}`;
}

function macroPolicyTextV26618(data) {
  const policy = data?.macro_policy || data?.policy || "";
  if (policy) return String(policy);
  return macroDecisionV26618(data);
}

function macroInlineHTMLV26618(data) {
  const label = macroLabelV26618(data);
  const score = macroScoreV26618(data);
  const total = macroTotalV26618(data);
  const decision = macroDecisionV26618(data);
  const confidence = macroConfidenceV26618(data);
  const change = macroChangeTextV26618(data);

  const scoreText = `${score}/${total}`;
  const changeHTML = change ? `<span class="macro-change-v26618">${change}</span>` : "";

  return `
    <div class="macro-value-v26618">
      <span>${label}｜分數 ${scoreText}</span>
      <span class="macro-pill-v26618">${decision}</span>
      <span class="macro-pill-v26618 macro-conf-v26618">${confidence}</span>
      ${changeHTML}
    </div>
  `;
}

function findMacroCardV26618() {
  // 優先找包含「總經狀態」的最小卡片
  const all = Array.from(document.querySelectorAll("body *"));
  let best = null;

  for (const el of all) {
    const txt = (el.textContent || "").trim();
    if (txt.includes("總經狀態")) {
      const box = el.closest(".card, .info-card, .summary-card, .stat-card, .field-card, div");
      best = box || el;
      break;
    }
  }

  if (best) return best;

  // fallback：找 dashboard 上所有淺色資訊卡，第3張通常是總經
  const cards = Array.from(document.querySelectorAll(".card, .info-card, .summary-card, .stat-card, .field-card"));
  if (cards.length >= 3) return cards[2];

  return null;
}

async function renderMacroEnhancedV26618() {
  try {
    // 移除舊的大卡與舊 inline，避免重複
    document.querySelectorAll(
      ".macro-explain-v266162, .macro-explain-v266153, .macro-explain-v266152, .macro-inline-hint-v26617, .macro-inline-hint-v266171"
    ).forEach(el => el.remove());

    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const card = findMacroCardV26618();
    if (!card) return;

    // 保留原本 label「總經狀態」，只替換數值區
    let valueEl = card.querySelector(".macro-value-v26618");
    if (!valueEl) {
      const children = Array.from(card.children);
      let oldValue = children.find(el => {
        const txt = (el.textContent || "").trim();
        return txt.includes("總經偏") || txt.includes("分數");
      });

      if (oldValue && !oldValue.textContent.trim().includes("總經狀態")) {
        oldValue.outerHTML = macroInlineHTMLV26618(data);
      } else {
        const wrap = document.createElement("div");
        wrap.innerHTML = macroInlineHTMLV26618(data);
        card.appendChild(wrap.firstElementChild);
      }
    } else {
      valueEl.outerHTML = macroInlineHTMLV26618(data);
    }
  } catch (e) {
    console.log("v266.18 macro enhanced render failed", e);
  }
}




// ===== v266.21 籌碼可用版：直接替換「總經偏多｜分數」那一行 =====
function macroTotalV26619(data) {
  const total = Number(data?.total_indicator_count || 0);
  const valid = Number(data?.valid_indicator_count || 0);
  const unknown = Number(data?.unknown_count || 0);
  if (total > 0) return total;
  if (valid + unknown > 0) return valid + unknown;
  return 7;
}

function macroScoreV26619(data) {
  const raw = Number(data?.macro_score ?? data?.score ?? 0);
  return Number.isFinite(raw) ? raw : 0;
}

function macroLabelV26619(data) {
  const label = data?.macro_label || data?.macro_regime_label || "";
  if (label) return String(label);
  const score = macroScoreV26619(data);
  if (score >= 2) return "總經偏多";
  if (score <= -2) return "總經偏空";
  return "總經中性";
}

function macroConfidenceV26619(data) {
  const unknown = Number(data?.unknown_count || 0);
  const valid = Number(data?.valid_indicator_count || 0);
  const total = macroTotalV26619(data);
  const raw = String(data?.macro_confidence || data?.macro_confidence_label || "").toUpperCase();

  if (raw.includes("HIGH") || raw.includes("高")) return "📘 信心高";
  if (raw.includes("MEDIUM") || raw.includes("中")) return "📘 信心中";
  if (raw.includes("LOW") || raw.includes("低")) return "📘 信心低";

  if (total > 0 && valid / total >= 0.75) return "📘 信心高";
  if (total > 0 && valid / total >= 0.45) return "📘 信心中";
  if (unknown >= 4) return "📘 信心低";
  return "📘 信心未定";
}

function macroDecisionV26619(data) {
  const score = macroScoreV26619(data);
  const total = macroTotalV26619(data);
  const ratio = total ? score / total : 0;

  if (score >= 3 || ratio >= 0.45) return "🔥 可分批｜勿追高";
  if (score >= 1) return "🧭 試單｜不可重倉";
  if (score <= -2) return "⚠️ 防守｜停止新倉";
  return "⚖️ 中性｜控倉";
}

function macroChangeTextV26619(data) {
  const now = macroScoreV26619(data);
  const prevCandidates = [
    data?.prev_macro_score,
    data?.previous_macro_score,
    data?.yesterday_macro_score,
    data?.last_macro_score
  ];
  const found = prevCandidates.find(v => v !== undefined && v !== null && v !== "");
  if (found === undefined) return "";
  const prev = Number(found);
  if (!Number.isFinite(prev)) return "";

  const diff = now - prev;
  const sign = diff > 0 ? "+" : "";
  const word = diff > 0 ? "轉強" : diff < 0 ? "轉弱" : "持平";
  return `📈 ${sign}${diff.toFixed(1)} ${word}`;
}

function macroInlineHTMLV26619(data) {
  const label = macroLabelV26619(data);
  const score = macroScoreV26619(data);
  const total = macroTotalV26619(data);
  const decision = macroDecisionV26619(data);
  const confidence = macroConfidenceV26619(data);
  const change = macroChangeTextV26619(data);

  return `
    <span class="macro-line-v26619">
      <span class="macro-main-v26619">${label}｜分數 ${score}/${total}</span>
      <span class="macro-pill-v26619">${decision}</span>
      <span class="macro-pill-v26619 macro-conf-v26619">${confidence}</span>
      ${change ? `<span class="macro-pill-v26619 macro-change-v26619">${change}</span>` : ""}
    </span>
  `;
}

function findMacroValueElementV26619() {
  const all = Array.from(document.querySelectorAll("body *"));

  const direct = all.find(el => {
    if (el.children.length > 2) return false;
    const txt = (el.textContent || "").trim();
    return (
      txt.includes("總經偏") &&
      txt.includes("分數") &&
      !txt.includes("風險模式") &&
      !txt.includes("市場狀態") &&
      !txt.includes("macro")
    );
  });
  if (direct) return direct;

  const label = all.find(el => (el.textContent || "").trim() === "總經狀態");
  if (label) {
    const card = label.parentElement || label.closest("div");
    if (card) {
      const candidates = Array.from(card.querySelectorAll("div, span, b, strong")).filter(el => {
        const t = (el.textContent || "").trim();
        return t && t !== "總經狀態" && (t.includes("總經") || t.includes("分數"));
      });
      if (candidates.length) return candidates[candidates.length - 1];
    }
  }

  return null;
}

async function renderMacroPreciseV26619() {
  try {
    document.querySelectorAll(
      ".macro-explain-v266162, .macro-explain-v266153, .macro-explain-v266152, .macro-inline-hint-v26617, .macro-inline-hint-v266171, .macro-value-v26618"
    ).forEach(el => el.remove());

    const res = await fetch("./data/macro_regime.json?ts=" + Date.now(), { cache: "no-store" });
    const data = await res.json();

    const valueEl = findMacroValueElementV26619();
    if (!valueEl) return;

    valueEl.innerHTML = macroInlineHTMLV26619(data);
    valueEl.classList.add("macro-value-host-v26619");
  } catch (e) {
    console.log("v266.19 macro precise render failed", e);
  }
}

// v333.2 disabled legacy macro overwrite: setTimeout(renderMacroPreciseV26619, 400);
// v333.2 disabled legacy macro overwrite: setTimeout(renderMacroPreciseV26619, 1200);
// v333.2 disabled legacy macro overwrite: setTimeout(renderMacroPreciseV26619, 2400);
// v333.2 disabled legacy macro overwrite: setTimeout(renderMacroPreciseV26619, 4200);



// ===== v266.21 籌碼信心顯示 =====
function chipDisplayV26621(row) {
  const display = row.chip_display || row["籌碼集中度"];
  const conf = row.chip_confidence || row["籌碼信心"] || "";
  if (display && String(display).trim() !== "--") {
    return conf ? `${safeText(display)}｜${safeText(conf)}` : safeText(display);
  }

  const scoreRaw = row.chip_score || row.chip_concentration_score || row["籌碼分數"];
  const score = Number(scoreRaw);
  if (!Number.isFinite(score)) return "--";

  let label = "🟡 普通";
  if (score >= 80) label = "🔥 高度集中";
  else if (score >= 60) label = "🟢 偏集中";
  else if (score >= 40) label = "🟡 普通";
  else if (score >= 20) label = "⚠️ 分散";
  else label = "❌ 極度分散";

  const base = `${Math.round(score)}（${label}）`;
  return conf ? `${base}｜${safeText(conf)}` : base;
}

function chipReasonV26621(row) {
  return safeText(
    row.chip_reason ||
    row.chip_concentration_reason ||
    row["籌碼原因"],
    "籌碼依策略判斷"
  );
}

function chipHintV26621(row) {
  return safeText(
    row.chip_hint ||
    row.chip_concentration_hint ||
    row["籌碼提示"],
    "籌碼依策略判斷，只能當輔助，不可重倉。"
  );
}



/* ===== v266.30B hotfix：只修正顯示，不再動原本區塊 ===== */
function injectV26630BPositionColorStyle() {
  if (document.getElementById("v26630b-position-color-style")) return;
  const style = document.createElement("style");
  style.id = "v26630b-position-color-style";
  style.textContent = `
    .position-merged-v26630.sell,
    .position-merged-v26630.reduce {
      background: #fff1f1 !important;
      border: 3px solid #f0a3a3 !important;
      border-radius: 24px !important;
      padding: 18px !important;
      margin-top: 14px !important;
    }
    .position-merged-v26630.hold,
    .position-merged-v26630.watch {
      background: #effcf3 !important;
      border: 3px solid #89e5a4 !important;
      border-radius: 24px !important;
      padding: 18px !important;
      margin-top: 14px !important;
    }
    .position-merged-pill-v26630.sell,
    .position-merged-pill-v26630.reduce {
      background: #fde2e2 !important;
      color: #b91c1c !important;
      border-radius: 999px !important;
      padding: 8px 14px !important;
      font-weight: 900 !important;
    }
    .position-merged-pill-v26630.hold,
    .position-merged-pill-v26630.watch {
      background: #dcfce7 !important;
      color: #166534 !important;
      border-radius: 999px !important;
      padding: 8px 14px !important;
      font-weight: 900 !important;
    }
    .position-merged-head-v26630 {
      display: flex !important;
      align-items: center !important;
      gap: 12px !important;
      margin-bottom: 16px !important;
    }
    .position-merged-head-v26630 b {
      flex: 1 !important;
      font-size: 1.28em !important;
      font-weight: 900 !important;
    }
    .position-merged-head-v26630 strong {
      font-size: 1.08em !important;
      font-weight: 900 !important;
    }
  `;
  document.head.appendChild(style);
}
try { injectV26630BPositionColorStyle(); } catch(e) {}
document.addEventListener("DOMContentLoaded", injectV26630BPositionColorStyle);
/* =========================================================
   v266.65 Turn Field UI Patch / 轉折欄位顯示補丁
   使用方式：
   1) 直接貼到 app.js 最底部
   2) 不刪原本函式
   3) 這段會覆寫同名顯示/排序函式，但不動持倉、同步、Actions
   ========================================================= */

const TURN_PATCH_VERSION_V26665 = "v266.65_turn_field_ui_patch";

/* ---------- 轉折欄位工具 ---------- */
function validTurnTextV26665(v) {
  if (v === undefined || v === null) return false;
  const s = String(v).trim();
  return !!s && !["--", "-", "nan", "NaN", "undefined", "null", "None", "NONE", "無事件"].includes(s);
}

function pickTurnV26665(row, keys, fallback = "--") {
  row = row || {};
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(row, k) && validTurnTextV26665(row[k])) return String(row[k]).trim();
  }
  return fallback;
}

function numTurnV26665(v, fallback = 999999) {
  const n = Number(String(v ?? "").replace(/,/g, "").replace("%", ""));
  return Number.isFinite(n) ? n : fallback;
}

function turnLabelV26665(row) {
  return pickTurnV26665(row, [
    "turn_event_label_v26664_1",
    "turn_event_label_v26664",
    "first_trigger_label_v26663",
    "trigger_event_label_v26662",
    "state_transition_label_v26660"
  ], "--");
}

function turnScoreV26665(row) {
  return pickTurnV26665(row, [
    "turn_event_score_v26664_1",
    "turn_event_score_v26664",
    "first_trigger_score_v26663",
    "trigger_event_score_v26662",
    "state_transition_score_v26660"
  ], "--");
}

function turnRankV26665(row) {
  const fields = [
    "turn_first_rank_v26664_1",
    "turn_first_rank_v26664",
    "first_trigger_rank_v26663",
    "trigger_rank_v26662",
    "master_trend_rank_v26661"
  ];

  for (const f of fields) {
    const n = numTurnV26665(row?.[f], null);
    if (n !== null && Number.isFinite(n)) return n;
  }
  return 999999;
}

function turnReasonV26665(row) {
  return pickTurnV26665(row, [
    "turn_event_reason_v26664_1",
    "turn_event_reason_v26664",
    "first_trigger_reason_v26663",
    "trigger_event_reason_v26662",
    "state_transition_reason_v26660",
    "evolution_reason"
  ], "尚無轉折原因。");
}

function turnHintV26665(row) {
  const direct = pickTurnV26665(row, [
    "turn_priority_hint_v26664",
    "trigger_action_hint_v26662"
  ], "");

  if (direct) return direct;

  const lb = turnLabelV26665(row);
  if (lb === "TURN_FIRST") return "第一優先：轉折剛出現，隔日看不破 MA5 / MA10。";
  if (lb === "EARLY_TURN") return "早期轉強：可列入試單或優先觀察，不要一次重倉。";
  if (lb === "WATCH_TURN") return "觀察轉強：等待第二根確認，確認承接再提高權重。";
  if (lb === "AVOID_CHASE" || lb === "過熱/風險事件") return "避免追高：等回檔不破 MA10 / MA20 再評估。";
  if (lb === "準點火事件" || lb === "準IGNITION") return "具備點火條件：觀察隔日是否不破關鍵均線。";
  if (lb === "早期轉強事件") return "偏早期轉強：放入 WATCH，等待量能與籌碼二次確認。";
  return "依轉折欄位輔助判斷，不取代原策略。";
}

function turnCssClassV26665(label) {
  const s = String(label || "").toUpperCase();
  if (s.includes("TURN_FIRST") || s.includes("FIRST_IGNITION") || s.includes("強點火")) return "turn-hot";
  if (s.includes("EARLY") || s.includes("準") || s.includes("READY")) return "turn-early";
  if (s.includes("WATCH") || s.includes("觀察")) return "turn-watch";
  if (s.includes("AVOID") || s.includes("RISK") || s.includes("風險") || s.includes("過熱")) return "turn-risk";
  return "turn-none";
}

function turnBadgeV26665(row) {
  const lb = turnLabelV26665(row);
  if (!validTurnTextV26665(lb)) return "";
  const score = turnScoreV26665(row);
  const rank = turnRankV26665(row);
  const cls = turnCssClassV26665(lb);
  const rankText = rank !== 999999 ? `#${rank}` : "";
  return `<span class="turn-badge-v26665 ${cls}">⚡ ${lb}${rankText ? "｜" + rankText : ""}${score !== "--" ? "｜" + score : ""}</span>`;
}

function turnDetailBlockV26665(row) {
  const lb = turnLabelV26665(row);
  const score = turnScoreV26665(row);
  const rank = turnRankV26665(row);
  const reason = turnReasonV26665(row);
  const hint = turnHintV26665(row);
  const cls = turnCssClassV26665(lb);

  if (!validTurnTextV26665(lb) && !validTurnTextV26665(reason)) return "";

  return `
    <div class="turn-box-v26665 ${cls}">
      <div class="turn-box-head-v26665">
        <b>⚡ 轉折雷達</b>
        <span>${lb}</span>
      </div>
      <div class="turn-box-grid-v26665">
        <div><span>轉折分數</span><b>${score}</b></div>
        <div><span>轉折排名</span><b>${rank !== 999999 ? rank : "--"}</b></div>
      </div>
      <div class="detail-text"><b>轉折原因</b><p>${reason}</p></div>
      <div class="detail-text"><b>操作提示</b><p>${hint}</p></div>
    </div>
  `;
}

/* ---------- CSS 注入 ---------- */
function injectTurnStyleV26665() {
  if (document.getElementById("turn-style-v26665")) return;

  const style = document.createElement("style");
  style.id = "turn-style-v26665";
  style.textContent = `
    .turn-badge-v26665 {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 6px 10px;
      border-radius: 999px;
      font-weight: 900;
      font-size: 12px;
      white-space: nowrap;
      border: 1px solid rgba(0,0,0,.08);
    }
    .turn-badge-v26665.turn-hot,
    .turn-box-v26665.turn-hot {
      background: #fff7ed;
      color: #9a3412;
      border-color: #fdba74;
    }
    .turn-badge-v26665.turn-early,
    .turn-box-v26665.turn-early {
      background: #ecfeff;
      color: #155e75;
      border-color: #67e8f9;
    }
    .turn-badge-v26665.turn-watch,
    .turn-box-v26665.turn-watch {
      background: #fefce8;
      color: #854d0e;
      border-color: #fde047;
    }
    .turn-badge-v26665.turn-risk,
    .turn-box-v26665.turn-risk {
      background: #fef2f2;
      color: #991b1b;
      border-color: #fca5a5;
    }
    .turn-badge-v26665.turn-none,
    .turn-box-v26665.turn-none {
      background: #f3f4f6;
      color: #374151;
      border-color: #d1d5db;
    }
    .turn-box-v26665 {
      margin: 12px 0;
      padding: 14px;
      border-radius: 18px;
      border: 2px solid #e5e7eb;
    }
    .turn-box-head-v26665 {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }
    .turn-box-head-v26665 b {
      font-size: 16px;
      font-weight: 900;
    }
    .turn-box-head-v26665 span {
      font-size: 13px;
      font-weight: 900;
    }
    .turn-box-grid-v26665 {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-bottom: 10px;
    }
    .turn-box-grid-v26665 div {
      background: rgba(255,255,255,.65);
      border-radius: 12px;
      padding: 8px 10px;
    }
    .turn-box-grid-v26665 span {
      display: block;
      font-size: 11px;
      opacity: .72;
      margin-bottom: 3px;
    }
    .turn-box-grid-v26665 b {
      font-size: 15px;
      font-weight: 900;
    }
    .scan-main-live .turn-badge-v26665 {
      margin-left: 6px;
    }
  `;
  document.head.appendChild(style);
}

/* ---------- 排序覆寫：轉折排名優先 ---------- */
const __oldRowScoreV26665 = typeof rowScoreV26630 === "function" ? rowScoreV26630 : null;

rowScoreV26630 = function(row) {
  const turnAdjusted = Number(row?.turn_adjusted_score_v26664 ?? "");
  if (Number.isFinite(turnAdjusted)) return turnAdjusted;

  const turnScore = Number(row?.turn_event_score_v26664_1 ?? row?.turn_event_score_v26664 ?? "");
  if (Number.isFinite(turnScore)) return turnScore;

  const triggerScore = Number(row?.trigger_adjusted_score_v26662 ?? row?.trigger_event_score_v26662 ?? "");
  if (Number.isFinite(triggerScore)) return triggerScore;

  if (__oldRowScoreV26665) return __oldRowScoreV26665(row);
  const n = Number(row?.score || row?.opportunity_score || row?.entry_score || row?.rank_score || row?.liquidity_score || 0);
  return Number.isFinite(n) ? n : 0;
};

const __oldSortRowsV26665 = typeof sortRows === "function" ? sortRows : null;

sortRows = function(rows) {
  return (rows || []).slice().sort((a, b) => {
    const aa = normalizeAction(a.final_action || a.action);
    const bb = normalizeAction(b.final_action || b.action);
    const pa = ACTION_PRIORITY[aa] || 99;
    const pb = ACTION_PRIORITY[bb] || 99;
    if (pa !== pb) return pa - pb;

    const ra = turnRankV26665(a);
    const rb = turnRankV26665(b);
    if (ra !== rb) return ra - rb;

    const tsa = Number(a.turn_event_score_v26664_1 ?? a.turn_event_score_v26664 ?? a.first_trigger_score_v26663 ?? a.trigger_event_score_v26662 ?? -999);
    const tsb = Number(b.turn_event_score_v26664_1 ?? b.turn_event_score_v26664 ?? b.first_trigger_score_v26663 ?? b.trigger_event_score_v26662 ?? -999);
    if (Number.isFinite(tsa) && Number.isFinite(tsb) && tsa !== tsb) return tsb - tsa;

    const ta = getTopRankV26630(a);
    const tb = getTopRankV26630(b);
    if (ta !== tb) return ta - tb;

    const sb = rowScoreV26630(b);
    const sa = rowScoreV26630(a);
    if (sb !== sa) return sb - sa;

    const la = liquiditySortRank(a);
    const lb = liquiditySortRank(b);
    if (lb !== la) return lb - la;

    const va = Number(a.volume || 0);
    const vb = Number(b.volume || 0);
    if (vb !== va) return vb - va;

    return String(a.stock_id || "").localeCompare(String(b.stock_id || ""));
  });
};

/* ---------- renderScanRow 覆寫：主列與詳情顯示轉折雷達 ---------- */
const __oldRenderScanRowV26665 = typeof renderScanRow === "function" ? renderScanRow : null;

renderScanRow = function(row, key) {
  injectTurnStyleV26665();

  const html = __oldRenderScanRowV26665
    ? __oldRenderScanRowV26665(row, key)
    : "";

  if (!html) return html;

  const badge = turnBadgeV26665(row);
  const block = turnDetailBlockV26665(row);

  let out = html;

  // 主列：把轉折 badge 放在 entry 後面，手機上最容易看到
  if (badge && out.includes('<div class="scan-entry">')) {
    out = out.replace(
      /<div class="scan-entry">([\s\S]*?)<\/div>/,
      `<div class="scan-entry">$1 ${badge}</div>`
    );
  }

  // 詳情：放在 detail-grid 後面、原因前面
  if (block && out.includes('<div class="detail-grid">')) {
    out = out.replace(
      /(<div class="detail-grid">[\s\S]*?<\/div>\s*)/,
      `$1${block}`
    );
  }

  return out;
};

/* ---------- 初始化後補樣式 ---------- */
try { injectTurnStyleV26665(); } catch(e) {}
document.addEventListener("DOMContentLoaded", function() {
  try { injectTurnStyleV26665(); } catch(e) {}
});

/* =========================================================
   v266.65.2 Turn UI Below Grid Fix / 轉折欄位下移修正版

   目的：
   - 把轉折提示放到原本欄位格子「下面」
   - 不塞進左右兩欄 detail-grid
   - 不撐高股票名稱欄位
   - 字體縮小、卡片壓扁，保留原本 App 風格

   使用方式：
   直接貼到 app.js 最底部，放在 v266.65 / v266.65.1 後面
   ========================================================= */

const TURN_PATCH_VERSION_V266652 = "v266.65.2_turn_ui_below_grid_fix";

function injectTurnStyleV266652() {
  if (document.getElementById("turn-style-v266652")) return;

  const style = document.createElement("style");
  style.id = "turn-style-v266652";
  style.textContent = `
    .turn-below-v266652 {
      display: block;
      width: 100%;
      box-sizing: border-box;
      margin: 12px 0 4px 0;
      padding: 12px 14px;
      border-radius: 18px;
      background: #f8fafc;
      border: 2px solid #e5e7eb;
      color: #334155;
      line-height: 1.55;
      font-weight: 800;
    }

    .turn-below-v266652.hot {
      background: #fff7ed;
      border-color: #fed7aa;
      color: #9a3412;
    }

    .turn-below-v266652.early {
      background: #ecfeff;
      border-color: #a5f3fc;
      color: #155e75;
    }

    .turn-below-v266652.watch {
      background: #fefce8;
      border-color: #fde68a;
      color: #854d0e;
    }

    .turn-below-v266652.risk {
      background: #fef2f2;
      border-color: #fecaca;
      color: #991b1b;
    }

    .turn-below-head-v266652 {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 8px;
    }

    .turn-below-title-v266652 {
      font-size: 15px;
      font-weight: 900;
      white-space: nowrap;
    }

    .turn-below-meta-v266652 {
      font-size: 12px;
      font-weight: 900;
      opacity: .85;
      text-align: right;
    }

    .turn-below-reason-v266652 {
      font-size: 14px;
      font-weight: 800;
      opacity: .95;
      word-break: break-word;
    }

    .turn-below-hint-v266652 {
      margin-top: 5px;
      font-size: 13px;
      font-weight: 800;
      opacity: .82;
      word-break: break-word;
    }

    .turn-row-mini-v266652 {
      display: inline-block;
      margin-left: 4px;
      padding: 2px 7px;
      border-radius: 999px;
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      color: #475569;
      font-size: 11px;
      font-weight: 900;
      max-width: 88px;
      overflow: hidden;
      text-overflow: ellipsis;
      vertical-align: middle;
      white-space: nowrap;
    }

    .turn-box-v26665,
    .turn-inline-v266651 {
      display: none !important;
    }
  `;
  document.head.appendChild(style);
}

function turnSimpleClassV266652(label) {
  const s = String(label || "").toUpperCase();
  if (s.includes("TURN_FIRST") || s.includes("FIRST_IGNITION") || s.includes("強點火")) return "hot";
  if (s.includes("EARLY") || s.includes("準") || s.includes("READY")) return "early";
  if (s.includes("WATCH") || s.includes("觀察")) return "watch";
  if (s.includes("AVOID") || s.includes("RISK") || s.includes("風險") || s.includes("過熱")) return "risk";
  return "";
}

function turnBelowBlockV266652(row) {
  if (typeof turnLabelV26665 !== "function") return "";

  const label = turnLabelV26665(row);
  if (!label || label === "--" || label === "NONE" || label === "無事件") return "";

  const score = typeof turnScoreV26665 === "function" ? turnScoreV26665(row) : "--";
  const rank = typeof turnRankV26665 === "function" ? turnRankV26665(row) : 999999;
  const reason = typeof turnReasonV26665 === "function" ? turnReasonV26665(row) : "";
  const hint = typeof turnHintV26665 === "function" ? turnHintV26665(row) : "";

  const cls = turnSimpleClassV266652(label);
  const meta = [
    label,
    rank !== 999999 ? `排 ${rank}` : "",
    score !== "--" ? `分 ${score}` : ""
  ].filter(Boolean).join("｜");

  return `
    <div class="turn-below-v266652 ${cls}">
      <div class="turn-below-head-v266652">
        <div class="turn-below-title-v266652">⚡ 轉折提示</div>
        <div class="turn-below-meta-v266652">${meta}</div>
      </div>
      <div class="turn-below-reason-v266652">${reason || "尚無轉折原因"}</div>
      <div class="turn-below-hint-v266652">${hint || "依轉折欄位輔助判斷，不取代原策略。"}</div>
    </div>
  `;
}

function turnMiniBadgeV266652(row) {
  if (typeof turnLabelV26665 !== "function") return "";
  const label = turnLabelV26665(row);
  if (!label || label === "--" || label === "NONE" || label === "無事件") return "";
  return `<span class="turn-row-mini-v266652">⚡${label}</span>`;
}

const __baseRenderScanRowV266652 =
  (typeof __oldRenderScanRowV26665 === "function")
    ? __oldRenderScanRowV26665
    : (
        (typeof __oldRenderScanRowV266651 === "function")
          ? __oldRenderScanRowV266651
          : (typeof renderScanRow === "function" ? renderScanRow : null)
      );

renderScanRow = function(row, key) {
  injectTurnStyleV266652();

  const baseHtml = __baseRenderScanRowV266652
    ? __baseRenderScanRowV266652(row, key)
    : "";

  if (!baseHtml) return baseHtml;

  const mini = turnMiniBadgeV266652(row);
  const below = turnBelowBlockV266652(row);

  let out = baseHtml;

  if (mini && out.includes('<div class="scan-entry">')) {
    out = out.replace(
      /<div class="scan-entry">([\s\S]*?)<\/div>/,
      `<div class="scan-entry">$1 ${mini}</div>`
    );
  }

  if (below) {
    out = out.replace(
      /(<div class="detail-grid">[\s\S]*?<\/div>)/,
      `$1${below}`
    );
  }

  return out;
};

try { injectTurnStyleV266652(); } catch(e) {}
document.addEventListener("DOMContentLoaded", function() {
  try { injectTurnStyleV266652(); } catch(e) {}
});
/* =========================================================
   v266.65.4 Turn UI Exact Position Fix
   位置：
   K線結構 / 籌碼集中度
   ↓
   轉折提示
   ↓
   原因

   貼到 app.js 最底部，放在前面所有 v266.65 補丁後面。
   ========================================================= */

const TURN_PATCH_VERSION_V266654 = "v266.65.4_exact_position_fix";

function injectTurnStyleV266654() {
  if (document.getElementById("turn-style-v266654")) return;
  const style = document.createElement("style");
  style.id = "turn-style-v266654";
  style.textContent = `
    .turn-box-v26665,
    .turn-inline-v266651,
    .turn-below-v266652,
    .turn-row-mini-v266652,
    .turn-mini-badge-v266651,
    .turn-badge-v26665 {
      display: none !important;
    }

    .turn-original-v266654 {
      margin: 12px 0 14px 0;
      padding: 0;
      border: none;
      background: transparent;
      color: inherit;
      font-size: inherit;
      line-height: inherit;
      font-weight: inherit;
    }

    .turn-original-v266654 .turn-title-v266654 {
      margin: 0 0 6px 0;
      font-size: inherit;
      font-weight: 900;
      color: inherit;
    }

    .turn-original-v266654 .turn-text-v266654 {
      margin: 0 0 6px 0;
      font-size: inherit;
      font-weight: 800;
      line-height: 1.75;
      color: inherit;
      word-break: break-word;
    }

    .turn-original-v266654 .turn-meta-v266654 {
      display: inline-block;
      margin-left: 6px;
      font-size: .9em;
      font-weight: 900;
      opacity: .75;
    }
  `;
  document.head.appendChild(style);
}

function turnValueV266654(row, keys, fallback = "") {
  row = row || {};
  for (const k of keys) {
    const v = row[k];
    if (v === undefined || v === null) continue;
    const s = String(v).trim();
    if (!s || ["--", "-", "nan", "NaN", "undefined", "null", "None", "NONE", "無事件"].includes(s)) continue;
    return s;
  }
  return fallback;
}

function turnOriginalBlockV266654(row) {
  const label = turnValueV266654(row, [
    "turn_event_label_v26664_1",
    "turn_event_label_v26664",
    "first_trigger_label_v26663",
    "trigger_event_label_v26662",
    "state_transition_label_v26660"
  ]);

  if (!label) return "";

  const score = turnValueV266654(row, [
    "turn_event_score_v26664_1",
    "turn_event_score_v26664",
    "first_trigger_score_v26663",
    "trigger_event_score_v26662",
    "state_transition_score_v26660"
  ], "--");

  const rank = turnValueV266654(row, [
    "turn_first_rank_v26664_1",
    "turn_first_rank_v26664",
    "first_trigger_rank_v26663",
    "trigger_rank_v26662"
  ], "--");

  const reason = turnValueV266654(row, [
    "turn_event_reason_v26664_1",
    "turn_event_reason_v26664",
    "first_trigger_reason_v26663",
    "trigger_event_reason_v26662",
    "state_transition_reason_v26660"
  ], "尚無轉折原因");

  const hint = turnValueV266654(row, [
    "turn_priority_hint_v26664",
    "trigger_action_hint_v26662"
  ], "");

  const meta = [
    label,
    rank !== "--" ? `排 ${rank}` : "",
    score !== "--" ? `分 ${score}` : ""
  ].filter(Boolean).join("｜");

  return `
    <div class="turn-original-v266654">
      <div class="turn-title-v266654">⚡ 轉折提示 <span class="turn-meta-v266654">${meta}</span></div>
      <div class="turn-text-v266654">${reason}</div>
      ${hint ? `<div class="turn-text-v266654">${hint}</div>` : ""}
    </div>
  `;
}

const __baseRenderScanRowV266654 =
  (typeof __oldRenderScanRowV26665 === "function")
    ? __oldRenderScanRowV26665
    : (typeof renderScanRow === "function" ? renderScanRow : null);

renderScanRow = function(row, key) {
  injectTurnStyleV266654();

  const baseHtml = __baseRenderScanRowV266654
    ? __baseRenderScanRowV266654(row, key)
    : "";

  if (!baseHtml) return baseHtml;

  const turnBlock = turnOriginalBlockV266654(row);
  if (!turnBlock) return baseHtml;

  let out = baseHtml;

  /*
    插入 detail-grid 後面。
    你的 detail-grid 最後兩格是：
    K線結構 / 籌碼集中度
    所以這裡會剛好放在它們下面，原因上面。
  */
  const match = out.match(/<div class="detail-grid">[\s\S]*?<\/div>\s*(?=<div class="detail-text"|<h|<p|原因|中文決策提示|籌碼原因|系統提示)/);

  if (match && typeof match.index === "number") {
    const insertAt = match.index + match[0].length;
    return out.slice(0, insertAt) + turnBlock + out.slice(insertAt);
  }

  /*
    備援：放在第一個「原因」前面。
  */
  const reasonIdx = out.indexOf("原因");
  if (reasonIdx > -1) {
    return out.slice(0, reasonIdx) + turnBlock + out.slice(reasonIdx);
  }

  return out + turnBlock;
};

try { injectTurnStyleV266654(); } catch(e) {}
document.addEventListener("DOMContentLoaded", function() {
  try { injectTurnStyleV266654(); } catch(e) {}
});
/* =========================================================
   v273 FINAL UI SCORE PATCH / 連續分數前端顯示修補
   目的：
   - 不動 pipeline / strategy / 持倉 / watchlist / macro / workflow
   - 只修最後 UI 顯示與排序分數
   - 優先顯示 v273_continuous_score 等真實連續分數
   - 避免 58 / 52 / 80 模板分數蓋住畫面
   ========================================================= */
const V273_FINAL_UI_SCORE_PATCH = "v273_final_ui_score_patch_only";

function v273ScoreNumber(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim().replace(/,/g, "").replace("分", "");
  if (!s || ["--", "-", "nan", "NaN", "undefined", "null", "None"].includes(s)) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function v273IsTemplateScore(n) {
  if (!Number.isFinite(n)) return false;
  return [50, 52, 58, 62, 80].some(x => Math.abs(n - x) < 0.0001);
}

function v273PickRealScore(row) {
  row = row || {};

  const preferredFields = [
    "v273_continuous_score",
    "continuous_score",
    "final_continuous_score",
    "real_score",
    "true_score",
    "raw_score",
    "opportunity_score",
    "total_score",
    "entry_score",
    "rank_score",
    "composite_score",
    "ai_score",
    "alpha_score",
    "core_score"
  ];

  for (const k of preferredFields) {
    const n = v273ScoreNumber(row[k]);
    if (n !== null && !v273IsTemplateScore(n)) return n;
  }

  // 如果只有 v273_continuous_score 但剛好等於 80，也尊重 v273 欄位。
  for (const k of ["v273_continuous_score", "continuous_score", "final_continuous_score"]) {
    const n = v273ScoreNumber(row[k]);
    if (n !== null) return n;
  }

  const score = v273ScoreNumber(row.score);
  if (score !== null) return score;

  for (const k of preferredFields) {
    const n = v273ScoreNumber(row[k]);
    if (n !== null) return n;
  }

  return null;
}

function v273FormatScore(row) {
  const n = v273PickRealScore(row);
  if (n === null) return safeText?.(row?.score, "--") || "--";
  return n.toFixed(1);
}

function v273NormalizeRowScore(row) {
  const out = { ...(row || {}) };
  const score = v273FormatScore(out);
  out.score = score;
  out.entry_score = score;
  out.total_score = score;
  out.opportunity_score = score;
  out.rank_score = score;
  return out;
}

function v273InjectScoreStyle() {
  if (document.getElementById("v273-final-ui-score-style")) return;
  const style = document.createElement("style");
  style.id = "v273-final-ui-score-style";
  style.textContent = `
    .scan-score {
      min-width: 64px !important;
      width: auto !important;
      max-width: none !important;
      overflow: visible !important;
      text-overflow: clip !important;
      white-space: nowrap !important;
      font-variant-numeric: tabular-nums !important;
    }
    .scan-main-live {
      grid-template-columns: auto minmax(72px, 1fr) minmax(58px, auto) auto minmax(72px, auto) auto minmax(58px, auto) !important;
    }
    @media (max-width: 430px) {
      .scan-score {
        min-width: 56px !important;
        font-size: 0.92em !important;
      }
      .scan-main-live {
        column-gap: 8px !important;
      }
    }
  `;
  document.head.appendChild(style);
}

// 覆寫 rowScore：排序直接吃真實連續分數。
const __rowScoreBeforeV273 = typeof rowScoreV26630 === "function" ? rowScoreV26630 : null;
rowScoreV26630 = function(row) {
  const n = v273PickRealScore(row);
  if (n !== null) return n;
  return __rowScoreBeforeV273 ? __rowScoreBeforeV273(row) : 0;
};

// 覆寫 render：只把 row.score 換成真實分數，不動原本卡片 HTML / 展開 / 持倉 / workflow。
const __renderScanRowBeforeV273 = typeof renderScanRow === "function" ? renderScanRow : null;
renderScanRow = function(row, key) {
  v273InjectScoreStyle();
  const patchedRow = v273NormalizeRowScore(row);
  return __renderScanRowBeforeV273 ? __renderScanRowBeforeV273(patchedRow, key) : "";
};

// 覆寫 splitRows：分組後再以真實分數排序，避免 TEST / WATCH 仍沿用模板順序。
const __splitRowsBeforeV273 = typeof splitRows === "function" ? splitRows : null;
splitRows = function(rows) {
  const patched = (rows || []).map(v273NormalizeRowScore);
  const groups = __splitRowsBeforeV273 ? __splitRowsBeforeV273(patched) : { main: [], test: [], watch: [], block: [] };
  ["main", "test", "watch", "block"].forEach(k => {
    groups[k] = (groups[k] || []).slice().sort((a, b) => {
      const pa = ACTION_PRIORITY?.[normalizeAction(a.final_action || a.action)] || 99;
      const pb = ACTION_PRIORITY?.[normalizeAction(b.final_action || b.action)] || 99;
      if (pa !== pb) return pa - pb;
      const sb = v273PickRealScore(b) ?? 0;
      const sa = v273PickRealScore(a) ?? 0;
      if (sb !== sa) return sb - sa;
      return String(a.stock_id || "").localeCompare(String(b.stock_id || ""));
    });
  });
  return groups;
};

try { v273InjectScoreStyle(); } catch(e) {}
document.addEventListener("DOMContentLoaded", function() {
  try { v273InjectScoreStyle(); } catch(e) {}
});
/* ===== end v273 FINAL UI SCORE PATCH ===== */


/* =========================================================
   v281 Final Action UI Patch / 最終操作顯示修補
   目的：
   1. 只修前端顯示，不動 pipeline / strategy / 持倉 / macro / workflow
   2. 最終操作股票名稱空白時，從 stock_name_map / techMap / overlay 自動補名
   3. 最終操作區塊改成和 TEST / WATCH 一樣可收合
   ========================================================= */
const V281_FINAL_ACTION_UI_PATCH = "v281_final_action_name_and_collapse_patch";

function v281ValidText(v) {
  if (v === undefined || v === null) return false;
  const s = String(v).trim();
  return !!s && !["--", "-", "nan", "NaN", "undefined", "null", "None"].includes(s);
}

function v281StockId(row) {
  const raw = row?.stock_id || row?.stockId || row?.symbol || row?.code || "";
  if (typeof stockKeyV26630H === "function") return stockKeyV26630H(raw);
  const m = String(raw || "").match(/\d{4}/);
  return m ? m[0] : String(raw || "").trim();
}

function v281ResolveStockName(row) {
  row = row || {};
  const sid = v281StockId(row);

  const directFields = [
    row.stock_name,
    row.stockName,
    row.name,
    row.Name,
    row["股票名稱"],
    row["證券名稱"],
    row["證券簡稱"],
    row["公司簡稱"],
    row["公司名稱"]
  ];

  for (const v of directFields) {
    if (v281ValidText(v)) return String(v).trim();
  }

  const maps = [
    window.__stockNameMapV26630,
    window.__stockNameMap,
    window.stockNameMap,
  ];

  for (const mp of maps) {
    if (!mp || !sid) continue;
    const v = mp[sid] || mp[String(sid)] || mp[Number(sid)];
    if (v281ValidText(v)) return String(v).trim();
  }

  const tech = sid && window.__techMapV26637 ? window.__techMapV26637[sid] : null;
  if (tech) {
    const v = tech.stock_name || tech.stockName || tech.name || tech["股票名稱"] || tech["證券名稱"] || tech["公司簡稱"];
    if (v281ValidText(v)) return String(v).trim();
  }

  const overlay = sid && window.__positionOverlayMapV26630 ? window.__positionOverlayMapV26630[sid] : null;
  if (overlay) {
    const v = overlay.stock_name || overlay.stockName || overlay.name || overlay["股票名稱"] || overlay["證券名稱"] || overlay["公司簡稱"];
    if (v281ValidText(v)) return String(v).trim();
  }

  return "";
}

function v281PatchRowStockName(row) {
  const out = { ...(row || {}) };
  const name = v281ResolveStockName(out);
  if (v281ValidText(name)) out.stock_name = name;
  return out;
}

function v281PatchRowsStockName(rows) {
  return (rows || []).map(v281PatchRowStockName);
}

function v281MakeFinalActionCollapsible() {
  const list = document.getElementById("finalActionList");
  if (!list) return;

  const section = list.closest("section.card");
  if (!section || section.dataset.v281FinalCollapsible === "1") return;

  const oldHtml = list.innerHTML || "";
  section.dataset.v281FinalCollapsible = "1";
  section.classList.add("compact-card", "final-action-collapsible-v281");

  section.innerHTML = `
    <details open>
      <summary>🔥 最終操作</summary>
      <div class="hint">點擊股票可展開詳情</div>
      <div id="finalActionList">${oldHtml}</div>
    </details>
  `;
}

function v281InjectFinalActionStyle() {
  if (document.getElementById("v281-final-action-style")) return;
  const style = document.createElement("style");
  style.id = "v281-final-action-style";
  style.textContent = `
    .final-action-collapsible-v281 summary {
      cursor: pointer;
      user-select: none;
    }
    .final-action-collapsible-v281 .hint {
      margin: 8px 0 14px 0;
      color: #64748b;
      font-weight: 800;
    }
  `;
  document.head.appendChild(style);
}

// 1) renderAppShell 後立刻把「最終操作」區塊改成 details，可收合。
const __renderAppShellBeforeV281 = typeof renderAppShell === "function" ? renderAppShell : null;
if (__renderAppShellBeforeV281) {
  renderAppShell = function() {
    const ret = __renderAppShellBeforeV281.apply(this, arguments);
    try {
      v281InjectFinalActionStyle();
      v281MakeFinalActionCollapsible();
    } catch (e) {
      console.warn("v281 final collapsible patch failed", e);
    }
    return ret;
  };
}

// 2) renderFinalActions 前補股票名稱，並確保區塊保持可收合。
const __renderFinalActionsBeforeV281 = typeof renderFinalActions === "function" ? renderFinalActions : null;
if (__renderFinalActionsBeforeV281) {
  renderFinalActions = function(rows) {
    try {
      v281InjectFinalActionStyle();
      v281MakeFinalActionCollapsible();
      return __renderFinalActionsBeforeV281.call(this, v281PatchRowsStockName(rows || []));
    } finally {
      try {
        v281MakeFinalActionCollapsible();
        if (typeof bindToggle === "function") bindToggle();
      } catch (e) {}
    }
  };
}

// 3) renderScanRow 前補股票名稱，保護所有區塊的詳細欄位。
const __renderScanRowBeforeV281 = typeof renderScanRow === "function" ? renderScanRow : null;
if (__renderScanRowBeforeV281) {
  renderScanRow = function(row, key) {
    return __renderScanRowBeforeV281.call(this, v281PatchRowStockName(row), key);
  };
}

// 4) 若 stock map 載入後才完成，補一次重新渲染頁面上已存在的名稱欄位。
function v281RefreshBlankStockNameCells() {
  try {
    document.querySelectorAll(".detail-grid div").forEach(cell => {
      const label = cell.querySelector("span")?.textContent?.trim();
      const value = cell.querySelector("b");
      if (label !== "股票名稱" || !value) return;
      const old = String(value.textContent || "").trim();
      if (v281ValidText(old)) return;

      const article = cell.closest(".scan-item");
      const stock = article?.querySelector(".scan-stock")?.textContent?.trim();
      if (!stock) return;

      const sid = v281StockId({ stock_id: stock });
      const name = v281ResolveStockName({ stock_id: sid });
      if (v281ValidText(name)) value.textContent = name;
    });
  } catch (e) {}
}

try {
  v281InjectFinalActionStyle();
} catch (e) {}

document.addEventListener("DOMContentLoaded", function() {
  setTimeout(function() {
    try {
      v281InjectFinalActionStyle();
      v281MakeFinalActionCollapsible();
      v281RefreshBlankStockNameCells();
    } catch (e) {}
  }, 300);

  setTimeout(v281RefreshBlankStockNameCells, 1200);
  setTimeout(v281RefreshBlankStockNameCells, 2600);
});
/* ===== end v281 Final Action UI Patch ===== */




/* ===== v306.5 TOP ORDER RESTORE / 修復 TOP5 被刷掉 ===== */
(function restoreTopOrderV3065() {
  const oldSplitRows = typeof splitRows === "function" ? splitRows : null;

  function topAwareSortV3065(rows) {
    return (rows || []).slice().sort((a, b) => {
      const pa = ACTION_PRIORITY?.[normalizeAction(a.final_action || a.action)] || 99;
      const pb = ACTION_PRIORITY?.[normalizeAction(b.final_action || b.action)] || 99;
      if (pa !== pb) return pa - pb;

      const ta = typeof getTopRankV26630 === "function" ? getTopRankV26630(a) : 9999;
      const tb = typeof getTopRankV26630 === "function" ? getTopRankV26630(b) : 9999;
      if (ta !== tb) return ta - tb;

      const sa = typeof rowScoreV26630 === "function" ? rowScoreV26630(a) : Number(a.score || 0);
      const sb = typeof rowScoreV26630 === "function" ? rowScoreV26630(b) : Number(b.score || 0);
      if (sb !== sa) return sb - sa;

      const la = typeof liquiditySortRank === "function" ? liquiditySortRank(a) : 0;
      const lb = typeof liquiditySortRank === "function" ? liquiditySortRank(b) : 0;
      if (lb !== la) return lb - la;

      return String(a.stock_id || "").localeCompare(String(b.stock_id || ""));
    });
  }

  sortRows = function(rows) {
    return topAwareSortV3065(rows);
  };

  splitRows = function(rows) {
    const patched = (rows || []).map(r => {
      try {
        return typeof v273NormalizeRowScore === "function" ? v273NormalizeRowScore(r) : r;
      } catch (e) {
        return r;
      }
    });

    const groups = oldSplitRows
      ? oldSplitRows(patched)
      : { main: [], test: [], watch: [], block: [] };

    ["main", "test", "watch", "block"].forEach(k => {
      groups[k] = topAwareSortV3065(groups[k] || []);
    });

    return groups;
  };
})();
/* ===== end v306.5 ===== */


/* =========================================================
   v306.8 TOP5 HARD ORDER + WATCH FALLBACK FIX
   修正範圍只限 app.js 分組與排序：
   1. 同一清單內 TOP1~TOP5 永遠在最上方。
   2. WATCH 不再只吃 action=WATCH。
      非 SELL / REDUCE / BUY / TEST / BLOCK 的剩餘候選，全部回到 WATCH。
   3. 不動 renderScanRow、不動產業欄、不動轉折顯示、不動持倉、不動 GitHub Actions。
   ========================================================= */
(function top5HardOrderWatchFallbackFixV3068() {
  function normalizeActionSafeV3068(row) {
    try {
      return normalizeAction(row?.final_action || row?.action || row?.status || row?.decision || "");
    } catch (e) {
      const s = String(row?.final_action || row?.action || row?.status || row?.decision || "").trim().toUpperCase();
      if (["SELL", "REDUCE", "BUY", "TEST", "WATCH", "BLOCK"].includes(s)) return s;
      if (s === "賣出") return "SELL";
      if (s === "減碼") return "REDUCE";
      if (s === "買進") return "BUY";
      if (s === "試單") return "TEST";
      if (s === "觀察") return "WATCH";
      if (s === "禁止") return "BLOCK";
      return s || "WATCH";
    }
  }

  function actionPriorityV3068(row) {
    const a = normalizeActionSafeV3068(row);
    const fallback = { SELL: 1, REDUCE: 2, BUY: 3, TEST: 4, WATCH: 5, BLOCK: 6 };
    try {
      return ACTION_PRIORITY?.[a] || fallback[a] || 5;
    } catch (e) {
      return fallback[a] || 5;
    }
  }

  function hardTopRankV3068(row) {
    row = row || {};
    const fields = [
      row.top_rank,
      row.hard_top_rank,
      row.section_top_rank,
      row.section_opportunity_rank,
      row.opportunity_rank,
      row.section_top_opportunity,
      row.top_opportunity,
      row.execution_flag,
      row.system_note,
      row.note,
      row.reason
    ];

    for (const v of fields) {
      const s = String(v ?? "").trim();
      if (!s || s === "--") continue;

      const m = s.match(/TOP\s*[_-]?\s*([1-5])\b/i);
      if (m) return Number(m[1]);

      if (/^[1-5]$/.test(s)) return Number(s);
    }

    if (String(row.execution_flag || "").trim().toUpperCase() === "TOP") return 6;
    return 9999;
  }

  function scoreV3068(row) {
    row = row || {};
    const fields = [
      row.score,
      row.opportunity_score,
      row.entry_score,
      row.rank_score,
      row.turn_adjusted_score_v26664,
      row.turn_event_score_v26664_1,
      row.turn_event_score_v26664,
      row.first_trigger_score_v26663,
      row.trigger_event_score_v26662,
      row.liquidity_score
    ];
    for (const v of fields) {
      const n = Number(String(v ?? "").replace(/,/g, ""));
      if (Number.isFinite(n)) return n;
    }
    return 0;
  }

  function liqRankV3068(row) {
    try {
      return typeof liquiditySortRank === "function" ? liquiditySortRank(row) : 0;
    } catch (e) {
      return 0;
    }
  }

  function stockIdV3068(row) {
    return String(row?.stock_id || row?.code || row?.symbol || "");
  }

  function top5HardSortV3068(rows) {
    return (rows || []).slice().sort((a, b) => {
      const pa = actionPriorityV3068(a);
      const pb = actionPriorityV3068(b);
      if (pa !== pb) return pa - pb;

      const ta = hardTopRankV3068(a);
      const tb = hardTopRankV3068(b);
      if (ta !== tb) return ta - tb;

      const sb = scoreV3068(b);
      const sa = scoreV3068(a);
      if (sb !== sa) return sb - sa;

      const lb = liqRankV3068(b);
      const la = liqRankV3068(a);
      if (lb !== la) return lb - la;

      const vb = Number(String(b?.volume || 0).replace(/,/g, ""));
      const va = Number(String(a?.volume || 0).replace(/,/g, ""));
      if (Number.isFinite(vb) && Number.isFinite(va) && vb !== va) return vb - va;

      return stockIdV3068(a).localeCompare(stockIdV3068(b));
    });
  }

  function dedupeV3068(arr) {
    try {
      return typeof dedupeByStockV26630 === "function" ? dedupeByStockV26630(arr) : arr;
    } catch (e) {
      return arr;
    }
  }

  sortRows = function(rows) {
    return top5HardSortV3068(rows || []);
  };

  splitRows = function(rows) {
    const sorted = top5HardSortV3068(rows || []);

    const main = [];
    const test = [];
    const block = [];
    const watch = [];

    sorted.forEach(row => {
      const a = normalizeActionSafeV3068(row);

      if (["SELL", "REDUCE", "BUY"].includes(a)) {
        main.push(row);
      } else if (a === "TEST") {
        test.push(row);
      } else if (a === "BLOCK") {
        block.push(row);
      } else {
        // 重要修正：
        // 後端大量候選如果是 HOLD / WAIT / OBSERVE / NEUTRAL / 空值 / 其他標籤，
        // 一律回到 WATCH，不再被前端丟掉。
        watch.push({ ...row, final_action: "WATCH" });
      }
    });

    return {
      main: dedupeV3068(top5HardSortV3068(main)),
      test: dedupeV3068(top5HardSortV3068(test)),
      watch: dedupeV3068(top5HardSortV3068(watch)),
      block: dedupeV3068(top5HardSortV3068(block))
    };
  };

  const oldGetTopBadgeV3068 = typeof getTopBadge === "function" ? getTopBadge : null;
  getTopBadge = function(row) {
    const r = hardTopRankV3068(row);
    if (r >= 1 && r <= 5) return `🔥 TOP${r}`;
    if (oldGetTopBadgeV3068) return oldGetTopBadgeV3068(row);
    return "";
  };

  window.__TOP5_WATCH_FALLBACK_FIX_V3068__ = true;
})();
/* ===== end v306.8 TOP5 HARD ORDER + WATCH FALLBACK FIX ===== */


/* ===== v309.1 SAFE ROLLBACK: frontend sort lock removed to prevent blank screen. ===== */


// ===== v309.1 HARD NO BLANK SCREEN GUARD =====
(function hardNoBlankGuardV3091(){
  function showBootErrorV3091(msg) {
    try {
      if (!document.body) return;
      if (document.body.innerHTML && document.body.innerHTML.trim()) return;
      document.body.innerHTML =
        '<main style="padding:28px;font-family:-apple-system,BlinkMacSystemFont,Noto Sans TC,sans-serif;color:#111827;background:#f5f0e8;min-height:100vh">' +
        '<section style="background:#fff;border-radius:24px;padding:22px;box-shadow:0 8px 24px rgba(15,23,42,.08)">' +
        '<h1 style="font-size:28px;margin:0 0 12px">⚠️ app.js 啟動失敗</h1>' +
        '<p style="font-size:18px;line-height:1.6;margin:0">原因：' + String(msg || window.__APP_BOOT_ERROR__ || "未知錯誤").replace(/[<>&]/g, "") + '</p>' +
        '<p style="font-size:16px;color:#6b7280;line-height:1.6">請確認 app.js 已完整覆蓋，並重新整理 GitHub Pages。</p>' +
        '</section></main>';
    } catch (_) {}
  }
  window.addEventListener("error", function(e){
    window.__APP_BOOT_ERROR__ = e && e.message ? e.message : String(e || "");
    setTimeout(function(){ showBootErrorV3091(window.__APP_BOOT_ERROR__); }, 300);
  });
  window.addEventListener("unhandledrejection", function(e){
    window.__APP_BOOT_ERROR__ = e && e.reason ? (e.reason.message || String(e.reason)) : String(e || "");
    setTimeout(function(){ showBootErrorV3091(window.__APP_BOOT_ERROR__); }, 300);
  });
  document.addEventListener("DOMContentLoaded", function(){
    setTimeout(function(){ showBootErrorV3091(window.__APP_BOOT_ERROR__); }, 1800);
  });
})();


/* =========================================================
   v311.2 UI FINAL ACTION LOCK / 前端最終分類鎖
   - TEST / WATCH / BLOCK 優先讀 v311_locked_action
   - hard_reject / 0金額0權重 / 金融防守 / 低信心，不再被舊 action 塞回 TEST
   - append-only，不刪原本功能，避免空白頁
   ========================================================= */
const APP_UI_LOCK_VERSION_V3112 = "v311.2_ui_final_action_lock";

function toNumberV3112(v, fallback = 0) {
  const n = Number(String(v ?? "").replace(/,/g, "").replace("%", ""));
  return Number.isFinite(n) ? n : fallback;
}

function textV3112(v) {
  return String(v ?? "").trim();
}

function boolFlagV3112(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (["1", "TRUE", "YES", "Y"].includes(s)) return true;
  const n = Number(s);
  return Number.isFinite(n) && n >= 1;
}

function effectiveActionV3112(row) {
  row = row || {};

  const locked = textV3112(
    row.v311_locked_action ||
    row.locked_action_v311 ||
    row.final_locked_action ||
    row.final_action_locked ||
    ""
  ).toUpperCase();

  if (locked) return normalizeAction(locked);

  let action = normalizeAction(row.final_action || row.action || "WATCH");

  const industry = textV3112(row.industry || row.industry_name || row.sector || row.theme);
  const sid = textV3112(row.stock_id || row.code || row.symbol).slice(0, 4);

  const isFinance = /金融|保險|金控|銀行|證券/.test(industry) || /^(28|58)/.test(sid);
  const isDefensive = /航運|觀光|百貨|食品|水泥|塑膠|鋼鐵|紡織|金融|保險/.test(industry);

  const hardReject =
    boolFlagV3112(row.hard_reject_v310) ||
    boolFlagV3112(row.hard_reject_v309) ||
    boolFlagV3112(row.hard_reject_v308);

  const strictTest =
    boolFlagV3112(row.strict_test_ok_v310) ||
    boolFlagV3112(row.strict_test_ok_v309);

  const watchOk =
    boolFlagV3112(row.watch_ok_v310) ||
    boolFlagV3112(row.watch_ok_v309);

  const targetWeight = toNumberV3112(row.target_weight, 0);
  const suggestAmount = toNumberV3112(
    row.suggest_amount ?? row.suggested_amount ?? row.amount ?? row.recommended_amount,
    0
  );

  const chipScore = toNumberV3112(row.chip_score || row.chip_concentration_score, 999);
  const lowConfidence =
    /低信心|極度分散|分散/.test(textV3112(row.chip_display || row.chip_confidence || row.system_note || row.reason)) ||
    chipScore <= 35;

  if (hardReject || isFinance || isDefensive || lowConfidence) {
    if (watchOk && !isFinance && !hardReject) return "WATCH";
    return "BLOCK";
  }

  if (action === "TEST" && !strictTest) {
    if (watchOk) return "WATCH";
    return "BLOCK";
  }

  if (action === "TEST" && targetWeight <= 0 && suggestAmount <= 0) {
    if (watchOk) return "WATCH";
    return "BLOCK";
  }

  return action || "WATCH";
}

function normalizeRowActionV3112(row) {
  const out = { ...(row || {}) };
  const eff = effectiveActionV3112(out);
  out.v311_effective_action = eff;
  out.final_action = eff;
  out.action = eff;
  out.action_label = ACTION_LABEL[eff] || out.action_label || eff;
  return out;
}

function normalizeRowsActionV3112(rows) {
  return (rows || []).map(normalizeRowActionV3112);
}

const __oldGroupCountsV3112 = typeof groupCounts === "function" ? groupCounts : null;
groupCounts = function(rows) {
  const counts = { SELL: 0, REDUCE: 0, BUY: 0, TEST: 0, WATCH: 0, BLOCK: 0 };
  (rows || []).forEach(r => {
    const a = effectiveActionV3112(r);
    if (counts[a] !== undefined) counts[a]++;
  });
  return counts;
};

const __oldRowScoreV3112 = typeof rowScoreV26630 === "function" ? rowScoreV26630 : null;
rowScoreV26630 = function(row) {
  const fields = [
    row?.final_sort_score_v310,
    row?.attack_score_v310,
    row?.final_attack_score_v310,
    row?.final_sort_score_v309,
    row?.attack_score_v309,
    row?.score,
    row?.entry_score,
    row?.rank_score,
    row?.liquidity_score
  ];
  for (const v of fields) {
    const n = Number(String(v ?? "").replace(/,/g, ""));
    if (Number.isFinite(n)) return n;
  }
  return __oldRowScoreV3112 ? __oldRowScoreV3112(row) : 0;
};

sortRows = function(rows) {
  return (rows || []).slice().sort((a, b) => {
    const aa = effectiveActionV3112(a);
    const bb = effectiveActionV3112(b);
    const pa = ACTION_PRIORITY[aa] || 99;
    const pb = ACTION_PRIORITY[bb] || 99;
    if (pa !== pb) return pa - pb;

    const ta = getTopRankV26630(a);
    const tb = getTopRankV26630(b);
    if (ta !== tb) return ta - tb;

    const sb = rowScoreV26630(b);
    const sa = rowScoreV26630(a);
    if (sb !== sa) return sb - sa;

    const la = liquiditySortRank(a);
    const lb = liquiditySortRank(b);
    if (lb !== la) return lb - la;

    const va = Number(a.volume || 0);
    const vb = Number(b.volume || 0);
    if (vb !== va) return vb - va;

    return String(a.stock_id || "").localeCompare(String(b.stock_id || ""));
  });
};

splitRows = function(rows) {
  const normalized = normalizeRowsActionV3112(rows || []);
  const sorted = sortRows(normalized);
  const byAction = (actions) => sorted.filter(r => actions.includes(effectiveActionV3112(r)));
  return {
    main: dedupeByStockV26630(byAction(["SELL", "REDUCE", "BUY"])),
    test: dedupeByStockV26630(byAction(["TEST"])),
    watch: dedupeByStockV26630(byAction(["WATCH"])),
    block: dedupeByStockV26630(byAction(["BLOCK"]))
  };
};

const __baseRenderScanRowV3112 = typeof renderScanRow === "function" ? renderScanRow : null;
renderScanRow = function(row, key) {
  const fixed = normalizeRowActionV3112(row || {});
  return __baseRenderScanRowV3112 ? __baseRenderScanRowV3112(fixed, key) : "";
};

const __oldRenderSectionListV3112 = typeof renderSectionList === "function" ? renderSectionList : null;
renderSectionList = function(targetId, rows, prefix, limit = 80) {
  return __oldRenderSectionListV3112
    ? __oldRenderSectionListV3112(targetId, normalizeRowsActionV3112(rows || []), prefix, limit)
    : undefined;
};

const __oldRenderFinalActionsV3112 = typeof renderFinalActions === "function" ? renderFinalActions : null;
renderFinalActions = function(rows) {
  return __oldRenderFinalActionsV3112
    ? __oldRenderFinalActionsV3112(normalizeRowsActionV3112(rows || []))
    : undefined;
};

const __oldRenderDecisionV3112 = typeof renderDecision === "function" ? renderDecision : null;
renderDecision = function(rows) {
  return __oldRenderDecisionV3112
    ? __oldRenderDecisionV3112(normalizeRowsActionV3112(rows || []))
    : undefined;
};

const __oldRenderStatsV3112 = typeof renderStats === "function" ? renderStats : null;
renderStats = function(rows, summary) {
  return __oldRenderStatsV3112
    ? __oldRenderStatsV3112(normalizeRowsActionV3112(rows || []), summary)
    : undefined;
};

const __oldLoadFinalRowsV3112 = typeof loadFinalRows === "function" ? loadFinalRows : null;
loadFinalRows = async function() {
  const rows = __oldLoadFinalRowsV3112 ? await __oldLoadFinalRowsV3112() : [];
  return normalizeRowsActionV3112(rows || []);
};

window.__APP_UI_LOCK_VERSION_V3112 = APP_UI_LOCK_VERSION_V3112;
console.log("APP UI LOCK ACTIVE:", APP_UI_LOCK_VERSION_V3112);


/* =========================================================
   v326.1 BLANK PAGE HARD BOOT GUARD / 白屏硬啟動防護
   目的：
   - Safari / GitHub Pages 若 DOMContentLoaded 先觸發，仍強制 init
   - 若 init 中途錯誤，至少渲染外殼並顯示錯誤，不再整頁空白
   ========================================================= */
(function blankPageHardBootGuardV3261() {
  try {
    const oldInit = typeof init === "function" ? init : null;

    if (oldInit && !window.__INIT_WRAPPED_V3261__) {
      window.__INIT_WRAPPED_V3261__ = true;
      init = async function() {
        window.__APP_INIT_STARTED_V3261__ = true;
        try {
          return await oldInit();
        } catch (e) {
          try {
            if (typeof renderAppShell === "function" && !document.querySelector(".page")) {
              renderAppShell();
            }
            if (typeof setSyncStatus === "function") {
              setSyncStatus("❌ 前端初始化失敗：" + (e && e.message ? e.message : String(e)), "sync error");
            }
          } catch (_) {}
          console.error("init failed v326.1", e);
          return null;
        }
      };
    }

    function hasRealAppV3261() {
      return !!document.querySelector(".page, #syncStatus, #finalActionList, #ignitionList, #evolutionList");
    }

    function bootV3261() {
      try {
        if (!window.__APP_INIT_STARTED_V3261__ && typeof init === "function") {
          init();
        }
      } catch (e) {
        try {
          if (typeof renderAppShell === "function") renderAppShell();
          if (typeof setSyncStatus === "function") {
            setSyncStatus("❌ 前端啟動失敗：" + (e && e.message ? e.message : String(e)), "sync error");
          }
        } catch (_) {}
      }

      setTimeout(function() {
        try {
          if (!hasRealAppV3261() && typeof renderAppShell === "function") {
            renderAppShell();
            if (typeof setSyncStatus === "function") {
              setSyncStatus("⚠️ 已啟動白屏防護：外殼已恢復，請按重新整理或回報 Actions/Console 錯誤。", "sync error");
            }
          }
        } catch (e) {}
      }, 1200);

      setTimeout(function() {
        try {
          if (!hasRealAppV3261() && document.body) {
            document.body.innerHTML = '<main class="page" style="padding:28px;font-family:-apple-system,BlinkMacSystemFont,Noto Sans TC,sans-serif;color:#111827"><section class="card"><h1>⚠️ 前端載入失敗</h1><p>app.js 已載入，但主畫面沒有成功渲染。</p><p>請回報瀏覽器 Console 錯誤。</p></section></main>';
          }
        } catch (e) {}
      }, 3000);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", bootV3261, { once: true });
    } else {
      setTimeout(bootV3261, 0);
    }
  } catch (e) {
    console.error("blank guard install failed v326.1", e);
  }
})();


/* ===== v330 Local Position Test Mode / 本機持倉測試模式 =====
   - 移除同步持倉按鈕
   - 新增/更新只存在 localStorage
   - 重跑策略只觸發 data_pipeline，不回寫任何持倉 CSV
*/
function applyLocalPositionTestModeV330() {
  try {
    const syncBtn = document.getElementById("syncPositionBtn");
    if (syncBtn) syncBtn.remove();

    const hint = document.querySelector("#positionCard .section-head .hint");
    if (hint) hint.textContent = "輸入後只做本機重新評估，不回寫後端";

    const rerunBtn = document.getElementById("rerunWithPositionBtn");
    if (rerunBtn) rerunBtn.textContent = "重跑策略";
  } catch (e) {}
}

document.addEventListener("DOMContentLoaded", function() {
  applyLocalPositionTestModeV330();
  setTimeout(applyLocalPositionTestModeV330, 300);
  setTimeout(applyLocalPositionTestModeV330, 1200);
});



/* ===== v332 Protect Local Positions / 保護本機持倉 =====
   - 本機持倉 key：daily_dashboard_positions_v1
   - 不回寫 GitHub，不同步後端
   - 重新整理 / 重跑策略後仍保留，可用最新清單重新評估
*/
function isProtectedPositionStorageKeyV332(k) {
  return k === POS_STORAGE_KEY || k === "daily_dashboard_positions_v1" || /position/i.test(String(k || ""));
}

function verifyLocalPositionsStillExistV332() {
  try {
    const rows = loadPositions();
    if (rows && rows.length) {
      refreshPositionStatus("本機持倉已保留，可重新評估");
    }
  } catch (e) {}
}

document.addEventListener("DOMContentLoaded", function() {
  setTimeout(verifyLocalPositionsStillExistV332, 300);
  setTimeout(verifyLocalPositionsStillExistV332, 1200);
});



/* ===== v333.1 Macro / Market Display Safe Compatibility Patch =====
   只修：市場狀態、總經狀態、風險模式顯示。
   不動：持倉、清單、排序、GitHub Actions、時鐘、重跑策略。
   目的：讓前端相容 v333 後端輸出的新欄位：
   market_state_v333 / macro_score_v333 / risk_mode_v333 / market_summary_v333 / market_note_v333
*/
(function v3331MacroMarketDisplaySafePatch() {
  function v3331Text(v, fallback = "--") {
    if (v === undefined || v === null || v === "") return fallback;
    const s = String(v).trim();
    return s ? s : fallback;
  }

  function v3331Pick(obj, keys, fallback = "") {
    obj = obj || {};
    for (const k of keys) {
      if (obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") {
        return obj[k];
      }
    }
    return fallback;
  }

  function v3331MacroText(summary, macro) {
    const label = v3331Pick(summary, [
      "macro_bias_v333",
      "macro_label_v333",
      "macro_label"
    ], v3331Pick(macro, ["macro_bias_v333", "macro_label_v333", "macro_label"], ""));

    const score = v3331Pick(summary, [
      "macro_score_v333",
      "macro_score"
    ], v3331Pick(macro, ["macro_score_v333", "macro_score"], ""));

    const confidence = v3331Pick(summary, [
      "confidence_v333",
      "macro_confidence_label",
      "macro_confidence"
    ], v3331Pick(macro, ["confidence_v333", "macro_confidence_label", "macro_confidence"], ""));

    const scoreText = v3331Text(score, "--");
    const main = `${v3331Text(label, "總經中性")}｜分數 ${scoreText}`;
    return confidence ? `${main}｜${confidence}` : main;
  }

  function v3331MarketText(regime, summary) {
    const market = v3331Pick(summary, [
      "market_state_v333",
      "market_status_v333",
      "market_status",
      "market_label"
    ], v3331Pick(regime, [
      "market_state_v333",
      "market_status_v333",
      "market_status",
      "market_label",
      "label",
      "regime"
    ], ""));

    const riskScore = v3331Pick(summary, [
      "market_risk_score_v333"
    ], v3331Pick(regime, ["market_risk_score_v333"], ""));

    const extra = v3331Pick(summary, [
      "index_change_pct_text"
    ], v3331Pick(regime, ["index_change_pct_text"], ""));

    let out = v3331Text(market, "--");
    if (extra) out += ` ${extra}`;
    if (riskScore !== "" && riskScore !== undefined && riskScore !== null) out += `｜風險分 ${riskScore}`;
    return out;
  }

  function v3331RiskText(summary, regime, macro) {
    const summaryText = v3331Pick(summary, ["market_summary_v333"], "");
    const riskMode = v3331Pick(summary, [
      "risk_mode_v333",
      "risk_mode"
    ], v3331Pick(regime, ["risk_mode_v333", "risk_mode", "action_policy"], ""));

    const note = v3331Pick(summary, [
      "market_note_v333"
    ], v3331Pick(regime, ["market_note_v333"], ""));

    if (summaryText) return summaryText;
    const macroLabel = v3331Pick(summary, ["macro_bias_v333", "macro_label"], v3331Pick(macro, ["macro_bias_v333", "macro_label"], ""));
    const marketLabel = v3331Pick(summary, ["market_state_v333", "market_status"], v3331Pick(regime, ["market_state_v333", "market_status", "market_label", "label"], ""));
    const parts = [macroLabel, marketLabel, riskMode].filter(x => v3331Text(x, "") !== "");
    const base = parts.length ? parts.join("｜") : "--";
    return note ? `${base}｜${note}` : base;
  }

  function v3331ResolveUpdatedAt(regime, summary) {
    try {
      if (typeof resolveBackendUpdatedAtV266573 === "function") {
        return resolveBackendUpdatedAtV266573(regime || {}, summary || {});
      }
    } catch (e) {}
    return v3331Pick(summary, ["workflow_completed_at", "backend_completed_at", "completed_at", "updated_at", "generated_at"],
      v3331Pick(regime, ["workflow_completed_at", "backend_completed_at", "completed_at", "updated_at", "generated_at"], "--"));
  }

  function v3331TradeDate(regime, summary) {
    try {
      if (typeof resolveTradeDateV26630 === "function") return resolveTradeDateV26630(regime || {}, summary || {});
    } catch (e) {}
    return v3331Pick(summary, ["trade_date", "next_trade_date"], v3331Pick(regime, ["trade_date", "next_trade_date"], "--"));
  }

  const oldRenderMeta = typeof renderMeta === "function" ? renderMeta : null;

  renderMeta = function(regime, summary, macro, rows) {
    try {
      regime = regime || {};
      summary = summary || {};
      macro = macro || {};
      rows = Array.isArray(rows) ? rows : [];

      const metaBox = typeof qs === "function" ? qs("metaBox") : document.getElementById("metaBox");
      if (!metaBox) {
        if (oldRenderMeta) return oldRenderMeta(regime, summary, macro, rows);
        return;
      }

      const marketText = v3331MarketText(regime, summary);
      const macroText = v3331MacroText(summary, macro);
      const riskText = v3331RiskText(summary, regime, macro);
      const signalDate = v3331Text(summary.signal_date || summary.latest_date || regime.signal_date || regime.latest_date || regime.date || summary.generated_at, "--");
      const tradeDate = v3331TradeDate(regime, summary);
      const backendUpdatedAt = v3331ResolveUpdatedAt(regime, summary);

      metaBox.innerHTML = `
        <div class="mini"><span>來源版本</span><b>C 完整交易系統</b></div>
        <div class="mini"><span>市場狀態</span><b>${marketText}</b></div>
        <div class="mini"><span>總經狀態</span><b>${macroText}</b></div>
        <div class="mini"><span>風險模式</span><b>${riskText}</b></div>
        <div class="mini"><span>訊號日</span><b>${signalDate}</b></div>
        <div class="mini"><span>交易日</span><b>${tradeDate}</b></div>
        <div class="mini"><span>最後更新</span><b>${backendUpdatedAt}</b></div>
        <div class="mini"><span>操作筆數</span><b>${rows.length}</b></div>
      `;

      try {
        if (typeof setIdleSyncStatusV266574 === "function") {
          setIdleSyncStatusV266574(`✅ 最終操作表已同步｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
        } else if (typeof setSyncStatus === "function") {
          setSyncStatus(`✅ 最終操作表已同步｜現在時間 <span id="liveClock">${formatTWClock(new Date())}</span>`, "sync ok");
        }
        if (typeof startLiveClock === "function") startLiveClock();
      } catch (e) {}
    } catch (e) {
      console.warn("v333.1 renderMeta fallback", e);
      if (oldRenderMeta) return oldRenderMeta(regime || {}, summary || {}, macro || {}, rows || []);
    }
  };

  window.__v3331MacroMarketDisplaySafePatch = true;
})();


/* ===== v333.2 Meta Semantic Layer Guard / 首頁三層語意防覆寫 =====
   防止舊版 macro inline 補丁在 renderMeta 後把「總經狀態 / 風險模式」重新混在一起。
   不影響持倉、清單、排序、Actions。
*/
window.__META_SEMANTIC_LAYER_V3332__ = true;



/* =========================================================
   v266.66 Stage UI Badge Patch / IGNITION・EVOLUTION 顏色與提示
   只改前端顯示：
   - IGNITION：吸籌
   - EVOLUTION：慢推
   不改策略、不改 CSV、不改 Actions、不改持倉。
   ========================================================= */

function stageTypeV26666(row) {
  const s = [
    row?.strategy_type,
    row?.bucket,
    row?.source,
    row?.strategy_name,
    row?.reason,
    row?.system_note
  ].map(v => String(v ?? "")).join(" ").toUpperCase();

  if (s.includes("IGNITION")) return "IGNITION";
  if (s.includes("EVOLUTION")) return "EVOLUTION";
  return "";
}

function stageBadgeTextV26666(stage) {
  if (stage === "IGNITION") return "🟠 吸籌";
  if (stage === "EVOLUTION") return "🟣 慢推";
  return "";
}

function stageMainHintV26666(stage) {
  if (stage === "IGNITION") {
    return "主力開始吸｜觀察承接｜尚未主升";
  }
  if (stage === "EVOLUTION") {
    return "主力控盤慢推｜回檔有人接｜未過熱";
  }
  return "";
}

function stageDetailBlockV26666(row) {
  const stage = stageTypeV26666(row);
  if (stage === "IGNITION") {
    return `
      <div class="stage-hint-box-v26666 ignition">
        <div class="stage-hint-title-v26666">🟠 IGNITION｜主力吸籌階段</div>
        <p>定位：主力開始吸貨、承接開始出現，但還不是主升確認。</p>
        <p>操作：以觀察 / 小量試單為主，不追高、不重倉；等承接、量能、均線再確認。</p>
      </div>
    `;
  }
  if (stage === "EVOLUTION") {
    return `
      <div class="stage-hint-box-v26666 evolution">
        <div class="stage-hint-title-v26666">🟣 EVOLUTION｜主力控盤慢推</div>
        <p>定位：主力已開始控節奏，回檔有人接，均線慢慢打開，尚未全面追價。</p>
        <p>操作：可列為優先觀察 / 試單升級名單；仍需確認未過熱，不把它當追強榜。</p>
      </div>
    `;
  }
  return "";
}

function injectStageStyleV26666() {
  if (document.getElementById("stage-style-v26666")) return;
  const style = document.createElement("style");
  style.id = "stage-style-v26666";
  style.textContent = `
    .scan-item.stage-ignition-v26666 {
      border-color: #f59e0b !important;
      box-shadow: 0 0 0 2px rgba(245,158,11,.14) !important;
      background: linear-gradient(180deg, rgba(255,251,235,.98), rgba(255,255,255,.98)) !important;
    }
    .scan-item.stage-evolution-v26666 {
      border-color: #8b5cf6 !important;
      box-shadow: 0 0 0 2px rgba(139,92,246,.14) !important;
      background: linear-gradient(180deg, rgba(250,245,255,.98), rgba(255,255,255,.98)) !important;
    }
    .scan-main-live.stage-ignition-main-v26666 {
      border-color: rgba(245,158,11,.55) !important;
      background: linear-gradient(90deg, rgba(255,251,235,.98), rgba(255,255,255,.98)) !important;
    }
    .scan-main-live.stage-evolution-main-v26666 {
      border-color: rgba(139,92,246,.58) !important;
      background: linear-gradient(90deg, rgba(250,245,255,.98), rgba(255,255,255,.98)) !important;
    }
    .scan-action.stage-ignition-badge-v26666 {
      background: #fef3c7 !important;
      color: #92400e !important;
      border: 1px solid #f59e0b !important;
      font-weight: 950 !important;
    }
    .scan-action.stage-evolution-badge-v26666 {
      background: #ede9fe !important;
      color: #5b21b6 !important;
      border: 1px solid #8b5cf6 !important;
      font-weight: 950 !important;
    }
    .stage-mini-hint-v26666 {
      display: block;
      margin-top: 5px;
      font-size: 12px;
      line-height: 1.25;
      font-weight: 850;
      opacity: .88;
      white-space: normal;
    }
    .stage-mini-hint-v26666.ignition {
      color: #92400e;
    }
    .stage-mini-hint-v26666.evolution {
      color: #5b21b6;
    }
    .stage-hint-box-v26666 {
      margin: 12px 0 14px;
      padding: 14px 16px;
      border-radius: 18px;
      border: 2px solid #e5e7eb;
      font-weight: 800;
      line-height: 1.55;
    }
    .stage-hint-box-v26666 p {
      margin: 7px 0 0;
      color: #4b5563;
      font-weight: 760;
    }
    .stage-hint-box-v26666.ignition {
      background: #fffbeb;
      border-color: #f59e0b;
      color: #92400e;
    }
    .stage-hint-box-v26666.evolution {
      background: #faf5ff;
      border-color: #8b5cf6;
      color: #5b21b6;
    }
    .stage-hint-title-v26666 {
      font-size: 15px;
      font-weight: 950;
      margin-bottom: 4px;
    }
  `;
  document.head.appendChild(style);
}

const __oldRenderScanRowV26666 = typeof renderScanRow === "function" ? renderScanRow : null;

renderScanRow = function(row, key) {
  injectStageStyleV26666();

  let html = __oldRenderScanRowV26666 ? __oldRenderScanRowV26666(row, key) : "";
  if (!html) return html;

  const stage = stageTypeV26666(row);
  if (!stage) return html;

  const isIgnition = stage === "IGNITION";
  const itemCls = isIgnition ? "stage-ignition-v26666" : "stage-evolution-v26666";
  const mainCls = isIgnition ? "stage-ignition-main-v26666" : "stage-evolution-main-v26666";
  const badgeCls = isIgnition ? "stage-ignition-badge-v26666" : "stage-evolution-badge-v26666";
  const hintCls = isIgnition ? "ignition" : "evolution";
  const badgeText = stageBadgeTextV26666(stage);
  const hintText = stageMainHintV26666(stage);
  const detailBlock = stageDetailBlockV26666(row);

  html = html.replace(
    '<article class="scan-item ',
    `<article class="scan-item ${itemCls} `
  );

  html = html.replace(
    'class="scan-main scan-main-live',
    `class="scan-main scan-main-live ${mainCls}`
  );

  html = html.replace(
    /<div class="scan-action ([^"]*)">[\s\S]*?<\/div>/,
    `<div class="scan-action $1 ${badgeCls}">${badgeText}</div>`
  );

  html = html.replace(
    /<div class="scan-entry">([\s\S]*?)<\/div>/,
    `<div class="scan-entry">$1<span class="stage-mini-hint-v26666 ${hintCls}">${hintText}</span></div>`
  );

  if (detailBlock && html.includes('<div class="detail-grid">')) {
    html = html.replace(
      /(<div class="detail-grid">[\s\S]*?<\/div>\s*)/,
      `$1${detailBlock}`
    );
  }

  return html;
};

try { injectStageStyleV26666(); } catch(e) {}
document.addEventListener("DOMContentLoaded", function() {
  try { injectStageStyleV26666(); } catch(e) {}
});

