const DATA_DIR = "./data";
const GH_CONFIG_KEY = "v3_github_config";
const DEBUG_MODE = true;

document.addEventListener("DOMContentLoaded", async () => {
  try {
    debugLog("DOMContentLoaded");
    bindUI();
    loadSavedConfig();
    await loadAll();
  } catch (err) {
    console.error(err);
    setBanner(`初始化失敗：${err.message}`, "#b42318");
    debugLog(`初始化失敗: ${err.stack || err.message}`);
  }
});

function bindUI() {
  debugLog("bindUI start");

  safeBind("refreshBtn", "click", async () => {
    setBanner("頁面重新同步中…", "#2f7d32");
    await loadAll(true);
  });

  safeBind("updateBtn", "click", async () => {
    const cfg = getGithubConfig();
    if (!cfg) return;
    setBanner("正在送出主流程…", "#92400e");
    await dispatchWorkflow("v1_stable_pipeline.yml", {}, "已送出主流程，等待資料更新…");
  });

  safeBind("saveConfigBtn", "click", saveConfig);
  safeBind("clearConfigBtn", "click", clearConfig);

  safeBind("addPositionBtn", "click", submitAddPosition);
  safeBind("addWatchlistBtn", "click", submitAddWatch);

  document.addEventListener("click", async (e) => {
    const btn = e.target.closest("[data-remove-position]");
    if (btn) {
      const stockId = btn.getAttribute("data-remove-position");
      await submitRemovePosition(stockId);
      return;
    }

    const btn2 = e.target.closest("[data-remove-watch]");
    if (btn2) {
      const stockId = btn2.getAttribute("data-remove-watch");
      await submitRemoveWatch(stockId);
    }
  });

  debugLog("bindUI done");
}

function safeBind(id, eventName, handler) {
  const el = document.getElementById(id);
  if (!el) {
    debugLog(`找不到元素: ${id}`);
    return;
  }

  el.addEventListener(eventName, async (e) => {
    try {
      debugLog(`事件觸發: ${id}.${eventName}`);
      await handler(e);
    } catch (err) {
      console.error(err);
      setBanner(`事件失敗 ${id}: ${err.message}`, "#b42318");
      debugLog(`事件失敗 ${id}: ${err.stack || err.message}`);
    }
  });
}

function loadSavedConfig() {
  try {
    const raw = localStorage.getItem(GH_CONFIG_KEY);
    if (!raw) {
      text("configStatus", "未儲存");
      debugLog("localStorage 無 github config");
      return;
    }
    const cfg = JSON.parse(raw);
    val("ghOwner", cfg.owner || "");
    val("ghRepo", cfg.repo || "");
    val("ghBranch", cfg.branch || "main");
    val("ghToken", cfg.token || "");
    text("configStatus", "✅ 已儲存本機設定");
    debugLog(`已載入本機設定 owner=${cfg.owner}, repo=${cfg.repo}, branch=${cfg.branch}`);
  } catch (err) {
    text("configStatus", "讀取失敗");
    debugLog(`讀取設定失敗: ${err.message}`);
  }
}

function saveConfig() {
  const cfg = {
    owner: val("ghOwner").trim(),
    repo: val("ghRepo").trim(),
    branch: val("ghBranch").trim() || "main",
    token: val("ghToken").trim(),
  };

  debugLog(`準備儲存設定 owner=${cfg.owner}, repo=${cfg.repo}, branch=${cfg.branch}, token_length=${cfg.token.length}`);

  if (!cfg.owner || !cfg.repo || !cfg.branch || !cfg.token) {
    setBanner("GitHub 設定不可空白", "#b42318");
    text("configStatus", "欄位不完整");
    debugLog("儲存設定失敗：欄位不完整");
    return;
  }

  localStorage.setItem(GH_CONFIG_KEY, JSON.stringify(cfg));
  text("configStatus", "✅ 已儲存本機設定");
  setBanner("GitHub 本機設定已儲存", "#2f7d32");
  debugLog("儲存設定成功");
}

function clearConfig() {
  localStorage.removeItem(GH_CONFIG_KEY);
  ["ghOwner", "ghRepo", "ghBranch", "ghToken"].forEach(id => val(id, ""));
  text("configStatus", "已清除");
  setBanner("GitHub 本機設定已清除", "#92400e");
  debugLog("已清除設定");
}

function getGithubConfig() {
  const raw = localStorage.getItem(GH_CONFIG_KEY);
  if (!raw) {
    setBanner("請先在 GitHub 本機設定區儲存 owner / repo / branch / token", "#b42318");
    text("configStatus", "未儲存");
    debugLog("送出失敗：本機設定不存在");
    return null;
  }
  try {
    const cfg = JSON.parse(raw);
    debugLog(`讀取設定成功 owner=${cfg.owner}, repo=${cfg.repo}, branch=${cfg.branch}, token_length=${(cfg.token || "").length}`);
    return cfg;
  } catch (err) {
    setBanner("GitHub 本機設定格式錯誤，請重新儲存", "#b42318");
    text("configStatus", "格式錯誤");
    debugLog(`讀取設定失敗: ${err.message}`);
    return null;
  }
}

async function dispatchWorkflow(workflowId, inputs = {}, successMessage = "已送出") {
  const cfg = getGithubConfig();
  if (!cfg) return false;

  const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${workflowId}/dispatches`;
  debugLog(`dispatchWorkflow -> ${workflowId}`);
  debugLog(`POST ${url}`);
  debugLog(`inputs=${JSON.stringify(inputs)}`);

  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${cfg.token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        ref: cfg.branch,
        inputs
      })
    });
  } catch (err) {
    setBanner(`GitHub 連線失敗：${err.message}`, "#b42318");
    debugLog(`fetch 失敗: ${err.stack || err.message}`);
    return false;
  }

  debugLog(`response status=${res.status}`);

  if (!res.ok) {
    const txt = await res.text();
    setBanner(`送出 workflow 失敗：${res.status}`, "#b42318");
    debugLog(`response body=${txt}`);
    return false;
  }

  setBanner(successMessage, "#2f7d32");
  debugLog(`workflow 送出成功: ${workflowId}`);
  return true;
}

async function submitAddPosition() {
  const stockId = val("positionStockInput").trim();
  const shares = val("positionSharesInput").trim();
  const avgCost = val("positionCostInput").trim();

  debugLog(`submitAddPosition stock=${stockId}, shares=${shares}, avg_cost=${avgCost}`);

  if (!stockId || !shares || !avgCost) {
    setBanner("加入持倉前請填完整：股票代號 / 股數 / 成本", "#b42318");
    debugLog("submitAddPosition 失敗：欄位不完整");
    return;
  }

  const ok = await dispatchWorkflow("v3_position_writeback.yml", {
    action: "add",
    stock_id: stockId,
    shares: shares,
    avg_cost: avgCost
  }, `已送出持倉新增：${stockId}`);

  if (ok) {
    val("positionStockInput", "");
    val("positionSharesInput", "");
    val("positionCostInput", "");
  }
}

async function submitRemovePosition(stockId) {
  debugLog(`submitRemovePosition stock=${stockId}`);
  if (!confirm(`確定要移除持倉 ${stockId} 嗎？`)) {
    debugLog("submitRemovePosition 已取消");
    return;
  }

  await dispatchWorkflow("v3_position_writeback.yml", {
    action: "remove",
    stock_id: stockId,
    shares: "",
    avg_cost: ""
  }, `已送出持倉移除：${stockId}`);
}

async function submitAddWatch() {
  const stockId = val("watchlistStockInput").trim();
  debugLog(`submitAddWatch stock=${stockId}`);

  if (!stockId) {
    setBanner("加入自選股前請先輸入股票代號", "#b42318");
    debugLog("submitAddWatch 失敗：stock_id 空白");
    return;
  }

  const ok = await dispatchWorkflow("v3_watchlist_writeback.yml", {
    action: "add",
    stock_id: stockId
  }, `已送出自選股新增：${stockId}`);

  if (ok) {
    val("watchlistStockInput", "");
  }
}

async function submitRemoveWatch(stockId) {
  debugLog(`submitRemoveWatch stock=${stockId}`);
  if (!confirm(`確定要移除自選股 ${stockId} 嗎？`)) {
    debugLog("submitRemoveWatch 已取消");
    return;
  }

  await dispatchWorkflow("v3_watchlist_writeback.yml", {
    action: "remove",
    stock_id: stockId
  }, `已送出自選股移除：${stockId}`);
}

async function loadAll(force = false) {
  try {
    debugLog(`loadAll force=${force}`);
    const [meta, tradePlan, position, watchlist, summary, debug] = await Promise.all([
      fetchJSON(`${DATA_DIR}/meta.json`, force),
      fetchCSV(`${DATA_DIR}/trade_plan.csv`, force),
      fetchCSV(`${DATA_DIR}/position_monitor.csv`, force),
      fetchCSV(`${DATA_DIR}/watchlist_monitor.csv`, force),
      fetchCSV(`${DATA_DIR}/full_summary.csv`, force),
      fetchCSV(`${DATA_DIR}/selection_debug.csv`, force),
    ]);

    debugLog(`loadAll success meta=${!!meta}, tradePlan=${tradePlan.length}, position=${position.length}, watchlist=${watchlist.length}`);

    renderMeta(meta);
    renderTradePlan(tradePlan);
    renderPosition(position);
    renderWatchlist(watchlist);
    renderSummary(summary);
    renderDebug(debug);
    renderTierSummary(tradePlan, position, watchlist);
    renderActionSummary(tradePlan, position);

    if (!document.getElementById("syncBanner").textContent.includes("已送出")) {
      setBanner("頁面資料已同步", "#2f7d32");
    }
  } catch (err) {
    console.error(err);
    setBanner(`讀取失敗：${err.message}`, "#b42318");
    debugLog(`loadAll 失敗: ${err.stack || err.message}`);
  }
}

async function fetchJSON(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  debugLog(`fetchJSON ${finalUrl}`);
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) throw new Error(`JSON 讀取失敗：${url}`);
  return await res.json();
}

async function fetchCSV(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  debugLog(`fetchCSV ${finalUrl}`);
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) throw new Error(`CSV 讀取失敗：${url}`);
  const text = await res.text();
  return parseCSV(text);
}

function parseCSV(text) {
  const cleaned = text.replace(/^\uFEFF/, "").trim();
  if (!cleaned) return [];
  const lines = cleaned.split(/\r?\n/);
  const headers = splitCSVLine(lines[0]).map(h => h.trim());
  return lines.slice(1).filter(Boolean).map(line => {
    const values = splitCSVLine(line);
    const row = {};
    headers.forEach((h, i) => row[h] = (values[i] ?? "").trim());
    return row;
  });
}

function splitCSVLine(line) {
  const result = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
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

function renderMeta(meta) {
  text("nowTime", meta.now_time || meta.generated_at || "--");
  text("generatedAt", meta.generated_at || "--");
  text("signalDate", meta.signal_date || "--");
  text("tradeDate", meta.trade_date || "--");
  text("panelDate", meta.price_panel_latest_date || "--");
  text("dataState", prettyDataState(meta.data_state));
  text("sourceName", meta.source || "--");
  text("writebackState", prettyWriteback(meta.position_writeback_state));
}

function renderTradePlan(rows) {
  const body = document.getElementById("tradePlanBody");
  if (!body) return;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty">目前沒有資料</td></tr>`;
    return;
  }
  body.innerHTML = rows.map(r => `
    <tr>
      <td>${badgeForAction(r.action)}</td>
      <td>${safe(r.stock_id)}</td>
      <td>${prettyTier(r.price_tier)}</td>
      <td>${safe(r.ref_price)}</td>
      <td>${safe(r.target_weight)}</td>
      <td>${safeMoney(r.suggested_amount)}</td>
      <td>${safe(r.note)}</td>
    </tr>
  `).join("");
}

function renderPosition(rows) {
  const body = document.getElementById("positionBody");
  if (!body) return;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="10" class="empty">目前沒有持倉資料</td></tr>`;
    return;
  }
  body.innerHTML = rows.map(r => `
    <tr>
      <td>${safe(r.stock_id)}</td>
      <td>${prettyTier(r.price_tier)}</td>
      <td>${safe(r.ref_price)}</td>
      <td>${safeInt(r.shares)}</td>
      <td>${safe(r.avg_cost)}</td>
      <td>${safePct(r.pnl_pct)}</td>
      <td>${safe(r.target_weight)}</td>
      <td>${badgeForAction(r.action)}</td>
      <td>${safe(r.note)}</td>
      <td><button class="btn-remove" data-remove-position="${safe(r.stock_id)}">移除</button></td>
    </tr>
  `).join("");
}

function renderWatchlist(rows) {
  const body = document.getElementById("watchlistBody");
  if (!body) return;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="8" class="empty">目前沒有自選股資料</td></tr>`;
    return;
  }
  body.innerHTML = rows.map(r => `
    <tr>
      <td>${safe(r.stock_id)}</td>
      <td>${prettyTier(r.price_tier)}</td>
      <td>${safe(r.ref_price)}</td>
      <td>${safe(r.holding_status)}</td>
      <td>${safe(r.strategy_bucket)}</td>
      <td>${badgeForWatchAction(r.action)}</td>
      <td>${safePct(r.pnl_pct)}</td>
      <td><button class="btn-remove" data-remove-watch="${safe(r.stock_id)}">移除</button></td>
    </tr>
  `).join("");
}

function renderSummary(rows) {
  const row = rows[0] || {};
  text("returnVal", pctDisplay(row["return"]));
  text("mddVal", pctDisplay(row["mdd"]));
  text("sharpeVal", blankDash(row["sharpe_daily"]));
}

function renderDebug(rows) {
  const row = rows[0] || {};
  text("dbgTotal", blankDash(row.total_input));
  text("dbgValid", blankDash(row.valid_after_na));
  text("dbgCorePrimary", blankDash(row.core_primary_count));
  text("dbgAlphaPrimary", blankDash(row.alpha_primary_count));
  text("dbgCoreFinal", blankDash(row.core_final_count));
  text("dbgAlphaFinal", blankDash(row.alpha_final_count));
}

function renderTierSummary(tradeRows, positionRows, watchRows) {
  const all = [...tradeRows, ...positionRows, ...watchRows];
  const tiers = {};
  all.forEach(r => {
    const key = prettyTier(r.price_tier || "unknown");
    tiers[key] = (tiers[key] || 0) + 1;
  });

  const container = document.getElementById("tierSummary");
  if (!container) return;
  const entries = Object.entries(tiers);
  if (!entries.length) {
    container.innerHTML = `<div class="tier-box"><div class="tier-label">分層狀態</div><div class="tier-value">--</div><div class="tier-sub">目前沒有可展示的價格分層</div></div>`;
    return;
  }

  container.innerHTML = entries.map(([k, v]) => `
    <div class="tier-box">
      <div class="tier-label">${k}</div>
      <div class="tier-value">${v}</div>
      <div class="tier-sub">此分層目前有 ${v} 檔資料</div>
    </div>
  `).join("");
}

function renderActionSummary(tradeRows, positionRows) {
  const buys = tradeRows.filter(r => (r.action || "").toUpperCase() === "BUY");
  const positionActions = positionRows.filter(r => ["ADD", "REDUCE", "SELL", "STOP_LOSS"].includes((r.action || "").toUpperCase()));

  let headline = "觀察";
  let desc = "今天沒有新的買進動作";
  if (buys.length > 0) {
    headline = "偏多";
    desc = `今天有 ${buys.length} 檔新進場候選`;
  } else if (positionActions.length > 0) {
    headline = "調整";
    desc = `今天有 ${positionActions.length} 筆持倉調整`;
  }

  const totalBuyAmount = buys.reduce((acc, r) => acc + toNum(r.suggested_amount), 0);
  const stopLossCount = positionRows.filter(r => (r.action || "").toUpperCase() === "STOP_LOSS").length;

  text("headlineAction", headline);
  text("headlineDesc", desc);
  text("buyCount", String(buys.length));
  text("buyAmount", `建議金額：${moneyDisplay(totalBuyAmount)}`);
  text("positionActionCount", String(positionActions.length));
  text("positionActionDesc", positionActions.length ? "請優先查看持倉監控區" : "目前無加減碼");
  text("riskLevel", stopLossCount > 0 ? "偏高" : buys.length > 8 ? "中等" : "正常");
  text("riskDesc", stopLossCount > 0 ? `有 ${stopLossCount} 筆停損訊號` : "目前沒有明顯停損警示");
}

function badgeForAction(actionRaw) {
  const action = (actionRaw || "").toUpperCase();
  const map = {
    BUY: ["badge-buy", "買進"],
    HOLD: ["badge-hold", "持有"],
    SELL: ["badge-sell", "賣出"],
    ADD: ["badge-add", "加碼"],
    REDUCE: ["badge-reduce", "減碼"],
    STOP_LOSS: ["badge-stop", "停損"],
  };
  const pair = map[action] || ["badge-hold", safe(actionRaw)];
  return `<span class="badge ${pair[0]}">${pair[1]}</span>`;
}

function badgeForWatchAction(actionRaw) {
  const action = (actionRaw || "").toUpperCase();
  const map = {
    WATCH: ["badge-watch", "觀察"],
    HOLD_MONITOR: ["badge-hold", "持有監控"],
    BUY_READY: ["badge-buy", "可留意"],
    CANDIDATE: ["badge-add", "候選"],
  };
  const pair = map[action] || ["badge-watch", safe(actionRaw)];
  return `<span class="badge ${pair[0]}">${pair[1]}</span>`;
}

function prettyTier(tier) {
  const map = {
    lt_50: "50以下",
    p50_100: "50-100",
    p100_300: "100-300",
    p300_500: "300-500",
    p500_1000: "500-1000",
    gt_1000: "1000以上",
    unknown: "未分類",
  };
  return map[tier] || tier || "未分類";
}

function prettyDataState(state) {
  const map = {
    fresh: "✅ 最新資料",
    ok: "✅ 正常",
    stale: "⚠️ 舊資料",
    loading: "⌛ 讀取中",
    idle: "待命",
  };
  return map[state] || state || "--";
}

function prettyWriteback(state) {
  const map = {
    idle: "待命",
    submitted: "已送出",
    syncing: "同步中",
    success: "已完成",
    failed: "失敗",
  };
  return map[state] || state || "--";
}

function pctDisplay(v) {
  const n = toNum(v);
  if (Number.isNaN(n)) return "--";
  return `${(n * 100).toFixed(2)}%`;
}

function safePct(v) {
  const n = toNum(v);
  if (Number.isNaN(n)) return "--";
  return `${(n * 100).toFixed(2)}%`;
}

function moneyDisplay(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
}

function safeMoney(v) {
  const n = Number(String(v || "").replace(/,/g, ""));
  if (!Number.isFinite(n)) return "--";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
}

function safeInt(v) {
  const n = Number(String(v || "").replace(/,/g, ""));
  if (!Number.isFinite(n)) return "--";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
}

function blankDash(v) {
  return (v === undefined || v === null || v === "") ? "--" : String(v);
}

function safe(v) {
  return blankDash(v);
}

function toNum(v) {
  const n = Number(String(v ?? "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function text(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function val(id, setValue) {
  const el = document.getElementById(id);
  if (!el) return "";
  if (typeof setValue !== "undefined") {
    el.value = setValue;
  }
  return el.value || "";
}

function setBanner(text, color = "#2f7d32") {
  const el = document.getElementById("syncBanner");
  if (el) {
    el.textContent = text;
    el.style.color = color;
  }
  debugLog(`BANNER: ${text}`);
}

function debugLog(msg) {
  if (!DEBUG_MODE) return;
  console.log("[DEBUG]", msg);
}
