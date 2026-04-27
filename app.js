const DATA_DIR = "./data";
const GH_CONFIG_KEY = "v3_github_config";

let POSITION_CACHE = [];
let TRADE_CACHE = [];
let CLOCK_TIMER = null;

injectActionSplitStyle();

document.addEventListener("DOMContentLoaded", async () => {
  bindUI();
  loadSavedConfig();
  startLocalClock();
  await loadAll();
});

function bindUI() {
  bind("refreshBtn", "click", async () => {
    setBanner("頁面重新同步中…", "#2f7d32");
    await loadAll(true);
  });

  bind("updateBtn", "click", async () => {
    await dispatchWorkflow(
      "v3_1_auto_update.yml",
      {},
      "已送出更新資料與策略，請等待 Actions 跑完後重新整理。"
    );
  });

  bind("saveConfigBtn", "click", saveConfig);
  bind("clearConfigBtn", "click", clearConfig);
  bind("addPositionBtn", "click", submitAddPositionInstant);

  document.addEventListener("click", async (e) => {
    const b = e.target.closest("[data-remove-position]");
    if (b) await submitRemovePositionInstant(b.getAttribute("data-remove-position"));
  });
}

function bind(id, event, handler) {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener(event, async (e) => {
    try {
      await handler(e);
    } catch (err) {
      console.error(err);
      setBanner(`操作失敗：${err.message}`, "#b42318");
    }
  });
}


function injectActionSplitStyle() {
  if (document.getElementById("actionSplitStyle")) return;

  const style = document.createElement("style");
  style.id = "actionSplitStyle";
  style.textContent = `
    .action-split {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 4px;
      line-height: 1.15;
      min-width: 86px;
    }

    .action-split .badge {
      white-space: nowrap;
      font-weight: 800;
      padding: 8px 12px;
      border-radius: 999px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }

    .action-sub {
      font-size: 12px;
      color: #6b7280;
      font-weight: 700;
      white-space: nowrap;
      padding-left: 2px;
    }

    .note-main {
      font-weight: 700;
      line-height: 1.35;
      white-space: normal;
      min-width: 220px;
    }

    .badge-watch {
      background: #ede9fe;
      color: #6d28d9;
    }

    .badge-add {
      background: #dbeafe;
      color: #1d4ed8;
    }

    .badge-buy {
      background: #dcfce7;
      color: #166534;
    }

    .badge-hold {
      background: #e5e7eb;
      color: #374151;
    }

    .badge-sell, .badge-stop {
      background: #fee2e2;
      color: #991b1b;
    }

    .badge-reduce {
      background: #fef3c7;
      color: #92400e;
    }
  `;
  document.head.appendChild(style);
}


/* =========================
   GitHub Local Config
========================= */

function loadSavedConfig() {
  try {
    const raw = localStorage.getItem(GH_CONFIG_KEY);
    if (!raw) {
      text("configStatus", "未儲存");
      val("ghOwner", "bichcs5566-alt");
      val("ghBranch", "main");
      return;
    }

    const cfg = JSON.parse(raw);
    val("ghOwner", cfg.owner || "bichcs5566-alt");
    val("ghRepo", cfg.repo || "");
    val("ghBranch", cfg.branch || "main");
    val("ghToken", cfg.token || "");
    text("configStatus", "✅ 已儲存本機設定");
  } catch (err) {
    console.error(err);
    text("configStatus", "讀取失敗");
  }
}

function saveConfig() {
  const cfg = {
    owner: val("ghOwner").trim(),
    repo: val("ghRepo").trim(),
    branch: val("ghBranch").trim() || "main",
    token: val("ghToken").trim(),
  };

  if (!cfg.owner || !cfg.repo || !cfg.branch || !cfg.token) {
    setBanner("GitHub 設定不可空白", "#b42318");
    text("configStatus", "欄位不完整");
    return;
  }

  localStorage.setItem(GH_CONFIG_KEY, JSON.stringify(cfg));
  text("configStatus", "✅ 已儲存本機設定");
  setBanner("GitHub 本機設定已儲存", "#2f7d32");
}

function clearConfig() {
  localStorage.removeItem(GH_CONFIG_KEY);
  ["ghOwner", "ghRepo", "ghBranch", "ghToken"].forEach((id) => val(id, ""));
  text("configStatus", "已清除");
  setBanner("GitHub 本機設定已清除", "#92400e");
}

function getGithubConfig() {
  const raw = localStorage.getItem(GH_CONFIG_KEY);
  if (!raw) {
    setBanner("請先在 GitHub 本機設定區儲存 owner / repo / branch / token", "#b42318");
    text("configStatus", "未儲存");
    return null;
  }

  try {
    const cfg = JSON.parse(raw);
    if (!cfg.owner || !cfg.repo || !cfg.branch || !cfg.token) {
      setBanner("GitHub 本機設定不完整，請重新儲存", "#b42318");
      text("configStatus", "欄位不完整");
      return null;
    }
    return cfg;
  } catch (err) {
    console.error(err);
    setBanner("GitHub 本機設定格式錯誤，請重新儲存", "#b42318");
    text("configStatus", "格式錯誤");
    return null;
  }
}

async function dispatchWorkflow(workflowId, inputs = {}, successMessage = "已送出") {
  const cfg = getGithubConfig();
  if (!cfg) return false;

  const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${workflowId}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${cfg.token}`,
      "Content-Type": "application/json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify({ ref: cfg.branch, inputs }),
  });

  if (!res.ok) {
    const txt = await res.text();
    console.error(txt);
    setBanner(`同步送出失敗：${res.status}，畫面已回復`, "#b42318");
    return false;
  }

  setBanner(successMessage, "#2f7d32");
  return true;
}

/* =========================
   Position Writeback
========================= */

async function submitAddPositionInstant() {
  const stockId = val("positionStockInput").trim();
  const shares = val("positionSharesInput").trim();
  const avgCost = val("positionCostInput").trim();

  if (!stockId || !shares || !avgCost) {
    setBanner("加入持倉前請填完整：股票代號 / 股數 / 成本", "#b42318");
    return;
  }

  if (!Number.isFinite(toNum(shares)) || toNum(shares) <= 0) {
    setBanner("股數必須是大於 0 的數字", "#b42318");
    return;
  }

  if (!Number.isFinite(toNum(avgCost)) || toNum(avgCost) <= 0) {
    setBanner("成本必須是大於 0 的數字", "#b42318");
    return;
  }

  const oldCache = deepClone(POSITION_CACHE);
  const row = {
    stock_id: stockId,
    price_tier: priceTierFromPrice(avgCost),
    ref_price: "同步中",
    shares,
    avg_cost: avgCost,
    pnl_pct: "",
    target_weight: "",
    action: "SYNCING",
    note: "正在寫回 GitHub...",
  };

  POSITION_CACHE = upsertByStockId(POSITION_CACHE, row);
  renderAllFromCache();
  setBanner(`已先加入畫面：${stockId}，背景同步中...`, "#2f7d32");

  val("positionStockInput", "");
  val("positionSharesInput", "");
  val("positionCostInput", "");

  const ok = await dispatchWorkflow(
    "v3_position_writeback.yml",
    {
      action: "add",
      stock_id: stockId,
      shares,
      avg_cost: avgCost,
    },
    `✅ 已送出持倉新增 / 更新：${stockId}。Actions 跑完後重新整理即可確認正式資料。`
  );

  if (!ok) {
    POSITION_CACHE = oldCache;
    renderAllFromCache();
  }
}

async function submitRemovePositionInstant(stockId) {
  stockId = String(stockId || "").trim();

  if (!stockId) {
    setBanner("找不到要移除的股票代號", "#b42318");
    return;
  }

  if (!confirm(`確定要移除持倉 ${stockId} 嗎？`)) return;

  const oldCache = deepClone(POSITION_CACHE);
  POSITION_CACHE = POSITION_CACHE.filter((r) => String(r.stock_id).trim() !== stockId);
  renderAllFromCache();
  setBanner(`已先從畫面移除：${stockId}，背景同步中...`, "#2f7d32");

  const ok = await dispatchWorkflow(
    "v3_position_writeback.yml",
    {
      action: "remove",
      stock_id: stockId,
      shares: "",
      avg_cost: "",
    },
    `✅ 已送出持倉移除：${stockId}。Actions 跑完後重新整理即可確認正式資料。`
  );

  if (!ok) {
    POSITION_CACHE = oldCache;
    renderAllFromCache();
  }
}

/* =========================
   Load Data
========================= */

async function loadAll(force = false) {
  try {
    setLoadingState();

    const [metaRes, tradePlanRes, positionRes, summaryRes, debugRes] = await Promise.allSettled([
      fetchJSON(`${DATA_DIR}/meta.json`, force),
      fetchCSV(`${DATA_DIR}/trade_plan.csv`, force),
      fetchCSV(`${DATA_DIR}/position_monitor.csv`, force),
      fetchCSV(`${DATA_DIR}/full_summary.csv`, force),
      fetchCSV(`${DATA_DIR}/selection_debug.csv`, force),
    ]);

    const meta = valueOrDefault(metaRes, {});
    const tradePlan = valueOrDefault(tradePlanRes, []);
    const position = valueOrDefault(positionRes, []);
    const summary = valueOrDefault(summaryRes, []);
    const debug = valueOrDefault(debugRes, []);

    TRADE_CACHE = tradePlan;
    POSITION_CACHE = position;

    renderMeta(meta);
    renderTradePlan(TRADE_CACHE);
    renderPosition(POSITION_CACHE);
    renderSummary(summary);
    renderDebug(debug);
    renderTierSummary(POSITION_CACHE);
    renderActionSummary(TRADE_CACHE, POSITION_CACHE);

    const failed = [metaRes, tradePlanRes, positionRes, summaryRes, debugRes].filter((r) => r.status === "rejected");
    if (failed.length > 0) {
      console.warn("部分資料讀取失敗：", failed);
      setBanner(`頁面已載入，但有 ${failed.length} 個資料檔缺失或讀取失敗`, "#92400e");
    } else {
      setBanner("頁面資料已同步", "#2f7d32");
    }
  } catch (err) {
    console.error(err);
    setBanner(`讀取失敗：${err.message}`, "#b42318");
  }
}

function valueOrDefault(result, fallback) {
  if (result && result.status === "fulfilled") return result.value;
  return fallback;
}

function setLoadingState() {
  text("dataState", "⌛ 讀取中");
}

async function fetchJSON(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) throw new Error(`JSON 讀取失敗：${url} (${res.status})`);
  return await res.json();
}

async function fetchCSV(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) throw new Error(`CSV 讀取失敗：${url} (${res.status})`);
  return parseCSV(await res.text());
}

function parseCSV(textRaw) {
  const cleaned = String(textRaw || "").replace(/^\uFEFF/, "").trim();
  if (!cleaned) return [];

  const lines = cleaned.split(/\r?\n/);
  const headers = splitCSVLine(lines[0]).map((h) => h.trim());

  return lines.slice(1).filter(Boolean).map((line) => {
    const values = splitCSVLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = (values[i] ?? "").trim();
    });
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

/* =========================
   Render
========================= */

function renderAllFromCache() {
  renderPosition(POSITION_CACHE);
  renderTierSummary(POSITION_CACHE);
  renderActionSummary(TRADE_CACHE, POSITION_CACHE);
}

function renderMeta(meta) {
  text("generatedAt", meta.generated_at || "--");
  text("signalDate", meta.signal_date || "--");
  text("tradeDate", meta.trade_date || "--");
  text("panelDate", meta.price_panel_latest_date || "--");
  text("dataState", prettyDataState(meta.data_state));
  text("sourceName", meta.source || "--");
  text("writebackState", prettyWriteback(meta.position_writeback_state));

  // nowTime 用本機時鐘即時跳動；若 meta 有時間，仍由 startLocalClock 接手更新。
  if (!CLOCK_TIMER) text("nowTime", meta.now_time || meta.generated_at || "--");
}

function renderTradePlan(rows) {
  const body = document.getElementById("tradePlanBody");
  if (!body) return;

  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty">目前沒有今日操作資料</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((r) => `
    <tr>
      <td>${renderActionCell(r)}</td>
      <td>${safe(r.stock_id)}</td>
      <td>${prettyTier(r.price_tier)}</td>
      <td>${safe(r.ref_price)}</td>
      <td>${safeWeight(r.target_weight)}</td>
      <td>${safeMoney(r.suggested_amount)}</td>
      <td>${renderNoteCell(r)}</td>
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

  body.innerHTML = rows.map((r) => `
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
      <td><button class="btn-remove" data-remove-position="${escapeAttr(r.stock_id)}">移除</button></td>
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

function renderTierSummary(positionRows) {
  const container = document.getElementById("tierSummary");
  if (!container) return;

  const tiers = {};
  positionRows.forEach((r) => {
    const key = prettyTier(r.price_tier || priceTierFromPrice(r.avg_cost) || "unknown");
    tiers[key] = (tiers[key] || 0) + 1;
  });

  const entries = Object.entries(tiers);
  if (!entries.length) {
    container.innerHTML = `
      <div class="tier-box">
        <div class="tier-label">分層狀態</div>
        <div class="tier-value">--</div>
        <div class="tier-sub">目前沒有持倉可展示</div>
      </div>`;
    return;
  }

  container.innerHTML = entries.map(([k, v]) => `
    <div class="tier-box">
      <div class="tier-label">${safe(k)}</div>
      <div class="tier-value">${v}</div>
      <div class="tier-sub">此分層目前有 ${v} 檔持倉</div>
    </div>
  `).join("");
}

function renderActionSummary(tradeRows, positionRows) {
  const buys = tradeRows.filter((r) => actionKey(r) === "BUY");
  const tests = tradeRows.filter((r) => ["TEST", "BUY_SMALL", "WATCH_A"].includes(actionKey(r)));
  const ready = tradeRows.filter((r) => ["READY", "WATCH_B", "WATCH"].includes(actionKey(r)));

  const positionActions = positionRows.filter((r) =>
    ["ADD", "REDUCE", "SELL", "STOP_LOSS", "SYNCING"].includes(actionKey(r))
  );

  let headline = "觀察";
  let desc = "目前沒有直接買進動作";

  if (buys.length > 0) {
    headline = "可進場";
    desc = `今天有 ${buys.length} 檔買進訊號`;
  } else if (tests.length > 0) {
    headline = "試單";
    desc = `今天有 ${tests.length} 檔接近突破`;
  } else if (ready.length > 0) {
    headline = "準備";
    desc = `今天有 ${ready.length} 檔觀察名單`;
  } else if (positionActions.length > 0) {
    headline = "調整";
    desc = `今天有 ${positionActions.length} 筆持倉調整或同步`;
  }

  const totalBuyAmount = tradeRows.reduce((acc, r) => acc + toNumOrZero(r.suggested_amount), 0);
  const stopLossCount = positionRows.filter((r) => actionKey(r) === "STOP_LOSS").length;

  text("headlineAction", headline);
  text("headlineDesc", desc);
  text("buyCount", String(buys.length));
  text("buyAmount", `建議金額：${moneyDisplay(totalBuyAmount)}`);

  text("positionActionCount", String(positionActions.length));
  text("positionActionDesc", positionActions.length ? "請優先查看持倉監控區" : "目前無加減碼");

  text("riskLevel", stopLossCount > 0 ? "偏高" : buys.length > 5 ? "中等" : "正常");
  text("riskDesc", stopLossCount > 0 ? `有 ${stopLossCount} 筆停損訊號` : "目前沒有明顯停損警示");
}

/* =========================
   Display Helpers
========================= */

function actionKey(rowOrAction) {
  if (typeof rowOrAction === "string") return rowOrAction.toUpperCase().trim();
  const r = rowOrAction || {};
  return String(r.action || r.raw_action || "").toUpperCase().trim();
}

function actionMeta(row) {
  const action = actionKey(row);
  const score = safeScore(row.entry_score);
  const subFromRow = row.action_sub || "";

  const map = {
    BUY: {
      cls: "badge-buy",
      label: row.action_label || "🟢 買進",
      sub: subFromRow || "突破確認",
      note: "直接買"
    },
    TEST: {
      cls: "badge-add",
      label: row.action_label || "🔵 試單",
      sub: subFromRow || "接近突破",
      note: "先小倉"
    },
    BUY_SMALL: {
      cls: "badge-add",
      label: row.action_label || "🔵 試單",
      sub: subFromRow || "接近突破",
      note: "先小倉"
    },
    WATCH_A: {
      cls: "badge-watch",
      label: row.action_label || "🔵 試單",
      sub: subFromRow || "接近突破",
      note: "可小倉"
    },
    READY: {
      cls: "badge-hold",
      label: row.action_label || "🟡 準備",
      sub: subFromRow || "等待突破",
      note: "盯盤"
    },
    WATCH_B: {
      cls: "badge-hold",
      label: row.action_label || "🟡 準備",
      sub: subFromRow || "等待方向",
      note: "不追高"
    },
    WATCH: {
      cls: "badge-watch",
      label: row.action_label || "🟣 觀察",
      sub: subFromRow || "等待確認",
      note: "觀察"
    },
    SKIP: {
      cls: "badge-hold",
      label: row.action_label || "⚪ 不碰",
      sub: subFromRow || "條件不足",
      note: "跳過"
    },
    HOLD: {
      cls: "badge-hold",
      label: row.action_label || "持有",
      sub: subFromRow || "續抱",
      note: "持有"
    },
    SELL: {
      cls: "badge-sell",
      label: row.action_label || "賣出",
      sub: subFromRow || "轉弱出場",
      note: "出場"
    },
    ADD: {
      cls: "badge-add",
      label: row.action_label || "加碼",
      sub: subFromRow || "強勢續抱",
      note: "加碼"
    },
    REDUCE: {
      cls: "badge-reduce",
      label: row.action_label || "減碼",
      sub: subFromRow || "降低風險",
      note: "減碼"
    },
    STOP_LOSS: {
      cls: "badge-stop",
      label: row.action_label || "停損",
      sub: subFromRow || "風險出場",
      note: "停損"
    },
    SYNCING: {
      cls: "badge-watch",
      label: row.action_label || "同步中",
      sub: subFromRow || "寫回中",
      note: "同步"
    },
  };

  const meta = map[action] || {
    cls: "badge-hold",
    label: row.action_label || safe(action),
    sub: subFromRow || "",
    note: ""
  };

  if (score !== "--" && !meta.sub.includes("分數")) {
    meta.sub = `${meta.sub}${meta.sub ? "｜" : ""}分數${score}`;
  }

  return meta;
}

function renderActionCell(row) {
  const meta = actionMeta(row || {});
  return `
    <div class="action-split">
      <div><span class="badge ${meta.cls}">${safe(meta.label)}</span></div>
      <div class="action-sub">${safe(meta.sub)}</div>
    </div>
  `;
}

function renderNoteCell(row) {
  const note = row.note || "";
  const detail = row.detail_note || "";

  if (!note && !detail) return "--";

  const shortNote = note || detail;
  return `
    <div class="note-main">${safe(shortNote)}</div>
  `;
}

function badgeForAction(actionRaw) {
  return renderActionCell({ action: actionRaw });
}

function upsertByStockId(rows, row) {
  const sid = String(row.stock_id).trim();
  const out = rows.filter((r) => String(r.stock_id).trim() !== sid);
  out.push(row);
  return out;
}

function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj || []));
}

function priceTierFromPrice(v) {
  const n = toNum(v);
  if (!Number.isFinite(n)) return "unknown";
  if (n < 50) return "lt_50";
  if (n < 100) return "p50_100";
  if (n < 300) return "p100_300";
  if (n < 500) return "p300_500";
  if (n < 1000) return "p500_1000";
  return "gt_1000";
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
  if (!Number.isFinite(n)) return "--";
  return `${(n * 100).toFixed(2)}%`;
}

function safePct(v) {
  const n = toNum(v);
  if (!Number.isFinite(n)) return "--";
  return `${(n * 100).toFixed(2)}%`;
}

function moneyDisplay(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
}


function safeWeight(v) {
  const n = toNum(v);
  if (!Number.isFinite(n)) return "--";
  if (Math.abs(n) < 0.00001) return "0";
  return n.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
}

function safeScore(v) {
  const n = toNum(v);
  if (!Number.isFinite(n)) return "--";
  return String(Math.round(n));
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
  return v === undefined || v === null || v === "" ? "--" : String(v);
}

function safe(v) {
  return escapeHTML(blankDash(v));
}

function escapeHTML(v) {
  return String(v)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(v) {
  return escapeHTML(blankDash(v));
}

function toNum(v) {
  const n = Number(String(v ?? "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function toNumOrZero(v) {
  const n = toNum(v);
  return Number.isFinite(n) ? n : 0;
}

function text(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function val(id, setValue) {
  const el = document.getElementById(id);
  if (!el) return "";
  if (typeof setValue !== "undefined") el.value = setValue;
  return el.value || "";
}

function setBanner(msg, color = "#2f7d32") {
  const el = document.getElementById("syncBanner");
  if (el) {
    el.textContent = msg;
    el.style.color = color;
  }
}

function startLocalClock() {
  if (CLOCK_TIMER) clearInterval(CLOCK_TIMER);

  const tick = () => {
    const now = new Date();
    const formatted = now.toLocaleString("zh-TW", {
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    text("nowTime", formatted);
  };

  tick();
  CLOCK_TIMER = setInterval(tick, 1000);
}
