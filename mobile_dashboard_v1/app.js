const DATA_DIR = "./data";

document.addEventListener("DOMContentLoaded", async () => {
  bindUI();
  await loadAll();
});

function bindUI() {
  document.getElementById("refreshBtn").addEventListener("click", async () => {
    setBanner("頁面重新同步中…", "#2f7d32");
    await loadAll(true);
  });

  document.getElementById("updateBtn").addEventListener("click", () => {
    setBanner("v2.9 先定稿介面。更新資料仍沿用你現在已打通的 workflow。", "#92400e");
  });
}

function setBanner(text, color = "#2f7d32") {
  const el = document.getElementById("syncBanner");
  el.textContent = text;
  el.style.color = color;
}

async function loadAll(force = false) {
  try {
    const [meta, tradePlan, position, watchlist, summary, debug] = await Promise.all([
      fetchJSON(`${DATA_DIR}/meta.json`, force),
      fetchCSV(`${DATA_DIR}/trade_plan.csv`, force),
      fetchCSV(`${DATA_DIR}/position_monitor.csv`, force),
      fetchCSV(`${DATA_DIR}/watchlist_monitor.csv`, force),
      fetchCSV(`${DATA_DIR}/full_summary.csv`, force),
      fetchCSV(`${DATA_DIR}/selection_debug.csv`, force),
    ]);

    renderMeta(meta);
    renderTradePlan(tradePlan);
    renderPosition(position);
    renderWatchlist(watchlist);
    renderSummary(summary);
    renderDebug(debug);
    renderTierSummary(tradePlan, position, watchlist);
    renderActionSummary(tradePlan, position);
    setBanner("頁面資料已同步", "#2f7d32");
  } catch (err) {
    console.error(err);
    setBanner(`讀取失敗：${err.message}`, "#b42318");
  }
}

async function fetchJSON(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) throw new Error(`JSON 讀取失敗：${url}`);
  return await res.json();
}

async function fetchCSV(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
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
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="9" class="empty">目前沒有持倉資料</td></tr>`;
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
    </tr>
  `).join("");
}

function renderWatchlist(rows) {
  const body = document.getElementById("watchlistBody");
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty">目前沒有自選股資料</td></tr>`;
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
  return map[tier] || tier || "--";
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
  document.getElementById(id).textContent = value;
}
