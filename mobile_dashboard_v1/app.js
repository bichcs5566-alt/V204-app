const DATA_DIR = "./data";
const GH_CONFIG_KEY = "v265_github_config";

document.addEventListener("DOMContentLoaded", async () => {
  bindUI();
  loadSavedConfig();
  await loadAll(true);
});

function bindUI() {
  bind("refreshBtn", "click", async () => {
    setBanner("頁面重新同步中…", "#2f7d32");
    await loadAll(true);
  });

  bind("updateBtn", "click", async () => {
    await dispatchWorkflow(
      "v265_2_tradeable_mainline.yml",
      {},
      "已送出 v265.2 更新，Actions 跑完後請重新整理。"
    );
  });

  bind("saveConfigBtn", "click", saveConfig);
  bind("clearConfigBtn", "click", clearConfig);
}

function bind(id, event, handler) {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener(event, async e => {
    try {
      await handler(e);
    } catch (err) {
      console.error(err);
      setBanner(`操作失敗：${err.message}`, "#b42318");
    }
  });
}

function loadSavedConfig() {
  try {
    const raw = localStorage.getItem(GH_CONFIG_KEY);
    if (!raw) {
      setText("configStatus", "未儲存");
      setVal("ghOwner", "bichcs5566-alt");
      setVal("ghBranch", "main");
      return;
    }
    const cfg = JSON.parse(raw);
    setVal("ghOwner", cfg.owner || "bichcs5566-alt");
    setVal("ghRepo", cfg.repo || "");
    setVal("ghBranch", cfg.branch || "main");
    setVal("ghToken", cfg.token || "");
    setText("configStatus", "✅ 已儲存本機設定");
  } catch {
    setText("configStatus", "讀取失敗");
  }
}

function saveConfig() {
  const cfg = {
    owner: getVal("ghOwner").trim(),
    repo: getVal("ghRepo").trim(),
    branch: getVal("ghBranch").trim() || "main",
    token: getVal("ghToken").trim()
  };

  if (!cfg.owner || !cfg.repo || !cfg.branch || !cfg.token) {
    setBanner("GitHub 設定不可空白", "#b42318");
    setText("configStatus", "欄位不完整");
    return;
  }

  localStorage.setItem(GH_CONFIG_KEY, JSON.stringify(cfg));
  setText("configStatus", "✅ 已儲存本機設定");
  setBanner("GitHub 本機設定已儲存", "#2f7d32");
}

function clearConfig() {
  localStorage.removeItem(GH_CONFIG_KEY);
  ["ghOwner", "ghRepo", "ghBranch", "ghToken"].forEach(id => setVal(id, ""));
  setText("configStatus", "已清除");
  setBanner("GitHub 本機設定已清除", "#92400e");
}

function getGithubConfig() {
  const raw = localStorage.getItem(GH_CONFIG_KEY);
  if (!raw) {
    setBanner("請先儲存 GitHub owner / repo / branch / token", "#b42318");
    setText("configStatus", "未儲存");
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    setBanner("GitHub 本機設定格式錯誤，請重新儲存", "#b42318");
    setText("configStatus", "格式錯誤");
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
      "X-GitHub-Api-Version": "2022-11-28"
    },
    body: JSON.stringify({ ref: cfg.branch, inputs })
  });

  if (!res.ok) {
    const txt = await res.text();
    console.error(txt);
    setBanner(`同步送出失敗：${res.status}`, "#b42318");
    return false;
  }

  setBanner(successMessage, "#2f7d32");
  return true;
}

async function loadAll(force = false) {
  try {
    const [meta, tradePlan, core, alpha, debug, summary] = await Promise.all([
      fetchJSON(`${DATA_DIR}/meta.json`, force),
      fetchCSV(`${DATA_DIR}/trade_plan.csv`, force),
      fetchCSV(`${DATA_DIR}/core_candidates.csv`, force),
      fetchCSV(`${DATA_DIR}/alpha_candidates.csv`, force),
      fetchCSV(`${DATA_DIR}/selection_debug.csv`, force),
      fetchCSV(`${DATA_DIR}/full_summary.csv`, force)
    ]);

    renderMeta(meta);
    renderHero(meta, tradePlan, debug);
    renderTradePlan(tradePlan);
    renderDebug(debug);
    renderCandidates("coreList", core, "Core 候選");
    renderCandidates("alphaList", alpha, "Alpha 候選");
    renderSummary(summary);

    setBanner("頁面資料已同步", "#2f7d32");
  } catch (err) {
    console.error(err);
    setBanner(`讀取失敗：${err.message}`, "#b42318");
  }
}

async function fetchJSON(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) return {};
  const text = await res.text();
  if (!text.trim()) return {};
  return JSON.parse(text.replace(/^\uFEFF/, ""));
}

async function fetchCSV(url, force = false) {
  const finalUrl = force ? `${url}?t=${Date.now()}` : url;
  const res = await fetch(finalUrl, { cache: "no-store" });
  if (!res.ok) return [];
  return parseCSV(await res.text());
}

function parseCSV(text) {
  const cleaned = (text || "").replace(/^\uFEFF/, "").trim();
  if (!cleaned) return [];
  const lines = cleaned.split(/\r?\n/);
  if (lines.length <= 1) return [];
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
  setText("sourceName", meta.source || "--");
  setText("dataState", prettyDataState(meta.data_state));
  setText("generatedAt", meta.generated_at || meta.bridge_generated_at || "--");
  setText("signalDate", meta.signal_date || "--");
  setText("tradeDate", meta.trade_date || "--");
  setText("tradeCount", meta.trade_plan_count ?? "--");
}

function renderHero(meta, tradeRows, debugRows) {
  const d = debugRows[0] || {};
  const buy = Number(meta.buy_count ?? d.trade_buy_count ?? countAction(tradeRows, "BUY"));
  const test = Number(meta.test_count ?? d.trade_test_count ?? countAction(tradeRows, "TEST"));
  const watch = Number(meta.watch_count ?? d.trade_watch_count ?? countAction(tradeRows, "WATCH"));

  let main = "觀察";
  let sub = "目前沒有正式買進訊號";
  let cls = "hero-watch";

  if (buy > 0) {
    main = "買進";
    sub = `今日有 ${buy} 檔買進候選，請分批執行`;
    cls = "hero-buy";
  } else if (test > 0) {
    main = "試單";
    sub = `今日有 ${test} 檔可小倉試單`;
    cls = "hero-test";
  } else if (watch > 0) {
    main = "觀察";
    sub = `今日有 ${watch} 檔觀察名單，等待確認`;
    cls = "hero-watch";
  }

  const hero = document.getElementById("heroCard");
  if (hero) {
    hero.className = `card hero ${cls}`;
    hero.innerHTML = `
      <div class="hero-label">今日主判斷</div>
      <div class="hero-main">${main}</div>
      <div class="hero-sub">${sub}</div>
      <div class="hero-stats">
        <div><b>${buy}</b><span>買進</span></div>
        <div><b>${test}</b><span>試單</span></div>
        <div><b>${watch}</b><span>觀察</span></div>
      </div>
    `;
  }
}

function renderTradePlan(rows) {
  const container = document.getElementById("tradePlanList");
  if (!container) return;

  if (!rows.length) {
    container.innerHTML = `<div class="empty">目前沒有操作資料</div>`;
    return;
  }

  const sorted = rows.slice().sort((a, b) => actionRank(a.action) - actionRank(b.action) || toNum(b.entry_score) - toNum(a.entry_score));

  container.innerHTML = sorted.map(r => {
    const action = normalizeAction(r.action);
    const cls = actionClass(action);
    const score = toNum(r.entry_score);
    const amount = toNum(r.suggested_amount);
    const targetWeight = toNum(r.target_weight) * 100;

    return `
      <article class="trade-card ${cls}">
        <div class="trade-top">
          <div class="badge ${cls}">${displayAction(r)}</div>
          <div class="stock">${safe(r.stock_id)}</div>
          <div class="score">${formatScore(score)}</div>
        </div>

        <div class="trade-grid">
          <div><span>價格分層</span><b>${safe(r.price_tier)}</b></div>
          <div><span>參考價</span><b>${formatNum(r.ref_price)}</b></div>
          <div><span>目標權重</span><b>${formatPct(targetWeight)}</b></div>
          <div><span>建議金額</span><b>${money(amount)}</b></div>
        </div>

        <div class="sub">${safe(r.action_sub)}</div>
        <div class="note">${safe(r.note)}</div>
      </article>
    `;
  }).join("");
}

function renderDebug(rows) {
  const d = rows[0] || {};
  const box = document.getElementById("debugGrid");
  if (!box) return;

  box.innerHTML = `
    ${statBox("輸入資料", d.total_input_rows)}
    ${statBox("最新股票", d.latest_stock_count)}
    ${statBox("BUY", d.buy_count)}
    ${statBox("TEST", d.test_count)}
    ${statBox("WATCH", d.watch_count)}
    ${statBox("SKIP", d.skip_count)}
    ${statBox("Core", d.core_count)}
    ${statBox("Alpha", d.alpha_count)}
    ${statBox("最高分", d.max_score)}
    ${statBox("平均分", d.avg_score)}
  `;
}

function renderCandidates(id, rows, title) {
  const box = document.getElementById(id);
  if (!box) return;

  if (!rows.length) {
    box.innerHTML = `<h2>${title}</h2><div class="empty small">目前沒有候選資料</div>`;
    return;
  }

  box.innerHTML = `
    <h2>${title}</h2>
    <div class="candidate-list">
      ${rows.slice(0, 12).map(r => {
        const action = normalizeAction(r.action);
        const cls = actionClass(action);
        return `
          <div class="candidate-row">
            <div class="candidate-stock">${safe(r.stock_id)}</div>
            <div class="candidate-score">${formatScore(toNum(r.entry_score))}</div>
            <div class="badge ${cls}">${displayAction(r)}</div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function renderSummary(rows) {
  const row = rows[0] || {};
  setText("returnVal", pctDecimal(row["return"]));
  setText("mddVal", pctDecimal(row["mdd"]));
  setText("sharpeVal", blank(row["sharpe_daily"]));
}

function statBox(label, value) {
  return `<div class="stat-box"><span>${label}</span><b>${blank(value)}</b></div>`;
}

function countAction(rows, action) {
  return rows.filter(r => normalizeAction(r.action) === action).length;
}

function normalizeAction(action) {
  return String(action || "").trim().toUpperCase();
}

function actionRank(action) {
  const a = normalizeAction(action);
  if (a === "BUY") return 1;
  if (a === "TEST") return 2;
  if (a === "WATCH") return 3;
  if (a === "NO_TRADE") return 4;
  return 9;
}

function actionClass(action) {
  const a = normalizeAction(action);
  if (a === "BUY") return "buy";
  if (a === "TEST") return "test";
  if (a === "WATCH") return "watch";
  if (a === "NO_TRADE") return "none";
  return "skip";
}

function displayAction(row) {
  const a = normalizeAction(row.action);
  if (row.action_label) return row.action_label;
  if (a === "BUY") return "買進";
  if (a === "TEST") return "試單";
  if (a === "WATCH") return "觀察";
  if (a === "NO_TRADE") return "觀察";
  if (a === "SKIP") return "排除";
  return row.action || "--";
}

function prettyDataState(state) {
  const map = {
    fresh: "✅ 最新資料",
    ok: "✅ 正常",
    stale: "⚠️ 舊資料",
    loading: "⌛ 讀取中",
    idle: "待命"
  };
  return map[state] || state || "--";
}

function pctDecimal(v) {
  const n = toNum(v);
  if (!Number.isFinite(n)) return "--";
  return `${(n * 100).toFixed(2)}%`;
}

function formatPct(v) {
  if (!Number.isFinite(v)) return "--";
  if (v === 0) return "0%";
  return `${v.toFixed(2)}%`;
}

function formatScore(v) {
  if (!Number.isFinite(v)) return "--";
  return v.toFixed(1);
}

function formatNum(v) {
  const n = toNum(v);
  if (!Number.isFinite(n)) return "--";
  return n.toFixed(2).replace(/\.00$/, "");
}

function money(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "--";
  if (n === 0) return "0";
  return n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
}

function safe(v) {
  return blank(v);
}

function blank(v) {
  return v === undefined || v === null || v === "" ? "--" : String(v);
}

function toNum(v) {
  const n = Number(String(v ?? "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function getVal(id) {
  const el = document.getElementById(id);
  return el ? (el.value || "") : "";
}

function setVal(id, value) {
  const el = document.getElementById(id);
  if (el) el.value = value;
}

function setBanner(msg, color = "#2f7d32") {
  const el = document.getElementById("syncBanner");
  if (!el) return;
  el.textContent = msg;
  el.style.color = color;
}
