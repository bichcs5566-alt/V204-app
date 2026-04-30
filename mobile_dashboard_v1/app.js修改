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

const GITHUB_CONFIG_KEY = "daily_trading_dashboard_github_config_v1";
const GITHUB_WORKFLOW_ID = "data_pipeline.yml";
let pipelineTimer = null;

function loadGitHubConfig() {
  try {
    const raw = localStorage.getItem(GITHUB_CONFIG_KEY);
    if (!raw) return { owner: "", repo: "", branch: "main", token: "" };
    return { owner: "", repo: "", branch: "main", token: "", ...JSON.parse(raw) };
  } catch (e) {
    return { owner: "", repo: "", branch: "main", token: "" };
  }
}

function saveGitHubConfig(config) {
  localStorage.setItem(GITHUB_CONFIG_KEY, JSON.stringify(config));
}

function clearGitHubConfig() {
  localStorage.removeItem(GITHUB_CONFIG_KEY);
}

function formatClock(d = new Date()) {
  const pad = n => String(n).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function formatElapsed(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const sec = total % 60;
  const pad = n => String(n).padStart(2, "0");
  if (h > 0) return `${h}:${pad(m)}:${pad(sec)}`;
  return `${pad(m)}:${pad(sec)}`;
}

function setSyncStatus(text, cls = "ok") {
  const el = qs("syncStatus");
  if (!el) return;
  el.innerHTML = text;
  el.className = `sync ${cls}`;
}

function getGitHubApiBase(config) {
  return `https://api.github.com/repos/${encodeURIComponent(config.owner)}/${encodeURIComponent(config.repo)}`;
}

async function githubFetch(config, url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${config.token}`,
      "X-GitHub-Api-Version": "2022-11-28",
      ...(options.headers || {})
    }
  });
  return res;
}

function startPipelineTicker(startTime, label = "data_pipeline 執行中") {
  if (pipelineTimer) clearInterval(pipelineTimer);
  pipelineTimer = setInterval(() => {
    setSyncStatus(`⏳ ${label}｜已等待 ${formatElapsed(Date.now() - startTime)}｜${formatClock()}`, "ok");
  }, 1000);
}

function stopPipelineTicker() {
  if (pipelineTimer) clearInterval(pipelineTimer);
  pipelineTimer = null;
}

async function triggerDataPipeline() {
  const config = loadGitHubConfig();
  if (!config.owner || !config.repo || !config.branch || !config.token) {
    setSyncStatus("❌ 尚未完成 GitHub 本機設定：owner / repo / branch / token 都要填。", "error");
    return;
  }

  const startedAt = Date.now();
  const startedIso = new Date(startedAt - 15000).toISOString();
  startPipelineTicker(startedAt, "已觸發 data_pipeline，等待 GitHub Actions 接手");

  try {
    const dispatchUrl = `${getGitHubApiBase(config)}/actions/workflows/${encodeURIComponent(GITHUB_WORKFLOW_ID)}/dispatches`;
    const dispatchRes = await githubFetch(config, dispatchUrl, {
      method: "POST",
      body: JSON.stringify({ ref: config.branch })
    });

    if (dispatchRes.status !== 204) {
      stopPipelineTicker();
      const text = await dispatchRes.text();
      setSyncStatus(`❌ 觸發失敗 ${dispatchRes.status}：${text || "請檢查 token 權限 / repo / workflow 檔名"}`, "error");
      return;
    }

    await waitForPipelineCompletion(config, startedAt, startedIso);
  } catch (e) {
    stopPipelineTicker();
    setSyncStatus(`❌ 觸發失敗：${e.message}`, "error");
  }
}

async function waitForPipelineCompletion(config, startedAt, startedIso) {
  let targetRun = null;
  let lastStatus = "";

  for (let i = 0; i < 180; i++) {
    await new Promise(resolve => setTimeout(resolve, i === 0 ? 3500 : 5000));

    const runsUrl =
      `${getGitHubApiBase(config)}/actions/workflows/${encodeURIComponent(GITHUB_WORKFLOW_ID)}/runs` +
      `?branch=${encodeURIComponent(config.branch)}&event=workflow_dispatch&per_page=10`;

    const runsRes = await githubFetch(config, runsUrl);
    if (!runsRes.ok) {
      const text = await runsRes.text();
      stopPipelineTicker();
      setSyncStatus(`❌ 查詢進度失敗 ${runsRes.status}：${text}`, "error");
      return;
    }

    const data = await runsRes.json();
    const runs = Array.isArray(data.workflow_runs) ? data.workflow_runs : [];

    if (!targetRun) {
      targetRun = runs
        .filter(r => r.created_at >= startedIso)
        .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))[0] || null;
    }

    if (!targetRun) {
      setSyncStatus(`⏳ 已觸發 data_pipeline｜等待 GitHub 建立 run｜${formatElapsed(Date.now() - startedAt)}｜${formatClock()}`, "ok");
      continue;
    }

    const latest = runs.find(r => r.id === targetRun.id) || targetRun;
    targetRun = latest;

    const statusText = latest.status === "completed"
      ? `completed / ${latest.conclusion || "unknown"}`
      : latest.status;

    if (statusText !== lastStatus) {
      lastStatus = statusText;
    }

    if (latest.status !== "completed") {
      setSyncStatus(`⏳ data_pipeline ${statusText}｜已等待 ${formatElapsed(Date.now() - startedAt)}｜${formatClock()}`, "ok");
      continue;
    }

    stopPipelineTicker();

    if (latest.conclusion === "success") {
      setSyncStatus(`✅ 後端更新完成｜用時 ${formatElapsed(Date.now() - startedAt)}｜${formatClock()}｜正在重新整理資料...`, "ok");
      setTimeout(() => location.reload(), 2500);
      return;
    }

    const runUrl = latest.html_url ? `｜請到 Actions 查看失敗紀錄` : "";
    setSyncStatus(`❌ data_pipeline 結束但失敗：${latest.conclusion || "unknown"}${runUrl}`, "error");
    return;
  }

  stopPipelineTicker();
  setSyncStatus("⚠️ 已觸發，但等待超過 15 分鐘尚未完成；請到 GitHub Actions 查看進度。", "error");
}

function renderGitHubSettings() {
  const config = loadGitHubConfig();
  const ownerEl = qs("ghOwner");
  const repoEl = qs("ghRepo");
  const branchEl = qs("ghBranch");
  const tokenEl = qs("ghToken");
  const savedEl = qs("ghSavedStatus");
  if (!ownerEl || !repoEl || !branchEl || !tokenEl || !savedEl) return;

  ownerEl.value = config.owner || "";
  repoEl.value = config.repo || "";
  branchEl.value = config.branch || "main";
  tokenEl.value = config.token || "";
  savedEl.textContent = config.owner && config.repo && config.branch && config.token ? "已儲存" : "未儲存";

  qs("ghSaveBtn").addEventListener("click", () => {
    saveGitHubConfig({
      owner: ownerEl.value.trim(),
      repo: repoEl.value.trim(),
      branch: branchEl.value.trim() || "main",
      token: tokenEl.value.trim()
    });
    savedEl.textContent = "已儲存";
    setSyncStatus(`✅ GitHub 本機設定已儲存｜${formatClock()}`, "ok");
  });

  qs("ghClearBtn").addEventListener("click", () => {
    clearGitHubConfig();
    ownerEl.value = "";
    repoEl.value = "";
    branchEl.value = "main";
    tokenEl.value = "";
    savedEl.textContent = "未儲存";
    setSyncStatus(`✅ GitHub 本機設定已清除｜${formatClock()}`, "ok");
  });
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

      <section class="card github-settings-card">
        <details>
          <summary>🔐 GitHub 本機設定</summary>
          <div class="gh-form">
            <input id="ghOwner" placeholder="owner，例如 bichcs5566-alt" autocomplete="off" />
            <input id="ghRepo" placeholder="repo，例如 V204-app" autocomplete="off" />
            <input id="ghBranch" placeholder="branch，例如 main" autocomplete="off" />
            <input id="ghToken" type="password" placeholder="token，只存在本機瀏覽器" autocomplete="off" />
            <div class="gh-actions">
              <button id="ghSaveBtn" type="button">儲存</button>
              <button id="ghClearBtn" type="button" class="secondary">清除</button>
            </div>
            <div class="source-line">狀態：<b id="ghSavedStatus">未儲存</b>｜Workflow：data_pipeline.yml</div>
          </div>
        </details>
      </section>
    </main>
  `;

  qs("refreshBtn").addEventListener("click", () => location.reload());
  qs("updateBtn").addEventListener("click", triggerDataPipeline);
  renderGitHubSettings();
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

  setSyncStatus(`✅ 最終操作表已同步｜${formatClock()}`, "ok");
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

function renderScanRow(row, key) {
  const action = normalizeAction(row.final_action || row.action);
  const cls = ACTION_CLASS[action] || "watch";
  const label = ACTION_LABEL[action] || action;
  const emoji = ACTION_EMOJI[action] || "⚪";
  const top = isTop(row) ? "🔥TOP" : "";
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
    <article class="scan-item ${cls}">
      <div class="scan-main" data-toggle="${key}">
        <div class="scan-action ${cls}">${emoji} ${label}</div>
        <div class="scan-stock">${stock}</div>
        <div class="scan-score">${score}</div>
        <div class="scan-top">${top}</div>
        <div class="scan-entry">${entry}</div>
        <div class="scan-close">${close}</div>
      </div>

      <div class="scan-detail" id="${key}">
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
