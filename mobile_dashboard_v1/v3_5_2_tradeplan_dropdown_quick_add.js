/*
v3.5.2_tradeplan_dropdown_quick_add.js

目的：
只新增「今日操作下拉選股 + 一鍵加入持股」。
不改 v1、不改策略、不改 yml、不覆蓋 app.js。

功能：
1. 從 mobile_dashboard_v1/data/trade_plan.csv 讀取今日操作名單
2. 用「價位分層」做下拉選單
3. 選價位後再選股票
4. 自動用 ref_price 當成本、用 suggested_amount 推算股數
5. 一鍵寫回 current_positions.csv
6. 自動觸發 v2_8_auto_update.yml 重跑策略
7. 內建 409 retry，避免 GitHub SHA 衝突
*/

(function () {
  const VERSION = "v3.5.2-tradeplan-dropdown-quick-add";

  const DATA_DIR = "mobile_dashboard_v1/data";
  const TRADE_PLAN_PATH = `${DATA_DIR}/trade_plan.csv`;
  const POS_DASHBOARD = `${DATA_DIR}/current_positions.csv`;
  const POS_ROOT = "current_positions.csv";
  const MAIN_WORKFLOW = "v2_8_auto_update.yml";

  function getInputValue(keys) {
    const inputs = Array.from(document.querySelectorAll("input"));
    for (const input of inputs) {
      const t = `${input.placeholder || ""} ${input.name || ""} ${input.id || ""}`.toLowerCase();
      if (keys.some(k => t.includes(k))) return input.value.trim();
    }
    return "";
  }

  function getConfig() {
    return {
      owner: localStorage.getItem("github_owner") || localStorage.getItem("gh_owner") || getInputValue(["owner", "帳號"]) || "",
      repo: localStorage.getItem("github_repo") || localStorage.getItem("gh_repo") || getInputValue(["repo", "倉庫"]) || "",
      branch: localStorage.getItem("github_branch") || localStorage.getItem("gh_branch") || getInputValue(["branch", "分支"]) || "main",
      token: localStorage.getItem("github_token") || localStorage.getItem("gh_token") || getInputValue(["token", "ghp"]) || "",
    };
  }

  function statusBox() {
    let box = document.getElementById("quickadd-status-box");
    if (box) return box;

    box = document.createElement("div");
    box.id = "quickadd-status-box";
    box.style.cssText = [
      "display:none",
      "margin:12px 0",
      "padding:14px 16px",
      "border-radius:16px",
      "font-weight:800",
      "font-size:16px",
      "line-height:1.45",
      "background:#eef6ee",
      "color:#2f6b2f",
      "border:1px solid rgba(47,107,47,.18)"
    ].join(";");

    const firstCard = document.querySelector(".card");
    if (firstCard && firstCard.parentNode) firstCard.parentNode.insertBefore(box, firstCard);
    else document.body.prepend(box);

    return box;
  }

  function setStatus(msg, type = "ok") {
    const box = statusBox();
    const map = {
      ok: ["#eef6ee", "#2f6b2f"],
      info: ["#eef3ff", "#25406f"],
      warn: ["#fff8e6", "#8a5a00"],
      err: ["#fdecec", "#9c2f2f"],
    };
    const [bg, color] = map[type] || map.ok;
    box.style.background = bg;
    box.style.color = color;
    box.style.display = "block";
    box.textContent = msg;
  }

  function csvEscape(v) {
    const s = String(v ?? "");
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }

  function splitLine(line) {
    const out = [];
    let cur = "";
    let quote = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (quote && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          quote = !quote;
        }
      } else if (ch === "," && !quote) {
        out.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out;
  }

  function parseCSV(text) {
    const lines = String(text || "").trim().split(/\r?\n/).filter(Boolean);
    if (!lines.length) return [];
    const headers = splitLine(lines[0]).map(h => h.trim());
    return lines.slice(1).map(line => {
      const vals = splitLine(line);
      const row = {};
      headers.forEach((h, i) => row[h] = vals[i] || "");
      return row;
    });
  }

  function toPositionsCSV(rows) {
    const headers = ["stock_id", "shares", "avg_cost", "last_action_date", "note"];
    const body = rows.map(r => headers.map(h => csvEscape(r[h] || "")).join(","));
    return [headers.join(","), ...body].join("\n") + "\n";
  }

  function encPath(path) {
    return path.split("/").map(encodeURIComponent).join("/");
  }

  async function gh(path, opt = {}) {
    const cfg = getConfig();
    if (!cfg.owner || !cfg.repo || !cfg.token) {
      throw new Error("GitHub 設定不完整，請確認 owner / repo / token");
    }

    const res = await fetch(`https://api.github.com${path}`, {
      ...opt,
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${cfg.token}`,
        "X-GitHub-Api-Version": "2022-11-28",
        ...(opt.headers || {}),
      },
    });

    const text = await res.text();
    if (!res.ok) {
      throw new Error(`GitHub API ${res.status}: ${text.slice(0, 300)}`);
    }
    return text ? JSON.parse(text) : {};
  }

  async function getFile(path) {
    const cfg = getConfig();
    try {
      const data = await gh(`/repos/${cfg.owner}/${cfg.repo}/contents/${encPath(path)}?ref=${encodeURIComponent(cfg.branch)}`);
      const content = data.content ? decodeURIComponent(escape(atob(data.content.replace(/\n/g, "")))) : "";
      return { content, sha: data.sha };
    } catch (e) {
      if (String(e.message).includes("404")) return { content: "", sha: null };
      throw e;
    }
  }

  async function putFileOnce(path, content, message) {
    const cfg = getConfig();
    const old = await getFile(path);
    const body = {
      message,
      content: btoa(unescape(encodeURIComponent(content))),
      branch: cfg.branch,
    };
    if (old.sha) body.sha = old.sha;

    return gh(`/repos/${cfg.owner}/${cfg.repo}/contents/${encPath(path)}`, {
      method: "PUT",
      body: JSON.stringify(body),
    });
  }

  async function putFileWithRetry(path, content, message, tries = 3) {
    let lastError = null;
    for (let i = 1; i <= tries; i++) {
      try {
        if (i > 1) setStatus(`⚠️ GitHub 檔案版本衝突，自動重試第 ${i} 次...`, "warn");
        return await putFileOnce(path, content, message);
      } catch (e) {
        lastError = e;
        const msg = String(e.message || "");
        if (!msg.includes("409") || i === tries) break;
        await new Promise(r => setTimeout(r, 800 * i));
      }
    }
    throw lastError;
  }

  async function triggerWorkflow() {
    const cfg = getConfig();
    return gh(`/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${encodeURIComponent(MAIN_WORKFLOW)}/dispatches`, {
      method: "POST",
      body: JSON.stringify({ ref: cfg.branch }),
    });
  }

  async function fetchCSV(path) {
    const res = await fetch(`${path}?ts=${Date.now()}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`讀取失敗：${path}`);
    return parseCSV(await res.text());
  }

  function normAction(v) {
    const s = String(v || "").toUpperCase();
    if (s.includes("BUY") || s.includes("買")) return "BUY";
    return s || "BUY";
  }

  function getStockId(row) {
    return row.stock_id || row.stock || row.symbol || row["股票"] || "";
  }

  function getTier(row) {
    return row.price_tier || row.tier || row["價格分層"] || "未分類";
  }

  function getRefPrice(row) {
    return Number(row.ref_price || row.close || row.price || row["參考價格"] || 0);
  }

  function getSuggestedAmount(row) {
    return Number(row.suggested_amount || row.amount || row["建議金額"] || 0);
  }

  function calcShares(row) {
    const ref = getRefPrice(row);
    const amount = getSuggestedAmount(row);

    if (!ref || ref <= 0 || !amount || amount <= 0) return 1000;

    const raw = Math.floor(amount / ref);
    const lot = Math.floor(raw / 1000) * 1000;
    return Math.max(lot || 0, 1000);
  }

  function findTradePlanSection() {
    const body = document.getElementById("tradePlanBody");
    if (!body) return null;
    return body.closest("section") || body.closest(".card");
  }

  function addQuickAddPanel() {
    if (document.getElementById("tradeplan-quick-add-panel")) return;

    const section = findTradePlanSection();
    if (!section) return;

    const panel = document.createElement("div");
    panel.id = "tradeplan-quick-add-panel";
    panel.style.cssText = [
      "margin:14px 0 18px",
      "padding:14px",
      "border-radius:18px",
      "background:#f7f8fa",
      "border:1px solid rgba(16,24,40,.08)"
    ].join(";");

    panel.innerHTML = `
      <div style="font-weight:900;font-size:18px;margin-bottom:10px;">➕ 從今日操作直接加入持股</div>
      <div style="display:grid;grid-template-columns:1fr;gap:10px;">
        <select id="quickTierSelect" style="width:100%;padding:14px;border-radius:14px;border:1px solid #d0d5dd;font-size:16px;background:white;">
          <option value="">選擇價位分層</option>
        </select>
        <select id="quickStockSelect" style="width:100%;padding:14px;border-radius:14px;border:1px solid #d0d5dd;font-size:16px;background:white;">
          <option value="">先選價位分層</option>
        </select>
        <div id="quickStockInfo" style="font-size:15px;line-height:1.5;color:#475467;">尚未選擇股票</div>
        <button id="quickAddPositionBtn" style="width:100%;padding:16px;border-radius:18px;border:0;background:#2f4778;color:white;font-size:18px;font-weight:900;">
          加入持股並重跑策略
        </button>
      </div>
    `;

    const tableWrap = section.querySelector(".table-wrap");
    if (tableWrap) section.insertBefore(panel, tableWrap);
    else section.appendChild(panel);

    loadTradePlanIntoPanel();
  }

  async function loadTradePlanIntoPanel() {
    try {
      const rows = (await fetchCSV(TRADE_PLAN_PATH))
        .filter(r => normAction(r.action || r["動作"]) === "BUY")
        .filter(r => getStockId(r));

      window.__tradePlanQuickRows = rows;

      const tierSel = document.getElementById("quickTierSelect");
      const stockSel = document.getElementById("quickStockSelect");
      const info = document.getElementById("quickStockInfo");

      if (!tierSel || !stockSel || !info) return;

      const tiers = Array.from(new Set(rows.map(getTier))).sort((a, b) => String(a).localeCompare(String(b)));
      tierSel.innerHTML = `<option value="">選擇價位分層</option>` + tiers.map(t => `<option value="${csvEscape(t)}">${t}（${rows.filter(r => getTier(r) === t).length}檔）</option>`).join("");

      tierSel.onchange = () => {
        const tier = tierSel.value;
        const list = rows.filter(r => getTier(r) === tier);
        stockSel.innerHTML = `<option value="">選擇股票</option>` + list.map((r, i) => {
          const ref = getRefPrice(r);
          const amount = getSuggestedAmount(r);
          return `<option value="${i}">${getStockId(r)}｜參考 ${ref || "--"}｜建議 ${amount ? amount.toLocaleString() : "--"}</option>`;
        }).join("");
        stockSel.dataset.tier = tier;
        info.textContent = list.length ? `此分層有 ${list.length} 檔，請選一檔加入持股。` : "此分層沒有股票。";
      };

      stockSel.onchange = () => {
        const tier = stockSel.dataset.tier || "";
        const list = rows.filter(r => getTier(r) === tier);
        const row = list[Number(stockSel.value)];
        if (!row) {
          info.textContent = "尚未選擇股票";
          return;
        }
        const ref = getRefPrice(row);
        const amount = getSuggestedAmount(row);
        const shares = calcShares(row);
        info.textContent = `股票 ${getStockId(row)}｜分層 ${getTier(row)}｜成本 ${ref || "--"}｜預估股數 ${shares.toLocaleString()}｜建議金額 ${amount ? amount.toLocaleString() : "--"}`;
      };

      document.getElementById("quickAddPositionBtn").onclick = quickAddSelectedStock;
    } catch (e) {
      console.error(e);
      setStatus(`❌ 今日操作下拉載入失敗：${e.message}`, "err");
    }
  }

  async function loadPositionsFromGitHub() {
    const file = await getFile(POS_DASHBOARD);
    return parseCSV(file.content);
  }

  async function quickAddSelectedStock() {
    try {
      const tierSel = document.getElementById("quickTierSelect");
      const stockSel = document.getElementById("quickStockSelect");
      if (!tierSel || !stockSel || !tierSel.value || stockSel.value === "") {
        throw new Error("請先選擇價位分層與股票");
      }

      const rows = window.__tradePlanQuickRows || [];
      const list = rows.filter(r => getTier(r) === tierSel.value);
      const selected = list[Number(stockSel.value)];
      if (!selected) throw new Error("找不到選擇的股票");

      const stockId = getStockId(selected);
      const ref = getRefPrice(selected);
      const shares = calcShares(selected);
      if (!stockId) throw new Error("股票代號空白");
      if (!ref || ref <= 0) throw new Error("參考價格無效，不能自動加入持股");

      const row = {
        stock_id: String(stockId),
        shares: String(shares),
        avg_cost: String(ref),
        last_action_date: new Date().toISOString().slice(0, 10),
        note: `quick_add_from_trade_plan_${getTier(selected)}`
      };

      setStatus(`🚀 正在把 ${stockId} 從今日操作加入持股...`, "info");

      const existing = await loadPositionsFromGitHub();
      const map = new Map();
      existing.forEach(r => { if (r.stock_id) map.set(String(r.stock_id).trim(), r); });
      map.set(row.stock_id, row);

      const nextRows = Array.from(map.values()).sort((a, b) => String(a.stock_id).localeCompare(String(b.stock_id)));
      const csv = toPositionsCSV(nextRows);
      const msg = `quick add position from trade plan: ${row.stock_id}`;

      await putFileWithRetry(POS_DASHBOARD, csv, msg);
      await putFileWithRetry(POS_ROOT, csv, msg);

      setStatus(`✅ ${stockId} 已加入持股，正在觸發 Actions 重跑策略...`, "ok");
      await triggerWorkflow();

      localStorage.setItem("last_position_writeback_at", new Date().toISOString());
      localStorage.setItem("last_position_writeback_stock", stockId);

      setStatus(`✅ 已送出 ${stockId}。Actions 會重跑策略，約 1–2 分鐘後按「重新整理頁面」。`, "ok");
    } catch (e) {
      console.error(e);
      setStatus(`❌ 加入失敗：${e.message}`, "err");
      alert(`加入失敗：${e.message}`);
    }
  }

  function boot() {
    statusBox();
    addQuickAddPanel();
    new MutationObserver(addQuickAddPanel).observe(document.body, { childList: true, subtree: true });
    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
