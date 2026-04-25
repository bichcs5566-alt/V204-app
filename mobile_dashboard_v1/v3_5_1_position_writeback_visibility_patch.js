/*
v3.5.1_position_writeback_visibility_patch.js
只補「持倉寫回可見化」，不改 v1、不改策略、不改 pipeline。

使用：
1. 把本檔內容存成：v3_5_1_position_writeback_visibility_patch.js
2. 在 index.html 原本主 JS 後面加：
   <script src="v3_5_1_position_writeback_visibility_patch.js"></script>
*/

(function () {
  const POS_DASHBOARD = "mobile_dashboard_v1/data/current_positions.csv";
  const POS_ROOT = "current_positions.csv";
  const MAIN_WORKFLOW = "v2_8_auto_update.yml";

  function q(sel) { return document.querySelector(sel); }

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
    let box = document.getElementById("writeback-status-box");
    if (box) return box;
    box = document.createElement("div");
    box.id = "writeback-status-box";
    box.style.cssText = "display:none;margin:12px 0;padding:14px 16px;border-radius:16px;font-weight:800;font-size:16px;line-height:1.45;background:#eef6ee;color:#2f6b2f;border:1px solid rgba(47,107,47,.18);";
    (document.querySelector("main") || document.body).prepend(box);
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
        if (quote && line[i + 1] === '"') { cur += '"'; i++; }
        else quote = !quote;
      } else if (ch === "," && !quote) {
        out.push(cur); cur = "";
      } else cur += ch;
    }
    out.push(cur);
    return out;
  }

  function parseCSV(text) {
    const lines = String(text || "").trim().split(/\r?\n/).filter(Boolean);
    if (!lines.length) return [];
    const headers = splitLine(lines[0]);
    return lines.slice(1).map(line => {
      const vals = splitLine(line);
      const row = {};
      headers.forEach((h, i) => row[h] = vals[i] || "");
      return row;
    });
  }

  function toCSV(rows) {
    const headers = ["stock_id", "shares", "avg_cost", "last_action_date", "note"];
    return headers.join(",") + "\n" + rows.map(r => headers.map(h => csvEscape(r[h] || "")).join(",")).join("\n") + "\n";
  }

  function encPath(path) {
    return path.split("/").map(encodeURIComponent).join("/");
  }

  async function gh(path, opt = {}) {
    const cfg = getConfig();
    if (!cfg.owner || !cfg.repo || !cfg.token) throw new Error("GitHub 設定不完整，請確認 owner / repo / token");
    const res = await fetch(`https://api.github.com${path}`, {
      ...opt,
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${cfg.token}`,
        "X-GitHub-Api-Version": "2022-11-28",
        ...(opt.headers || {}),
      },
    });
    if (!res.ok) throw new Error(`GitHub API ${res.status}: ${(await res.text()).slice(0, 250)}`);
    return res.text().then(t => t ? JSON.parse(t) : {});
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

  async function putFile(path, content, message) {
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

  async function triggerWorkflow() {
    const cfg = getConfig();
    return gh(`/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${encodeURIComponent(MAIN_WORKFLOW)}/dispatches`, {
      method: "POST",
      body: JSON.stringify({ ref: cfg.branch }),
    });
  }

  async function loadPositions() {
    try {
      const res = await fetch(`${POS_DASHBOARD}?ts=${Date.now()}`, { cache: "no-store" });
      if (!res.ok) return [];
      return parseCSV(await res.text());
    } catch {
      return [];
    }
  }

  function readForm() {
    const stock_id = getInputValue(["股票代號", "stock", "2330"]);
    const shares = getInputValue(["股數", "shares", "1000"]);
    const avg_cost = getInputValue(["成本", "avg", "cost", "950"]);

    if (!stock_id) throw new Error("請輸入股票代號");
    if (!shares || Number(shares) <= 0) throw new Error("請輸入有效股數");
    if (!avg_cost || Number(avg_cost) <= 0) throw new Error("請輸入有效成本");

    return {
      stock_id: String(stock_id).trim(),
      shares: String(Number(shares)),
      avg_cost: String(Number(avg_cost)),
      last_action_date: new Date().toISOString().slice(0, 10),
      note: "manual_writeback",
    };
  }

  async function writebackPosition() {
    try {
      const row = readForm();
      setStatus(`🚀 已送出 ${row.stock_id}，正在寫回 GitHub...`, "info");

      const rows = await loadPositions();
      const map = new Map();
      rows.forEach(r => { if (r.stock_id) map.set(String(r.stock_id).trim(), r); });
      map.set(row.stock_id, row);

      const nextRows = Array.from(map.values()).sort((a, b) => String(a.stock_id).localeCompare(String(b.stock_id)));
      const csv = toCSV(nextRows);
      const msg = `position writeback: upsert ${row.stock_id}`;

      await putFile(POS_DASHBOARD, csv, msg);
      await putFile(POS_ROOT, csv, msg);

      setStatus(`✅ ${row.stock_id} 已寫回，正在觸發策略重跑...`, "ok");
      await triggerWorkflow();

      localStorage.setItem("last_position_writeback_at", new Date().toISOString());
      localStorage.setItem("last_position_writeback_stock", row.stock_id);

      setStatus(`✅ 已送出 ${row.stock_id}。Actions 會重跑策略，約 1–2 分鐘後按「重新整理頁面」。`, "ok");
    } catch (e) {
      console.error(e);
      setStatus(`❌ 寫回失敗：${e.message}`, "err");
      alert(`寫回失敗：${e.message}`);
    }
  }

  function attach() {
    const buttons = Array.from(document.querySelectorAll("button"));
    const btn = buttons.find(b => /加入|更新持倉|加入持倉|寫回/.test(b.textContent || ""));
    if (!btn || btn.dataset.writeback351 === "1") return;
    btn.dataset.writeback351 = "1";
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      writebackPosition();
    }, true);
  }

  function addDebug() {
    if (document.getElementById("writeback-debug-panel")) return;
    const panel = document.createElement("div");
    panel.id = "writeback-debug-panel";
    panel.style.cssText = "margin:16px 0;padding:16px;border-radius:18px;background:#f7f8fa;color:#101828;font-size:15px;line-height:1.6;border:1px solid rgba(16,24,40,.08);";
    panel.innerHTML = `<strong>🧾 寫回狀態</strong><br>最後送出：${localStorage.getItem("last_position_writeback_at") || "尚無"}<br>最新股票：${localStorage.getItem("last_position_writeback_stock") || "尚無"}<br>狀態：寫回後會觸發 v2_8_auto_update 重跑策略`;
    (document.querySelector("main") || document.body).appendChild(panel);
  }

  function boot() {
    statusBox();
    addDebug();
    attach();
    new MutationObserver(attach).observe(document.body, { childList: true, subtree: true });
    console.log("v3.5.1 writeback visibility patch loaded");
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
