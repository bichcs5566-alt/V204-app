/*
v3.5.6_safe_inline_add_override.js

目的：
只新增補丁，不動已完成主體。
修正：從「今日操作」加入持倉時，舊持倉可能被洗掉。

本補丁：
1. 攔截「今日操作」表格右側的加入持倉按鈕
2. 不再走原本表單 submit
3. 直接從 GitHub 讀最新 current_positions.csv
4. 舊持倉 + 新股票合併後寫回
5. 同步寫 mobile_dashboard_v1/data/current_positions.csv 與 root/current_positions.csv
6. 觸發 v2_8_auto_update.yml
7. 409 自動重試
*/

(function () {
  const VERSION = "v3.5.6-safe-inline-add-override";

  const POS_DASHBOARD = "mobile_dashboard_v1/data/current_positions.csv";
  const POS_ROOT = "current_positions.csv";
  const WORKFLOW = "v2_8_auto_update.yml";
  const HEADERS = ["stock_id", "shares", "avg_cost", "last_action_date", "note"];

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
    return document.getElementById("quickadd-status-box") || document.getElementById("writeback-status-box");
  }

  function setStatus(msg, type = "ok") {
    let box = statusBox();
    if (!box) {
      box = document.createElement("div");
      box.id = "quickadd-status-box";
      box.style.cssText = "margin:12px 0;padding:14px 16px;border-radius:16px;font-weight:800;font-size:16px;line-height:1.45;";
      document.body.prepend(box);
    }

    const map = {
      ok: ["#eef6ee", "#2f6b2f"],
      info: ["#eef3ff", "#25406f"],
      warn: ["#fff8e6", "#8a5a00"],
      err: ["#fdecec", "#9c2f2f"],
    };
    const [bg, color] = map[type] || map.ok;
    box.style.display = "block";
    box.style.background = bg;
    box.style.color = color;
    box.textContent = msg;
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
    }).filter(r => String(r.stock_id || "").trim());
  }

  function csvEscape(v) {
    const s = String(v ?? "");
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }

  function toCSV(rows) {
    return HEADERS.join(",") + "\n" +
      rows.map(r => HEADERS.map(h => csvEscape(r[h] || "")).join(",")).join("\n") +
      "\n";
  }

  function b64enc(s) {
    return btoa(unescape(encodeURIComponent(s)));
  }

  function b64dec(s) {
    return s ? decodeURIComponent(escape(atob(String(s).replace(/\n/g, "")))) : "";
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

    const text = await res.text();
    if (!res.ok) throw new Error(`GitHub API ${res.status}: ${text.slice(0, 260)}`);
    return text ? JSON.parse(text) : {};
  }

  async function getFile(path) {
    const cfg = getConfig();
    try {
      const data = await gh(`/repos/${cfg.owner}/${cfg.repo}/contents/${encPath(path)}?ref=${encodeURIComponent(cfg.branch)}`);
      return {
        sha: data.sha || null,
        rows: parseCSV(b64dec(data.content || "")),
      };
    } catch (e) {
      if (String(e.message).includes("404")) return { sha: null, rows: [] };
      throw e;
    }
  }

  function mergeRows(oldRows, newRow) {
    const map = new Map();

    for (const r of oldRows || []) {
      const id = String(r.stock_id || "").trim();
      if (!id) continue;
      map.set(id, {
        stock_id: id,
        shares: String(r.shares || ""),
        avg_cost: String(r.avg_cost || ""),
        last_action_date: String(r.last_action_date || ""),
        note: String(r.note || ""),
      });
    }

    const id = String(newRow.stock_id || "").trim();
    map.set(id, {
      stock_id: id,
      shares: String(newRow.shares || ""),
      avg_cost: String(newRow.avg_cost || ""),
      last_action_date: String(newRow.last_action_date || new Date().toISOString().slice(0, 10)),
      note: String(newRow.note || "safe_inline_add"),
    });

    return Array.from(map.values()).sort((a, b) => String(a.stock_id).localeCompare(String(b.stock_id)));
  }

  async function putMerged(path, row, msg, tries = 3) {
    const cfg = getConfig();

    for (let i = 1; i <= tries; i++) {
      try {
        const latest = await getFile(path);
        const merged = mergeRows(latest.rows, row);
        const body = {
          message: msg,
          content: b64enc(toCSV(merged)),
          branch: cfg.branch,
        };
        if (latest.sha) body.sha = latest.sha;

        const result = await gh(`/repos/${cfg.owner}/${cfg.repo}/contents/${encPath(path)}`, {
          method: "PUT",
          body: JSON.stringify(body),
        });

        return { result, oldCount: latest.rows.length, newCount: merged.length };
      } catch (e) {
        if (!String(e.message).includes("409") || i === tries) throw e;
        setStatus(`⚠️ GitHub 版本衝突，自動重試第 ${i + 1} 次...`, "warn");
        await new Promise(r => setTimeout(r, 900 * i));
      }
    }
  }

  async function triggerWorkflow() {
    const cfg = getConfig();
    await gh(`/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${encodeURIComponent(WORKFLOW)}/dispatches`, {
      method: "POST",
      body: JSON.stringify({ ref: cfg.branch }),
    });
  }

  function norm(s) {
    return String(s || "").replace(/\s+/g, "").trim();
  }

  function parseNum(s) {
    const n = Number(String(s || "").replace(/,/g, "").replace(/[^\d.\-]/g, ""));
    return Number.isFinite(n) ? n : 0;
  }

  function getHeaders(table) {
    return Array.from(table.querySelectorAll("thead th")).map(th => norm(th.textContent));
  }

  function idx(headers, keys) {
    for (const k of keys) {
      const i = headers.findIndex(h => h.includes(k));
      if (i >= 0) return i;
    }
    return -1;
  }

  function calcShares(amount, ref) {
    if (!amount || !ref) return 1000;
    const raw = Math.floor(amount / ref);
    const lot = Math.floor(raw / 1000) * 1000;
    return Math.max(lot, 1000);
  }

  function readRowFromButton(btn) {
    const tr = btn.closest("tr");
    const table = btn.closest("table");
    if (!tr || !table) throw new Error("找不到今日操作列");

    const headers = getHeaders(table);
    const cells = Array.from(tr.children);

    const stockIdx = idx(headers, ["股票", "stock"]);
    const refIdx = idx(headers, ["參考價格", "價格"]);
    const amountIdx = idx(headers, ["建議金額", "金額"]);

    if (stockIdx < 0) throw new Error("找不到股票欄位");

    const stockId = String(cells[stockIdx]?.textContent || "").trim();
    const ref = refIdx >= 0 ? parseNum(cells[refIdx]?.textContent) : 0;
    const amount = amountIdx >= 0 ? parseNum(cells[amountIdx]?.textContent) : 0;
    const shares = calcShares(amount, ref);

    if (!stockId) throw new Error("股票代號空白");
    if (!ref || ref <= 0) throw new Error(`${stockId} 參考價格無效`);

    return {
      stock_id: stockId,
      shares: String(shares),
      avg_cost: String(ref),
      last_action_date: new Date().toISOString().slice(0, 10),
      note: "safe_inline_add_from_trade_plan",
    };
  }

  async function safeAdd(btn) {
    const row = readRowFromButton(btn);

    setStatus(`🛡️ 安全加入 ${row.stock_id}：正在保留舊持倉並合併...`, "info");

    const msg = `safe inline add position: ${row.stock_id}`;

    const a = await putMerged(POS_DASHBOARD, row, msg);
    await putMerged(POS_ROOT, row, msg);

    localStorage.setItem("last_position_writeback_at", new Date().toISOString());
    localStorage.setItem("last_position_writeback_stock", row.stock_id);

    setStatus(`✅ 已安全加入 ${row.stock_id}。原持倉 ${a.oldCount} 筆，合併後 ${a.newCount} 筆。Actions 會重跑策略。`, "ok");

    await triggerWorkflow();
  }

  document.addEventListener("click", function (e) {
    const btn = e.target && e.target.closest ? e.target.closest("button") : null;
    if (!btn) return;

    const body = document.getElementById("tradePlanBody");
    if (!body || !body.contains(btn)) return;

    if (!/加入持倉|加入/.test(btn.textContent || "")) return;
    if (/已持有/.test(btn.textContent || "")) return;

    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();

    safeAdd(btn).catch(err => {
      console.error(err);
      setStatus(`❌ 安全加入失敗：${err.message}`, "err");
      alert(`安全加入失敗：${err.message}`);
    });
  }, true);

  console.log(`${VERSION} loaded`);
})();
