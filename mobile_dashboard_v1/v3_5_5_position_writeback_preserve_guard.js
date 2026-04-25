/*
v3.5.5_position_writeback_preserve_guard.js

目的：
只做新增補充，不動已完成主體。
不改 v1、不改 yml、不改 app.js、不改 v3.5.1 / v3.5.3 / v3.5.4。

修補問題：
有時加入新持倉時，原本 current_positions.csv 內的持倉會被洗掉。
原因通常是：前端讀取舊持倉失敗 → 用空表 + 新股票覆蓋。

本補丁做法：
1. 攔截 GitHub API 寫入 current_positions.csv 的請求
2. 寫入前強制重新讀 GitHub 最新 current_positions.csv
3. 把「舊持倉」與「新持倉」合併
4. 同股票更新，不同股票保留
5. 再交給原本 fetch 寫回
6. 保留原本所有 UI / 寫回流程 / Actions 觸發

使用方式：
index.html 在所有既有補丁後面新增：
<script src="./v3_5_5_position_writeback_preserve_guard.js"></script>
*/

(function () {
  const VERSION = "v3.5.5-position-writeback-preserve-guard";

  const TARGET_FILES = [
    "mobile_dashboard_v1/data/current_positions.csv",
    "current_positions.csv"
  ];

  const REQUIRED_HEADERS = ["stock_id", "shares", "avg_cost", "last_action_date", "note"];

  const originalFetch = window.fetch.bind(window);

  function isCurrentPositionsApi(url, options) {
    const method = String(options?.method || "GET").toUpperCase();
    if (method !== "PUT") return false;

    const s = String(url || "");
    if (!s.includes("api.github.com/repos/")) return false;
    if (!s.includes("/contents/")) return false;

    return TARGET_FILES.some(path => {
      const encoded = path.split("/").map(encodeURIComponent).join("/");
      return s.includes(encoded);
    });
  }

  function getConfigFromApiUrl(url) {
    const m = String(url).match(/api\.github\.com\/repos\/([^/]+)\/([^/]+)\/contents\/(.+)$/);
    if (!m) return null;

    const owner = decodeURIComponent(m[1]);
    const repo = decodeURIComponent(m[2]);
    let rest = m[3];
    const qIdx = rest.indexOf("?");
    if (qIdx >= 0) rest = rest.slice(0, qIdx);
    const path = rest.split("/").map(decodeURIComponent).join("/");

    let branch = localStorage.getItem("github_branch") || localStorage.getItem("gh_branch") || "main";
    try {
      const u = new URL(String(url));
      branch = u.searchParams.get("ref") || branch;
    } catch {}

    return { owner, repo, path, branch };
  }

  function getTokenFromOptions(options) {
    const headers = options?.headers || {};
    if (headers instanceof Headers) {
      return headers.get("Authorization") || headers.get("authorization") || "";
    }
    return headers.Authorization || headers.authorization || "";
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
    return REQUIRED_HEADERS.join(",") + "\n" +
      rows.map(r => REQUIRED_HEADERS.map(h => csvEscape(r[h] || "")).join(",")).join("\n") +
      "\n";
  }

  function decodeBase64(content) {
    if (!content) return "";
    return decodeURIComponent(escape(atob(String(content).replace(/\n/g, ""))));
  }

  function encodeBase64(text) {
    return btoa(unescape(encodeURIComponent(text)));
  }

  async function readLatestFile(apiUrl, options, branch) {
    const getUrl = String(apiUrl).split("?")[0] + `?ref=${encodeURIComponent(branch)}`;
    const res = await originalFetch(getUrl, {
      method: "GET",
      headers: options.headers,
      cache: "no-store"
    });

    if (res.status === 404) {
      return { rows: [], sha: null, content: "" };
    }

    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`讀取最新持倉失敗 ${res.status}: ${txt.slice(0, 200)}`);
    }

    const data = await res.json();
    const content = decodeBase64(data.content || "");
    return { rows: parseCSV(content), sha: data.sha || null, content };
  }

  function decodeRequestBody(options) {
    const body = typeof options.body === "string" ? JSON.parse(options.body) : (options.body || {});
    const content = decodeBase64(body.content || "");
    return { body, rows: parseCSV(content), content };
  }

  function mergeRows(oldRows, newRows) {
    const map = new Map();

    for (const r of oldRows || []) {
      const id = String(r.stock_id || "").trim();
      if (!id) continue;
      map.set(id, {
        stock_id: id,
        shares: String(r.shares || ""),
        avg_cost: String(r.avg_cost || ""),
        last_action_date: String(r.last_action_date || ""),
        note: String(r.note || "")
      });
    }

    for (const r of newRows || []) {
      const id = String(r.stock_id || "").trim();
      if (!id) continue;

      const old = map.get(id) || {};
      map.set(id, {
        stock_id: id,
        shares: String(r.shares || old.shares || ""),
        avg_cost: String(r.avg_cost || old.avg_cost || ""),
        last_action_date: String(r.last_action_date || old.last_action_date || ""),
        note: String(r.note || old.note || "")
      });
    }

    return Array.from(map.values())
      .filter(r => r.stock_id)
      .sort((a, b) => String(a.stock_id).localeCompare(String(b.stock_id)));
  }

  function updateStatus(message, type = "info") {
    const box =
      document.getElementById("quickadd-status-box") ||
      document.getElementById("writeback-status-box") ||
      null;

    if (!box) return;

    const colors = {
      ok: ["#eef6ee", "#2f6b2f"],
      info: ["#eef3ff", "#25406f"],
      warn: ["#fff8e6", "#8a5a00"],
      err: ["#fdecec", "#9c2f2f"],
    };
    const [bg, color] = colors[type] || colors.info;

    box.style.display = "block";
    box.style.background = bg;
    box.style.color = color;
    box.textContent = message;
  }

  async function guardedFetch(url, options = {}) {
    if (!isCurrentPositionsApi(url, options)) {
      return originalFetch(url, options);
    }

    try {
      const info = getConfigFromApiUrl(url);
      if (!info) return originalFetch(url, options);

      updateStatus("🛡️ 寫回保護中：正在讀取最新持倉並保留原資料...", "info");

      const latest = await readLatestFile(url, options, info.branch);
      const incoming = decodeRequestBody(options);

      const merged = mergeRows(latest.rows, incoming.rows);
      const mergedCsv = toCSV(merged);

      const nextBody = {
        ...incoming.body,
        content: encodeBase64(mergedCsv),
      };

      if (latest.sha) {
        nextBody.sha = latest.sha;
      } else {
        delete nextBody.sha;
      }

      const nextOptions = {
        ...options,
        body: JSON.stringify(nextBody)
      };

      const res = await originalFetch(url, nextOptions);

      if (res.ok) {
        updateStatus(`✅ 寫回保護完成：已保留 ${latest.rows.length} 筆舊持倉，合併後 ${merged.length} 筆。`, "ok");
      }

      return res;
    } catch (e) {
      console.error(`${VERSION} failed`, e);
      updateStatus(`❌ 寫回保護失敗：${e.message}`, "err");
      return originalFetch(url, options);
    }
  }

  window.fetch = guardedFetch;

  console.log(`${VERSION} loaded`);
})();
