/*
v3.5.4_position_duplicate_guard.js

目的：
只做新增補充，不動已完成主體。
不改 v1、不改 yml、不改 app.js、不改既有 v3.5.1 / v3.5.3。

功能：
1. 讀取 current_positions.csv
2. 今日操作中已持有的股票，按鈕改成「已持有」
3. 已持有股票按鈕變灰，避免重複加入
4. 保留既有加入持股流程，不覆蓋原功能
5. 防 cache：讀取 current_positions.csv 時加 ts
*/

(function () {
  const VERSION = "v3.5.4-position-duplicate-guard";
  const POS_PATH = "mobile_dashboard_v1/data/current_positions.csv";

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

  async function loadHeldStocks() {
    try {
      const res = await fetch(`${POS_PATH}?ts=${Date.now()}`, { cache: "no-store" });
      if (!res.ok) return new Set();
      const rows = parseCSV(await res.text());
      return new Set(
        rows
          .map(r => String(r.stock_id || r.stock || r.symbol || "").trim())
          .filter(Boolean)
      );
    } catch (e) {
      console.warn("loadHeldStocks failed", e);
      return new Set();
    }
  }

  function normText(s) {
    return String(s || "").replace(/\s+/g, "").trim();
  }

  function getTradeTable() {
    const body = document.getElementById("tradePlanBody");
    if (!body) return null;
    return body.closest("table");
  }

  function getHeaders(table) {
    return Array.from(table.querySelectorAll("thead th")).map(th => normText(th.textContent));
  }

  function indexOfHeader(headers, candidates) {
    for (const c of candidates) {
      const i = headers.findIndex(h => h.includes(c));
      if (i >= 0) return i;
    }
    return -1;
  }

  function setButtonHeld(btn) {
    btn.textContent = "已持有";
    btn.disabled = true;
    btn.style.background = "#98A2B3";
    btn.style.color = "#fff";
    btn.style.opacity = "0.85";
    btn.style.cursor = "not-allowed";
    btn.dataset.heldGuard = "1";
  }

  function setRowHeldStyle(tr) {
    tr.dataset.alreadyHeld = "1";
    tr.style.background = "rgba(152, 162, 179, 0.08)";
  }

  async function applyDuplicateGuard() {
    const table = getTradeTable();
    if (!table) return;

    const held = await loadHeldStocks();
    if (!held.size) return;

    const headers = getHeaders(table);
    const stockIdx = indexOfHeader(headers, ["股票", "stock"]);
    if (stockIdx < 0) return;

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      if (!stockId || !held.has(stockId)) return;

      const btns = Array.from(tr.querySelectorAll("button"));
      const addBtn = btns.find(b => /加入持倉|加入/.test(b.textContent || ""));
      if (addBtn) setButtonHeld(addBtn);
      setRowHeldStyle(tr);
    });

    updateHint();
  }

  function updateHint() {
    const hint = document.getElementById("tradeplanFilterHint");
    if (!hint) return;
    if (hint.dataset.v354Done === "1") return;
    hint.dataset.v354Done = "1";
    hint.textContent = `${hint.textContent}｜已持有股票會顯示灰色，避免重複加入。`;
  }

  function boot() {
    applyDuplicateGuard();

    new MutationObserver(() => {
      clearTimeout(window.__v354GuardTimer);
      window.__v354GuardTimer = setTimeout(applyDuplicateGuard, 400);
    }).observe(document.body, { childList: true, subtree: true });

    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
