/*
v3.7.1b_chip_light_ui_patch_fixed.js

修正 v3.7.1 欄位錯位問題。

原則：
1. 不動主系統
2. 不動策略
3. 不動 trade_plan.csv
4. 不動持倉寫回
5. 只修正前端顯示

做法：
- 不再把「籌碼狀態」插到備註前面
- 固定把「籌碼狀態」加在表格最右邊
- 若偵測到舊版 v3.7.1 已插入錯位欄位，先清掉舊版標記後重新加
*/

(function () {
  const VERSION = "v3.7.1b-chip-light-ui-fixed";
  const CHIP_PATH = "mobile_dashboard_v1/data/chip_light.csv";

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

  async function loadChip() {
    try {
      const res = await fetch(`${CHIP_PATH}?ts=${Date.now()}`, { cache: "no-store" });
      if (!res.ok) return new Map();
      const rows = parseCSV(await res.text());
      const map = new Map();
      rows.forEach(r => {
        const id = String(r.stock_id || "").trim();
        if (id) map.set(id, r);
      });
      return map;
    } catch (e) {
      console.warn("chip_light load failed", e);
      return new Map();
    }
  }

  function norm(s) {
    return String(s || "").replace(/\s+/g, "").trim();
  }

  function getTable() {
    const body = document.getElementById("tradePlanBody");
    if (!body) return null;
    return body.closest("table");
  }

  function getHeaders(table) {
    return Array.from(table.querySelectorAll("thead th")).map(th => norm(th.textContent));
  }

  function findStockIndex(headers) {
    return headers.findIndex(h =>
      h.includes("股票") ||
      h.includes("stock") ||
      h.includes("代號")
    );
  }

  function removeOldChipColumn(table) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const ths = Array.from(headerRow.children);
    const chipIndexes = [];

    ths.forEach((th, i) => {
      if (norm(th.textContent).includes("籌碼狀態")) chipIndexes.push(i);
    });

    // 從右往左刪，避免 index 位移
    chipIndexes.reverse().forEach(i => {
      if (headerRow.children[i]) headerRow.children[i].remove();

      Array.from(document.querySelectorAll("#tradePlanBody tr")).forEach(tr => {
        if (tr.children[i]) tr.children[i].remove();
        delete tr.dataset.v371Done;
        delete tr.dataset.v371bDone;
      });
    });
  }

  function ensureRightColumn(table) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const headers = getHeaders(table);
    if (headers.some(h => h === "籌碼狀態")) return;

    const th = document.createElement("th");
    th.textContent = "籌碼狀態";
    th.dataset.v371bChip = "1";
    headerRow.appendChild(th);
  }

  function chipText(row) {
    if (!row) return "—";
    const label = row.chip_label || "普通";
    const tags = row.chip_tags || "";
    const note = row.chip_note || "";
    if (tags && tags !== "普通") return `${label}｜${tags}`;
    if (note) return `${label}｜${note}`;
    return label;
  }

  function chipStyle(text) {
    if (text.includes("🔥") || text.includes("強勢")) {
      return "font-weight:900;color:#b42318;white-space:nowrap;";
    }
    if (text.includes("🟢") || text.includes("偏強")) {
      return "font-weight:900;color:#087443;white-space:nowrap;";
    }
    if (text.includes("⚠️")) {
      return "font-weight:900;color:#b54708;white-space:nowrap;";
    }
    return "color:#667085;white-space:nowrap;";
  }

  function addHint() {
    if (document.getElementById("chipLightHint")) return;

    const section = document.getElementById("tradePlanBody")?.closest("section");
    if (!section) return;

    const hint = document.createElement("div");
    hint.id = "chipLightHint";
    hint.style.cssText = "margin:8px 0 12px;color:#667085;font-size:14px;line-height:1.5;";
    hint.textContent = "v3.7.1b：籌碼狀態為右側輕量標記，只輔助判斷，不改變原本買賣名單。";

    const wrap = section.querySelector(".table-wrap");
    if (wrap) section.insertBefore(hint, wrap);
    else section.appendChild(hint);
  }

  async function applyChipLabels() {
    const table = getTable();
    if (!table) return;

    // 第一次執行時，先把舊版錯位欄位清掉
    if (table.dataset.v371bCleaned !== "1") {
      removeOldChipColumn(table);
      table.dataset.v371bCleaned = "1";
    }

    ensureRightColumn(table);

    const headers = getHeaders(table);
    const stockIdx = findStockIndex(headers);
    if (stockIdx < 0) return;

    const chipMap = await loadChip();

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371bDone === "1") return;

      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      const row = chipMap.get(stockId);
      const text = chipText(row);

      const td = document.createElement("td");
      td.textContent = text;
      td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
      td.style.cssText = chipStyle(text);
      td.dataset.v371bChip = "1";

      tr.appendChild(td);
      tr.dataset.v371bDone = "1";
    });
  }

  function refresh() {
    addHint();
    applyChipLabels();
  }

  function boot() {
    refresh();

    new MutationObserver(() => {
      clearTimeout(window.__v371bTimer);
      window.__v371bTimer = setTimeout(refresh, 500);
    }).observe(document.body, { childList: true, subtree: true });

    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
