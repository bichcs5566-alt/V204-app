/*
v3.7.1c_chip_light_ui_patch_fixed_order_and_position.js

目的：
修正 v3.7.1b 欄位順序：
正確順序：
  備註 ｜ 籌碼狀態 ｜ 加入持倉

並新增：
  持倉監控表格也顯示「籌碼狀態」

原則：
1. 不動主系統
2. 不動策略
3. 不動 trade_plan.csv
4. 不動 current_positions.csv
5. 不動寫回流程
6. 只新增 / 修正前端顯示
*/

(function () {
  const VERSION = "v3.7.1c-chip-light-ui-fixed-order-and-position";
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

  function getHeaders(table) {
    return Array.from(table.querySelectorAll("thead th")).map(th => norm(th.textContent));
  }

  function findIndex(headers, keys) {
    for (const k of keys) {
      const i = headers.findIndex(h => h.includes(k));
      if (i >= 0) return i;
    }
    return -1;
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
    let css = "white-space:nowrap;min-width:120px;";
    if (text.includes("🔥") || text.includes("強勢")) {
      return css + "font-weight:900;color:#b42318;";
    }
    if (text.includes("🟢") || text.includes("偏強")) {
      return css + "font-weight:900;color:#087443;";
    }
    if (text.includes("⚠️")) {
      return css + "font-weight:900;color:#b54708;";
    }
    return css + "color:#667085;";
  }

  function getTradeTable() {
    const body = document.getElementById("tradePlanBody");
    if (!body) return null;
    return body.closest("table");
  }

  function getPositionTable() {
    // 優先用常見 tbody id
    const ids = [
      "positionMonitorBody",
      "positionBody",
      "positionsBody",
      "currentPositionsBody"
    ];

    for (const id of ids) {
      const body = document.getElementById(id);
      if (body) return body.closest("table");
    }

    // fallback：找含「持倉監控」區塊下的第一個表格
    const sections = Array.from(document.querySelectorAll("section, .card, div"));
    const sec = sections.find(el => /持倉監控/.test(el.textContent || "") && el.querySelector("table"));
    return sec ? sec.querySelector("table") : null;
  }

  function removeChipColumns(table, bodySelector) {
    if (!table) return;

    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const ths = Array.from(headerRow.children);
    const chipIndexes = [];

    ths.forEach((th, i) => {
      if (norm(th.textContent).includes("籌碼狀態")) chipIndexes.push(i);
    });

    chipIndexes.reverse().forEach(i => {
      if (headerRow.children[i]) headerRow.children[i].remove();

      const rows = bodySelector
        ? Array.from(document.querySelectorAll(`${bodySelector} tr`))
        : Array.from(table.querySelectorAll("tbody tr"));

      rows.forEach(tr => {
        if (tr.children[i]) tr.children[i].remove();
        delete tr.dataset.v371Done;
        delete tr.dataset.v371bDone;
        delete tr.dataset.v371cDone;
      });
    });
  }

  function ensureChipBeforeAction(table) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const headers = getHeaders(table);
    if (headers.includes("籌碼狀態")) return;

    const actionIdx = findIndex(headers, ["加入持倉", "加入", "操作", "移除"]);
    const th = document.createElement("th");
    th.textContent = "籌碼狀態";
    th.dataset.v371cChip = "1";

    if (actionIdx >= 0 && headerRow.children[actionIdx]) {
      headerRow.insertBefore(th, headerRow.children[actionIdx]);
    } else {
      headerRow.appendChild(th);
    }
  }

  function addTradeHint() {
    const old = document.getElementById("chipLightHint");
    if (old) {
      old.textContent = "v3.7.1c：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";
      return;
    }

    const section = document.getElementById("tradePlanBody")?.closest("section");
    if (!section) return;

    const hint = document.createElement("div");
    hint.id = "chipLightHint";
    hint.style.cssText = "margin:8px 0 12px;color:#667085;font-size:14px;line-height:1.5;";
    hint.textContent = "v3.7.1c：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";

    const wrap = section.querySelector(".table-wrap");
    if (wrap) section.insertBefore(hint, wrap);
    else section.appendChild(hint);
  }

  async function applyTradeChip() {
    const table = getTradeTable();
    if (!table) return;

    if (table.dataset.v371cCleaned !== "1") {
      removeChipColumns(table, "#tradePlanBody");
      table.dataset.v371cCleaned = "1";
    }

    ensureChipBeforeAction(table);

    const headers = getHeaders(table);
    const stockIdx = findIndex(headers, ["股票", "stock", "代號"]);
    const actionIdx = findIndex(headers, ["加入持倉", "加入"]);

    if (stockIdx < 0) return;

    const chipMap = await loadChip();

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371cDone === "1") return;

      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      const row = chipMap.get(stockId);
      const text = chipText(row);

      const td = document.createElement("td");
      td.textContent = text;
      td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
      td.style.cssText = chipStyle(text);
      td.dataset.v371cChip = "1";

      const currentHeaders = getHeaders(table);
      const currentActionIdx = findIndex(currentHeaders, ["加入持倉", "加入"]);

      if (currentActionIdx >= 0 && tr.children[currentActionIdx]) {
        tr.insertBefore(td, tr.children[currentActionIdx]);
      } else {
        tr.appendChild(td);
      }

      tr.dataset.v371cDone = "1";
    });

    addTradeHint();
  }

  function findPositionBody(table) {
    if (!table) return null;
    return table.querySelector("tbody");
  }

  function ensurePositionChipColumn(table) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const headers = getHeaders(table);
    if (headers.includes("籌碼狀態")) return;

    const actionIdx = findIndex(headers, ["移除", "操作", "動作"]);
    const th = document.createElement("th");
    th.textContent = "籌碼狀態";
    th.dataset.v371cChip = "1";

    if (actionIdx >= 0 && headerRow.children[actionIdx]) {
      headerRow.insertBefore(th, headerRow.children[actionIdx]);
    } else {
      headerRow.appendChild(th);
    }
  }

  async function applyPositionChip() {
    const table = getPositionTable();
    if (!table) return;

    // 避免誤抓到今日操作表
    if (table === getTradeTable()) return;

    if (table.dataset.v371cPositionCleaned !== "1") {
      removeChipColumns(table, null);
      table.dataset.v371cPositionCleaned = "1";
    }

    ensurePositionChipColumn(table);

    const body = findPositionBody(table);
    if (!body) return;

    const headers = getHeaders(table);
    const stockIdx = findIndex(headers, ["股票", "stock", "代號"]);
    if (stockIdx < 0) return;

    const chipMap = await loadChip();

    const rows = Array.from(body.querySelectorAll("tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371cPositionDone === "1") return;

      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      if (!stockId) return;

      const row = chipMap.get(stockId);
      const text = chipText(row);

      const td = document.createElement("td");
      td.textContent = text;
      td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
      td.style.cssText = chipStyle(text);
      td.dataset.v371cChip = "1";

      const currentHeaders = getHeaders(table);
      const actionIdx = findIndex(currentHeaders, ["移除", "操作", "動作"]);

      if (actionIdx >= 0 && tr.children[actionIdx]) {
        tr.insertBefore(td, tr.children[actionIdx]);
      } else {
        tr.appendChild(td);
      }

      tr.dataset.v371cPositionDone = "1";
    });
  }

  function refresh() {
    applyTradeChip();
    applyPositionChip();
  }

  function boot() {
    refresh();

    new MutationObserver(() => {
      clearTimeout(window.__v371cTimer);
      window.__v371cTimer = setTimeout(refresh, 600);
    }).observe(document.body, { childList: true, subtree: true });

    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
