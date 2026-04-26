/*
v3.7.1e_chip_light_ui_patch_position_reverse.js

修正：
1. 今日操作：備註｜籌碼狀態｜加入持倉
2. 持倉監控：備註｜籌碼狀態｜移除

原則：
- 不動主策略
- 不動 trade_plan.csv
- 不動 current_positions.csv
- 不動寫回流程
- 只修前端顯示
*/

(function () {
  const VERSION = "v3.7.1e-chip-light-ui-position-reverse";
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
        } else quote = !quote;
      } else if (ch === "," && !quote) {
        out.push(cur);
        cur = "";
      } else cur += ch;
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
    if (text.includes("🔥") || text.includes("強勢")) return css + "font-weight:900;color:#b42318;";
    if (text.includes("🟢") || text.includes("偏強")) return css + "font-weight:900;color:#087443;";
    if (text.includes("⚠️")) return css + "font-weight:900;color:#b54708;";
    return css + "color:#667085;";
  }

  function getTradeTable() {
    const body = document.getElementById("tradePlanBody");
    return body ? body.closest("table") : null;
  }

  function getPositionTable() {
    const ids = ["positionMonitorBody", "positionBody", "positionsBody", "currentPositionsBody"];
    for (const id of ids) {
      const body = document.getElementById(id);
      if (body) return body.closest("table");
    }

    const candidates = Array.from(document.querySelectorAll("section, .card, div"));
    const sec = candidates.find(el => /持倉監控/.test(el.textContent || "") && el.querySelector("table"));
    return sec ? sec.querySelector("table") : null;
  }

  function removeAllChipColumns(table) {
    if (!table) return;
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const chipIndexes = [];
    Array.from(headerRow.children).forEach((th, i) => {
      if (norm(th.textContent).includes("籌碼狀態")) chipIndexes.push(i);
    });

    chipIndexes.reverse().forEach(i => {
      if (headerRow.children[i]) headerRow.children[i].remove();

      Array.from(table.querySelectorAll("tbody tr")).forEach(tr => {
        if (tr.children[i]) tr.children[i].remove();

        delete tr.dataset.v371Done;
        delete tr.dataset.v371bDone;
        delete tr.dataset.v371cDone;
        delete tr.dataset.v371dDone;
        delete tr.dataset.v371eDone;
        delete tr.dataset.v371cPositionDone;
        delete tr.dataset.v371dPositionDone;
        delete tr.dataset.v371ePositionDone;
      });
    });
  }

  function addTradeHint() {
    const old = document.getElementById("chipLightHint");
    if (old) {
      old.textContent = "v3.7.1e：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";
      return;
    }

    const section = document.getElementById("tradePlanBody")?.closest("section");
    if (!section) return;

    const hint = document.createElement("div");
    hint.id = "chipLightHint";
    hint.style.cssText = "margin:8px 0 12px;color:#667085;font-size:14px;line-height:1.5;";
    hint.textContent = "v3.7.1e：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";

    const wrap = section.querySelector(".table-wrap");
    if (wrap) section.insertBefore(hint, wrap);
    else section.appendChild(hint);
  }

  function ensureChipHeaderBeforeAction(table, actionKeys) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const headers = getHeaders(table);
    if (headers.includes("籌碼狀態")) return;

    const actionIdx = findIndex(headers, actionKeys);
    const th = document.createElement("th");
    th.textContent = "籌碼狀態";
    th.dataset.v371eChip = "1";

    if (actionIdx >= 0 && headerRow.children[actionIdx]) {
      headerRow.insertBefore(th, headerRow.children[actionIdx]);
    } else {
      headerRow.appendChild(th);
    }
  }

  async function applyTradeChip() {
    const table = getTradeTable();
    if (!table) return;

    if (table.dataset.v371eCleaned !== "1") {
      removeAllChipColumns(table);
      table.dataset.v371eCleaned = "1";
    }

    ensureChipHeaderBeforeAction(table, ["加入持倉", "加入"]);

    const headers = getHeaders(table);
    const stockIdx = findIndex(headers, ["股票", "stock", "代號"]);
    const chipIdx = findIndex(headers, ["籌碼狀態"]);
    if (stockIdx < 0 || chipIdx < 0) return;

    const chipMap = await loadChip();

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371eDone === "1") return;

      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      const row = chipMap.get(stockId);
      const text = chipText(row);

      const td = document.createElement("td");
      td.textContent = text;
      td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
      td.style.cssText = chipStyle(text);
      td.dataset.v371eChip = "1";

      if (tr.children[chipIdx]) {
        tr.insertBefore(td, tr.children[chipIdx]);
      } else {
        tr.appendChild(td);
      }

      tr.dataset.v371eDone = "1";
    });

    addTradeHint();
  }

  async function applyPositionChip() {
    const table = getPositionTable();
    if (!table) return;
    if (table === getTradeTable()) return;

    if (table.dataset.v371ePositionCleaned !== "1") {
      removeAllChipColumns(table);
      table.dataset.v371ePositionCleaned = "1";
    }

    // 持倉監控：籌碼狀態插在「移除」前面
    ensureChipHeaderBeforeAction(table, ["移除"]);

    const headers = getHeaders(table);
    const stockIdx = findIndex(headers, ["股票", "stock", "代號"]);
    const chipIdx = findIndex(headers, ["籌碼狀態"]);
    if (stockIdx < 0 || chipIdx < 0) return;

    const chipMap = await loadChip();
    const body = table.querySelector("tbody");
    if (!body) return;

    const rows = Array.from(body.querySelectorAll("tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371ePositionDone === "1") return;

      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      if (!stockId) return;

      const row = chipMap.get(stockId);
      const text = chipText(row);

      const td = document.createElement("td");
      td.textContent = text;
      td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
      td.style.cssText = chipStyle(text);
      td.dataset.v371eChip = "1";

      if (tr.children[chipIdx]) {
        tr.insertBefore(td, tr.children[chipIdx]);
      } else {
        tr.appendChild(td);
      }

      tr.dataset.v371ePositionDone = "1";
    });
  }

  function refresh() {
    applyTradeChip();
    applyPositionChip();
  }

  function boot() {
    refresh();

    new MutationObserver(() => {
      clearTimeout(window.__v371eTimer);
      window.__v371eTimer = setTimeout(refresh, 700);
    }).observe(document.body, { childList: true, subtree: true });

    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
