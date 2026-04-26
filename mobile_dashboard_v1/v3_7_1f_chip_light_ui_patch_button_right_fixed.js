/*
v3.7.1f_chip_light_ui_patch_button_right_fixed.js

修正：
1. 今日操作：備註｜籌碼狀態｜加入持倉
2. 持倉監控：備註｜籌碼狀態｜移除
3. 按鈕永遠固定在最右邊
4. 不再用猜 index 的方式插入資料列
5. 直接尋找「加入持倉 / 移除」按鈕所在 td，再把籌碼欄插在按鈕 td 前面

原則：
- 不動主策略
- 不動 trade_plan.csv
- 不動 current_positions.csv
- 不動寫回流程
- 只修前端顯示
*/

(function () {
  const VERSION = "v3.7.1f-chip-light-ui-button-right-fixed";
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
        delete tr.dataset.v371fDone;
        delete tr.dataset.v371cPositionDone;
        delete tr.dataset.v371dPositionDone;
        delete tr.dataset.v371ePositionDone;
        delete tr.dataset.v371fPositionDone;
      });
    });
  }

  function ensureChipHeaderBeforeAction(table, actionKeys) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;

    const headers = getHeaders(table);
    if (headers.includes("籌碼狀態")) return;

    const actionIdx = findIndex(headers, actionKeys);
    const th = document.createElement("th");
    th.textContent = "籌碼狀態";
    th.dataset.v371fChip = "1";

    if (actionIdx >= 0 && headerRow.children[actionIdx]) {
      headerRow.insertBefore(th, headerRow.children[actionIdx]);
    } else {
      headerRow.appendChild(th);
    }
  }

  function findActionCellByButtonText(tr, texts) {
    const cells = Array.from(tr.children);
    for (const td of cells) {
      const t = norm(td.textContent);
      if (texts.some(x => t.includes(x))) return td;
      const btn = td.querySelector("button");
      if (btn) {
        const bt = norm(btn.textContent);
        if (texts.some(x => bt.includes(x))) return td;
      }
    }
    return null;
  }

  function getStockIdFromRow(tr, table) {
    const headers = getHeaders(table);
    const stockIdx = findIndex(headers, ["股票", "stock", "代號"]);
    if (stockIdx >= 0 && tr.children[stockIdx]) {
      return String(tr.children[stockIdx].textContent || "").trim();
    }

    // fallback：找第一個像台股代號的格子
    for (const td of Array.from(tr.children)) {
      const text = String(td.textContent || "").trim();
      if (/^[0-9]{4}[A-Z]?$/.test(text)) return text;
    }

    return "";
  }

  function makeChipTd(stockId, chipMap) {
    const row = chipMap.get(stockId);
    const text = chipText(row);

    const td = document.createElement("td");
    td.textContent = text;
    td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
    td.style.cssText = chipStyle(text);
    td.dataset.v371fChip = "1";
    return td;
  }

  function addTradeHint() {
    const old = document.getElementById("chipLightHint");
    if (old) {
      old.textContent = "v3.7.1f：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";
      return;
    }

    const section = document.getElementById("tradePlanBody")?.closest("section");
    if (!section) return;

    const hint = document.createElement("div");
    hint.id = "chipLightHint";
    hint.style.cssText = "margin:8px 0 12px;color:#667085;font-size:14px;line-height:1.5;";
    hint.textContent = "v3.7.1f：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";

    const wrap = section.querySelector(".table-wrap");
    if (wrap) section.insertBefore(hint, wrap);
    else section.appendChild(hint);
  }

  async function applyTradeChip() {
    const table = getTradeTable();
    if (!table) return;

    if (table.dataset.v371fCleaned !== "1") {
      removeAllChipColumns(table);
      table.dataset.v371fCleaned = "1";
    }

    ensureChipHeaderBeforeAction(table, ["加入持倉", "加入"]);

    const chipMap = await loadChip();

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371fDone === "1") return;

      const stockId = getStockIdFromRow(tr, table);
      const chipTd = makeChipTd(stockId, chipMap);

      // 直接找「加入持倉」按鈕所在格，插在它前面，保證按鈕留在右邊
      const actionCell = findActionCellByButtonText(tr, ["加入持倉", "加入"]);
      if (actionCell) {
        tr.insertBefore(chipTd, actionCell);
      } else {
        tr.appendChild(chipTd);
      }

      tr.dataset.v371fDone = "1";
    });

    addTradeHint();
  }

  async function applyPositionChip() {
    const table = getPositionTable();
    if (!table) return;
    if (table === getTradeTable()) return;

    if (table.dataset.v371fPositionCleaned !== "1") {
      removeAllChipColumns(table);
      table.dataset.v371fPositionCleaned = "1";
    }

    ensureChipHeaderBeforeAction(table, ["移除"]);

    const chipMap = await loadChip();
    const body = table.querySelector("tbody");
    if (!body) return;

    const rows = Array.from(body.querySelectorAll("tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371fPositionDone === "1") return;

      const stockId = getStockIdFromRow(tr, table);
      if (!stockId) return;

      const chipTd = makeChipTd(stockId, chipMap);

      // 直接找「移除」按鈕所在格，插在它前面，保證移除按鈕留在右邊
      const removeCell = findActionCellByButtonText(tr, ["移除"]);
      if (removeCell) {
        tr.insertBefore(chipTd, removeCell);
      } else {
        tr.appendChild(chipTd);
      }

      tr.dataset.v371fPositionDone = "1";
    });
  }

  function refresh() {
    applyTradeChip();
    applyPositionChip();
  }

  function boot() {
    refresh();

    new MutationObserver(() => {
      clearTimeout(window.__v371fTimer);
      window.__v371fTimer = setTimeout(refresh, 700);
    }).observe(document.body, { childList: true, subtree: true });

    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
