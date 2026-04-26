/*
v3.7.1g_chip_light_ui_patch_path_fixed.js

最終修正重點：
1. 自動嘗試多個 chip_light.csv 路徑
2. 修正 GitHub Pages / repo page / root page 路徑差異
3. 今日操作：備註｜籌碼狀態｜加入持倉
4. 持倉監控：備註｜籌碼狀態｜移除
5. 按鈕永遠固定右邊
6. 不動主策略、不動資料、不動寫回，只修前端顯示
*/

(function () {
  const VERSION = "v3.7.1g-chip-light-ui-path-fixed";

  function getCandidateChipPaths() {
    const path = window.location.pathname || "/";
    const parts = path.split("/").filter(Boolean);
    const repo = parts.length ? parts[0] : "";

    const candidates = [
      "./mobile_dashboard_v1/data/chip_light.csv",
      "mobile_dashboard_v1/data/chip_light.csv",
      "./data/chip_light.csv",
      "data/chip_light.csv"
    ];

    if (repo) {
      candidates.push(`/${repo}/mobile_dashboard_v1/data/chip_light.csv`);
      candidates.push(`/${repo}/data/chip_light.csv`);
    }

    candidates.push("/mobile_dashboard_v1/data/chip_light.csv");

    return [...new Set(candidates)];
  }

  let CHIP_LOADED_FROM = "";
  let CHIP_LOAD_ERROR = "";

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
    const headers = splitLine(lines[0]).map(h => h.trim().replace(/^\ufeff/, ""));
    return lines.slice(1).map(line => {
      const vals = splitLine(line);
      const row = {};
      headers.forEach((h, i) => row[h] = vals[i] || "");
      return row;
    });
  }

  function cleanId(v) {
    return String(v || "")
      .replace(/^\ufeff/, "")
      .replace(/[^\dA-Za-z]/g, "")
      .trim();
  }

  async function loadChip() {
    const paths = getCandidateChipPaths();
    const errors = [];

    for (const p of paths) {
      try {
        const joiner = p.includes("?") ? "&" : "?";
        const url = `${p}${joiner}v=${Date.now()}`;
        const res = await fetch(url, { cache: "no-store" });

        if (!res.ok) {
          errors.push(`${p} -> ${res.status}`);
          continue;
        }

        const text = await res.text();
        const rows = parseCSV(text);

        if (!rows.length) {
          errors.push(`${p} -> empty`);
          continue;
        }

        const map = new Map();
        rows.forEach(r => {
          const id = cleanId(r.stock_id || r.stock || r.symbol || r.code);
          if (id) map.set(id, r);
        });

        if (map.size > 0) {
          CHIP_LOADED_FROM = p;
          CHIP_LOAD_ERROR = "";
          console.log("chip_light loaded from:", p, "rows:", map.size);
          return map;
        }

        errors.push(`${p} -> no stock_id`);
      } catch (e) {
        errors.push(`${p} -> ${e.message || e}`);
      }
    }

    CHIP_LOADED_FROM = "";
    CHIP_LOAD_ERROR = errors.join(" | ");
    console.warn("chip_light load failed:", CHIP_LOAD_ERROR);
    return new Map();
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
        delete tr.dataset.v371gDone;
        delete tr.dataset.v371cPositionDone;
        delete tr.dataset.v371dPositionDone;
        delete tr.dataset.v371ePositionDone;
        delete tr.dataset.v371fPositionDone;
        delete tr.dataset.v371gPositionDone;
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
    th.dataset.v371gChip = "1";

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
      const id = cleanId(tr.children[stockIdx].textContent);
      if (id) return id;
    }

    for (const td of Array.from(tr.children)) {
      const id = cleanId(td.textContent);
      if (/^[0-9]{4}[A-Z]?$/.test(id)) return id;
    }

    return "";
  }

  function makeChipTd(stockId, chipMap) {
    const key = cleanId(stockId);
    const row = chipMap.get(key);
    const text = chipText(row);

    const td = document.createElement("td");
    td.textContent = text;
    td.title = row ? (row.chip_note || text) : `尚無籌碼標記：${key}`;
    td.style.cssText = chipStyle(text);
    td.dataset.v371gChip = "1";
    return td;
  }

  function addTradeHint(chipMap) {
    const section = document.getElementById("tradePlanBody")?.closest("section");
    if (!section) return;

    let hint = document.getElementById("chipLightHint");
    if (!hint) {
      hint = document.createElement("div");
      hint.id = "chipLightHint";
      hint.style.cssText = "margin:8px 0 12px;color:#667085;font-size:14px;line-height:1.5;";
      const wrap = section.querySelector(".table-wrap");
      if (wrap) section.insertBefore(hint, wrap);
      else section.appendChild(hint);
    }

    if (CHIP_LOADED_FROM) {
      hint.textContent = `v3.7.1g：籌碼狀態已載入（${chipMap.size}檔），只輔助判斷，不改變原本買賣名單。`;
    } else {
      hint.textContent = "v3.7.1g：籌碼資料尚未載入，請確認 chip_light.csv 路徑或 Pages 快取。";
    }
  }

  async function applyTradeChip(chipMap) {
    const table = getTradeTable();
    if (!table) return;

    if (table.dataset.v371gCleaned !== "1") {
      removeAllChipColumns(table);
      table.dataset.v371gCleaned = "1";
    }

    ensureChipHeaderBeforeAction(table, ["加入持倉", "加入"]);

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371gDone === "1") return;

      const stockId = getStockIdFromRow(tr, table);
      const chipTd = makeChipTd(stockId, chipMap);

      const actionCell = findActionCellByButtonText(tr, ["加入持倉", "加入"]);
      if (actionCell) tr.insertBefore(chipTd, actionCell);
      else tr.appendChild(chipTd);

      tr.dataset.v371gDone = "1";
    });

    addTradeHint(chipMap);
  }

  async function applyPositionChip(chipMap) {
    const table = getPositionTable();
    if (!table) return;
    if (table === getTradeTable()) return;

    if (table.dataset.v371gPositionCleaned !== "1") {
      removeAllChipColumns(table);
      table.dataset.v371gPositionCleaned = "1";
    }

    ensureChipHeaderBeforeAction(table, ["移除"]);

    const body = table.querySelector("tbody");
    if (!body) return;

    const rows = Array.from(body.querySelectorAll("tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371gPositionDone === "1") return;

      const stockId = getStockIdFromRow(tr, table);
      const chipTd = makeChipTd(stockId, chipMap);

      const removeCell = findActionCellByButtonText(tr, ["移除"]);
      if (removeCell) tr.insertBefore(chipTd, removeCell);
      else tr.appendChild(chipTd);

      tr.dataset.v371gPositionDone = "1";
    });
  }

  let chipCache = null;
  let chipCacheTime = 0;

  async function getChipMap() {
    const now = Date.now();
    if (chipCache && now - chipCacheTime < 30000) return chipCache;
    chipCache = await loadChip();
    chipCacheTime = now;
    return chipCache;
  }

  async function refresh() {
    const chipMap = await getChipMap();
    await applyTradeChip(chipMap);
    await applyPositionChip(chipMap);
  }

  function boot() {
    refresh();

    new MutationObserver(() => {
      clearTimeout(window.__v371gTimer);
      window.__v371gTimer = setTimeout(refresh, 700);
    }).observe(document.body, { childList: true, subtree: true });

    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

