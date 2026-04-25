/*
v3.7.1_chip_light_ui_patch.js
只新增籌碼輕量標記顯示，不動主系統。
*/

(function () {
  const VERSION = "v3.7.1-chip-light-ui";
  const CHIP_PATH = "mobile_dashboard_v1/data/chip_light.csv";

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

  function norm(s) { return String(s || "").replace(/\s+/g, "").trim(); }
  function getTable() {
    const body = document.getElementById("tradePlanBody");
    if (!body) return null;
    return body.closest("table");
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

  function ensureColumn(table) {
    const headerRow = table.querySelector("thead tr");
    if (!headerRow) return;
    const headers = getHeaders(table);
    if (headers.some(h => h.includes("籌碼狀態"))) return;
    const noteIdx = idx(headers, ["備註"]);
    const th = document.createElement("th");
    th.textContent = "籌碼狀態";
    if (noteIdx >= 0 && headerRow.children[noteIdx]) headerRow.insertBefore(th, headerRow.children[noteIdx]);
    else headerRow.appendChild(th);
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
    if (text.includes("🔥") || text.includes("強勢")) return "font-weight:900;color:#b42318;white-space:nowrap;";
    if (text.includes("🟢") || text.includes("偏強")) return "font-weight:900;color:#087443;white-space:nowrap;";
    if (text.includes("⚠️")) return "font-weight:900;color:#b54708;white-space:nowrap;";
    return "color:#667085;white-space:nowrap;";
  }

  async function applyChipLabels() {
    const table = getTable();
    if (!table) return;
    const chipMap = await loadChip();
    ensureColumn(table);
    const headers = getHeaders(table);
    const stockIdx = idx(headers, ["股票", "stock"]);
    if (stockIdx < 0) return;

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v371Done === "1") return;
      const cells = Array.from(tr.children);
      const stockId = String(cells[stockIdx]?.textContent || "").trim();
      const row = chipMap.get(stockId);
      const text = chipText(row);
      const td = document.createElement("td");
      td.textContent = text;
      td.title = row ? (row.chip_note || text) : "尚無籌碼標記";
      td.style.cssText = chipStyle(text);
      const currentHeaders = getHeaders(table);
      const currentNoteIdx = idx(currentHeaders, ["備註"]);
      if (currentNoteIdx >= 0 && tr.children[currentNoteIdx]) tr.insertBefore(td, tr.children[currentNoteIdx]);
      else tr.appendChild(td);
      tr.dataset.v371Done = "1";
    });
  }

  function addHint() {
    if (document.getElementById("chipLightHint")) return;
    const section = document.getElementById("tradePlanBody")?.closest("section");
    if (!section) return;
    const hint = document.createElement("div");
    hint.id = "chipLightHint";
    hint.style.cssText = "margin:8px 0 12px;color:#667085;font-size:14px;line-height:1.5;";
    hint.textContent = "v3.7.1：籌碼狀態為輕量標記，只輔助判斷，不改變原本買賣名單。";
    const wrap = section.querySelector(".table-wrap");
    if (wrap) section.insertBefore(hint, wrap);
    else section.appendChild(hint);
  }

  function refresh() {
    addHint();
    applyChipLabels();
  }

  function boot() {
    refresh();
    new MutationObserver(() => {
      clearTimeout(window.__v371Timer);
      window.__v371Timer = setTimeout(refresh, 500);
    }).observe(document.body, { childList: true, subtree: true });
    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
