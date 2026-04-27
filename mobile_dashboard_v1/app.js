/*
v3.5.3_tradeplan_inline_filter_add.js

目的：
只修「今日操作」區塊，不動 v1、不動策略、不動 yml、不改 app.js。

你要的效果：
1. 今日操作表格上方新增「價位分層下拉」
2. 選價位後，只顯示該分層股票
3. 每一列右側新增「加入持倉」按鈕
4. 按下後直接把該股票填入持倉區：
   - 股票代號 = 今日操作該列股票
   - 成本 = 參考價格
   - 股數 = 建議金額 / 參考價格，取整張 1000 股，最低 1000
5. 自動觸發原本的「加入 / 更新持倉」按鈕
6. 不自己讀 trade_plan.csv，避免路徑錯誤
*/

(function () {
  const VERSION = "v3.5.3-tradeplan-inline-filter-add";

  function normText(s) {
    return String(s || "").replace(/\s+/g, "").trim();
  }

  function parseNumber(s) {
    const n = Number(String(s || "").replace(/,/g, "").replace(/[^\d.\-]/g, ""));
    return Number.isFinite(n) ? n : 0;
  }

  function getTradeTable() {
    const body = document.getElementById("tradePlanBody");
    if (!body) return null;
    return body.closest("table");
  }

  function getTradeSection() {
    const table = getTradeTable();
    if (!table) return null;
    return table.closest("section") || table.closest(".card");
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

  function ensureFilterPanel() {
    const section = getTradeSection();
    if (!section) return;

    if (document.getElementById("tradeplan-inline-filter-panel")) return;

    const panel = document.createElement("div");
    panel.id = "tradeplan-inline-filter-panel";
    panel.style.cssText = [
      "margin:12px 0 16px",
      "padding:14px",
      "border-radius:18px",
      "background:#f7f8fa",
      "border:1px solid rgba(16,24,40,.08)"
    ].join(";");

    panel.innerHTML = `
      <div style="font-weight:900;font-size:18px;margin-bottom:10px;">🔎 價位快速篩選</div>
      <select id="tradeplanTierFilter" style="width:100%;padding:14px;border-radius:14px;border:1px solid #d0d5dd;font-size:16px;background:#fff;">
        <option value="ALL">全部價位</option>
      </select>
      <div id="tradeplanFilterHint" style="font-size:14px;color:#667085;margin-top:8px;">可依價位分層快速找股票，並在名單右側直接加入持股。</div>
    `;

    const wrap = section.querySelector(".table-wrap");
    if (wrap) section.insertBefore(panel, wrap);
    else section.appendChild(panel);

    const sel = panel.querySelector("#tradeplanTierFilter");
    sel.addEventListener("change", applyTierFilter);
  }

  function ensureAddColumn() {
    const table = getTradeTable();
    if (!table) return;

    const theadRow = table.querySelector("thead tr");
    if (!theadRow) return;

    const headers = getHeaders(table);
    const hasAdd = headers.some(h => h.includes("加入持倉"));
    if (!hasAdd) {
      const th = document.createElement("th");
      th.textContent = "加入持倉";
      theadRow.appendChild(th);
    }
  }

  function fillTierOptions() {
    const table = getTradeTable();
    const sel = document.getElementById("tradeplanTierFilter");
    if (!table || !sel) return;

    const headers = getHeaders(table);
    const tierIdx = indexOfHeader(headers, ["價格分層", "分層"]);
    if (tierIdx < 0) return;

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    const tiers = Array.from(new Set(rows.map(tr => {
      const cells = Array.from(tr.children);
      return (cells[tierIdx] && cells[tierIdx].textContent.trim()) || "未分類";
    }))).filter(Boolean);

    const current = sel.value || "ALL";
    sel.innerHTML = `<option value="ALL">全部價位（${rows.length}檔）</option>` +
      tiers.map(t => {
        const count = rows.filter(tr => {
          const cells = Array.from(tr.children);
          return ((cells[tierIdx] && cells[tierIdx].textContent.trim()) || "未分類") === t;
        }).length;
        return `<option value="${t}">${t}（${count}檔）</option>`;
      }).join("");

    if (Array.from(sel.options).some(o => o.value === current)) sel.value = current;
  }

  function computeShares(amount, refPrice) {
    if (!amount || !refPrice) return 1000;
    const raw = Math.floor(amount / refPrice);
    const lot = Math.floor(raw / 1000) * 1000;
    return Math.max(lot, 1000);
  }

  function ensureRowButtons() {
    const table = getTradeTable();
    if (!table) return;

    const headers = getHeaders(table);
    const stockIdx = indexOfHeader(headers, ["股票", "stock"]);
    const tierIdx = indexOfHeader(headers, ["價格分層", "分層"]);
    const refIdx = indexOfHeader(headers, ["參考價格", "價格"]);
    const amountIdx = indexOfHeader(headers, ["建議金額", "金額"]);

    if (stockIdx < 0) return;

    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    rows.forEach(tr => {
      if (tr.dataset.v353Ready === "1") return;
      tr.dataset.v353Ready = "1";

      const cells = Array.from(tr.children);
      const stockId = cells[stockIdx]?.textContent.trim() || "";
      const tier = tierIdx >= 0 ? (cells[tierIdx]?.textContent.trim() || "") : "";
      const refPrice = refIdx >= 0 ? parseNumber(cells[refIdx]?.textContent) : 0;
      const amount = amountIdx >= 0 ? parseNumber(cells[amountIdx]?.textContent) : 0;
      const shares = computeShares(amount, refPrice);

      const td = document.createElement("td");
      const btn = document.createElement("button");
      btn.textContent = "加入持倉";
      btn.style.cssText = [
        "padding:10px 14px",
        "border-radius:14px",
        "border:0",
        "background:#087443",
        "color:#fff",
        "font-weight:900",
        "font-size:15px",
        "white-space:nowrap"
      ].join(";");

      btn.addEventListener("click", function () {
        quickFillAndSubmit({ stockId, tier, refPrice, shares });
      });

      td.appendChild(btn);
      tr.appendChild(td);
    });
  }

  function quickFillAndSubmit({ stockId, refPrice, shares }) {
    const stockInput = document.getElementById("positionStockInput");
    const sharesInput = document.getElementById("positionSharesInput");
    const costInput = document.getElementById("positionCostInput");
    const addBtn = document.getElementById("addPositionBtn");

    if (!stockInput || !sharesInput || !costInput || !addBtn) {
      alert("找不到持倉輸入區，請確認目前是 v3.4 版 index.html。");
      return;
    }

    if (!stockId) {
      alert("股票代號空白，無法加入持股。");
      return;
    }

    if (!refPrice || refPrice <= 0) {
      alert(`${stockId} 參考價格無效，無法自動帶入成本。`);
      return;
    }

    stockInput.value = stockId;
    sharesInput.value = String(shares || 1000);
    costInput.value = String(refPrice);

    const msg = `已帶入 ${stockId}｜股數 ${shares || 1000}｜成本 ${refPrice}，準備寫回持倉。`;
    const sync = document.getElementById("syncBanner");
    if (sync) {
      sync.textContent = msg;
      sync.style.color = "#2f6b2f";
    }

    addBtn.click();
  }

  function applyTierFilter() {
    const table = getTradeTable();
    const sel = document.getElementById("tradeplanTierFilter");
    const hint = document.getElementById("tradeplanFilterHint");
    if (!table || !sel) return;

    const headers = getHeaders(table);
    const tierIdx = indexOfHeader(headers, ["價格分層", "分層"]);
    if (tierIdx < 0) return;

    const want = sel.value || "ALL";
    const rows = Array.from(document.querySelectorAll("#tradePlanBody tr"))
      .filter(tr => !tr.querySelector(".empty"));

    let shown = 0;
    rows.forEach(tr => {
      const cells = Array.from(tr.children);
      const tier = (cells[tierIdx] && cells[tierIdx].textContent.trim()) || "未分類";
      const show = want === "ALL" || tier === want;
      tr.style.display = show ? "" : "none";
      if (show) shown++;
    });

    if (hint) {
      hint.textContent = want === "ALL"
        ? `目前顯示全部 ${shown} 檔，可直接按右側「加入持倉」。`
        : `目前顯示 ${want}：${shown} 檔，可直接按右側「加入持倉」。`;
    }
  }

  function removeOldPanelIfExists() {
    const old = document.getElementById("tradeplan-quick-add-panel");
    if (old) old.remove();
  }

  function refresh() {
    removeOldPanelIfExists();
    ensureFilterPanel();
    ensureAddColumn();
    fillTierOptions();
    ensureRowButtons();
    applyTierFilter();
  }

  function boot() {
    refresh();
    new MutationObserver(() => {
      clearTimeout(window.__v353TradeTimer);
      window.__v353TradeTimer = setTimeout(refresh, 250);
    }).observe(document.body, { childList: true, subtree: true });
    console.log(`${VERSION} loaded`);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
