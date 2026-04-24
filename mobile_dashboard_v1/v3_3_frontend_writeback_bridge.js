/*
v3.3_frontend_writeback_bridge.js

用途：
1. 接通前端按鈕 → GitHub Actions workflow_dispatch
2. 不改 pipeline / Python / bridge
3. token 只存在本機 localStorage，不寫進 repo

使用方式：
- 把本檔內容貼到 mobile_dashboard_v1/app.js 最下方
- 或另存為 mobile_dashboard_v1/v3_3_frontend_writeback_bridge.js
  並在 index.html 的 </body> 前加入：
  <script src="./v3_3_frontend_writeback_bridge.js"></script>
*/

(function () {
  const STORAGE_KEY = "V3_GITHUB_LOCAL_CONFIG";

  function getConfig() {
    let cfg = {};
    try {
      cfg = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
    } catch (e) {
      cfg = {};
    }

    return {
      owner: (cfg.owner || document.querySelector("#githubOwner")?.value || "bichcs5566-alt").trim(),
      repo: (cfg.repo || document.querySelector("#githubRepo")?.value || "V204-app").trim(),
      branch: (cfg.branch || document.querySelector("#githubBranch")?.value || "main").trim(),
      token: (cfg.token || document.querySelector("#githubToken")?.value || "").trim()
    };
  }

  function saveConfigFromInputs() {
    const old = getConfig();
    const cfg = {
      owner: (document.querySelector("#githubOwner")?.value || old.owner || "bichcs5566-alt").trim(),
      repo: (document.querySelector("#githubRepo")?.value || old.repo || "V204-app").trim(),
      branch: (document.querySelector("#githubBranch")?.value || old.branch || "main").trim(),
      token: (document.querySelector("#githubToken")?.value || old.token || "").trim()
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg));
    return cfg;
  }

  function setStatus(msg, isError = false) {
    const targets = [
      "#syncBanner",
      "#githubStatus",
      "#statusText",
      "#writebackStatus"
    ];

    let found = false;
    for (const sel of targets) {
      const el = document.querySelector(sel);
      if (el) {
        el.textContent = msg;
        el.style.color = isError ? "#a22" : "#2f7d32";
        found = true;
      }
    }

    if (!found) {
      console.log(isError ? "[ERROR]" : "[OK]", msg);
    }
  }

  async function dispatchWorkflow(workflowFile, inputs) {
    const cfg = getConfig();

    if (!cfg.token) {
      throw new Error("尚未設定 GitHub token。請先在 GitHub 本機設定區儲存 token。");
    }

    const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/actions/workflows/${workflowFile}/dispatches`;

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${cfg.token}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        ref: cfg.branch || "main",
        inputs
      })
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`GitHub workflow 送出失敗：${res.status} ${text}`);
    }

    return true;
  }

  function normalizeStockId(v) {
    return String(v || "").trim();
  }

  async function addWatchlistFromInput() {
    saveConfigFromInputs();

    const input =
      document.querySelector("#watchStockInput") ||
      document.querySelector("#watchlistStockInput") ||
      document.querySelector("#watchlistInput") ||
      document.querySelector("input[placeholder='股票代號']");

    const stockId = normalizeStockId(input?.value);

    if (!stockId) {
      alert("請輸入股票代號");
      return;
    }

    setStatus(`送出自選股新增：${stockId}...`);

    try {
      await dispatchWorkflow("v3_watchlist_writeback.yml", {
        action: "add",
        stock_id: stockId
      });
      setStatus(`✅ 已送出自選股新增：${stockId}，請等 GitHub Actions 跑完後重新整理。`);
    } catch (e) {
      setStatus(`❌ ${e.message}`, true);
      alert(e.message);
    }
  }

  async function removeWatchlist(stockId) {
    saveConfigFromInputs();

    stockId = normalizeStockId(stockId);
    if (!stockId) {
      alert("找不到要移除的股票代號");
      return;
    }

    const ok = confirm(`確定移除自選股 ${stockId}？`);
    if (!ok) return;

    setStatus(`送出自選股移除：${stockId}...`);

    try {
      await dispatchWorkflow("v3_watchlist_writeback.yml", {
        action: "remove",
        stock_id: stockId
      });
      setStatus(`✅ 已送出自選股移除：${stockId}，請等 GitHub Actions 跑完後重新整理。`);
    } catch (e) {
      setStatus(`❌ ${e.message}`, true);
      alert(e.message);
    }
  }

  async function addPositionFromInput() {
    saveConfigFromInputs();

    const stockInput =
      document.querySelector("#positionStockInput") ||
      document.querySelector("#positionStockId") ||
      document.querySelector("#posStockInput") ||
      document.querySelector("#stockIdInput");

    const sharesInput =
      document.querySelector("#positionSharesInput") ||
      document.querySelector("#positionShares") ||
      document.querySelector("#posSharesInput") ||
      document.querySelector("#sharesInput");

    const costInput =
      document.querySelector("#positionCostInput") ||
      document.querySelector("#positionAvgCost") ||
      document.querySelector("#posCostInput") ||
      document.querySelector("#avgCostInput");

    const stockId = normalizeStockId(stockInput?.value);
    const shares = normalizeStockId(sharesInput?.value);
    const avgCost = normalizeStockId(costInput?.value);

    if (!stockId || !shares || !avgCost) {
      alert("請輸入股票代號、股數、成本");
      return;
    }

    setStatus(`送出持倉新增：${stockId}...`);

    try {
      await dispatchWorkflow("v3_position_writeback.yml", {
        action: "add",
        stock_id: stockId,
        shares: shares,
        avg_cost: avgCost
      });
      setStatus(`✅ 已送出持倉新增：${stockId}，請等 GitHub Actions 跑完後重新整理。`);
    } catch (e) {
      setStatus(`❌ ${e.message}`, true);
      alert(e.message);
    }
  }

  async function removePosition(stockId) {
    saveConfigFromInputs();

    stockId = normalizeStockId(stockId);
    if (!stockId) {
      alert("找不到要移除的股票代號");
      return;
    }

    const ok = confirm(`確定移除持倉 ${stockId}？`);
    if (!ok) return;

    setStatus(`送出持倉移除：${stockId}...`);

    try {
      await dispatchWorkflow("v3_position_writeback.yml", {
        action: "remove",
        stock_id: stockId
      });
      setStatus(`✅ 已送出持倉移除：${stockId}，請等 GitHub Actions 跑完後重新整理。`);
    } catch (e) {
      setStatus(`❌ ${e.message}`, true);
      alert(e.message);
    }
  }

  function guessStockIdFromRow(btn) {
    const row = btn.closest("tr");
    if (!row) return "";
    const firstCell = row.querySelector("td");
    return normalizeStockId(firstCell?.textContent);
  }

  function wireButtons() {
    const addWatchBtn =
      document.querySelector("#addWatchBtn") ||
      document.querySelector("#addWatchlistBtn") ||
      document.querySelector("[data-action='add-watchlist']");

    if (addWatchBtn && !addWatchBtn.dataset.v33Wired) {
      addWatchBtn.dataset.v33Wired = "1";
      addWatchBtn.addEventListener("click", addWatchlistFromInput);
    }

    const addPosBtn =
      document.querySelector("#addPositionBtn") ||
      document.querySelector("#positionAddBtn") ||
      document.querySelector("[data-action='add-position']");

    if (addPosBtn && !addPosBtn.dataset.v33Wired) {
      addPosBtn.dataset.v33Wired = "1";
      addPosBtn.addEventListener("click", addPositionFromInput);
    }

    document.querySelectorAll("[data-action='remove-watchlist'], .remove-watchlist-btn").forEach((btn) => {
      if (btn.dataset.v33Wired) return;
      btn.dataset.v33Wired = "1";
      btn.addEventListener("click", () => {
        const stockId = btn.dataset.stockId || btn.getAttribute("data-stock-id") || guessStockIdFromRow(btn);
        removeWatchlist(stockId);
      });
    });

    document.querySelectorAll("[data-action='remove-position'], .remove-position-btn").forEach((btn) => {
      if (btn.dataset.v33Wired) return;
      btn.dataset.v33Wired = "1";
      btn.addEventListener("click", () => {
        const stockId = btn.dataset.stockId || btn.getAttribute("data-stock-id") || guessStockIdFromRow(btn);
        removePosition(stockId);
      });
    });

    const saveBtn =
      document.querySelector("#saveGithubConfigBtn") ||
      document.querySelector("#saveConfigBtn") ||
      document.querySelector("button[data-action='save-github-config']");

    if (saveBtn && !saveBtn.dataset.v33Wired) {
      saveBtn.dataset.v33Wired = "1";
      saveBtn.addEventListener("click", () => {
        saveConfigFromInputs();
        setStatus("✅ 已儲存本機 GitHub 設定");
      });
    }
  }

  window.v33DispatchWorkflow = dispatchWorkflow;
  window.v33AddWatchlistFromInput = addWatchlistFromInput;
  window.v33RemoveWatchlist = removeWatchlist;
  window.v33AddPositionFromInput = addPositionFromInput;
  window.v33RemovePosition = removePosition;
  window.v33WireButtons = wireButtons;

  document.addEventListener("DOMContentLoaded", () => {
    wireButtons();
    setTimeout(wireButtons, 1000);
    setTimeout(wireButtons, 3000);
  });
})();
