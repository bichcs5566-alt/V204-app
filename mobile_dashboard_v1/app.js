// 🔥 掃描列表 + 展開詳情版（精簡高度）

function renderFinalActions(rows) {
  const container = document.getElementById("finalActionList");

  container.innerHTML = rows.map((row, idx) => {
    const action = row.final_action || row.action || "WATCH";

    return `
      <div class="scan-item ${action}" onclick="toggleDetail(${idx})">
        
        <div class="scan-main">
          <div class="scan-action">${action}</div>
          <div class="scan-stock">${row.stock_id}</div>
          <div class="scan-score">${row.score}</div>
          <div class="scan-entry">${row.entry_type || "--"}</div>
          <div class="scan-price">${row.close || "--"}</div>
        </div>

        <div class="scan-detail" id="detail-${idx}">
          <div class="detail-grid">
            <div><span>來源</span><b>${row.source}</b></div>
            <div><span>策略層</span><b>${row.bucket}</b></div>
            <div><span>進場型態</span><b>${row.entry_type}</b></div>
            <div><span>參考價</span><b>${row.close}</b></div>
            <div><span>建議金額</span><b>${row.suggested_amount || "--"}</b></div>
            <div><span>目標權重</span><b>${row.target_weight || "--"}</b></div>
          </div>

          <div class="detail-text">
            <b>原因</b>
            <p>${row.reason || "--"}</p>
          </div>

          <div class="detail-text">
            <b>系統提示</b>
            <p>${row.system_note || "--"}</p>
          </div>
        </div>

      </div>
    `;
  }).join("");
}

function toggleDetail(idx) {
  const el = document.getElementById(`detail-${idx}`);
  if (el.style.display === "block") {
    el.style.display = "none";
  } else {
    el.style.display = "block";
  }
}
