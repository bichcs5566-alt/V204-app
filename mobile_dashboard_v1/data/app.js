async function loadCSV(path) {
  const res = await fetch(path);
  const text = await res.text();
  return text.trim().split('\n').map(r => r.split(','));
}

function renderTrade(data) {
  const tbody = document.querySelector("#trade-table tbody");
  tbody.innerHTML = "";
  data.slice(1).forEach(row => {
    const tr = document.createElement("tr");
    const action = row[2];
    tr.innerHTML = `
      <td class="${action.toLowerCase()}">${action}</td>
      <td>${row[3]}</td>
      <td>${row[4]}</td>
      <td>${row[8]}</td>
      <td>${row[12]}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderPosition(data) {
  const tbody = document.querySelector("#position-table tbody");
  tbody.innerHTML = "";
  data.slice(1).forEach(row => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row[0]}</td>
      <td>${row[1]}</td>
      <td>${row[2]}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderSummary(data) {
  const tbody = document.querySelector("#summary-table tbody");
  tbody.innerHTML = "";
  data.slice(1).forEach(row => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row[0]}</td>
      <td>${row[1]}</td>
      <td>${row[2]}</td>
    `;
    tbody.appendChild(tr);
  });
}

async function init() {
  try {
    const trade = await loadCSV("data/trade_plan.csv");
    const pos = await loadCSV("data/current_positions.csv");
    const sum = await loadCSV("data/full_summary.csv");

    renderTrade(trade);
    renderPosition(pos);
    renderSummary(sum);

    document.getElementById("update-time").innerText = "最後更新：" + new Date().toLocaleString();
  } catch (e) {
    document.getElementById("update-time").innerText = "讀取資料失敗";
  }
}

init();
