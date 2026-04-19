// v266.2 FIXED - 正確資料路徑版本

const DATA_BASE = "./data";

// CSV 讀取工具
async function loadCSV(file) {
    const res = await fetch(`${DATA_BASE}/${file}`);
    const text = await res.text();
    const rows = text.trim().split("\n").map(r => r.split(","));
    const header = rows.shift();
    return rows.map(r => Object.fromEntries(header.map((h,i)=>[h,r[i]])));
}

// 初始化
async function init() {
    try {
        const trade = await loadCSV("trade_plan.csv");
        const pos = await loadCSV("current_positions.csv");
        const summary = await loadCSV("full_summary.csv");
        const debug = await loadCSV("selection_debug.csv");

        renderTrade(trade);
        renderPos(pos);
        renderSummary(summary);
        renderDebug(debug);

        document.getElementById("lastUpdate").innerText = new Date().toLocaleString();
    } catch(e) {
        console.error("讀取資料失敗", e);
    }
}

// 今日操作
function renderTrade(data){
    const el = document.getElementById("trade");
    el.innerHTML = data.map(d=>`
        <tr>
            <td>${d.action}</td>
            <td>${d.symbol}</td>
            <td>${d.price_tier}</td>
            <td>${d.ref_price}</td>
            <td>${d.target_weight}</td>
            <td>${d.suggested_amount}</td>
        </tr>
    `).join("");
}

// 持倉
function renderPos(data){
    const el = document.getElementById("pos");
    el.innerHTML = data.map(d=>`
        <tr>
            <td>${d.symbol}</td>
            <td>${d.shares}</td>
            <td>${d.cost}</td>
        </tr>
    `).join("");
}

// 績效
function renderSummary(data){
    if(!data.length) return;
    document.getElementById("ret").innerText = data[0].return;
    document.getElementById("dd").innerText = data[0].max_dd;
    document.getElementById("sharpe").innerText = data[0].sharpe;
}

// 篩選除錯
function renderDebug(data){
    const el = document.getElementById("debug");
    el.innerHTML = data.map(d=>`
        <tr>
            <td>${d.input}</td>
            <td>${d.valid}</td>
            <td>${d.core_raw}</td>
            <td>${d.alpha_raw}</td>
            <td>${d.core_final}</td>
            <td>${d.alpha_final}</td>
        </tr>
    `).join("");
}

// 啟動
init();
