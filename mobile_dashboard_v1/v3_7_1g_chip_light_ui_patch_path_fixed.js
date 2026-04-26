// ===============================
// v3.7.1h FULL VERSION（安全整合版）
// ===============================

// 👉 等整頁載入完再執行（避免抓不到 DOM）
window.addEventListener("load", () => {
    initChipModule();
});

async function initChipModule() {

    const chipMap = await loadChipData();

    // 延遲一點，確保 dashboard 已渲染
    setTimeout(() => {
        applyChipToTradePlan(chipMap);   // 今日操作
        applyChipToPositions(chipMap);   // 持倉
    }, 1500);
}


// ===============================
// 🔹 讀取籌碼資料
// ===============================
async function loadChipData() {
    try {
        const res = await fetch('./data/chip_analysis.csv');
        const text = await res.text();
        const lines = text.split('\n').slice(1);

        const chipMap = {};

        lines.forEach(line => {
            const cols = line.split(',');
            const stock_id = cols[0]?.trim();
            const chip_label = cols[2]?.trim();
            const chip_tags = cols[3]?.trim();

            if (stock_id) {
                chipMap[stock_id] = {
                    label: chip_label,
                    tags: chip_tags
                };
            }
        });

        console.log("✅ chip loaded:", Object.keys(chipMap).length);
        return chipMap;

    } catch (e) {
        console.error('❌ chip load error', e);
        return {};
    }
}


// ===============================
// 🔹 今日操作（trade_plan）
// ===============================
function applyChipToTradePlan(chipMap) {

    const tables = document.querySelectorAll("table");

    tables.forEach(table => {

        // 👉 找有「加入持倉」按鈕的表
        if (!table.innerText.includes("加入持倉")) return;

        const rows = table.querySelectorAll("tbody tr");

        rows.forEach(row => {

            const cells = row.querySelectorAll("td");
            if (cells.length === 0) return;

            const stockId = cells[0].innerText.trim();

            let chipCell = row.querySelector(".chip-cell");

            if (!chipCell) {
                chipCell = document.createElement("td");
                chipCell.className = "chip-cell";
                chipCell.style.fontSize = "12px";
                chipCell.style.whiteSpace = "nowrap";

                row.appendChild(chipCell);
            }

            if (chipMap[stockId]) {
                chipCell.innerHTML = `
                    <span style="color:#16a34a">● ${chipMap[stockId].label}</span><br>
                    <span style="color:#dc2626">${chipMap[stockId].tags}</span>
                `;
            } else {
                chipCell.innerText = "—";
            }
        });

    });
}


// ===============================
// 🔹 持倉（重點修正）
// ===============================
function applyChipToPositions(chipMap) {

    const table = findPositionTable();

    if (!table) {
        console.warn("❌ 找不到持倉 table");
        return;
    }

    const rows = table.querySelectorAll("tbody tr");

    rows.forEach(row => {

        const cells = row.querySelectorAll("td");
        if (cells.length === 0) return;

        // 👉 股票代號在第一欄
        const stockId = cells[0].innerText.trim();

        let chipCell = row.querySelector(".chip-cell");

        if (!chipCell) {
            chipCell = document.createElement("td");
            chipCell.className = "chip-cell";
            chipCell.style.fontSize = "12px";
            chipCell.style.whiteSpace = "nowrap";

            row.appendChild(chipCell);
        }

        if (chipMap[stockId]) {
            chipCell.innerHTML = `
                <span style="color:#16a34a">● ${chipMap[stockId].label}</span><br>
                <span style="color:#dc2626">${chipMap[stockId].tags}</span>
            `;
        } else {
            chipCell.innerText = "—";
        }
    });
}


// ===============================
// 🔹 找「正確持倉 table」（關鍵）
// ===============================
function findPositionTable() {

    const tables = document.querySelectorAll("table");

    for (let table of tables) {

        const text = table.innerText;

        if (
            text.includes("股票") &&
            text.includes("股數") &&
            text.includes("參考價格")
        ) {
            return table;
        }
    }

    return null;
}
