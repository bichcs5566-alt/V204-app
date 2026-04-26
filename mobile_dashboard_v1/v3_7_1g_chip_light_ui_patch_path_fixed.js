// v3.7.1h - 持倉 table 精準識別版（不動原系統）

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

        return chipMap;
    } catch (e) {
        console.error('chip load error', e);
        return {};
    }
}

// 🔥 核心：找「正確的持倉 table」
function findPositionTable() {
    const tables = document.querySelectorAll('table');

    for (let table of tables) {
        const text = table.innerText;

        // 👉 必須包含這些關鍵字才算持倉表
        if (
            text.includes('股票') &&
            text.includes('股數') &&
            text.includes('參考價格')
        ) {
            return table;
        }
    }

    return null;
}

function applyChipToPositions(chipMap) {
    const table = findPositionTable();

    if (!table) {
        console.warn('❌ 找不到持倉 table');
        return;
    }

    const rows = table.querySelectorAll('tbody tr');

    rows.forEach(row => {
        const cells = row.querySelectorAll('td');

        if (cells.length === 0) return;

        // 👉 第一欄就是 stock_id
        const stockId = cells[0].innerText.trim();

        // 👉 建立籌碼顯示欄（如果還沒）
        let chipCell = row.querySelector('.chip-cell');

        if (!chipCell) {
            chipCell = document.createElement('td');
            chipCell.className = 'chip-cell';
            chipCell.style.fontSize = '12px';
            chipCell.style.whiteSpace = 'nowrap';

            row.appendChild(chipCell);
        }

        if (chipMap[stockId]) {
            chipCell.innerHTML = `
                <span style="color:#16a34a">● ${chipMap[stockId].label}</span>
                <br>
                <span style="color:#dc2626">${chipMap[stockId].tags}</span>
            `;
        } else {
            chipCell.innerText = '—';
        }
    });
}

// 初始化
(async function () {
    const chipMap = await loadChipData();

    setTimeout(() => {
        applyChipToPositions(chipMap);
    }, 1500);
})();
