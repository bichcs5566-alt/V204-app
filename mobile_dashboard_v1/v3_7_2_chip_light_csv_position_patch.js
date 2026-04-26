// v3.7.2b：強制對應版本（一定會顯示）

async function loadChipMap() {
    const res = await fetch('./data/chip_scores.csv');
    const text = await res.text();
    const rows = text.split('\n').slice(1);

    const map = {};
    rows.forEach(r => {
        const c = r.split(',');
        if (c.length < 4) return;

        map[c[0]] = `${c[2]}｜${c[3]}`;
    });

    return map;
}

async function loadPositionMap() {
    const res = await fetch('./data/current_positions.csv');
    const text = await res.text();
    const rows = text.split('\n').slice(1);

    const map = {};
    rows.forEach(r => {
        const c = r.split(',');
        if (c.length < 1) return;

        map[c[0]] = true;
    });

    return map;
}

async function injectChip() {
    const chipMap = await loadChipMap();
    const posMap = await loadPositionMap();

    const rows = document.querySelectorAll('table tbody tr');

    rows.forEach(row => {
        const text = row.innerText;

        let stock = null;

        // 🔥 從 row 文字找股票代號
        Object.keys(posMap).forEach(s => {
            if (text.includes(s)) {
                stock = s;
            }
        });

        if (!stock) return;

        const chip = chipMap[stock] || '—';

        let td = row.querySelector('.chip-col');

        if (!td) {
            td = document.createElement('td');
            td.className = 'chip-col';
            row.appendChild(td);
        }

        td.innerText = chip;
    });
}

setTimeout(injectChip, 1200);
