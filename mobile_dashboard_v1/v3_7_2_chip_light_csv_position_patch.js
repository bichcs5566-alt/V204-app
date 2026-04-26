// v3.7.2：持倉改為讀 CSV + 修正籌碼顯示

async function loadChipData() {
    try {
        const res = await fetch('./data/chip_scores.csv');
        const text = await res.text();
        const rows = text.split('\n').slice(1);

        const map = {};
        rows.forEach(r => {
            const cols = r.split(',');
            if (cols.length < 4) return;

            const stock = cols[0];
            const label = cols[2];
            const tags = cols[3];

            map[stock] = `${label}｜${tags}`;
        });

        return map;

    } catch (e) {
        console.log('chip load fail', e);
        return {};
    }
}

async function loadPositions() {
    try {
        const res = await fetch('./data/current_positions.csv');
        const text = await res.text();
        const rows = text.split('\n').slice(1);

        return rows.map(r => r.split(',')[0]);

    } catch (e) {
        console.log('position load fail', e);
        return [];
    }
}

async function injectChipToPositions() {
    const chipMap = await loadChipData();
    const positions = await loadPositions();

    const rows = document.querySelectorAll('#positions-table tbody tr');

    rows.forEach((row, i) => {
        const stock = positions[i];
        if (!stock) return;

        const chip = chipMap[stock] || '—';

        let td = row.querySelector('.chip-col');

        if (!td) {
            td = document.createElement('td');
            td.className = 'chip-col';
            row.insertBefore(td, row.children[2]); // 插在備註旁
        }

        td.innerText = chip;
    });
}

setTimeout(() => {
    injectChipToPositions();
}, 1000);
