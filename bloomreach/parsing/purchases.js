const fs = require('fs');

const { date, readCSV } = require('./helper.js');

const purchases = new Map();

(async () => {

    await readCSV('./purchased_producs.csv', row => {
        purchases.set(row.product_id, (purchases.get(row.product_id) || 0) + 1);
    });

    console.log(Array.from(purchases.entries()).sort((a, b) => b[1] - a[1]).slice(0, 100).map(e => e[0]))
})();