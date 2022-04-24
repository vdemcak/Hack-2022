const fs = require('fs');

const { date, readCSV } = require('./helper.js');


const visited = new Map();
const popular = new Map();
const purchased = new Map();

let processed = 0;



(async () => {

    const test = false;

    // Categorize customer visits
    await readCSV(test ? './visited.csv' : './visited_products.csv', row => {
        const userVisited = visited.get(row.customer_id) || new Map();

        // Count how many this customer visited this product
        const data = userVisited.get(row.product_id);
        userVisited.set(row.product_id, data ? [data[0] + 1, date(row.timestamp), data[2]] : [1, date(row.timestamp), []]);

        visited.set(row.customer_id, userVisited);

        // Find most popular products
        const data2 = popular.get(row.product_id);
        popular.set(row.product_id, data2 ? [data2[0] + 1, date(row.timestamp)] : [1, date(row.timestamp)]);

        if (processed % 100000 === 0)
            console.log(processed);

        processed++;
    });

    // Categorize purchases
    await readCSV('./purchased_producs.csv', row => {
        if (!visited.has(row.customer_id)) return;
        const userVisited = visited.get(row.customer_id);

        // Purchases have a weight 10:1 compared to visits
        const data = userVisited.get(row.product_id);
        userVisited.set(row.product_id, data ? [data[0] + 10, data[1], data[2]] : [10, 0, []]);

        const data2 = popular.get(row.product_id);
        popular.set(row.product_id, data2 ? [data2[0] + 10, date(row.timestamp)] : [10, date(row.timestamp)]);
    });

    // Manually picked most purchased
    const top5 = [
        ['0f943312-7141-4606-abfa-81fd63a5498f'],
        ['8369aebc-fba5-4957-b3fb-7da05f327dff'],
        ['a8f0292d-5fca-42b9-b0d3-b38e7efa416b'],
        ['dd664935-460b-4b90-adba-8b0b416dd4c2'],
        ['3e2c84d6-e258-49ae-a91e-b9dfb0068557']
    ];


    const tries = [''];

    for (let i = 0; i < tries.length; i++) {
        let file = 'customer_id,product_id\n';

        await readCSV('./target_group.csv', row => {
            const userVisited = visited.has(row.customer_id) ? Array.from(visited.get(row.customer_id).entries()) : [];
            const topClicked = userVisited.sort((a, b) => {
                const clickF = b[1][0] - a[1][0];
                const timeF = b[1][1] - a[1][1];

                return clickF + timeF / 1000 / 3600 / 24 / 2.3;
            });


            const slice = topClicked.slice(0, 5);

            slice.push(...top5);
            slice.length = 5;

            slice.forEach(product => {
                file += `${row.customer_id},${product[0]}\n`;
            });
        });

        fs.writeFileSync(`./submit${tries[i]}.csv`, file);

    }
    
})();