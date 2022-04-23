const csv = require('csv-parser');
const fs = require('fs');

const visited = new Map();

let c = 0;

fs.createReadStream('./visited_products.csv')
    .pipe(csv())
    .on('data', row => {
        const userVisited = visited.get(row.customer_id) || new Map();

        const data = userVisited.get(row.product_id);
        let newData;

        if(data) {
            newData = [data[0] + 1, data[1], row.price];
        }
        else {
            newData = [1, row.price, row.price];
        }

        userVisited.set(row.product_id, newData);

        visited.set(row.customer_id, userVisited);

        if (c++ % 100000 === 0) {
            console.log(c);
        }
    })
    .on('end', () => {
        console.log('Visit loaded');

        fs.createReadStream('./purchased_producs.csv')
            .pipe(csv())
            .on('data', row => {
                if (!visited.has(row.customer_id)) return;

                visited.get(row.customer_id).set(row.product_id, 0);


            })
            .on('end', () => {
                let file = 'customer_id,product_id\n';

                fs.createReadStream('./target_group.csv')
                    .pipe(csv())
                    .on('data', row => {
                        const userVisited = visited.has(row.customer_id) ? Array.from(visited.get(row.customer_id).entries()) : [];
                        const topClicked = userVisited.sort((a, b) => {
                            const da = a[1], db = b[1];

                            const clickF = db[0] - da[0];
                            const priceF = (da[2] - da[1]) - (db[2] - db[1]);

                            return clickF || priceF;
                        });

                        topClicked.slice(0, 5).forEach(product => {
                            file += `${row.customer_id},${product[0]}\n`; 
                        });
                    })
                    .on('end', () => {
                        fs.writeFileSync('./submit.csv', file);
                    });
            });
    });

