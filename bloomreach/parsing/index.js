const csv = require('csv-parser');
const fs = require('fs');

const visited = new Map();


const popular = new Map();
const purchased = new Map();

const array = new Array(1000000);

let c = 0;

function date(d) {
    return new Date(d).getTime();
}

fs.createReadStream('./visited_products.csv')
    .pipe(csv())
    .on('data', row => {
        const userVisited = visited.get(row.customer_id) || new Map();

        const data = userVisited.get(row.product_id);
        userVisited.set(row.product_id, data ? [data[0] + 1, date(row.timestamp)] : [1, date(row.timestamp)]);
       

        visited.set(row.customer_id, userVisited);
        popular.set(row.product_id, popular.get(row.product_id) + 1 || 1);

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

                const userVisited = visited.get(row.customer_id);

                const data = userVisited.get(row.product_id);
                userVisited.set(row.product_id, data ? [data[0] + 10, data[1]] : [10, 0]);

                purchased.set(row.product_id, purchased.get(row.product_id) + 1 || 1);
            })
            .on('end', () => {
                const top5 = Array.from(purchased.entries()).filter(data => data[1] > 100).map(data => [data[0], data[1] / popular.get(data[0])/*, data[1], popular.get(data[0])*/]).sort((a, b) => b[1] - a[1]).slice(0, 5);

                console.log(top5);
               
                let file = 'customer_id,product_id\n';

                fs.createReadStream('./target_group.csv')
                    .pipe(csv())
                    .on('data', row => {
                        const userVisited = visited.has(row.customer_id) ? Array.from(visited.get(row.customer_id).entries()) : [];
                        const topClicked = userVisited.sort((a, b) => {

                            const clickF = b[1][0] - a[1][0];
                            const timeF = b[1][1] - a[1][1];

                            return clickF || timeF;
                        });

                        /*const purchaseCount = (visited.get(row.customer_id) || new Map()).size;

                        array[purchaseCount] = array[purchaseCount] || 0;
                        array[purchaseCount]++;*/

                        const slice = topClicked.slice(0, 5);
                        slice.push(...top5);
                        slice.length = 5;


                        slice.forEach(product => {
                            file += `${row.customer_id},${product[0]}\n`; 
                        });
                    })
                    .on('end', () => {
                        /*for(let i = 0; i < array.length; i++) {
                            if(array[i] > 0) {
                                console.log(`${array[i]} of users viewed ${i} items`);
                            }
                        }*/

                        fs.writeFileSync('./submit.csv', file);
                    });
            });
    });

