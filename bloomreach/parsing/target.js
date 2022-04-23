const csv = require('csv-parser');
const fs = require('fs');

const purchases = new Map();
const array = new Array(1000000);


fs.createReadStream('./purchased_producs.csv')
    .pipe(csv())
    .on('data', row => {
        purchases.set(row.customer_id, (purchases.get(row.customer_id) || 0) + 1);
    })
    .on('end', () => {
        let file = 'customer_id,product_id\n';

        fs.createReadStream('./target_group.csv')
            .pipe(csv())
            .on('data', row => {
                //console.log(row.customer_id);
                const purchaseCount = purchases.get(row.customer_id) || 0;

                array[purchaseCount] = array[purchaseCount] || 0;
                array[purchaseCount]++;
            })
            .on('end', () => {
                //console.log('u');
                for(let i = 0; i < array.length; i++) {
                    if(array[i] > 0) {
                        console.log(`${array[i]} of users purchased ${i} items`);
                    }
                }
            });
    });
