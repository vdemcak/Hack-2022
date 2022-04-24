const fs = require('fs');
const csv = require('csv-parser');

function date(d) {
    return new Date(d).getTime() - new Date('2021-03-22 04:39:24.101977+00:00').getTime();
}

async function readCSV(file, onLine) {
    return new Promise((resolve, reject) => {
        fs.createReadStream(file)
            .pipe(csv())
            .on('data', row => {
                onLine(row);
            })
            .on('end', () => {
                resolve();
            });
    });
}

module.exports = { date, readCSV };