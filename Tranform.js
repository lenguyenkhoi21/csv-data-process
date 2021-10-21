const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const dataflow = new Promise((resolve, reject) => {
    const FACT_GPU_PRICE = './FACT_GPU_PRICE.csv'
    fs.createReadStream(path.resolve(__dirname, FACT_GPU_PRICE))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            reject(error)
        })
        .on('data', row => {

        })
        .on('end', rowCount => {
            console.log(`Parsed ${rowCount} rows`)
            resolve()
        })
})
