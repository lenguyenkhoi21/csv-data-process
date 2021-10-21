const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const dataflow = new Promise((resolve, reject) => {
    const map = new Map()
    const FACT_GPU_PRICE = './FACT_GPU_PRICE.csv'
    const colId = `ProdId`
    const colPrice = `Price_Original`
    fs.createReadStream(path.resolve(__dirname, FACT_GPU_PRICE))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            reject(error)
        })
        .on('data', row => {
            if (map.get(row[colId]) === undefined) {
                map.set(row[colId], parseFloat(row[colPrice]))
            } else {
                const newPrice = row[colPrice]
                const olPrice = map.get(row[colId])
                if (olPrice > newPrice) {
                    map.set(row[colId], newPrice)
                }
            }
        })
        .on('end', rowCount => {
            console.log(`Parsed ${rowCount} rows`)
            resolve({map})
        })
})

const readGPUData = ({map}) => {
    const GPU_PROD = 'DIM_GPU_PROD.csv'
    const data = []
}
