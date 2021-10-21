const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const dataflow = new Promise((resolve, reject) => {
    const FACT_GPU_PRICE = 'FACT_GPU_PRICE.csv'
    const ProdId = `ProdId`
    const TimeId = `TimeId`
    const RegionId = `RegionId`
    const MerchantId = `MerchantId`
    const Price_USD = `Price_USD`
    const headers = [ProdId, TimeId, RegionId, MerchantId, Price_USD]
    const data = []
    data.push(headers)
    fs.createReadStream(path.resolve(__dirname, FACT_GPU_PRICE))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            console.log(`Error at processing file ${FACT_GPU_PRICE}`)
            console.log(error)
            reject(error)
        })
        .on('data', row => {
            const newRow = {}
            newRow[ProdId] = row[ProdId]
            newRow[TimeId] = row[TimeId]
            newRow[RegionId] = row[RegionId]
            newRow[MerchantId] = row[MerchantId]
            newRow[Price_USD] = row[Price_USD]
            data.push(newRow)
        })
        .on('end', rowCount => {
            console.log(`Parsed ${rowCount} rows in file ${FACT_GPU_PRICE}`)
            resolve(data)
        })
})

const writeData = (data) => {
    writeToPath(path.resolve(__dirname, 'fact_price.csv'), data)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}

dataflow
    .then(writeData)
    .then(value => {
        console.log(value)
    })
