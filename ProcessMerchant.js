const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const dataflow = new Promise((resolve, reject) => {
    const DIM_MERCHANT = 'DIM_MERCHANT.csv'
    const data = []
    const Region_Id = `Region_Id`
    const headers = [`Id`, `Merchant`, Region_Id]
    data.push(headers)
    const randomIntFromInterval = (min, max) => {
        return Math.floor(Math.random() * (max - min + 1) + min)
    }
    fs.createReadStream(path.resolve(__dirname, DIM_MERCHANT))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            console.log(`Error at processing file ${DIM_MERCHANT}`)
            console.log(error)
            reject(error)
        })
        .on('data', row => {
            row[Region_Id] = randomIntFromInterval(1,11)
            data.push(row)
        })
        .on('end', rowCount => {
            console.log(`Parsed ${rowCount} rows in file ${DIM_MERCHANT}`)
            resolve(data)
        })
})

const writeData = (data) => {
    writeToPath(path.resolve(__dirname, 'merchant.csv'), data)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}

dataflow
    .then(writeData)
    .catch(reason => {
        console.log(reason)
    })



