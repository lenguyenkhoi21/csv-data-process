const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const dataflow = new Promise((resolve, reject) => {
    const map = new Map()
    const FACT_GPU_PRICE = 'FACT_GPU_PRICE.csv'
    const colId = `ProdId`
    const colPrice = `Price_Original`
    fs.createReadStream(path.resolve(__dirname, FACT_GPU_PRICE))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            console.log(`Error at processing file ${FACT_GPU_PRICE}`)
            console.log(error)
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
            console.log(`Parsed ${rowCount} rows in file ${FACT_GPU_PRICE}`)
            resolve(map)
        })
})

dataflow.then(map => {
    const readGPUData = new Promise((resolve, reject) => {
        const GPU_PROD = 'DIM_GPU_PROD.csv'
        const Id = 'Id'
        const header = [`Id`,`Processor_Manufacturer`,`Processor`,`GPU_Manufacturer`, `Memory_Capacity`, `Memory_Type`, `Price_Original`]
        const data = []
        data.push(header)
        const Price_Original = 'Price_Original'
        fs.createReadStream(path.resolve(__dirname, GPU_PROD))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${GPU_PROD}.csv`)
                reject(error)
            })
            .on('data', row => {
                row[Price_Original] = map.get(row[Id]) === undefined ? 0 : map.get(row[Id])
                data.push(row)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${GPU_PROD}`)
                resolve(data)
            })
    })

    const writeData = (data) => {
        writeToPath(path.resolve(__dirname, 'gpu.csv'), data)
            .on('error', err => console.error(err))
            .on('finish', () => console.log('Done writing.'))
    }

    readGPUData
        .then(writeData)
        .catch(reason => {
            console.log(reason)
        })
})
    .catch(reason => {
        console.log(reason)
    })




