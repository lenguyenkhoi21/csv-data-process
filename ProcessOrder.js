const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Oder_id = `Oder_id`
const Date = `Date`
const Day = `Day`
const Month = `Month`
const Year = `Year`
const Order_Quantity = `Order_Quantity`
const Profit = `Profit`
const Cost = `Cost`
const Revenue = `Revenue`

const readDataSales = () => {
    return new Promise((resolve, reject) => {
        const Sales = 'Sales.csv'
        const data = []
        let index = 0
        fs.createReadStream(path.resolve(__dirname, Sales))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Sales}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[Oder_id] = ++index
                line[Date] = row[Date]
                line[Day] = row[Day]
                line[Month] = row[Month]
                line[Year] = row[Year]
                line[Order_Quantity] = row[Order_Quantity]
                line[Profit] = row[Profit]
                line[Cost] = row[Cost]
                line[Revenue] = row[Revenue]
                data.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Sales}`)
                resolve(data)
            })
    })
}

const writeData = (data) => {

    const headers = [Oder_id, Date, Day, Month, Year, Order_Quantity, Profit, Cost, Revenue]
    const output = [headers, ...data]
    const csvFile = 'order.csv'
    writeToPath(path.resolve(__dirname, csvFile), output)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}


const dataflow = async () => {
    const data = await readDataSales()
    writeData(data)
}

dataflow().then(()=>{})
