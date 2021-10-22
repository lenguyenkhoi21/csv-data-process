const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Oder_id = `Oder_id`

// For Time => For Time Id
const Date = `Date`
const Day = `Day`
const Month = `Month`
const Year = `Year`

// For Customer => Id of customer
const Customer_Age = `Customer_Age`
const Age_Group = `Age_Group`
const Customer_Gender = `Customer_Gender`
const Country = `Country`
const State = `State`

// For Product => Id of product
const Product = `Product`

// For Oder table
const Order_Quantity = `Order_Quantity`

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

                //For time
                line[Date] = row[Date]
                line[Day] = row[Day]
                line[Month] = row[Month]
                line[Year] = row[Year]

                // For Customer
                line[Customer_Age] = row[Customer_Age]
                line[Age_Group] = row[Age_Group]
                line[Customer_Gender] = row[Customer_Gender]
                line[Country] = row[Country]
                line[State] = row[State]

                //For Product
                line[Product] = row[Product]

                // For Oder
                line[Order_Quantity] = row[Order_Quantity]
                data.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Sales}`)
                resolve(data)
            })
    })
}

const writeData = (data) => {

    const headers = [Oder_id, Date, Day, Month, Year, Customer_Age, Age_Group, Customer_Gender, Country, State, Product, Order_Quantity]
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
