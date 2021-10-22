const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Customer_Id = `Customer_Id`
const Customer_Age = `Customer_Age`
const Age_Group = `Age_Group`
const Customer_Gender = `Customer_Gender`
const Country = `Country`
const State = `State`

const readDataSales = () => {
    return new Promise((resolve, reject) => {
        const Sales = 'Sales.csv'
        const map = new Map()
        let index = 0
        fs.createReadStream(path.resolve(__dirname, Sales))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Sales}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const data = row[Customer_Age] + "!" +
                    row[Age_Group] + "!" +
                    row[Customer_Gender] + "!" +
                    row[Country] + "!" +
                    row[State]

                if (map.get(data) === undefined) {
                    map.set(data, ++index)
                }
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Sales}`)
                resolve(map)
            })
    })
}

const processDataCustomer = (map) => {
    const data = []
    const headers = [Customer_Id, Customer_Age, Age_Group, Customer_Gender, Country, State]
    data.push(headers)
    map.forEach((value, key) => {
        const row = {}
        const info = key.split('!')
        row[Customer_Id] = value
        row[Customer_Age] = info[0]
        row[Age_Group] = info[1]
        row[Customer_Gender] = info[2]
        row[Country] = info[3]
        row[State] = info[4]
        data.push(row)
    })
    return data
}

const writeData = (data) => {
    const csvFile = 'customer.csv'
    writeToPath(path.resolve(__dirname, csvFile), data)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}

const dataflow = async () => {
    const map = await readDataSales()
    const data = processDataCustomer(map)
    writeData(data)
}

dataflow().then(()=>{})
