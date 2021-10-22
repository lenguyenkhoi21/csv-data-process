const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Time_id = `Time_id`
const Date = `Date`
const Day = `Day`
const Month = `Month`
const Year = `Year`

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
                const line = row[Date] + "!" + row[Day] + "!" + row[Month] + "!" + row[Year]
                if (map.get(line) === undefined) {
                    map.set(line, ++index)
                }
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Sales}`)
                resolve(map)
            })
    })
}

const processTimeData = (map) => {
    const data = []
    map.forEach((value, key) => {
        const line = {}
        const info = key.split("!")
        line[Time_id] = value
        line[Date] = info[0]
        line[Day] = info[1]
        line[Month] = info[2]
        line[Year] = info[3]
        data.push(line)
    })
    return data
}

const writeData = (data) => {
    const headers = [Time_id, Date, Day, Month, Year]
    const output = [headers, ...data]
    const csvFile = 'time.csv'
    writeToPath(path.resolve(__dirname, csvFile), output)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}


const dataflow = async () => {
    const map = await readDataSales()
    const data = processTimeData(map)
    writeData(data)
}

dataflow().then(()=>{})
