const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const myPromise = new Promise((resolve, reject) => {
    const data = []
    fs.createReadStream(path.resolve(__dirname, 'Book1.csv'))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            console.error(error)
            reject()
        })
        .on('data', row => {
            console.log(row)
            data.push(row)
        })
        .on('end', rowCount => {
            console.log(`Parsed ${rowCount} rows`)
            resolve(data)
        })
})

const handle = (data) => {
    const keys = Object.keys(data[0])
    keys.push("Secret")
    console.log(data)
    const output = []
    output.push(keys)
    data.forEach((value, index) => {
        const row = []
        keys.forEach(key => {
            row.push(data[index][key] + " Pro")
        })
        row.push("Secret " + index)
        output.push(row)
    })
    console.log(output)
    return output
}

const writeCSV = (data) => {
    writeToPath(path.resolve(__dirname, 'result.csv'), data)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'));
}

myPromise
    .then(handle)
    .then(writeCSV)
    .catch(reason => {
        console.log(reason)
    })






