const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const myPromise = new Promise((resolve, reject) => {
    const data = []
    const map = new Map()
    fs.createReadStream(path.resolve(__dirname, 'Book1.csv'))
        .pipe(csv.parse({headers: true}))
        .on('error', error => {
            console.error(error)
            reject()
        })
        .on('data', row => {
            //console.log("row: " +  row[`ID`] + " " + row[`Name`])

            if (map.get(row[`ID`]) === undefined) {
                map.set(row[`ID`], parseFloat(row[`Name`]))
                //console.log(map.get(row[`ID`]))
            } else {
                const newPrice = row[`Name`]
                const olPrice = map.get(row[`ID`])
                if (newPrice < olPrice) {
                    map.set(row[`ID`], newPrice)
                }
                //console.log(map.get(row[`ID`]))
            }

            console.log("The smallest " +  map.get(row[`ID`]))
            data.push(row)
        })
        .on('end', rowCount => {
            console.log(`Parsed ${rowCount} rows`)
            resolve({
                data : data,
                map : map
            })
        })
})

const handle = ({data, map}) => {
    console.log(map)

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

// myPromise
//     .then(handle)
//     .then(writeCSV)
//     .catch(reason => {
//         console.log(reason)
//     })

// const map = new Map()
// map.set('a', 1)
// const col = `a`
// console.log(map.get(col))


// const row = {name : `Khoi`}
//
// row[`age`] = 23
// console.log(row)

const string = "Khoi Milu"
const data = string.split(" ")

console.log(data)

