const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Product_Category = `Product_Category`
const Sub_Category = `Sub_Category`
const Sub_Category_Id = `Sub_Category_Id`
const Product_Category_Id = `Product_Category_Id`

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
                const data = row[Product_Category] + "," + row[Sub_Category]
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

const readProductCsv = (map) => {
    const data = []
    map.forEach((value, key) =>{
        const row = {}
        const subject = key.split(",")
        row[Product_Category] = subject[0]
        row[Sub_Category] = subject[1]
        row[Sub_Category_Id] = value
        data.push(row)
    })

    return new Promise((resolve, reject) => {
        const product_category_csv = `product_category.csv`
        const product_category = []
        fs.createReadStream(path.resolve(__dirname, product_category_csv))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${product_category_csv}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[Product_Category_Id] = row[Product_Category_Id]
                line[Product_Category] = row[Product_Category]
                product_category.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${product_category_csv}`)
                resolve({data, product_category})
            })
    })
}

const writeData = ({data, product_category}) => {
    const output = []
    const header = [Sub_Category_Id, Sub_Category, Product_Category_Id]
    output.push(header)
    data.forEach(element => {
        // element[Product_Category]
        const row = {}
        product_category.forEach(product => {
            if (product[Product_Category] === element[Product_Category]) {
                element[Product_Category_Id] = product[Product_Category_Id]
            }

        })

        row[Sub_Category_Id] = element[Sub_Category_Id]
        row[Sub_Category] = element[Sub_Category]
        row[Product_Category_Id] = element[Product_Category_Id]

        output.push(row)
    })

    const csvFile = 'sub_category_next.csv'
    writeToPath(path.resolve(__dirname, csvFile), output)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}

const dataflow = async () => {
    const map = await readDataSales()
    const {data, product_category} = await readProductCsv(map)
    writeData({data, product_category})
}

dataflow().then(() => {})
