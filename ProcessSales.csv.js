const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const readDataSales = () => {
    return new Promise((resolve, reject) => {
        const Sales = 'Sales.csv'
        const map = new Map()

        // product_category
        //const Product_Category = `Product_Category`

        // sub_category
        //const Sub_Category = `Sub_Category`

        const product = `Product`

        let index = 0
        fs.createReadStream(path.resolve(__dirname, Sales))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Sales}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                // product_category
                // if (map.get(row[Product_Category]) === undefined) {
                //     map.set(row[Product_Category], ++index)
                // }

                // sub_category
                // if (map.get(row[Sub_Category]) === undefined) {
                //     map.set(row[Sub_Category], ++index)
                // }

                // Product
                if (map.get(row[product]) === undefined) {
                    const data = {}
                    data[`Unit_Cost`] = row[`Unit_Cost`]
                    data[`Unit_Price`] = row[`Unit_Price`]
                    data[`Product_id`] = ++index
                    map.set(row[product], data)
                }

            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Sales}`)
                resolve(map)
            })
    })
}

const writeData = (map) => {
    const output = []

    // product_category
    // const header = [`Product_Category_Id`, `Product_Category`]

    // sub_category
    // const header = [`Sub_Category_Id`, `Sub_Category`]

    // Product
    const header = [`Product_id`, `Product_name`, `Unit_Cost`, `Unit_Price`]

    output.push(header)
    map.forEach((value, key) => {
        const row = {}

        // product_category
        // row[`Product_Category_Id`] = value
        // row[`Product_Category`] = key

        // sub_category
        // row[`Sub_Category_Id`] = value
        // row[`Sub_Category`] = key

        // product
        row[`Product_id`] = value[`Product_id`]
        row[`Product_name`] = key
        row[`Unit_Cost`] = value[`Unit_Cost`]
        row[`Unit_Price`] = value[`Unit_Price`]

        output.push(row)
    })

    // product_category
    //const product_category_csv = `product_category.csv`

    //sub_category
    //const sub_category_csv = `sub_category.csv`

    //product
    const product = `product.csv`

    writeToPath(path.resolve(__dirname, product), output)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}

const dataflow = async () => {
    const data = await readDataSales()
    writeData(data)
}
dataflow().then(() => {})
