const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Sub_Category = `Sub_Category`
const Product = `Product`

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
                const data = row[Product] + "~" + row[Sub_Category]
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
        const subject = key.split("~")
        row[Product] = subject[0]
        row[Sub_Category] = subject[1]
        data.push(row)
    })

    return new Promise((resolve, reject) => {
        const product_csv = `product.csv`
        const product_output = []
        const col1 = `Product_id`
        const col2 = `Product_name`
        const col3 = `Unit_Cost`
        const col4 = `Unit_Price`
        fs.createReadStream(path.resolve(__dirname, product_csv))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${product_csv}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[col1] = row[col1]
                line[col2] = row[col2]
                line[col3] = row[col3]
                line[col4] = row[col4]
                product_output.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${product_csv}`)
                resolve({map_data : data, product_data: product_output})
            })
    })
}

const readDataSubCategory = ({map_data, product_data}) => {

    return new Promise((resolve, reject) => {
        const sub_category_next_csv = `sub_category_next.csv`
        const sub_category_next_output = []
        const col1 = `Sub_Category_Id`
        const col2 = `Sub_Category`
        const col3 = `Product_Category_Id`
        fs.createReadStream(path.resolve(__dirname, sub_category_next_csv))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${sub_category_next_csv}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[col1] = row[col1]
                line[col2] = row[col2]
                line[col3] = row[col3]
                sub_category_next_output.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${sub_category_next_csv}`)
                resolve({map_data_final : map_data, product_data_final : product_data, sub_data_final : sub_category_next_output})
            })
    })
}

const processData = ({map_data_final, product_data_final, sub_data_final}) => {
    const product_final = []
    const col1 = `Product_id`
    const col2 = `Product_name`
    const col3 = `Unit_Cost`
    const col4 = `Unit_Price`
    const col5 = `Sub_Category_Id`
    const headers = [col1, col2, col3, col4, col5]
    product_final.push(headers)

    const prd_length = product_data_final.length
    const sub_length = sub_data_final.length
    const map_length = map_data_final.length

    for (let i = 0; i < prd_length; i++) {
        const line = {}
        line[col1] = product_data_final[i][col1]
        line[col2] = product_data_final[i][col2]
        line[col3] = product_data_final[i][col3]
        line[col4] = product_data_final[i][col4]

        for (let j = 0; j < map_length; j++) {
            if (product_data_final[i][col2] ===  map_data_final[j][Product]) {

                for (let k = 0; k < sub_length; k++) {
                    if (map_data_final[j][Sub_Category] === sub_data_final[k][Sub_Category]) {
                        line[col5] = sub_data_final[k][col5]
                        break
                    }
                }

                break
            }
        }
        product_final.push(line)
    }

    return product_final
}

const writeData = (data) => {
    const csvFile = 'product_final.csv'
    writeToPath(path.resolve(__dirname, csvFile), data)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}

const dataflow = async () => {
    const map = await readDataSales()
    const {map_data, product_data} = await readProductCsv(map)
    const {map_data_final, product_data_final, sub_data_final} = await readDataSubCategory({map_data, product_data})
    const output = processData({map_data_final, product_data_final, sub_data_final})
    writeData(output)
}

dataflow().then(() => {})
