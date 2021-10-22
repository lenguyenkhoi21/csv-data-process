const fs = require('fs')
const path = require('path')
const csv = require('fast-csv')
const { writeToPath } = require('@fast-csv/format')

const Oder_id = `Oder_id`

// For Time => For Time Id
const Time_id = `Time_id`
const Date = `Date`
const Day = `Day`
const Month = `Month`
const Year = `Year`

// For Customer => Id of customer
const Customer_Id = `Customer_Id`
const Customer_Age = `Customer_Age`
const Age_Group = `Age_Group`
const Customer_Gender = `Customer_Gender`
const Country = `Country`
const State = `State`

// For Product => Id of product
const Product_id = `Product_id`
const Product = `Product`

// For Oder table
const Order_Quantity = `Order_Quantity`

// Content
const contentTime = `contentTime`
const contentCustomer = `contentCustomer`
const contentProduct = `contentProduct`

const Product_name = `Product_name`

const readDataOrder = () => {
    return new Promise((resolve, reject) => {
        const Order = 'order.csv'
        const data = []
        let index = 0
        fs.createReadStream(path.resolve(__dirname, Order))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Order}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[Oder_id] = ++index

                //For time
                line[Time_id] = row[Date] + "!" + row[Day] + "!" + row[Month] + "!" + row[Year]

                // For Customer
                line[Customer_Id] = row[Customer_Age] + "!" +
                    row[Age_Group] + "!" +
                    row[Customer_Gender] + "!" +
                    row[Country] + "!" +
                    row[State]

                //For Product
                line[Product_id] = row[Product]

                // For Oder
                line[Order_Quantity] = row[Order_Quantity]

                data.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Order}`)
                const DataOrder = {
                    data
                }
                resolve(DataOrder)
            })
    })
}

const readDataTime = (DataOrder) => {

    return new Promise((resolve, reject) => {
        const Time_csv = 'time.csv'
        const timeData = []
        fs.createReadStream(path.resolve(__dirname, Time_csv))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Time_csv}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[Time_id] = row[Time_id]
                line[contentTime] = row[Date] + "!" + row[Day] + "!" + row[Month] + "!" + row[Year]
                timeData.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Time_csv}`)
                const DataOrderTime = { timeData ,...DataOrder}
                resolve(DataOrderTime)
            })
    })
}

const readDataCustomer = (DataOrderTime) => {

    return new Promise((resolve, reject) => {
        const Customer_csv = 'customer.csv'
        const customerData = []
        fs.createReadStream(path.resolve(__dirname, Customer_csv))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Customer_csv}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[Customer_Id] = row[Customer_Id]
                line[contentCustomer] = row[Customer_Age] + "!" +
                    row[Age_Group] + "!" +
                    row[Customer_Gender] + "!" +
                    row[Country] + "!" +
                    row[State]
                customerData.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Customer_csv}`)
                const DataOrderTimeCustomer = { customerData ,...DataOrderTime }
                resolve(DataOrderTimeCustomer)
            })
    })
}

const readDataProduct = (DataOrderTimeCustomer) => {

    return new Promise((resolve, reject) => {
        const Product_csv = 'product_final.csv'
        const productData = []
        fs.createReadStream(path.resolve(__dirname, Product_csv))
            .pipe(csv.parse({headers: true}))
            .on('error', error => {
                console.log(`Error at processing file ${Product_csv}`)
                console.log(error)
                reject(error)
            })
            .on('data', row => {
                const line = {}
                line[Product_id] = row[Product_id]
                line[contentProduct] = row[Product_name]
                productData.push(line)
            })
            .on('end', rowCount => {
                console.log(`Parsed ${rowCount} rows in file ${Product_csv}`)
                const DataOrderTimeCustomerProduct = { productData ,...DataOrderTimeCustomer }
                resolve(DataOrderTimeCustomerProduct)
            })
    })
}

const processData = (DataOrderTimeCustomerProduct) => {
    const data = DataOrderTimeCustomerProduct.data
    const dataLength = data.length

    const productArr = DataOrderTimeCustomerProduct.productData
    const productLength = productArr.length

    const timeArr = DataOrderTimeCustomerProduct.timeData
    const timeLength = timeArr.length

    const customerArr = DataOrderTimeCustomerProduct.customerData
    const customerLength = customerArr.length

    const headers = [Oder_id, Time_id, Customer_Id, Product_id, Order_Quantity]
    const output = []
    output.push(headers)
    for (let i = 0; i < dataLength; i++) {
        const row = {}
        row[Oder_id] = data[i][Oder_id]

        for (let j = 0; j < timeLength; j++) {
            if (data[i][Time_id] === timeArr[j][contentTime]) {
                row[Time_id] = timeArr[j][Time_id]
                break
            }
        }

        for (let j = 0; j < customerLength; j++) {
            if (data[i][Customer_Id] === customerArr[j][contentCustomer]) {
                row[Customer_Id] = customerArr[j][Customer_Id]
                break
            }
        }

        for (let j = 0; j < productLength; j++) {
            if (data[i][Product_id] === productArr[j][contentProduct]) {
                row[Product_id] = productArr[j][Product_id]
                break
            }
        }

        row[Order_Quantity] = data[i][Order_Quantity]
        output.push(row)
    }

    return output
}

const writeData = (data) => {

    const csvFile = 'order_final.csv'
    writeToPath(path.resolve(__dirname, csvFile), data)
        .on('error', err => console.error(err))
        .on('finish', () => console.log('Done writing.'))
}


const dataflow = async () => {
    const DataOrder = await readDataOrder()
    const DataOrderTime = await readDataTime(DataOrder)
    const DataOrderTimeCustomer = await readDataCustomer(DataOrderTime)
    const DataOrderTimeCustomerProduct = await readDataProduct(DataOrderTimeCustomer)
    const data = processData(DataOrderTimeCustomerProduct)
    writeData(data)
}

dataflow().then(()=>{})

