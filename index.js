const fs = require('fs');
const pgp = require('pg-promise')();
const csv = require('csv-parser');
const dbConfig = require('./config/db');
const { dataType, columnTranslation } = require('./config/constants');

// Specify the path to your CSV file
const folder = './csvFiles';
const mainFile = 'main.csv';

async function getColumnMap() {
    const mapping = {};

    try {
        const stream = fs.createReadStream(`${folder}/${mainFile}`);

        for await (const row of stream.pipe(csv())) {
            const shipping_invoice_number = ['-', ''].includes(row['shipping_invoice_number']) ? null : row['shipping_invoice_number'];
            const surcharge_cost = ['-', ''].includes(row['surcharge_cost']) ? null : row['surcharge_cost'];
            const insurance_cost = ['-', ''].includes(row['insurance_cost']) ? null : row['insurance_cost'];
            const tax_charges = ['-', ''].includes(row['tax_charges']) ? null : row['tax_charges'];
            const fuel_charges = ['-', ''].includes(row['fuel_charges']) ? null : row['fuel_charges'];
            const other_costs = ['-', ''].includes(row['other_costs']) ? null : row['other_costs'];
            const shipping_cost = ['-', ''].includes(row['shipping_cost']) ? null : row['shipping_cost'];
            const shipment_currency = ['-', ''].includes(row['shipment_currency']) ? null : row['shipment_currency'];
            const total_shipment_cost = ['-', ''].includes(row['total_shipment_cost']) ? null : row['total_shipment_cost'];

            const file = {
                name: row['FileNumber'],
                tables: {
                    Shipments: {
                        tracking_code: row['tracking_code'],
                        shipping_invoice_number: shipping_invoice_number,
                    },
                    ShipmentCosts: {
                        shipment_currency: shipment_currency,
                        total_shipment_cost: total_shipment_cost,
                        tax_charges: tax_charges,
                        fuel_charges: fuel_charges,
                        other_costs: other_costs,
                        shipping_cost: shipping_cost,
                        surcharge_cost: surcharge_cost,
                        insurance_cost: insurance_cost,
                        status: row['status'],
                    },
                }
            }
            mapping[file.name] = file;
        }
    
        console.log('CSV file processing complete.');
      } catch (error) {
        console.error('Error processing CSV:', error);
      }

    return mapping;
}

async function getFileContent(fileId, mapping) {
    const content = [];

    try {
        const stream = fs.createReadStream(`${folder}/${fileId}.csv`);

        for await (const row of stream.pipe(csv())) {
            const  data = {};
            Object.entries(mapping).forEach(([key, field]) => {
                // console.log(`Key: ${key}, Value: ${field}`);
        
                if (field.type === 'column') {
                    data[key] = row[field.file];
                } else {
                    data[key] = field.file;
                }
            });
            content.push(data);
        }
    
        console.log('CSV file processing complete.');
      } catch (error) {
        console.error('Error processing CSV:', error);
      }

    return content;
}

async function init() {
    const db = pgp(dbConfig);
    // const fileMapping = await getColumnMap();

    await Promise.all(Object.entries(columnTranslation).map(async ([fileId, mapping]) => {
        if (fileId !== '1') return; //TODO: temp change, testing on 1 file
        // console.log(`Key: ${fileId}, Value: ${mapping}`);
        const whereKey = ['17', '18'].includes(fileId) ? 'shipping_invoice_number' : 'tracking_code';

        const content = await getFileContent(fileId, mapping);
            // console.log(content);

        for (let index = 0; index < content.length; index++) {
            let result = null;
            const row = content[index];

            if (['-', '', null].includes(row[whereKey].trim())) {
                console.log(`Invalid value found for ${whereKey} = ${row[whereKey]} not found!`);
                continue;
            }
            // console.log(row);
            const values = [row[whereKey]];
            
            let query = `select s.tracking_code, s."shipping_invoice_number", sc.shipment_currency , sc.total_shipment_cost,
            sc.tax_charges , sc.fuel_charges, sc.other_costs , sc.shipping_cost , sc.surcharge_cost , sc.insurance_cost , sc.status
            from "Shipments" s inner join "ShipmentCosts" sc on s."ShipmentCostId" = sc.id
            where s.${whereKey} = $1`;

            try {
                result = await db.one(query, values);
            } catch (error) {
                console.error(`Error finding row with tracking_code = ${row.tracking_code}`, error);
                continue;
            }

            if (result == null) {
                console.log(`Shipment cost of tracking_code = ${row.tracking_code} not found!`);
                continue;
            }
            
            const keys = Object.keys(row).filter(column => mapping[column].table === 'ShipmentCosts' && result[column] === null && row[column] !== null);

            if (keys.length === 0) {
                console.log(`No columns to update for tracking_code = ${row.tracking_code}`);
                continue;
            }
            const keyFormatted = keys.map( (key, index) => {
                values.push(row[key]);
                return `${key} = $${index+2}`;
            });
            query = `update "ShipmentCosts"
            set ${keyFormatted.join(', ')}
            from "Shipments"
            where "Shipments"."ShipmentCostId" = "ShipmentCosts".id
                and "Shipments".${whereKey} = $1;`;
            console.log(query);
            console.log(values);

            try {
                await db.none(query, values);
            } catch (error) {
                console.error(`Error updating row with tracking_code = ${row.tracking_code}`, error);
            }
            return;  //TODO: temp change
        }
    }));

    // console.log(columnTranslation);
    db.$pool.end(); // Close the database connection
}

init()

