const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const indexName = 'heroes'

async function run() {
  // Create index
  await checkIndices();
  // Import data
  importData();
}

// Read the csv and import the data to elasticsearch
function importData() {
  let heroes = [];

  // Read CSV file
  fs.createReadStream('all-heroes.csv')
    .pipe(csv({
      separator: ','
    }))
    .on('data', (data) => {
      heroes.push({
        'id': data.id,
        'name': data.name,
        'description': data.description,
        'imageUrl': data.imageUrl,
        'backgroundUrl': data.backgroundUrl,
        'secretIdentities': data.secretIdentities,
        'aliases': data.aliases,
        'partners': data.partners,
        'universe': data.universe,
        'gender': data.gender,
        'suggest': [
          {
            "input": data.name,
            "weight": 10
          },
          {
            "input": data.aliases,
            "weight": 5
          },
          {
            "input": data.secretIdentities,
            "weight": 5
          },
        ]
      });
    })
    .on('end', () => {
      console.log('push data');
      sendData(heroes).then(() => {
        console.log('End');
        client.close();
      }, (err) => {
        console.log('End because of an error');
        console.trace(err);
        client.close();
      });
    });
}

// Send the data to elasticsearch
async function sendData(dataSet) {
  return new Promise(async function (resolve, reject) {
    const { body: bulkResponse } = await client.bulk(createBulkInsertQuery(dataSet));
    if (bulkResponse.errors) {
      reject(errors);
    } else {
      console.log(`${dataSet.length} data sent`);
    }
    resolve();
  });
}

// Format data for a bulk insert
function createBulkInsertQuery(dataSet) {
  const body = dataSet.reduce((acc, data) => {
    acc.push({ index: { _index: indexName, _type: '_doc', _id: data.id } })
    delete data.id;
    acc.push(data);
    return acc
  }, []);

  return { body };
}

// Create an index if not exist
async function checkIndices() {
  const { body: exist } = await client.indices.exists({ index: indexName });
  if (exist) {
    console.log('index already exists');
  } else {
    await client.indices.create({ index: indexName });
  }
}

run().catch(console.error);
