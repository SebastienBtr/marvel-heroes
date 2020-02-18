var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

async function run() {
  // Import data
  importData();
}

// Read the csv and import the data to mongo db
function importData(client) {
  let heroes = [];

  // Read CSV file
  fs.createReadStream('all-heroes.csv')
    .pipe(csv({
      separator: ','
    }))
    .on('data', (data) => {
      heroes.push(data);
    })
    .on('end', () => {
      console.log('push data');
      sendData(heroes).then(() => {
        console.log('End');
      }, (err) => {
        console.log('End because of an error');
        console.trace(err);
      });
    });
}

// Send the data to mongo db
async function sendData(dataSet) {
  return new Promise(async function (resolve, reject) {
    const client = await MongoClient.connect(mongoUrl).catch(err => {
      reject(err);
    });

    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    await collection.insertMany(dataSet).catch(err => {
      client.close();
      reject(err);
    });

    client.close();
    resolve();
  });
}

run().catch(console.error);