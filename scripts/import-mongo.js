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
      heroes.push({
        id: data.id,
        name: data.name,
        imageUrl: data.imageUrl,
        backgroundImageUrl: data.backgroundImageUrl,
        externalLink: data.externalLink,
        description: data.description,
        teams: createList(data.teams),
        powers: createList(data.powers),
        partners: createList(data.partners),
        creators: createList(data.creators),
        appearance: {
          gender: data.gender,
          type: data.type,
          race: data.race,
          height: data.height,
          weight: data.weight,
          eyeColor: data.eyeColor,
          hairColor: data.hairColor
        },
        identity: {
          secretIdentities: createList(data.secretIdentities),
          birthPlace: data.birthPlace,
          occupation: data.occupation,
          aliases: createList(data.aliases),
          alignment: data.alignment,
          firstAppearance: data.firstAppearance,
          yearAppearance: data.yearAppearance,
          universe: data.universe
        },
        skills: {
          intelligence: getIntValue(data.intelligence),
          strength: getIntValue(data.strength),
          speed: getIntValue(data.speed),
          durability: getIntValue(data.durability),
          combat: getIntValue(data.combat),
          power: getIntValue(data.power),
        }
      });
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

function createList(value) {
  return value.length ? value.split(',') : [];
}

function getIntValue(value) {
  return value != '' ? parseInt(value, 10) : 0;
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