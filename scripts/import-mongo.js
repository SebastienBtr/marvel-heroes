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
        teams: data.teams,
        powers: data.powers,
        partners: data.partners,
        creators: data.creators,
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
          secretIdentities: data.secretIdentities,
          birthPlace: data.birthPlace,
          occupation: data.occupation,
          aliases: data.aliases,
          alignment: data.alignment,
          firstAppearance: data.firstAppearance,
          yearAppearance: data.yearAppearance,
          universe: data.universe
        },
        skills: {
          intelligence: data.intelligence,
          strength: data.strength,
          speed: data.speed,
          durability: data.durability,
          combat: data.combat,
          power: data.power
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