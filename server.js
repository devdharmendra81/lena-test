const MongoClient = require('mongodb').MongoClient;
const AggregateService = require('./service/AggregateService');

const url = "mongodb://localhost:27017/";

MongoClient.connect(url, function(err, mongoDB) {
  if (err) {
    throw err;
  }
  const db = mongoDB.db("testDB");
  AggregateService.aggregateHistory(db);
});
