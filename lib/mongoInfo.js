var MongoClient = require('mongodb').MongoClient;
var async = require('async');

function getCollections(uri, cb) {
  var mongoClient;
  return async.waterfall([
    function(callback) {
      return MongoClient.connect(uri, callback);
    },
    function(client, callback) {
      mongoClient = client;
      mongoClient.collectionNames(callback);
    }
  ], function(err, result) {
    mongoClient.close();
    return cb(err, result);
  });
}

module.exports = {
  getCollections: getCollections
};
