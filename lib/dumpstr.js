var parseUri = require('mongo-uri').parse;
var log = require('loglevel');
var async = require('async');
var conf = require('./conf.js');
var ms = require('./mongoStream');
var ds = require('./dumpStream');
var zs = require('./zipStream');
var mi = require('./mongoInfo');

function dump(mongoUri, path, options, cb) {
  var uriInfo = parseUri(mongoUri);
  var processingCollections, zipStream, mongoStreams = [];

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  conf.setLogLevel();

  return async.waterfall([
    function getMongoCollections(callback) {
      return mi.getCollections(mongoUri, callback);
    },
    function processCollections(collections, callback) {
      processingCollections = collections;

      return async.each(collections, processCollection, callback);
    },
    startUploadStream,
    function startMongoStream(callback) {
      return async.each(mongoStreams, function(stream, callback) {
        stream.resume();
        return callback();
      }, callback);
    }
  ], cb);

  function processCollection(collection, callback) {
    var collectionName = collection.name.substring(collection.name.indexOf('.') + 1);
    log.info('starting: ', collectionName);
    return async.waterfall([
      function(callback) {
        return ms.getStream(uriInfo, collectionName, callback);
      },
      function(stream, callback) {
        // Add the collection dump to a zip
        zipStream = zs.addToZip(stream, collectionName + '.bson', zipStream);
        mongoStreams.push(stream);
        return callback();
      }
    ], callback);
  }

  // Dump is finished
  function startUploadStream(callback) {
    // Upload the zip file to S3
    zipStream.finalize();
    return ds.dump(zipStream, path, options, function (err, res) {
      if (!res) {
        err = new Error("No response from Amazon! " + JSON.stringify(arguments));
      }
      if (!res.Key) {
        err = new Error("No Key on response from Amazon! " + res.Code + ": " + res.Message);
      }

      return callback(err);
    });
  }
}

module.exports = {
  dump: dump,
  setConfig: conf.setConfig,
  setLogLevel: conf.setLogLevel
};
