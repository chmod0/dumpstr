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
  var zipStream;

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  conf.setLogLevel();

  return async.waterfall([
    function getZipStream(callback) {
      zipStream = zs.createZip();
      return callback();
    },
    function initUploadStream(callback) {
      ds.dump(zipStream, path, options, function(err, res) {
        if (!res) {
          err = new Error("No response from Amazon! " + JSON.stringify(arguments));
        }
        if (!res.Key) {
          err = new Error("No Key on response from Amazon! " + res.Code + ": " + res.Message);
        }
        return cb(err);
      });
      return callback();
    },
    function getMongoCollections(callback) {
      return mi.getCollections(mongoUri, callback);
    },
    function processCollections(collections, callback) {
      return async.each(collections, processCollection, callback);
    },
    function finalizeZip(callback) {
      zipStream.finalize();
      return callback();
    }
  ], function(err) {
    if (err) {
      return cb(err);
    }
  });

  function processCollection(collection, callback) {
    var collectionName = collection.name.substring(collection.name.indexOf('.') + 1);
    log.info('starting: ', collectionName);
    return async.waterfall([
      function(callback) {
        return ms.getStream(uriInfo, collectionName, callback);
      },
      function(stream, callback) {
        // Add the collection dump to a zip
        zs.addToZip(stream, collectionName + '.bson', zipStream);
        return callback();
      }
    ], callback);
  }
}

module.exports = {
  dump: dump,
  setConfig: conf.setConfig,
  setLogLevel: conf.setLogLevel
};
