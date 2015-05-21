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
  var processingCollections, zipStream, failures = {};

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
    finishStream
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
        if (conf.zip()) {
          // Add the collection dump to a zip
          zipStream = zs.addToZip(stream, collectionName + '.bson', zipStream);
          // stream.resume();
          return callback();

        } else {
          // Upload the collection dump to S3
          ds.dump(stream, path + '/' + collectionName + '.bson', options, function (err, res) {
            if (!res) {
              err = new Error("No response from Amazon! " + JSON.stringify(arguments));
            }
            if (!res.Key) {
              err = new Error("No Key on response from Amazon! " + res.Code + ": " + res.Message);
            }
            return callback(err);
          });
          stream.resume();
        }
      }
    ], function(err) {
      if (err) {
        return addToFailed(collectionName, err);
      }
      return callback();
    });
  }

  // Dump is finished
  function finishStream(callback) {
    if (!zipStream) {
      // End script
      return callback();
    }
    // Upload the zip file to S3
    zipStream.finalize();
    return ds.dump(zipStream, path, options, function (err, res) {
      zipStream = false;
      if (err) { return addToFailed('zip', err); }
      if (!res) { return addToFailed('zip',  new Error("No response from Amazon! " + JSON.stringify(arguments))); }
      if (!res.Key) { return addToFailed('zip', new Error("No Key on response from Amazon! " + res.Code + ": " + res.Message)); }

      return callback(null, failures);
    });
  }

  function addToFailed(collectionName, err) {
    log.error(err);
    log.error(err.stack);
    failures[collectionName] = err;
  }
}

module.exports = {
  dump: dump,
  setConfig: conf.setConfig,
  setLogLevel: conf.setLogLevel
};
