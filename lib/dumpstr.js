var conf = require('./conf.js')
  , ms = require('./mongoStream')
  , ds = require('./dumpStream')
  , zs = require('./zipStream')
  , mi = require('./mongoInfo')
  , parseUri = require('mongo-uri').parse
  , log = require('loglevel');

function dump (mongoUri, path, options, cb) {
  var uriInfo = parseUri(mongoUri)
    , processingCollections
    , zipStream
    , failures = {};

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  conf.setLogLevel();

  mi.getCollections(mongoUri, function (err, collections) {
    if (err) { return cb(err); }
    processingCollections = collections;
    dumpCollections();
  });

  function dumpCollections () {
    // Dump a new collection
    if (processingCollections.length > 0) {
      var collection = processingCollections.splice(0,1)[0];
      var collectionName = collection.name.substring(collection.name.indexOf('.') + 1);
      log.info('starting: ', collectionName);
      ms.getStream(uriInfo, collectionName, function (err, stream) {
        if (err) { addToFailed(collectionName, err); }

        if (conf.zip()) {
          // Add the collection dump to a zip
          zipStream = zs.addToZip(stream, collectionName + '.bson', zipStream);
          dumpCollections();

        } else {
          // Upload the collection dump to S3
          ds.dump(stream, path + '/' + collectionName + '.bson', options, function (err, res) {
            if (err) { return addToFailed(collectionName, err); }
            if (!res) { return addToFailed(collectionName,  new Error("No response from Amazon! " + JSON.stringify(arguments))); }
            if (!res.Key) { return addToFailed(collectionName, new Error("No Key on response from Amazon! " + res.Code + ": " + res.Message)); }
            dumpCollections();
          });
        }
        stream.resume();
      });

    // Dump is finished
    } else {
      if (!zipStream) {
        // End script
        cb(null, failures);
      } else {
        // Upload the zip file to S3
        zipStream.finalize();
        ds.dump(zipStream, path, options, function (err, res) {
          if (err) { return addToFailed(collectionName, err); }
          if (!res) { return addToFailed(collectionName,  new Error("No response from Amazon! " + JSON.stringify(arguments))); }
          if (!res.Key) { return addToFailed(collectionName, new Error("No Key on response from Amazon! " + res.Code + ": " + res.Message)); }
        });
      }
    }
  }


  function addToFailed (collectionName, err) {
    log.error(err);
    log.error(err.stack);
    failures[collectionName] = err;
    dumpCollections();
  }
}

module.exports = {
    dump: dump
  , setConfig: conf.setConfig
  , setLogLevel: conf.setLogLevel
}
