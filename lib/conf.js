var ll = require('loglevel');
var confPath = process.cwd() + '/conf.js';
var confSet = false;
var confObj;
var logLevel = 5;

function setConfig(pathOrConfig) {
  if (typeof pathOrConfig === 'string') {
    confSet = true;
    confPath = pathOrConfig;
  } else {
    confObj = pathOrConfig;
  }
}

function setLogLevel(n) {
  logLevel = n || logLevel;
  ll.setLevel(logLevel);
}

function aws() {
  if (confSet) {
    return require(confPath).aws;
  }
  if (confObj) {
    return confObj.aws;
  }
  if (process.env.AWS_KEY && process.env.AWS_SECRET && process.env.AWS_BUCKET) {
    return {
      key: process.env.AWS_KEY,
      secret: process.env.AWS_SECRET,
      bucket: process.env.AWS_BUCKET
    };
  }
  return require(confPath).aws;
}

function zip () {
  if (confSet) {
    return !!require(confPath).zip;
  }
  if (confObj) {
    return !!(confObj.zip);
  }
  return false;
}

module.exports = {
  aws: aws,
  zip: zip,
  setConfig: setConfig,
  setLogLevel: setLogLevel
};
