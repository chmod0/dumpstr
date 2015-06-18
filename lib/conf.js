var ll = require('loglevel');
var config;
var logLevel = 5;

function setConfig(configObj) {
  config = configObj;
}

function setLogLevel(n) {
  logLevel = n || logLevel;
  ll.setLevel(logLevel);
}

function aws() {
  return config.aws;
}

module.exports = {
  aws: aws,
  setConfig: setConfig,
  setLogLevel: setLogLevel
};
