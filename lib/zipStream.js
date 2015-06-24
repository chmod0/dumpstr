var archiver = require('archiver');

exports.createZip = function() {
  return archiver('zip');
};

exports.addToZip = function(stream, filepath, archive) {
  archive.append(stream, { name: filepath });
  return archive;
};
