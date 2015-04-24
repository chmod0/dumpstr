var archiver = require('archiver');

function addToZip (stream, filepath, archive) {
  if (!archive) {
    archive = archiver('zip');
  }

  archive.append(stream, { name: filepath });
  return archive;
}

module.exports.addToZip = addToZip;
