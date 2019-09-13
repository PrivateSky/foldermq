require('../../../psknode/bundles/pskruntime');
const fs = require('fs');
const mq = require('../../../modules/foldermq/lib/folderMQ');

const folderPath = './watcherChannel';
const inProgressFileName = '.in_progress';

const queue = mq.getFolderQueue(folderPath, function() {});
const assert = require('double-check').assert;
const args = process.argv.slice(2);

let ct = 0;
const numMessages = 4;
const expected = [];
const received = [];
const inProgressIndexes = args[0] || [2];

const flow = $$.flow.describe('watcherTest', {
  init: function(callback, inProgressIndexes) {
    // Try clear the dir before writing if anything exists
    try {
      for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
    } catch (e) {}
    this.cb = callback;
    this.inProgressIndexes = inProgressIndexes;
    this.registerConsumer();
    let setRef = setInterval(this.writeTestFile, 100);
    setTimeout(() => {
      clearInterval(setRef);
      this.checkResults();
    }, 1000);
  },
  registerConsumer: function() {
    queue.registerConsumer(this.consume);
  },
  consume: function(err, result) {
    assert.notEqual(result.test, null, 'Data in file is not fine');
    received.push(result.test);
  },
  writeTestFile: function() {
    expected.push(ct);
    if (typeof this.inProgressIndexes != 'number' && this.inProgressIndexes.indexOf(ct) == -1) {
      fs.writeFileSync(folderPath + '/file' + ct + '.test', JSON.stringify({ test: ct }));
    } else {
      fs.writeFileSync(
        folderPath + '/file' + ct + '.test' + inProgressFileName,
        JSON.stringify({ test: ct })
      );
    }
    ct++;
  },
  checkResults: function() {
    console.log(expected);
    console.log(received);
    if (inProgressIndexes.length > 1) {
      assert.equal(
        expected.length - inProgressIndexes.split(',').length,
        received.length,
        'Files with the in progress flag were consumed'
      );
    } else {
      assert.equal(
        expected.length - 1,
        received.length,
        'Files with the in progress flag were consumed'
      );
    }

    this.end();
  },
  end: function() {
    try {
      for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
      fs.rmdirSync(folderPath);
    } catch (e) {}

    console.log('Test passed');
    this.cb();
    process.exit();
  }
})();

assert.callback(
  'Files with the in progress flag are not consumed',
  function(callback) {
    console.log(`The in progress files indexes are:${inProgressIndexes}`);
    flow.init(callback, inProgressIndexes);
  },
  2000
);
