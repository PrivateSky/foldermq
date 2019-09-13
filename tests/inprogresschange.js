// change the in_progrees flag of some files to see if they are consumed

require("../../../psknode/bundles/pskruntime");
const fsExt = require('../../../libraries/utils/FSExtension').fsExt;
const mq = require("../../../modules/foldermq/lib/folderMQ");
const assert = require("double-check").assert;
const fs = require("fs");
const path = require("path");
const os = require("os");

var temp_path = path.join(os.tmpdir(), fsExt.guid());
var test_dir = path.join(temp_path, './testdir');
const queue = mq.getFolderQueue(test_dir, function () {
});

let steps = 0;
const phases = [];
const correctOrder = [1, 2, 3];


const flow = $$.flow.describe('changeInProgressFlag', {
    init: function (callback) {
        this.cb = callback;
        this.registerConsumer();
        this.writeTestFiles();
        this.writeFilelater();
        setTimeout(this.changeLabel, 1000);
        setTimeout(this.checkResults, 4000);

    },
    writeTestFiles: function () {
        fsExt.createDir(test_dir);
        fs.writeFileSync(path.join(test_dir, '/file1.test'), JSON.stringify({test: 1}));
        fs.writeFileSync(path.join(test_dir, '/file2.test'), JSON.stringify({test: 2}));

    },
    writeFilelater: function () {
        fs.writeFileSync(path.join(test_dir, '/file3.test.in_progress'), JSON.stringify({test: 3}));
    },
    changeLabel: function () {
        setTimeout(function () {
            fs.rename(path.join(test_dir, '/file3.test.in_progress'), path.join(test_dir, '/file3.test'), (err) => {
                assert.false(err, 'Error while renaming files');
            });

        }, 0);
    },
    consume: function (err, result) {
        assert.notEqual(result.test, null, "Bad data from folderMQ");
        if (typeof result.test !== 'undefined') {
            phases.push(result.test);
        }
        steps++;
    },

    registerConsumer: function () {
        queue.registerConsumer(this.consume);
    },
    checkResults: function (result) {
        assert.arraysMatch(phases, correctOrder);
        assert.equal(steps, 3, "The 3 files were not consumed");

        try {
            fs.rmdirSync(test_dir);
        } catch (e) {
        }

        this.cb();
        process.exit();
    }
})();

assert.callback("changeInProgressFlag", function (callback) {
    flow.init(callback);
}, 5000);

