// register two consumers on the same instance of foldermq should handle this

require("../../../psknode/bundles/pskruntime");
const fsExt = require('../../../libraries/utils/FSExtension').fsExt;
const mq = require("../../../modules/foldermq/lib/folderMQ");
const assert = require("double-check").assert;
const fs = require("fs");
const path = require("path");
const os = require("os");

var temp_path = path.join(os.tmpdir(), fsExt.guid());
var test_dir = path.join(temp_path, './testdir');
const queue = mq.getFolderQueue(test_dir, function () {});

let steps = 0;
const phases = [];
const order = [];
const correctOrder = [1, 2];

const flow = $$.flow.describe('tworegisteredconsumers', {
    init: function (callback) {
        this.cb = callback;
        this.registerConsumer();
        this.writeTestFiles();
        setTimeout(this.checkResults, 2000);

    },
    writeTestFiles: function () {
        fsExt.createDir(test_dir);
        fs.writeFileSync(path.join(test_dir, '/file1.test'), JSON.stringify({test: 1}));
        fs.writeFileSync(path.join(test_dir, '/file2.test'), JSON.stringify({test: 2}));

    },
    consume: function (err, result) {
        assert.notEqual(result.test, null, "Bad data from folderMQ");
        if (typeof result.test !== 'undefined') {
            phases.push(result.test);
            order.push(result.test);
        }
        steps++;
    },
    consume_two: function () {
        assert.notEqual(result.test, null, "Bad data from folderMQ")
    },

    registerConsumer: function () {
        queue.registerConsumer(this.consume);
        try{
            queue.registerConsumer(this.consume_two);
        }catch(err){
            console.log(err);
        }

    },
    checkResults: function () {

        assert.arraysMatch(phases, correctOrder);
        assert.equal(steps, 2, "The 2 files were not consumed");

        try {
            fs.rmdirSync(test_dir);
        } catch (e) {
        }

        this.cb();
        process.exit();
    }
})();

assert.callback("tworegisteredconsumers", function (callback) {
    flow.init(callback);
}, 10000);

