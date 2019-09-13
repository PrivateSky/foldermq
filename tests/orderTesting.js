////////////////////////////////////////////////////////////////////////
// Test if folderMQ consumes the contents of the folder queue in order
////////////////////////////////////////////////////////////////////////
require("../../../psknode/bundles/pskruntime");
const fs = require("fs");
const mq = require("../../../modules/foldermq/lib/folderMQ");

const os = require('os');

const tmpDir = os.tmpdir() || __dirname;
const folderPath = tmpDir + '/watcherChannel';

const queue  = mq.getFolderQueue(folderPath, function () {});
const assert = require("double-check").assert;

let ct = 0;
const expected = [];
const received = [];

const flow = $$.flow.describe('watcherTest', {
	init: function (callback) {
		// Try clear the dir before writing if anything exists
		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
		} catch (e) {}

		this.cb = callback;
		this.registerConsumer();
        let setRef = setInterval(this.writeTestFile, 100);
        setTimeout(() => {
            clearInterval(setRef);
            this.checkResults();
        },1000);
	},
	registerConsumer: function () {
		queue.registerConsumer(this.consume);
	},
	consume: function (err, result) {
		assert.equal(err,null);
		assert.notEqual(result.test, null, "Data in file is not fine");
        received.push(result.test);
	},
	writeTestFile: function () {
        expected.push(ct);
        fs.writeFileSync(folderPath + '/file' + ct +'.test', JSON.stringify({test: ct}));
        ct++;
	},
	checkResults: function () {
        console.log(expected);
        console.log(received);
        let flag = true;
        for (let i in expected){
            if(!(expected[i] === received[i])){
                flag=false;
            }
        }
        assert.equal(flag, true, 'Wrong Order');
        this.end();
    },
    end: function() {
		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
			fs.rmdirSync(folderPath);
		} catch (e) {}

		console.log("Test passed");
		this.cb();
		process.exit();
    }
})();

assert.callback("orderTest", function (callback) {
	flow.init(callback);
}, 2000);
