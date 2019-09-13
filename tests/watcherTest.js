require("../../../psknode/bundles/pskruntime"); 
const fs = require("fs");
const mq = require("../../../modules/foldermq/lib/folderMQ");

const folderPath = './watcherChannel';

const queue  = mq.getFolderQueue(folderPath, function () {});
const assert = require("double-check").assert;

let step = 0;


const flow = $$.flow.describe('watcherTest', {
	init: function (callback) {
		// Try clear the dir before writing if anything exists
		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
		} catch (e) {}

		this.cb = callback;
		this.registerConsumer();
		setTimeout(this.writeTestFile, 500);
		setTimeout(this.checkResults, 1000);

	},
	registerConsumer: function () {
		queue.registerConsumer(this.consume);
	},
	consume: function (err, result) {
		assert.equal(step, 1, "Consuming before writing the file?");
		assert.notEqual(result.test, null, "Data in file is not fine");
		step++;
	},
	writeTestFile: function () {
		step = 1;
		fs.writeFileSync(folderPath + '/file1.test', JSON.stringify({test: 1}));
	},
	checkResults: function () {
		assert.equal(step, 2, "Watcher was not triggered");

		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
			fs.rmdirSync(folderPath);
		} catch (e) {}

		console.log("Test passed");
		this.cb();
		process.exit();
	}
})();

assert.callback("consumeExistingTest", function (callback) {
	flow.init(callback);
}, 2000);
