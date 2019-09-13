require("../../../psknode/bundles/pskruntime");
const fs = require("fs");
const mq = require("../../../modules/foldermq/lib/folderMQ");

const folderPath = './ConsumeExistingChannel';

const queue  = mq.getFolderQueue(folderPath, function () {});
const assert = require("double-check").assert;
let steps    = 0;
const phases = [];


const flow = $$.flow.describe('consumeExistingTest', {
	init: function (callback) {
		// Try clear the dir before writing if anything exists
		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlink(folderPath + '/' + file);
		} catch (e) {}
		this.cb = callback;
		this.writeTestFiles();
		this.registerConsumer();
		setTimeout(this.checkResults, 1000);

	},
	writeTestFiles: function () {
		fs.writeFileSync(folderPath + '/file1.test', JSON.stringify({test: 1}));
		fs.writeFileSync(folderPath + '/file2.test', JSON.stringify({test: 2}));
		fs.writeFileSync(folderPath + '/file3.test', JSON.stringify({test: 3}));
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
	checkResults: function () {
		console.log("Read files order:", phases.join(", "));
		assert.equal(steps, 3, "The 3 files were not consumed");

		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
			fs.rmdirSync(folderPath);
		} catch (e) {}

		this.cb();
		process.exit();
	}
})();

assert.callback("consumeExistingTest", function (callback) {
	flow.init(callback);
}, 2000);

