require("../../../psknode/bundles/pskruntime");
const fs = require("fs");
const mq = require("../../../modules/foldermq/lib/folderMQ");

const folderPath         = './InProgressChannel';
const inProgressFileName = 'file1.test.in_progress';

const queue  = mq.getFolderQueue(folderPath, function () {});
const assert = require("double-check").assert;
let wasConsumed = 0;

const flow = $$.flow.describe('inProgressTest', {
	init: function (callback) {
		// Try clear the dir before writing if anything exists
		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
		} catch (e) {}
		this.cb = callback;
		this.writeTestFiles();
		this.registerConsumer();
		setTimeout(this.checkResults, 1000);
	},
	writeTestFiles: function () {
		fs.writeFileSync(folderPath + '/' + inProgressFileName, JSON.stringify({test: 1}));
	},
	consume: function () {
		wasConsumed = 1;
		assert.notEqual(result.test, 1, "Test failed. The `In progress` file was consumed.");
	},
	registerConsumer: function () {
		queue.registerConsumer(this.consume);
	},
	checkResults: function () {
		fs.unlinkSync(folderPath + '/' + inProgressFileName);
		fs.rmdirSync(folderPath);

		assert.true(wasConsumed === 0);

		this.cb();
		process.exit();
	}
})();

assert.callback("inProgressTest", function (callback) {
	flow.init(callback);
}, 2000);
