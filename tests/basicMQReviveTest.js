require("../../../psknode/bundles/pskruntime");
/*require("callflow");
require("launcher");*/
const fs         = require("fs");
const mq = require("foldermq");
const assert     = require("double-check").assert;
const beesHealer = require("swarmutils").beesHealer;
const folderPath = './BasicReviveChannel';

const queue = mq.createQue(folderPath, function () {
});
// Try clear the dir before writing if anything exists
try {
	for (const file of fs.readdirSync(folderPath)) fs.unlink(folderPath + '/' + file);
} catch (e) {}

// Describe and create a new swarm
const f = $$.swarm.describe("test", {
	public: {
		result: "int"
	},
	private: {
		a1: "int",
		a2: "int"
	},
	phaseOne: function (a1, a2) {
		this.a1 = a1;
		this.a2 = a2;
	},
	phaseTwo: function (a3, a4) {
		this.result = this.a1 + this.a2 + a3 + a4;
	},
	phaseThree: function () {
		this.result++;
	}
})();

let finalResult = null;

const flow = $$.flow.describe('basicMQReviveTest', {
	start: function (callback) {
		this.cb = callback;
		this.registerConsumer();
		this.sendSwarmToFolderMQ();
		setTimeout(this.finish, 500);

	},
	registerConsumer: function () {
		queue.registerConsumer(function (err, result) {
			assert.notEqual(result, null, "Nothing is consumed");
			// Revive the swarm
			var f2 = $$.swarmsInstancesManager.revive_swarm(result);
			// Then (3) run the phaseThree of the swarm
			f2.phaseThree();
			finalResult = f2.result;
		});
	},
	sendSwarmToFolderMQ: function (callback) {
		const producerHandler = queue.getHandler();
		f.observe(function () {
			// First (1) run the phase one before serialization
			f.phaseOne(7, 3);
			// Then (2) send phaseTwo to execution for the moment when it'll be revived
			let swarm = beesHealer.asJSON(f.getInnerValue(), "phaseTwo", [4, 8]);
			producerHandler.sendSwarmForExecution(swarm, callback);
		}, null, null);
		f.notify();
	},
	finish: function () {
		assert.equal(finalResult, 23, "Phase two wasn't executed");
		if (finalResult === 23) {
			console.log("Test passed");
		}

		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
			fs.rmdirSync(folderPath);
		} catch (e) {}

		this.cb();
		process.exit();
	}
})();

assert.callback("basicMQReviveTest", function (callback) {
	flow.start(callback);
}, 2000);