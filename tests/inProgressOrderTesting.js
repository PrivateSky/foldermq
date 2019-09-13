////////////////////////////////////////////////////////////////////////
// Test if folderMQ consumes the contents of the folder queue in order
// when in_progress files have their extension removed
////////////////////////////////////////////////////////////////////////
require("../../../psknode/bundles/pskruntime");
const fs = require("fs");
const os = require('os');
const mq = require("../../../modules/foldermq/lib/folderMQ");

const tmpDir = os.tmpdir() || __dirname;
const folderPath = tmpDir + '/watcherChannel';

const queue  = mq.getFolderQueue(folderPath, function () {});
const assert = require("double-check").assert;

let ct = 0;
const expected = [];
const in_progress = [];
const received = [];

const flow = $$.flow.describe('watcherTest', {
	init: function (callback) {
		// Try clear the dir before writing if anything exists
		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
		} catch (e) {}

		this.cb = callback;
		this.registerConsumer();
        let setRef = setInterval(this.writeTestFile, 10);
        let renameInterval;
        setTimeout(() => { // Write files for 0.5 seconds
            clearInterval(setRef);
            renameInterval = setInterval(this.clearInProgress, 10); // start clearing in_progress extensions
        },500);
        setTimeout(() => {
            clearInterval(renameInterval);
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
        if ( ct % 2 === 0 ){
            fs.writeFileSync(folderPath + '/file' + ct +'.test.in_progress', JSON.stringify({test: ct}));
            in_progress.push(ct);            
        }
        else {
            fs.writeFileSync(folderPath + '/file' + ct +'.test', JSON.stringify({test: ct}));
            expected.push(ct);    
        }
        ct++;
    },
    clearInProgress: function () {
        let tempCt = in_progress.shift(); 
        if (!tempCt) {
            return;
        }
        expected.push(tempCt);
        fs.renameSync(folderPath + '/file' + tempCt + '.test.in_progress', folderPath + '/file' + tempCt + '.test', 
        (err) => {
            if (err) {
                console.log(err);
            };
        });
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

assert.callback("inProgressOrderTest", function (callback) {
	flow.init(callback);
}, 2000);
