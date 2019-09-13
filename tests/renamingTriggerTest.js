require("../../../psknode/bundles/pskruntime"); 
const fs = require("fs");
const mq = require("foldermq");

const folderPath = './renamingTriggerTest';
const inProgressExtension = '.test.in_progress';

const queue = mq.createQue(folderPath, function () {});
const assert = require("double-check").assert;
let fileCount = 0;
const filesToTry = 30;


const flow = $$.flow.describe('renamingTriggerTest', {
	init: function (callback) {
		this.cb = callback;
		this.clean();
		this.writeTestFiles();
		this.registerConsumer();
		setTimeout(this.checkResults, 3000);
	},
	writeTestFiles: function () {
		fs.mkdirSync(folderPath);
		fs.writeFileSync(folderPath + '/main_file.test', JSON.stringify({test: 0}));
		for (let i = 1; i <= filesToTry; i++) {
			fs.writeFileSync(folderPath + '/file' + i + inProgressExtension, JSON.stringify({test: i}));
		}
	},
	__consume: function () {
		//console.log(arguments);
		fileCount++;
	},
	registerConsumer: function () {
		for (let i = 1; i <= filesToTry; i++) {
			let j = i;
			setTimeout(function () {
				//console.log("Renaming", '/file' + j + '.test', new Date().getTime());
				fs.rename(folderPath + '/file' + j + inProgressExtension, folderPath + '/file' + j + '.test', (err) => {
					assert.false(err, 'Error while renaming files');
				});
			}, j-1);
		}
		queue.registerConsumer(this.__consume);
	},
	checkResults: function () {
        this.clean();

		console.log("## Consumed " + fileCount + " of " + (filesToTry + 1) + " files");
		assert.equal(fileCount, filesToTry + 1, `Some files were not consumed, consumed ${fileCount} of ${filesToTry + 1} files`);

		this.cb();
		process.exit();
	},
	clean: function(){
        // Try clear the dir before writing if anything exists
        try {
            for (const file of fs.readdirSync(folderPath)){
            	console.log("removing", file);
                fs.unlinkSync(folderPath + '/' + file);
			}
            fs.rmdirSync(folderPath);
        } catch (e) {
        	console.log();
		}
	}
})();

assert.callback("renamingTriggerTest", function (callback) {
	flow.init(callback);
}, 5000);
