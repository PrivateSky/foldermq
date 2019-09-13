require("../../../psknode/bundles/pskruntime"); 
const mq = require("../../../modules/foldermq/lib/folderMQ");
const assert = require("double-check").assert;
const fs = require('fs');

const folderPath = './testFolderMQ';

const queue = mq.getFolderQueue(folderPath,function(){});


const f = $$.swarm.describe("test", {
    public:{
        value:"int"
    },
    init:function(callback){
        this.callback=callback;
        this.value = 1;
    }
})();


const flow = $$.flow.describe('prodConsTest', {
	init: function (callback) {
	    this.cb = callback;

		try {
			for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
		} catch (e) {}


		this.registerConsumer();
        this.producerHandler = queue.getHandler();
        this.startObserve(() => {
	        try {
		        for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
		        fs.rmdirSync(folderPath);
	        } catch (e) {}
            this.cb();
            process.exit();
        });
	},
	registerConsumer: function () {
		queue.registerConsumer(function (err, result) {
			assert.notEqual(result, null, "Nothing is consumed");
			f.callback();
		});
	},
	__filter: function (message) {
		return f.getInnerValue().meta.swarmId == message.meta.swarmId;
	},
	startObserve: function (callback) {
	    f.observe(() => {
		    f.init(callback);
		    this.producerHandler.addSwarm(f, function(){});
	    }, null,this.__filter);

	    //small fix after swarmId is set sync instead async
	    f.notify();
    }
})();

assert.callback("prodConsTest", function (callback) {
   flow.init(callback);
}, 2000);