require("../../../psknode/bundles/pskruntime"); 
var mq = require("../../../modules/foldermq/lib/folderMQ")
const fs = require('fs');

const folderPath = './testFolderMQ';

try {
	for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
} catch (e) {}

var queue = mq.getFolderQueue(folderPath,function(){});

var assert = require("double-check").assert;

var flow1 = $$.callflows.describe("test", {
    public:{
        value:"int"
    },
    init:function(value){
        this.value = value;
    }
})();

var flow2 = $$.callflows.start("test");
flow2.init(2);

var producerHandler = queue.getHandler();

function filter(){
    return flow1.getInnerValue().meta.swarmId;
}

var value=0;

flow1.observe(function(){
            flow1.init(1);
            producerHandler.addSwarm(flow1);
            producerHandler.addSwarm(flow2);
            queue.registerConsumer(function(err, result){
                value++;
                assert.notEqual(result,null,"Nothing is consumed");
            });
        }, null,filter);

//small fix after swarmId is set sync instead async
flow1.notify();

assert.callback("restartTest", function(callback){
    setTimeout(function(){
        assert.equal(value,2,"Not consumed enough");
        try {
            for (const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
            fs.rmdirSync(folderPath);
        } catch (e) {}
        callback();
        process.exit();
    }, 1000);
}, 1500)
