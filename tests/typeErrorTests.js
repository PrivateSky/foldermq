require("../../../psknode/bundles/pskruntime"); 
var mq = require("../../../modules/foldermq/lib/folderMQ");
var fs = require("fs");
var assert = require("double-check").assert;

var expectedErrors = 0;
var folderPath = './testFolderMQ';
var caughtErrors = 0;

var someSwarm = $$.swarm.describe("test", {
    public:{
        value:"int"
    },
    init:function(){
        this.value = 1;
    }
})();

var TS = [];

// Test 1: MQ creation folder name test
TS.push(prepareTest({
    testsToPass: [folderPath+"string"],
    testsToFail: [someSwarm, this, [1,'y'], {test:1}, 12345, 0, NaN, 1/0, function(){}, undefined, null, false, true],
    desc: "## Testing `mq.getFolderQueue(>>this<<, callback)`",
    fct: function(testCases){
        var queue;
        for(var i=0;i<testCases.length;i++){
            var testCase = testCases[i];
            try{
                queue = mq.getFolderQueue(testCase, function(){});
                console.log("[Test1] Succeed(passing "+typeof testCase+")");
            }catch(e){
                caughtErrors++;
                console.log("[Test1] Got expected error(passing "+typeof testCase+"):", e.message);
            }
        }
    }
}));
// Test 2: MQ creation callback tests
TS.push(prepareTest({
    testsToPass: [function(){}, undefined],
    testsToFail: [someSwarm, this, [1,'y'], {test:1}, "stringTest", 12345, 0, NaN, 1/0, null, false, true],
    desc: "## Testing `mq.getFolderQueue(folderPath, >>this<<)`",
    fct: function(testCases){
        var queue;
        for(var i=0;i<testCases.length;i++){
            var testCase = testCases[i];
            try{
                queue = mq.getFolderQueue(folderPath, testCase);
                console.log("[Test2] Succeed(passing "+typeof testCase+")");
            }catch(e){
                caughtErrors++;
                console.log("[Test2] Got expected error(passing "+typeof testCase+"):", e.message);
            }
        }
    }
}));
// Test 3: MQ consumer callback tests
TS.push(prepareTest({
    testsToPass: [function(){}],
    testsToFail: [someSwarm, this, [1,'y'], {test:1}, "stringTest", 12345, 0, NaN, 1/0, undefined, null, false, true],
    desc: "## Testing `queue.registerConsumer(>>this<<);`",
    fct: function(testCases){
        for(var i=0;i<testCases.length;i++){
            var testCase = testCases[i];
            try{
                var queue = mq.getFolderQueue(folderPath+i, function(){});
                queue.registerConsumer(testCase);
                console.log("[Test3] Succeed(passing "+typeof testCase+")");
            }catch(e){
                caughtErrors++;
                console.log("[Test3] Got expected error(passing "+typeof testCase+"):", e.message);
            }
        }
    }
}));
// Test 4: MQ producer swarm tests
TS.push(prepareTest({
    testsToPass: [someSwarm],
    testsToFail: [this, [1,'y'], {test:1}, "stringTest", 12345, 0, NaN, -1/0, function(){}, undefined, null, false, true],
    desc: "## Testing `producerHandler.addSwarm(>>this<<);`",
    fct: function(testCases){
        var queue = mq.getFolderQueue(folderPath+'Swarm', function(){});
        var producerHandler = queue.getHandler();
        for(var i=0;i<testCases.length;i++){
            var testCase = testCases[i];
            try{
                producerHandler.addSwarm(testCase);
                console.log("[Test4] Succeed(passing "+typeof testCase+")");
            }catch(e){
                caughtErrors++;
                console.log("[Test4] Got expected error(passing "+typeof testCase+"):", e.message);
            }
        }
    }
}));
// Test 5: MQ producer callback test
TS.push(prepareTest({
    testsToPass: [function(){}, undefined, null, false, 0, NaN],
    testsToFail: [someSwarm, this, [1,'y'], {test:1}, "stringTest", 12345, -1/0, true],
    waitForCallback: true,
    desc: "## Testing `producerHandler.addSwarm(swarm, >>this<<);`",
    fct: function(testCases, callback){
        var queue = mq.getFolderQueue(folderPath+'Swarm'+Math.random().toString().substr(-2), function(err,res){});
        var producerHandler = queue.getHandler();
        var registeredConsumer = 0;
        var swarmList = [];
        var waiting = 0;
        var done = 0;
        function cb(){
            done++;
            if(done == waiting){
                setTimeout(start_cb, 20);
            }
        }
        for(var i=0;i<testCases.length;i++){
            waiting++;
            swarmList[i] = $$.swarm.start("test");
            swarmList[i].init();
            swarmList[i].observe(cb);
            //small fix after swarmId is set sync instead async
            swarmList[i].notify();
        }
        queue.registerConsumer(function(){});

        function start_cb(){
            for(var i=0;i<testCases.length;i++){
                var testCase = testCases[i];
                try{
                    producerHandler.addSwarm(swarmList[i], testCase);
                    console.log("[Test5] Succeed(passing "+typeof testCase+")");
                }catch(e){
                    caughtErrors++;
                    console.log("[Test5] Got expected error(passing "+typeof testCase+"):", e.message);
                }
            }
            if(typeof callback == "function") callback();
        }
    }
}));

function prepareTest(test){
    return function(callback){
        var testItem = test;

        if(testItem.testsToPass){
            expectedErrors += testItem.testsToFail.length;
        }

        var currNrOfErr = caughtErrors;

        function done(){
            var foundErrors = caughtErrors - currNrOfErr;
            console.log("\n## Expected", testItem.testsToFail.length, "errors, and got", foundErrors, "errors of", testItem.testsToFail.length + testItem.testsToPass.length, "tests");
            console.log();
            callback();
        }

        if(testItem.desc){
            console.log("\n" + testItem.desc + "\n");
        }
        if(testItem.waitForCallback){
            console.log("# Tests expecting to pass");
            testItem.fct(testItem.testsToPass, function(){
                console.log("# Tests expecting to fail");
                testItem.fct(testItem.testsToFail, done);
            });
        }else{
            console.log("# Tests expecting to pass");
            testItem.fct(testItem.testsToPass);
            console.log("# Tests expecting to fail");
            testItem.fct(testItem.testsToFail);
            done();
        }
    }
}

TS.push(function(callback){
    console.log("## Caught "+caughtErrors+" of "+expectedErrors+" errors");
    assert.equal(caughtErrors, expectedErrors, "Didn't catch enough errors.");
    if(caughtErrors == expectedErrors){
        console.log("Test passed");
    }

    callback();
});

assert.steps("unit testing for FolderMQ", TS, 1500);

setTimeout(function(){
    try{
        try{
            for(const file of fs.readdirSync(folderPath)) fs.unlinkSync(folderPath + '/' + file);
            fs.rmdirSync(folderPath);
        }catch(e){};
        var dirname = folderPath.split('/').slice(-1)[0];
        var dirList = fs.readdirSync('.').filter(f => f.substr(0, dirname.length) === dirname);
        for(var dir of dirList){
            try{
                for(const file of fs.readdirSync(dir)) fs.unlinkSync(dir + '/' + file);
                fs.rmdirSync(dir);
            }catch(e){};
        }
    }catch(e){};
    process.exit();
}, 1500);
