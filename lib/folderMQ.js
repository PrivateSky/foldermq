const OwM = require("swarmutils").OwM;
var fs = require("fs");
var path = require("path");
var beesHealer = require("swarmutils").beesHealer;

//TODO: prevent a class of race condition type of errors by signaling with files metadata to the watcher when it is safe to consume

function FolderMQ(folder, callback = () => {}){

	if(typeof callback !== "function"){
		throw new Error("Second parameter should be a callback function");
	}

	folder = path.normalize(folder);

	fs.mkdir(folder, function(err, res){
		fs.exists(folder, function(exists) {
			if (exists) {
				callback(null, folder)
			} else {
				callback(err);
			}
		});
	});

	function mkFileName(swarmRaw){
		return path.normalize(folder + "/" + OwM.prototype.getMetaFrom(swarmRaw, "swarmId") + "."+OwM.prototype.getMetaFrom(swarmRaw, "swarmTypeName"));
	}

	this.getHandler = function(){
		if(producer){
			throw new Error("Only one consumer is allowed!");
		}
		producer = true;
		return {
			addStream : function(stream, callback){
				if(typeof callback !== "function"){
					throw new Error("Second parameter should be a callback function");
				}

				if(!stream || !stream.pipe || typeof stream.pipe !== "function"){
					callback(new Error("Something wrong happened"));
				}

				let swarm = "";
				stream.on('data', (chunk) =>{
					swarm += chunk;
				});

				stream.on("end", () => {
					writeFile(mkFileName(JSON.parse(swarm)), swarm, callback);
				});

				stream.on("error", (err) =>{
					callback(err);
				});
			},
			addSwarm : function(swarm, callback){
				if(!callback){
					callback = $$.defaultErrorHandlingImplementation;
				}else if(typeof callback !== "function"){
					throw new Error("Second parameter should be a callback function");
				}

				beesHealer.asJSON(swarm,null, null, function(err, res){
					writeFile(mkFileName(res), J(res), callback);
				});
			},
			sendSwarmForExecution: function(swarm, callback){
				if(!callback){
					callback = $$.defaultErrorHandlingImplementation;
				}else if(typeof callback !== "function"){
					throw new Error("Second parameter should be a callback function");
				}

                beesHealer.asJSON(swarm, OwM.prototype.getMetaFrom(swarm, "phaseName"), OwM.prototype.getMetaFrom(swarm, "args"), function(err, res){
                    var file = mkFileName(res);
                    var content = J(res);

                    //if there are no more FD's for files to be written we retry.
                    function wrapper(error, result){
                        if(error){
                        	console.log(`Caught an write error. Retry to write file [${file}]`);
                            setTimeout(()=>{
                            	writeFile(file, content, wrapper);
                            }, 10);
                        }else{
                            callback(error, result);
                        }
                    }

                    writeFile(file, content, wrapper);
                });
			}
		}
	};

	this.registerConsumer = function (callback, shouldDeleteAfterRead = true, shouldWaitForMore = () => true) {
		if(typeof callback !== "function"){
			throw new Error("First parameter should be a callback function");
		}
		if (consumer) {
			throw new Error("Only one consumer is allowed! " + folder);
		}

		consumer = callback;
		fs.mkdir(folder, function (err, res) {
			consumeAllExisting(shouldDeleteAfterRead, shouldWaitForMore);
		});
	};

	this.writeMessage = writeFile;

	this.unlinkContent = function (messageId, callback) {
		const messagePath = path.join(folder, messageId);

		fs.unlink(messagePath, (err) => {
			callback(err);
	});
	};


	/* ---------------- protected  functions */
	var consumer = null;
	var producer = null;

    function buildPathForFile(filename){
        return path.normalize(path.join(folder, filename));
    }

	function consumeMessage(filename, shouldDeleteAfterRead, callback) {
		var fullPath = buildPathForFile(filename);

		fs.readFile(fullPath, "utf8", function (err, data) {
			if (!err) {
				if (data !== "") {
					try {
						var message = JSON.parse(data);
					} catch (error) {
						console.log("Parsing error", error);
						err = error;
					}

					callback(err, message);
					if (shouldDeleteAfterRead) {

						fs.unlink(fullPath, function (err, res) {
							if (err) throw err
						});

					}
				}
			} else {
				console.log("Consume error", err);
				callback(err);
			}
		});
	}

	function consumeAllExisting(shouldDeleteAfterRead, shouldWaitForMore) {

		let currentFiles = [];

		fs.readdir(folder, 'utf8', function (err, files) {
			if (err) {
				$$.errorHandler.error(err);
				return;
			}
			currentFiles = files;
			iterateAndConsume(files);

		});

		function startWatching(){
			if (shouldWaitForMore()) {
				watchFolder(shouldDeleteAfterRead, shouldWaitForMore);
			}
		}

		function iterateAndConsume(files, currentIndex = 0) {
			if (currentIndex === files.length) {
				//console.log("start watching", new Date().getTime());
				startWatching();
				return;
			}

			if (path.extname(files[currentIndex]) !== in_progress) {
				consumeMessage(files[currentIndex], shouldDeleteAfterRead, (err, data) => {
					if (err) {
						iterateAndConsume(files, ++currentIndex);
						return;
					}
					consumer(null, data, path.basename(files[currentIndex]));
					if (shouldWaitForMore()) {
						iterateAndConsume(files, ++currentIndex);
					}
				})
			} else {
				iterateAndConsume(files, ++currentIndex);
			}
		}

	}

	const in_progress = ".in_progress";
	function writeFile(filename, content, callback){
		var tmpFilename = filename+in_progress;
		/*fs.writeFile(tmpFilename, content, function(error, res){
			if(!error){
				fs.rename(tmpFilename, filename, callback);
			}else{
				callback(error);
			}
		});*/
        try{
            if(fs.existsSync(tmpFilename) || fs.existsSync(filename)){
            	console.log(new Error(`Overwriting file ${filename}`));
			}
            fs.writeFileSync(tmpFilename, content);
            fs.renameSync(tmpFilename, filename);
        }catch(err){
            callback(err);
        }
        callback(null, content);
	}

	var alreadyKnownChanges = {};

	function alreadyFiredChanges(filename, change){
		var res = false;
		if(alreadyKnownChanges[filename]){
			res = true;
		}else{
			alreadyKnownChanges[filename] = change;
		}

		return res;
	}

	function watchFolder(shouldDeleteAfterRead, shouldWaitForMore){

		setTimeout(function(){
            fs.readdir(folder, 'utf8', function (err, files) {
                if (err) {
                    $$.errorHandler.error(err);
                    return;
                }

                for(var i=0; i<files.length; i++){
                    watchFilesHandler("change", files[i]);
				}
            });
		}, 1000);

		function watchFilesHandler(eventType, filename){
			console.log(`Got ${eventType} on ${filename}`);

			if(!filename || path.extname(filename) == in_progress){
				//caught a delete event of a file
				//or
				//file not ready to be consumed (in progress)
				return;
			}

			var f = buildPathForFile(filename);
			if(!fs.existsSync(f)){
				console.log("File not found", f);
				return;
			}

			console.log(`Preparing to consume ${filename}`);
			if(!alreadyFiredChanges(filename, eventType)){
				consumeMessage(filename, shouldDeleteAfterRead, (err, data) => {
					//allow a read a the file
					alreadyKnownChanges[filename] = undefined;

					if (err) {
						// ??
						console.log("\nCaught an error", err);
						return;
					}

					consumer(null, data, filename);


					if (!shouldWaitForMore()) {
						watcher.close();
					}
				});
            }else{
				console.log("Something happens...", filename)
			}
		}


		const watcher = fs.watch(folder, watchFilesHandler);

        const intervalTimer = setInterval(()=>{
				fs.readdir(folder, 'utf8', function (err, files) {
					if (err) {
						$$.errorHandler.error(err);
						return;
					}

					if(files.length > 0){
						console.log(`\n\nFound ${files.length} files not consumed yet in ${folder}`, new Date().getTime(),"\n\n");
                        //faking a rename event trigger
						watchFilesHandler("rename", files[0]);
					}
				});
			}, 5000);
	}
}

exports.getFolderQueue = function(folder, callback){
	return new FolderMQ(folder, callback);
};
