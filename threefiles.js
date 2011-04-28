
/*

threefiles provides a very efficient way to write a snapshot to disk, overwriting previous snapshots, 
without worrying that truncation could result in complete data loss (as is possible if we write to only one file.)

This is accomplished using three files:
1. this file is fsynced and contains the last full copy written
2. this file may be in the process of fsyncing
3. this file is being written to

The files are rotated at a rate specified by the syncPeriodInMs parameter, so an fsync call will happen every syncPeriodInMs milliseconds.
The maximum delay in data being written to disk is syncPeriodInMs*(11/10) milliseconds, unless the operating system takes longer than that to return from an fsync call.

It also allows for reading, of course.  Right now it's just a big blob.

*/

var path = require('path'),
	sys = require('sys'),
	fs = require('fs');

var _ = require('underscorem');

function readInt(buffer, offset){

	var result = 0;

	result += buffer[0+offset] << 24;
	result += buffer[1+offset] << 16;
	result += buffer[2+offset] << 8;
	result += buffer[3+offset] << 0;

	return result;
}
function writeInt(buffer, offset, value){

	buffer[0+offset] = value >> 24;
	buffer[1+offset] = value >> 16;
	buffer[2+offset] = value >> 8;
	buffer[3+offset] = value >> 0;
}

function errCheck(err){
	if(err) throw err;
}

function makeHandle(f, readyCb){

	var wfd;

	var versionBuffer = new Buffer(4);
	var currentVersion;
	
	function writeVersion(){
		writeInt(versionBuffer, 0, currentVersion);
		fs.write(wfd, versionBuffer, 0, versionBuffer.length, 0, errCheck);
	}

	
	function write(version, buf){
		_.assertLength(arguments, 2);
	
		currentVersion = -1;
		writeVersion();//write -1 version initially
		currentVersion = version;
		
		
		fs.write(wfd, versionBuffer, 0, versionBuffer.length, 0, errCheck);
		fs.truncate(wfd, buf.length+4, errCheck);
		fs.write(wfd, buf, 0, buf.length, 4, errCheck);
	}
	
	//first we fsync the data, then the correct version number
	function fsync(cb){
		fs.fsync(wfd, function(err){
			if(err) throw err;
			
			var vv = currentVersion;
			writeVersion();
			
			fs.fsync(wfd, function(err){
				if(err) throw err;
			
				cb();
			});
		});
	}
	
	var handle = {
		write: write,
		fsync: fsync
	};

	function prepareForWrite(versionNumber, dataBuf){		
		path.exists(f, function(exists){
			if(exists){
				fs.open(f, 'r+', 0666, function(err, fd){
					if(err) throw err;
					wfd = fd;
					readyCb(handle, versionNumber, dataBuf);
				});
			}else{
				fs.open(f, 'w', 0666, function(err, fd){
					if(err) throw err;
					wfd = fd;
					readyCb(handle, versionNumber, dataBuf);
				});
			}
		});
	}

	path.exists(f, function(exists){
	
		if(exists){
			fs.stat(f, function(stats){
				fs.readFile(f, function(err, data){
					if(err) throw err;
					
					if(data.length < 4){
						prepareForWrite();
					}else{
						versionNumber = readInt(data, 0);
						dataBuf = data.slice(4, data.length);
						prepareForWrite(versionNumber, dataBuf);
					}
				});
			});
		}else{
			prepareForWrite();
		}
	});
}

function make(dir, syncPeriodInMs, readyCb){

	//TODO support directly closing

	function finishMake(){
		makeHandle(dir+'/a.bin', readyProcessor.bind(undefined, 0));
		makeHandle(dir+'/b.bin', readyProcessor.bind(undefined, 1));
		makeHandle(dir+'/c.bin', readyProcessor.bind(undefined, 2));
	}
	
	path.exists(dir, function(exists){
	
		if(exists){
			finishMake();
		}else{
			fs.mkdir(dir, 0755, function(err){
				if(err) throw err;
				
				finishMake();
			});
		}
	});

	var currentBuf;

	var handles = [];
	var currentlySyncing;
	var cur;
	var dirty = false;
	var lastSync;
	
	var version;
	var callOnEnd;
	
	
	function inc(){
		cur = (cur + 1) % 3;
	}
	
	function doneSync(){
		currentlySyncing = undefined;
		if(dirty){
			incSync();
		}else{
			if(callOnEnd){
				callOnEnd();
			}
		}
	}
	
	function directIncSync(){
		currentlySyncing = cur;
		inc();
		dirty = false;
		handles[currentlySyncing].fsync(doneSync);
		lastSync = Date.now();
	}
	var incSync = _.throttle(directIncSync, syncPeriodInMs);
	
	function writer(buf){
		currentBuf = buf;
		diskWriter();
	}
	
	var handle = {
		write: writer,
		end: function(cb){
			if(cb){
				_.assertFunction(cb);
				if(currentlySyncing !== undefined || currentBuf !== undefined){
					callOnEnd = cb;
				}else{
					cb();
				}
			}
		}
	};
	
	var diskWriter = _.throttle(function(){
		var buf = currentBuf;
		++version;

		var h = handles[cur];
		h.write(version, buf);
		dirty = true;
		if(currentlySyncing === undefined){
			incSync(h);			
		}
		currentBuf = undefined;
	}, syncPeriodInMs/10);

	var options = [];
	
	var cdl = _.latch(3, function(){
	
		if(options.length === 0){
			cur = 0;
			version = 0;
			readyCb(handle);
		}else{
			var maxVersion = options[0][0];
			var index = 0;
			_.each(options, function(op, i){
				if(op === undefined) return;
				if(maxVersion < op[0]){
					maxVersion = op[0]; 
					index = i;
				}
			});

			cur = index;
			inc();
			version = maxVersion;
		
			readyCb(handle, options[index][1]);
		}
		options = undefined;
		cdl = undefined;
	});
	
	function readyProcessor(index, handle, versionNumber, buf){
		handles[index] = handle;
		if(versionNumber !== undefined){
			options[index] = [versionNumber, buf];
		}
		cdl();
	}
		
	
}

exports.make = make;
