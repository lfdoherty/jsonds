
var threefiles = require('./threefiles');

var sys = require('sys');

var _ = require('underscorem');

var crypto = require('crypto');

var lzo = require('mini-lzo-wrapper');

var bin = require('util/bin');

function computeHash(buf){
	var hasher = crypto.createHash('md5');
	hasher.update(buf);
	return hasher.digest('base64');
}

function make(dir, periodInMs, cb){

	_.assertLength(arguments, 3);
	_.assertString(dir);
	_.assertInt(periodInMs);
	_.assertFunction(cb);

	var start = Date.now();
	
	threefiles.make(dir, periodInMs, function(w, initialData){
	
	
		var ready = Date.now();

		var js;
		
		var last;
		
		if(initialData){

			var decompLen = bin.readInt(initialData, 0);
			var actualData = initialData.slice(4);
			
			sys.debug('decompLen: ' + decompLen);
			
			var decompData = new Buffer(decompLen);
			
			lzo.decompress(actualData, decompData);
			
			

			//var decompData = initialData;
			
			last = computeHash(decompData);
			
			js = JSON.parse(decompData.toString('utf8'));
			sys.debug('jsonds loaded ' + decompData.length + ' bytes of json in ' + (ready - start) + 'ms.');
		}else{
			js = {};
			sys.debug('jsonds created in ' + (ready - start) + 'ms.');
		}
		
		
		var ending = false;
		
		function doWrite(){
			if(ending) _.errout('already ending');
			
			var str = JSON.stringify(js);
			var buf = new Buffer(str);
			
			//console.log('checking if jsonds should write: ' + str);
			
			var hash = computeHash(buf);
			if(last === undefined || hash !== last){
				last = hash;
				
				var compBufferSize = Math.ceil(buf.length * 1.1) + 50;//the creator of LZO says 106% is the maximum he's ever seen - so 110% plus 50 bytes should be enough.
				
				var comp = new Buffer(compBufferSize);
				var len = lzo.compress(buf, comp);
				
				var diskBuf = new Buffer(len + 4);
				bin.writeInt(diskBuf, 0, buf.length);
				comp.copy(diskBuf, 4, 0, len);

				w.write(diskBuf);
				//sys.debug('jsonds writing to disk: ' + diskBuf.length + ' compressed bytes (' + buf.length + ' bytes uncompressed.)');
			}else{
				//sys.debug('skipping write, same hash');
			}
		}
		
		var writeHandle = setInterval(doWrite, periodInMs);

		function doEnd(endCb){
			clearInterval(writeHandle);
			doWrite();
			ending = true;					
			w.end(endCb);
		}

		var handle = {
			root: js,
			force: doWrite,
			end: doEnd
		};
		
		cb(handle);
	});
}

exports.make = make;

