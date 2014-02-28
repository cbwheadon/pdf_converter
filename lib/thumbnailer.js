var exec = require('child_process').exec,
	_ = require('underscore'),
	fs = require('fs'),
	config = require('./config').Config;

/**
 * Initialize the Thumbnailer
 */
function Thumbnailer(opts) {
	// for the benefit of testing
	// perform dependency injection.
	_.extend(this, {
		tmp: require('tmp')
	}, opts);
}

/**
 * Execute the image conversion command
 *
 * @param object description The job description
 * @param localPath The local path to the image
 * @param function onComplete The callback function
 */
Thumbnailer.prototype.execute = function(description, localPath, onComplete) {
	var _this = this;

	// parameters for a single execution
	// of the thumbnailer
	_.extend(this, {
		localPath: localPath,
		format: (description.format || 'png'),
		strategy: (description.strategy || 'pdf'),
		background: (description.background || 'black'),
		quality: (description.quality || 0),
		onComplete: onComplete,
		thumbnailTimeout: 0
	});

	this.createConversionPath(function(err) {

		if (err) {
			_this.onComplete(err);
			return;
		}

		// apply the thumbnail creation strategy.
		if (!_this[_this.strategy]) {
			_this.onComplete('could not find strategy ' + _this.strategy);
		} else {
			_this[_this.strategy]();
		}
	});
};

/**
 * Create a temp file for the converted image
 *
 * @param function callback The callback function
 */
Thumbnailer.prototype.createConversionPath = function(callback) {
    var _this = this;
    this.tmp.dir({prefix: config.get('tmpDir')}, function(err, convertedPath) {
	//fs.closeSync(fd); // close immediately, we do not use this file handle.
	_this.convertedPath = convertedPath;
	callback(err);
    });
};

/**
 * Execute the conversion command
 *
 * @param string command The command
 */
Thumbnailer.prototype.execCommand = function(command) {
	var _this = this;

	exec(command, {timeout: this.thumbnailTimeout}, function(err, stdout, stderr) {

		console.log('running command ', command);

		if (err) {
			_this.onComplete(err);
			return;
		}
	    console.log('path', _this.convertedPath);
	    fs.readdir(_this.convertedPath, function(err,files){
		if(err || files.length === 0){
		    err = 'No files created';
		    _this.onComplete(err);
		    return;
		}
		_this.onComplete(null, _this.convertedPath);
	    })
	});
};

/**
 * Convert a pdf
 */
Thumbnailer.prototype.pdf = function() {
	var qualityString = (this.quality ? '-quality ' + this.quality : ''),
		thumbnailCommand = config.get('convertCommand') + ' -fuzz 10% -transparent none -density 200 -trim "' + this.localPath + '" ' + qualityString + ' ' + this.convertedPath + '/%d.png';
	this.execCommand(thumbnailCommand);
};

/**
 * Convert the image using the matted strategy
 */
Thumbnailer.prototype.matted = function() {
	var qualityString = (this.quality ? '-quality ' + this.quality : ''),
		thumbnailCommand = config.get('convertCommand') + ' "' + this.localPath + '[0]" -thumbnail ' + (this.width * this.height) + '@ -gravity center -background ' + this.background + ' -extent ' + this.width + 'X' + this.height + ' ' + qualityString + ' ' + this.convertedPath;

	this.execCommand(thumbnailCommand);
};

/**
 * Convert the image using the bounded strategy
 */
Thumbnailer.prototype.bounded = function() {
	var dimensionsString = this.width + 'X' + this.height,
		qualityString = (this.quality ? '-quality ' + this.quality + ' ' : ''),
		thumbnailCommand = config.get('convertCommand') + ' "' + this.localPath + '[0]" -thumbnail ' + dimensionsString + ' ' + qualityString + this.convertedPath;

	this.execCommand(thumbnailCommand);
};

/**
 * Convert the image using the fill strategy
 */
Thumbnailer.prototype.fill = function() {
	var dimensionsString = this.width + 'X' + this.height,
		qualityString = (this.quality ? '-quality ' + this.quality : ''),
		thumbnailCommand = config.get('convertCommand') + ' "' + this.localPath + '[0]" -resize ' + dimensionsString + '^ -gravity center -extent ' + dimensionsString + ' ' + qualityString + ' ' + this.convertedPath;

	this.execCommand(thumbnailCommand);
};

exports.Thumbnailer = Thumbnailer;
