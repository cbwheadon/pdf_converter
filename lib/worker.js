var aws = require('aws-sdk'),
	_ = require('underscore'),
	config = require('./config').Config,
	Grabber = require('./grabber').Grabber,
	Thumbnailer = require('./thumbnailer').Thumbnailer,
	Saver = require('./saver').Saver,
	fs = require('fs'),
	async = require('async');

/**
 * Initialize the Worker
 *
 * @param object opts Worker configuration. Optional.
 */
function Worker(opts) {
	_.extend(this, {
		thumbnailer: null,
		grabber: null,
		saver: null
	}, opts);

	this.sqs = new aws.SQS({
		accessKeyId: config.get('awsKey'),
		secretAccessKey: config.get('awsSecret'),
		region: config.get('awsRegion')
	});

	config.set('sqsQueueUrl', this.sqs.endpoint.protocol + '//' + this.sqs.endpoint.hostname + '/' + config.get('sqsQueue'));

	config.set('sqsReplyUrl', this.sqs.endpoint.protocol + '//' + this.sqs.endpoint.hostname + '/' + config.get('sqsReply'));

}

/**
 * Start the worker
 */
Worker.prototype.start = function() {
	this._processSQSMessage();
};

/**
 * Process the next message in the queue
 */
Worker.prototype._processSQSMessage = function() {
	var _this = this;

	console.log('wait for message on ' + config.get('sqsQueue'));

	this.sqs.receiveMessage( { QueueUrl: config.get('sqsQueueUrl'), MaxNumberOfMessages: 1 }, function (err, job) {
		if (err) {
			console.log(err);
			_this._processSQSMessage();
			return;
		}

		if (!job.Messages || job.Messages.length === 0) {
			_this._processSQSMessage();
			return;
		}

		// Handle the message we pulled off the queue.
		var handle = job.Messages[0].ReceiptHandle,
			body = null;

		try { // a JSON string message body is accepted.
			body = JSON.parse( job.Messages[0].Body );
		} catch(e) {
			if (e instanceof SyntaxError) {
				// a Base64 encoded JSON string message body is also accepted.
				body = JSON.parse( new Buffer(job.Messages[0].Body, 'base64').toString( 'binary' ) );
			} else {
				throw e;
			}
		}

		_this._runJob(handle, body, function() {
			_this._processSQSMessage();
		});
	});
};

/**
 * Process a job from the queue
 *
 * @param string handle The SQS message handle
 * @param object job The job parameters
 * @param function callback The callback function
 */
Worker.prototype._runJob = function(handle, job, callback) {
	/**
		job = {
			"original": "/foo/awesome.jpg",
			"descriptions": [{
				"suffix": "small",
				"width": 64,
				"height": 64
			}],
		}
	*/
	var _this = this;

	this._downloadFromS3(job.original, function(err, localPath) {

		if (err) {
			console.log(err);
			callback();
			return;
		}

		_this._createThumbnails(localPath, job, function(err) {
			fs.unlink(localPath, function() {
				if (!err) {
					_this._deleteJob(handle);
				}
				callback();
			});
		});

	});
};

/**
 * Download the image from S3
 *
 * @param string remoteImagePath The s3 path to the image
 * @param function callback The callback function
 */
Worker.prototype._downloadFromS3 = function(remoteImagePath, callback) {
	this.grabber.download(remoteImagePath, function(err, localPath) {

		// Leave the job in the queue if an error occurs.
		if (err) {
			callback(err);
			return;
		}

		callback(null, localPath);
	});
};

/**
 * Create thumbnails for the image
 *
 * @param string localPath The local path to store the images
 * @param object job The job description
 * @param function callback The callback function
 */
Worker.prototype._createThumbnails = function(localPath, job, callback) {

    var _this = this,
    work = [];

    // Create thumbnailing work for each thumbnail description.
    work.push(function(done) {
	var remoteImagePath = job.original,
	thumbnailer = new Thumbnailer();

	thumbnailer.execute(job, localPath, function(err, convertedImagePath) {

	    if (err) {
		console.log(err);
		done();
	    } else {
		_this._saveThumbnailToS3(convertedImagePath, remoteImagePath);
		_this._sendReply(job.id, _this.files);
		done();
	    }

	});
    });

    // perform thumbnailing in parallel.
    async.parallel(work, function(err, results) {
	callback(err);
    });

};

/**
 * Reply
 *
 * @param string files The converted image paths
 */
Worker.prototype._sendReply = function(id, files){
    console.log("sending: ",id, files)
	this.sqs.sendMessage({QueueUrl: config.get('sqsReplyUrl'), MessageBody: JSON.stringify({
	    id: id,
	    files: files
	})}, function (err, result) {
	    console.log(err,result);
	});
};


/**
 * Save the thumbnail to S3
 *
 * @param string convertedImagePath The local path to the image
 * @param string remoteImagePath The S3 path for the image
 * @param function callback The callback function
 */
Worker.prototype._saveThumbnailToS3 = function(convertedImagePath, remoteImagePath, callback) {
    //Save each file in the folder
    //this.saver.save(convertedImagePath + '/0.png', remoteImagePath);
    var _this = this;
    files = fs.readdirSync(convertedImagePath);
    console.log('files: ',files);
    _this.files = files;
    for(i =0; i < files.length; i++){
	convertedFilePath = convertedImagePath + '/' + files[i];
	remoteFilePath = remoteImagePath.split('.')[0] + '.' + files[i];
	_this.saver.save(convertedFilePath, remoteFilePath, function(err){
	    //fs.unlinkSync(convertedFilePath);
	});
    }
    //fs.rmdirSync(convertedImagePath);
};

/**
 * Generate a path for this thumbnail
 *
 * @param string original The original image path
 * @param string suffix The thumbnail suffix. e.g. "small"
 * @param string format The thumbnail format. e.g. "jpg". Optional.
 */
Worker.prototype._thumbnailKey = function(original, suffix, format) {
	var extension = original.split('.').pop(),
		prefix = original.split('.').slice(0, -1).join('.');

	return prefix + '_' + suffix + '.' + (format || 'png');
};

/**
 * Remove a job from the queue
 *
 * @param string handle The SQS message handle
 */
Worker.prototype._deleteJob = function(handle) {
	this.sqs.deleteMessage({QueueUrl: config.get('sqsQueueUrl'), ReceiptHandle: handle}, function(err, resp) {
		if (err) {
			console.log("error deleting thumbnail job " + handle, err);
			return;
		}
		console.log('deleted thumbnail job ' + handle);
	});
};

exports.Worker = Worker;
