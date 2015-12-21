var redis = require('ioredis');
var Promise = redis.Promise;
var fs = require('fs');
var path = require('path');
var util = require('util');
var Transform = require('stream').Transform;

var generateWorkerName = require('./util').generateWorkerName;
var jobToStringArray = require('./job').toStringArray;
var jobFromObject = require('./job').fromObject;
var validateJob = require('./job').validate;
var jobIsValid = require('./job').isValid;
var toJob = require('./job').toJob;

// TODO: Real logging
var logger = console;

var QUEUED = "QUEUED";
var SCHEDULED = "SCHEDULED";
var WORKING = "WORKING";
var WORKERS = "WORKERS";
var FAILED = "FAILED";
var SEP = ":";

var QUEUES = {
    QUEUED: 1,
    SCHEDULED: 1,
    WORKING: 1,
    WORKERS: 1,
    FAILED: 1
};

var SCRIPT_SUFFIX = '.lua';
var SCRIPTS_DIR = path.join(__dirname, '..', 'scripts');
var LUA_SCRIPTS = {
    ack: 1,
    can_consume: 1,
    cancel: 1,
    close: 1,
    consume: 1,
    enqueue: 1,
    fail: 1,
    maxfailed: 1,
    maxjobs: 1,
    recover: 1,
    test: 1
};

var RE_HASHSLOT = /.*?\{.*?\}.*?/;

function makeKey(){
    var args = Array.prototype.slice.call(arguments);
    return args.join(SEP);
}

function RJQ(namespace, options) {
    this.namespace = namespace;
    // Surround the namespace with {} for cluster hashing.
    if(!RE_HASHSLOT.test(this.namespace)){
        this.namespace = "{" + this.namespace + "}";
    }
    options = options || {};
    this.jobExp = ((options.jobExp !== undefined) && options.jobExp) || 90
    this.workerName = options.workerName || generateWorkerName();
    this.jobExp = options.jobExp || 90;
    this.redisUrl = options.redisUrl;
    this.client = new redis(this.redisUrl);
    // Set the client name on the redis server itself.
    this.client.client('SETNAME', this.workerName);
    this.loadScripts();
}

RJQ.prototype.key = function key() {
    var args = Array.prototype.slice.call(arguments);
    args.unshift(this.namespace);
    return makeKey.apply(this, args);
};

RJQ.prototype.loadScripts = function loadScripts() {
    var luaFun, modPath, luaScript;
    for (luaFun in LUA_SCRIPTS){
        if(LUA_SCRIPTS.hasOwnProperty(luaFun)){
            modPath = path.join(SCRIPTS_DIR, luaFun + SCRIPT_SUFFIX);
            luaScript = fs.readFileSync(modPath);
            this.client.defineCommand(luaFun, {
                numberOfKeys: LUA_SCRIPTS[luaFun],
                lua: luaScript
            });
        }
    }
};

// All functions return a promise.

RJQ.prototype.enqueue = function enqueue(job, priority, queue, force) {
    try {
        validateJob(job);
    } catch (err) {
        return Promise.reject(err);
    }
    var args = [this.namespace, queue, priority, job.id, force];
    var jobArr = jobToStringArray(job);
    args = args.concat(jobArr);
    return this.client.enqueue.apply(this.client, args);
};

RJQ.prototype.add = function add(job){
    return this.enqueue(job, job.priority, QUEUED, "0");
};

RJQ.prototype.consume = function consume(jobid){
    jobid = jobid || "";
    var args = [this.namespace,
                this.workerName,
                jobid,
                new Date().getTime(),
                this.jobExp];
    return this.client.consume.apply(this.client, args).then(function(jobArray){
        return toJob(jobArray);
    });

};

RJQ.prototype.requeue = function requeue(job){
    return this.enqueue(job, job.priority, QUEUED, "1");
};

RJQ.prototype.schedule = function schedule(job, time){
    return this.enqueue(job, time, SCHEDULED, "0");
};

RJQ.prototype.reschedule = function reschedule(job, time){
    return this.enqueue(job, time, SCHEDULED, "1");
};

RJQ.prototype.canConsume = function canConsume(){
    return this.client.can_consume(this.namespace, new Date().getTime());
};

RJQ.prototype.get = function get(jobId){
    // returns an object, not an array.
    return this.client.hgetall(this.key("JOBS", jobId));
};

RJQ.prototype.ack = function ack(job){
    return this.client.ack(this.namespace, job.id);
};

RJQ.prototype.fail = function fail(job, requeue_seconds){
    if (!isFinite(requeue_seconds)){
        requeue_seconds = 3600 * Math.pow(job.Failures, 2);
    }
    return this.client.fail(this.namespace, job.id, new Date().getTime(), requeue_seconds);
};

RJQ.prototype.recover = function recover(){
    return this.client.recover(this.namespace, new Date().getTime(), 3600);
};

RJQ.prototype.cancel = function cancel(jobId){
    return this.client.cancel(this.namespace, jobId);
};

/* Stop receiving callbacks when Jobs are added to the QUEUED queue.
 */
RJQ.prototype.unsubscribe = function unsubscribe(){
    return this.pubsub.unsubscribe().then(function(){
        this.pubsub.disconnect();
    }.bind(this));
};

/* Call callback when a Job is added to the QUEUED queue.
 */
RJQ.prototype.subscribe = function subscribe(callback){
    this.pubsub = new redis(this.redisUrl);
    var pskey = this.key(QUEUED);

    pubsub.on('message', function(channel, message){
        callback(message);
    });

    return pubsub.client('SETNAME', this.workerName)
    .then(function(){
        return this.pubsub.subscribe(pskey);
    }.bind(this));
};

/* A node transform stream that turns Job IDs into Job objects.
 */
function JobStream(rjq) {
    Transform.call(this, {objectMode: true});
    this._rjq = rjq;
}

util.inherits(JobStream, Transform);

JobStream.prototype._transform = function(chunk, encoding, transCB){
    var self = this;
    // chunk: an array of [job1id, job1priority, job2id, job2priority... ]
    // encoding: IE. 'utf8'
    jobIds = [];
    for(i=0; i<chunk.length; i=i+2){
        jobIds.push(chunk[i]);
    }
    Promise.map(jobIds, function(jobId){
        return self._rjq.get(jobId).then(function(job){
            // turn object into Job
            try {
                job = jobFromObject(job);
                self.push(job);
                return job;
            } catch(err){
                logger.error(err);
                // validation error
                // TODO: Figure out what to do with this.
                // If any error happened with 1 job, error for the whole batch?
            }
            return null;
        });
    }).then(function(jobs){
        transCB();
    });
};

/* Returns a node stream of job objects from the specified queue. Use the
 * stream.on('data', ...) and stream.on('end', ...) to recieve the job objects.
 * Job objects are returned in priority order.
 *
 * queue: The queue from which to fetch job objects.
 * n: Integer. Optional. The maximum number of job objects to return.
 */
RJQ.prototype.peek = function peek(queue, n){
    if(!QUEUES.hasOwnProperty(queue)){
        throw Error("Invalid queue name.");
    }
    var opts = {};
    if(n !== undefined){
        opts.count = n;
    }
    var jstream = new JobStream(this);
    var zstream = this.client.zscanStream(this.key(queue), opts);
    zstream.pipe(jstream);
    return jstream;
};

/* Get/Set maxfailed.
 *
 * If `n` is provided, sets the maxfailed value for this namespace.
 * If `n` is not provided, simply returns the value of maxfailed for this
 * namespace.
 */
RJQ.prototype.maxfailed = function maxfailed(n){
    var args = [this.namespace];
    if(isFinite(n)){
        args.push(n);
    }
    return this.client.maxfailed.apply(this.client, args);
};

/* Get/Set maxjobs.
 *
 * If `n` is provided, sets the maxjobs value for this namespace.
 * If `n` is not provided, simply returns the value of maxjobs for this
 * namespace.
 */
RJQ.prototype.maxjobs = function maxjobs(n){
    var args = [this.namespace];
    if(isFinite(n)){
        args.push(n);
    }
    return this.client.maxjobs.apply(this.client, args);
};

RJQ.prototype.test = function test(){
    return this.client.test();
};

module.exports = {
    RJQ: RJQ,
    makeKey: makeKey,
    LUA_SCRIPTS: LUA_SCRIPTS,
    RE_HASHSLOT: RE_HASHSLOT,
    QUEUES: QUEUES
};
