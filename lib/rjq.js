var redis = require('ioredis');
var fs = require('fs');
var path = require('path');
var util = require('./util');
// var job = require('./job');
var jobAsStringArray = require('./job').asStringArray;
var validateJob = require('./job').validate;

var QUEUED = "QUEUED";
var SCHEDULED = "SCHEDULED";
var WORKING = "WORKING";
var WORKERS = "WORKERS";
var FAILED = "FAILED";
var SEP = ":";

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

function makeKey(){
    var args = Array.prototype.slice.call(arguments);
    return args.join(SEP);
}

function RJQ(namespace, options) {
    this.namespace = "{" + namespace + "}";
    options = options || {};
    this.workerName = options.workerName || util.generateWorkerName();
    this.jobExp = options.jobExp || 90;
    this.redisUrl = options.redisUrl;
    this.client = new redis.Redis(this.redisUrl);
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
        return redis.Promise.reject(err);
    }
    var args = [this.namespace, queue, priority, job.id, force];
    return this.client.enqueue.apply(this.client, args.concat(jobAsStringArray(job)));
};

RJQ.prototype.add = function add(job){
    return this.enqueue(job, job.priority, QUEUED, "0");
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

// TODO: peek/list
// TODO: Subscribe

/* Get/Set maxfailed.
 *
 * If `n` is provided, sets the maxfailed value for this namespace.
 * If `n` is not provided, simply returns the value of maxfailed for this
 * namespace.
 */
RJQ.prototype.maxfailed = function maxfailed(n){
    return this.client.maxfailed(this.namespace, (isFinite(n) && n) || "");
};

/* Get/Set maxjobs.
 *
 * If `n` is provided, sets the maxjobs value for this namespace.
 * If `n` is not provided, simply returns the value of maxjobs for this
 * namespace.
 */
RJQ.prototype.maxjobs = function maxjobs(n){
    return this.client.maxjobs(this.namespace, (isFinite(n) && n) || "");
};

RJQ.prototype.test = function test(){
    return this.client.test();
};
