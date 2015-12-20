var RJQ = require('../lib/rjq').RJQ;

var redis = require('ioredis');
var rjq = new RJQ("TEST:NS");
var job1 = {id: '123', priority: 2, payload: 'jobdata'};

// rjq.add(job1).then(function(res){
//     console.log("Added job1");
//     console.log(res);
// }).then(function(){
redis.Promise.resolve()
.then(function(){
    var p = new redis.Promise(function(resolve, reject){

        var jstream = rjq.peek("QUEUED");
        jstream.on('data', function(job){
            console.log("jstream received data");
            console.log(job);
        });

        jstream.on('end', function(){
            console.log("jstream end");
            resolve();
        });

        jstream.on('error', function(err){
            console.log("jstream error");
            console.log(err);
            reject();
        });

    });

    return p;
}).done();
