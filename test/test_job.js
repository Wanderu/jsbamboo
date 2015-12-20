var assert = require('assert');
var job = require('../lib/job');

function jobCmp(job1, job2){
    return job1.id === job2.id
        && job1.priority === job2.priority
        && job1.payload === job2.payload;
}

describe("Job", function(){
    it("Serializes to/from string array.", function(){
        var j0 = {
            id: '123',
            priority: 1,
            payload: 'stuff'
        };
        var jobStrArray = ['id', '123', 'priority', '1', 'payload', 'stuff'];
        var j1 = job.fromStringArray(jobStrArray);
        var jobStrArray2 = job.toStringArray(j1);
        var j2 = job.fromStringArray(jobStrArray2);
        assert.ok(jobCmp(j0, j1));
        assert.ok(jobCmp(j1, j2));
    });
});
