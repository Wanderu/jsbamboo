var os = require('os');

var CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

/* Generate a worker name based on the hostname, pid, and random characters.
 */
function genWorkerName(){
    var i;
    var name = os.hostname() + "-" + process.pid + "-";
    for (i=0; i<6; i++){
        name += CHARS[Math.floor(Math.random()*CHARS.length)];
    }
    return name;
}

module.exports = {
    genWorkerName: genWorkerName
};
