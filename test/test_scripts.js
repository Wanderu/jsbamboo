var assert = require('assert');
var RJQ = require('../lib/rjq').RJQ;
var LUA_SCRIPTS = require('../lib/rjq').LUA_SCRIPTS;

describe("Scripts", function() {
    it("Loads all expected scripts properly.", function(){
        var rjq = new RJQ("TEST:JSBAMBOO");
        var luaFun;
        for (luaFun in LUA_SCRIPTS){
            if(LUA_SCRIPTS.hasOwnProperty(luaFun)){
                assert(rjq.client.hasOwnProperty(luaFun));
            }
        }
    });
});
