var assert = require('assert');
var RJQ = require('../lib/rjq').RJQ;
var RE_HASHSLOT = require('../lib/rjq').RE_HASHSLOT;

describe("RJQ", function(){
    it("Regular Exp detects namespace keys.", function(){
        assert(RE_HASHSLOT.test("aaa{bbb}ccc"));
        assert(RE_HASHSLOT.test("aaa}{bbb}ccc"));
        assert(RE_HASHSLOT.test("aaa{}ccc"));
        assert(RE_HASHSLOT.test("aaa{bbb}"));
        assert(RE_HASHSLOT.test("{bbb}"));
        assert(RE_HASHSLOT.test("{bbb}ccc"));
        assert(RE_HASHSLOT.test("{}ccc"));
        assert(RE_HASHSLOT.test("aaa{}"));
        assert(!RE_HASHSLOT.test("aaa}bbb"));
        assert(!RE_HASHSLOT.test("aaa{bbb"));
        assert(!RE_HASHSLOT.test("aaa}{bbb"));
        assert(!RE_HASHSLOT.test("a}aa{bbb"));
    });
    it("Surrounds the namespace with {} when no hash key is specified.", function(){
        assert((new RJQ("TEST")).namespace === "{TEST}");
        assert((new RJQ("TEST:NS")).namespace === "{TEST:NS}");
        assert((new RJQ("{TEST:NS}")).namespace === "{TEST:NS}");
        assert((new RJQ("{TEST}:NS")).namespace === "{TEST}:NS");
        assert((new RJQ("TEST:{NS}")).namespace === "TEST:{NS}");
    });
});
