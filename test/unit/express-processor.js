var exproc = require('../../lib/expression-processor.js'),
assert = require('assert');

describe('Expression Processor', function(){
  describe('#eval()', function(){
    it('should evalute and process the expression', function(){
      var scope = {
        num: 100,
        two: 2,
        foo: 'bar',
        foos: ['bar','batz'],
        map: { attr: 'value' }
      };
      assert.equal(exproc.eval('1+1', scope), 2);
      assert.equal(exproc.eval('"abc"+"def"', scope), 'abcdef');
      assert.equal(exproc.eval('num+1', scope), 101);
      assert.equal(exproc.eval('num == 100', scope), true);
      assert.equal(exproc.eval('num+1 == 101', scope), true);
      assert.equal(exproc.eval('num+10 == 101', scope), false);
      assert.equal(exproc.eval('foo == "bar"', scope), true);
      assert.equal(exproc.eval('foo.length == 3', scope), true);
      assert.equal(exproc.eval('foo.toUpperCase()', scope), 'BAR');
      assert.equal(exproc.eval('foo.substr(1)', scope), 'ar');
      assert.equal(exproc.eval('foo.substring(1,two)', scope), 'a');
      assert.equal(exproc.eval('foos[0]', scope), 'bar');
      assert.equal(exproc.eval('map.attr == "value"', scope), true);
      assert.equal(exproc.eval('(num * two) + 3', scope), 203);
    });
  });
});
