var D = require('../../index.js').SimpleStore('aws');
  assert = require('assert'),
  util = require('util'),
  _ = require('underscore');

var ddl = new D('generic');



describe('DataAccessV2', function(){
  this.timeout(10000);
  describe('#put()', function(){
    it('should insert data into data store', function(done){
      var json = {
        str : 'string field',
        num : 100,
        strArr : ['hello', 'test'],
        map: {
          foo: 'string foo',
          bar: 100
        },
        mapArr: [ { fooA : 'barA'}],
        bool: true,
        null: null
      };
      ddl.put('row-id', 'generic-class-id', json, ['str', 'num'])
      .done(function(){
        done();
      }, function err(err){
        console.log(err);
        assert.ok(false);
      });
    });
  });

  var jsonData = null;
  describe('#get()', function(){
    it('should retrieve data from data store', function(done){
      ddl.get('row-id', 'generic-class-id')
      .done(function(results){
        jsonData = results;
        console.log(util.inspect(results, true, null));
        done();
      }, function err(err){
        throw err;
      });
    });

    it('should return null if it cannot find the item', function(done){
      ddl.get('not-exist-row-id', 'generic-class-id')
      .done(function(results){
        assert.equal(results, null,' should return null');
        done();
      }, function err(err){
        throw err;
      });
    });

  });


  describe('#update()', function(){
    it('should retrieve data from data store', function(done){
      var json = _.extend({}, jsonData);
      json.str = 'new string2';
      json.num = 200;
      ddl.update('row-id', 'generic-class-id', json)
      .done(function(results){
        console.log(util.inspect(results, true, null));
        done();
      }, function err(err){
        throw err;
      });
    });
    it('should not update with invalid revision number', function(done){
      var json = _.extend({}, jsonData);
      json.str = 'new string2';
      json.num = 200;
      json._rev = 'xxxx';
      ddl.update('row-id', 'generic-class-id', json)
      .done(function(results){
        assert.ok(false, 'should not be successful');
      }, function err(err){
        console.log(util.inspect(err, true, null));
        done();
      });
    });
  });

  describe('#query()', function(){
    it('should find the data based on the index', function(done){
      var expected = [{
        _id : 'row-id',
        data: { str : 'new string2', num: 200}
      }];
      ddl.query('generic-class-id', [{attr: 'str', op: '=', value:'new string2'}])
      .done(function(result){
        assert.deepEqual(result, expected);
        done();
      }, function(err){
        throw err;
      });
    });
  });

  describe('#delete()', function(){
    it('should delete item', function(done){
      ddl.delete('row-id', 'generic-class-id')
      .done(function(){
        done();
      }, function(err){
        throw err;
      });
    });
  });

});
