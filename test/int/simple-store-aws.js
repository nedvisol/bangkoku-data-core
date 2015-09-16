var D = require('../../index.js').SimpleStore('aws');
  assert = require('assert'),
  util = require('util'),
  _ = require('underscore');

var ddl = new D('generic');

/**** Initialize
var D = new (require('./index.js').SimpleStore('aws'))('generic');
D.initializeDataStore(1,1).done(function(){});
**/


describe('DataAccessV2', function(){
  this.timeout(10000);
  describe('#put()', function(){
    it('should insert data into data store 1', function(done){
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
      ddl.put('row-id', 'generic-class-id', json, { previewAttrs: ['str','num'], indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
      .delay(1000)
      .done(function(){
        done();
      }, function err(err){
        console.log(err);
        assert.ok(false);
      });
    });

    it('should insert data into data store 2', function(done){
      var json = {
        str : 'string field',
        num : 101,
      };
      ddl.put('row-id2', 'generic-class-id', json, { previewAttrs: ['str','num'], indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
      .delay(1000)
      .done(function(){
        done();
      }, function err(err){
        console.log(err);
        assert.ok(false);
      });
    });

    it('should insert data into data store 3', function(done){
      var json = {
        str : 'string fieldXX',
        num : 102,
      };
      ddl.put('row-id3', 'generic-class-id', json, { previewAttrs: ['str','num'], indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
      .delay(1000)
      .done(function(){
        done();
      }, function err(err){
        console.log(err);
        assert.ok(false);
      });
    });

  });

  describe('#query()', function(){
    it('should retrieve 2 rows based on str attr with sort', function(done){
      ddl.query('generic-class-id', {indexName: 'strSorted', value: 'string field', sort: 'D'}, null)
      .done(function(results){
        console.log('******* query results *****');
        console.log(util.inspect(results, true, null));
        console.log('***************************');
        assert.equal(2, results.length);
        done();
      });
    });

    it('should retrieve 2 rows based on str attr with no sort', function(done){
      ddl.query('generic-class-id', {indexName: 'str', value: 'string field'}, null)
      .done(function(results){
        console.log('******* query results *****');
        console.log(util.inspect(results, true, null));
        console.log('***************************');
        assert.equal(2, results.length);
        done();
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
      ddl.update('row-id', 'generic-class-id', json, { previewAttrs: ['str','num'], indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
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
/*
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
*/


describe('#query()', function(){
  it('should retrieve 1 rows based on str attr with sort', function(done){
    ddl.query('generic-class-id', {indexName: 'strSorted', value: 'string field', sort: 'D'}, null)
    .done(function(results){

      console.log('******* query results *****');
      console.log(util.inspect(results, true, null));
      console.log('***************************');
      assert.equal(1, results.length);
      done();
    });
  });

  it('should retrieve 1 rows based on str attr with no sort', function(done){
    ddl.query('generic-class-id', {indexName: 'str', value: 'string field'}, null)
    .done(function(results){
      console.log('******* query results *****');
      console.log(util.inspect(results, true, null));
      console.log('***************************');
      assert.equal(1, results.length);
      done();
    });
  });


});


  describe('#delete()', function(){
    it('should delete item', function(done){
      ddl.delete('row-id', 'generic-class-id', { indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
      .then(function(){
        return ddl.delete('row-id2', 'generic-class-id',{ indexOptions: { strSorted : {
            attr: 'str', sortAttr: 'num'
         } } })
      })
      .then(function(){
        return ddl.delete('row-id3', 'generic-class-id', {indexOptions: { strSorted : {
            attr: 'str', sortAttr: 'num'
         } } })
      })
      .done(function(){
        done();
      })
    });
  });

});
