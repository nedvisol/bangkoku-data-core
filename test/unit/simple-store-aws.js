var assert = require('assert');
var sinon = require('sinon');
var D = require('../../index.js').SimpleStore('aws');
var mmHash = require('../../lib/murmur-hash.js');
var Q = require('Q'),
  util = require('util'),
  jsep = require('jsep'),
  _ = require('underscore');

var ddl = new D('generic');

function restoreAll(stubs) {
  _.each(stubs, function(stub){
    stub.restore();
  });
}


describe('SimpleStore-AWS', function(){
  /*var genIdStub = sinon.stub(ddl, 'generateId', function(){
    return 'A0000';
  });*/
  describe('#generateId()', function() {
    it('should generate string representation of UUID v4', function(){
      var id = ddl.generateId();
      console.log('Generate ID: '+id);
      assert.ok(id.length > 20);
    });
  });
  describe('#_Json2DynDB', function(){
    it('should convert JSON obj to DynamoDB structure', function(){
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

      var expected = {
        str : { S : 'string field' },
        num : { N : '100' },
        strArr : { SS : ['hello', 'test'] },
        map : { M : {
          foo : { S : 'string foo'},
          bar : { N : '100' }
        }},
        mapArr : { L : [ { M :{ fooA : {S : 'barA'}}} ]},
        bool: { BOOL : true },
        null: { NULL : true }
      };

      var actual = ddl._Json2DynDB(json);

      //console.log(util.inspect(actual, true,  null));
      assert.deepEqual(expected, actual);
    });
  });


  describe('#_DynDB2Json', function(){
    it('should convert DynamoDB structure to JSON object', function(){
      var expected = {
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

      var dynDB = {
        str : { S : 'string field' },
        num : { N : '100' },
        strArr : { SS : ['hello', 'test'] },
        map : { M : {
          foo : { S : 'string foo'},
          bar : { N : '100' }
        }},
        mapArr : { L : [ { M : { fooA : {S : 'barA'}}} ]},
        bool: { BOOL : true },
        null: { NULL : true }
      };

      var actual = ddl._DynDB2Json(dynDB);

      //console.log(util.inspect(actual, true,  null));
      assert.deepEqual(expected, actual);
    });
  });


  describe('#_generateIndexItems', function(){
    it('should convert json data into index items', function(){
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


      var expected = [
        { PutRequest: { Item: {
          lookupKey : { S : 'classId.str=string field'},
          id : {S : 'rowId'},
          sortVal : { S : '0000000000000034'}, //base 32 of 100
          data : { M : { str : { S : 'string field'}, num : { N : '100'} }}
        }}},
        { PutRequest: { Item: {
          lookupKey : { S : 'classId.num=0000000000000034'},
          id : {S : 'rowId'},
          sortVal : { S : '0000000000000034'}, //base 32 of 100
          data : { M : { str : { S : 'string field'}, num : { N : '100'} }}
        }}}
      ];

      var previewData = { str: 'string field', num: 100};

      var actual = ddl._generateIndexItems('rowId', 'classId', json, previewData , { sortAttr: 'num'});
      assert.deepEqual(actual, expected);

    });
  });

  describe('#put()', function(){
    it ('should reject when json contains attr with leading underscore', function(done){
      ddl.put('rowId', 'classId', {_id : 'test'})
      .done(function(){
        throw 'this should not be successful';
      }, function(e){
        assert.ok(e.indexOf('underscore') >=0);
        done();
      });
    });

    it ('should store data in db', function(done) {
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
      var dbData = _.extend({_id: 'rowId', _class: 'classId', _rev : 'rev100', _previewAttrs: ['str','num']}, json);
      var previewData = { str: 'string field', num: 100};

      var expected = {
        RequestItems: {
          'dd-generic' : [
            { PutRequest: {
              Item : ddl._Json2DynDB(dbData)
            }}
          ],
          'dd-idx-generic' : ddl._generateIndexItems('rowId', 'classId', json, previewData , { sortAttr: 'num'})
        }
      }; //end expected

      var ss = {
        id : sinon.stub(ddl, 'generateId', function(){ return 'rev100'}),
        batchWrite: sinon.stub(ddl, '_awsBatchWrite')
      };

      ss.batchWrite.onCall(0).returns(Q(true));

      ddl.put('rowId', 'classId', json, ['str','num'], { indexOptions: {sortAttr: 'num'} })
      .done(function(results){
        assert.deepEqual(results, dbData);
        assert.deepEqual(ss.batchWrite.getCall(0).args, [expected]);
        restoreAll(ss);
        done();
      }, function(e) {
        throw e;
      });
    });

  });


  describe('#get()', function(){
    it('should invoke getItem method', function(done){
      var expected = {
        Key: {
          _id: { S : 'row-id'}
        },
        TableName : 'dd-generic',
      };
      var dynDB = {
        str : { S : 'string field' },
        num : { N : '100' },
        strArr : { SS : ['hello', 'test'] },
        map : { M : {
          foo : { S : 'string foo'},
          bar : { N : '100' }
        }},
        //mapArr : { L : [ { fooA : {S : 'barA'}} ]},
        bool: { BOOL : true },
        null: { NULL : true }
      };
      var expectedData = {
        _id : 'row-id',
        _class : 'generic-class-id',
        _rev : 0,
        str : 'string field',
        num : 100,
        strArr : ['hello', 'test'],
        map: {
          foo: 'string foo',
          bar: 100
        },
        bool: true,
        null: null
      };
      var actual = null;
      var stub = sinon.stub(ddl._dynamoDB, 'getItem', function(params, callback){
          actual = params;
          callback(false, {
            Item : {
              _id : { S : 'row-id' },
              _class: { S : 'generic-class-id' },
              _rev: { N : '0'},
              str : { S : 'string field' },
              num : { N : '100' },
              strArr : { SS : ['hello', 'test'] },
              map : { M : {
                foo : { S : 'string foo'},
                bar : { N : '100' }
              }},
              //mapArr : { L : [ { fooA : {S : 'barA'}} ]},
              bool: { BOOL : true },
              null: { NULL : true }
            }
          });
      });

      ddl.get('row-id', 'generic-class-id')
      .done(function(actualData){
        assert.ok(stub.calledOnce, 'getItem should be called');
        assert.deepEqual(actual, expected);
        assert.deepEqual(actualData, expectedData);
        stub.restore();
        done();
      }, function err(err){
        throw err;
        console.log(err);
        assert.ok(false);
      });

    });
  });

  describe('#update()', function(){
    it('should invoke updateItem', function(done){
      var expectedParams = {
        Key : {
          _id : { S : 'row-id'}
        },
        TableName: 'dd-generic',
        ExpressionAttributeNames: {
          '#rev' : '_rev',
          '#class' : '_class',
          '#str' : 'str',
          '#num' : 'num'
        },
        ExpressionAttributeValues: {
          ':str' : { S: 'new string'},
          ':num' : { N : '200'},
          ':_rev' : { S : 'A0000'},
          ':_class' : {S : 'generic-class-id'},
          ':_oldRev' : { S : 'rev0'}
        },
        UpdateExpression: 'SET #rev = :_rev,#str = :str,#num = :num',
        ConditionExpression: '#class = :_class AND #rev = :_oldRev',
        ReturnValues: 'ALL_NEW'
      };

      var expectedBatchParams = {
        RequestItems: {
          'dd-idx-generic' : [
            {
             PutRequest : {
              Item : {
                _id : { S : 'row-id.generic-class-id.str'},
                searchKey: {S : 'generic-class-id.str'},
                Svalue: { S : 'new string' },
                data: { M : {
                  str : {S : 'new string'},
                  num : { N : '200' }
                }}
              }
             }
           },
           {
            PutRequest : {
             Item : {
               _id : { S : 'row-id.generic-class-id.num'},
               searchKey: {S : 'generic-class-id.num'},
               Nvalue: { N : '200' },
               data: { M : {
                 str : {S : 'new string'},
                 num : { N : '200' }
               }}
             }
            }
           }
          ]
        }
      };


      var actualParams = null;
      var actualBatchParams = null;
      var stub = sinon.stub(ddl._dynamoDB, 'updateItem', function(params, callback){
        actualParams = params;
        callback(false, {
          Attributes: {
            _indexHint: {SS : ['str','num']}
          }
        });
      });
      var batchStub = sinon.stub(ddl._dynamoDB, 'batchWriteItem', function(params, callback){
        actualBatchParams = params;
        callback(false, {});
      });
      var genIdStub = sinon.stub(ddl, 'generateId', function(){
        return 'A0000';
      });

      var json = {
        _id : 'row-id',
        _class : 'generic-class-id',
        _rev : 'rev0',
        str : 'new string',
        num: 200
      };

      ddl.update('row-id', 'generic-class-id', json, true)
      .done(function(actualData){
        assert.ok(stub.calledOnce, 'updateItem should be called');
        assert.ok(batchStub.calledOnce, 'batchWriteItem should be called');
        assert.deepEqual(actualParams, expectedParams);
        assert.deepEqual(actualBatchParams, expectedBatchParams);
        //assert.deepEqual(actualData, expectedData);
        stub.restore();
        batchStub.restore();
        genIdStub.restore();
        done();
      }, function err(err){
        throw err;
        console.log(err);
        assert.ok(false);
      });

    });
  });


  describe('#delete()', function(){
    it('should invoke deleteItem', function(done){
      var expectedParams = {
        RequestItems : {
          'dd-generic' : [{
            DeleteRequest : {
              Key : {
                _id : {S : 'row-id'}
              }
            }
          }],
          'dd-idx-generic' : [
            { DeleteRequest : {
              Key : {
                _id : { S : 'row-id.generic-class-id.str'}
              }
            }},
            { DeleteRequest : {
              Key : {
                _id : { S : 'row-id.generic-class-id.num'}
              }
            }}
          ]
        }
      };

      var getItemStub = sinon.stub(ddl._dynamoDB, 'getItem', function(params, callback){
        callback(false, {
          Item:
            { _id : {S : 'row-id'},
              _class : { S : 'generic-class-id'},
              str : { S : 'str'},
              num : { N : '100'}
            }
        });
      });

      var actualParams;
      var batchWriteStub = sinon.stub(ddl._dynamoDB, 'batchWriteItem', function(params, callback){
        actualParams = params;
        callback(false, {});
      });

      ddl.delete('row-id', 'generic-class-id')
      .done(function(actualData){
        assert.ok(getItemStub.calledOnce, 'getItem should be called');
        assert.ok(batchWriteStub.calledOnce, 'batchWriteItem should be called');

        assert.deepEqual(actualParams, expectedParams);
        getItemStub.restore();
        batchWriteStub.restore();
        done();
      }, function err(err){
        throw err;
        console.log(err);
        assert.ok(false);
      });

    });
  });


});
