var assert = require('assert');
var sinon = require('sinon');
var D = require('../../index.js').SimpleStore('aws');
var Q = require('Q'),
  util = require('util');

var ddl = new D('generic');


describe('DataAccess V2', function(){
  var num = 0xffffffffffffffff;
  console.log('nnn '+num.toString(36));
  var genIdStub = sinon.stub(ddl, 'generateId', function(){
    return 'A0000';
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

  describe('#put()', function(){

    it('should call DynamoDB.putItem', function(done){
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
        RequestItems: {
          'dd-generic' : [
            {
              PutRequest: {
              Item : {
                _id : { S : 'row-id'},
                _class : { S : 'class-id' },
                _rev : { S : 'A0000' },
                _indexHint: { SS : ['str', 'num']},
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
              }
            }
          }
          ],
          'dd-idx-generic' : [
            {
             PutRequest : {
              Item : {
                _id : { S : 'row-id.class-id.str'},
                searchKey: {S : 'class-id.str'},
                Svalue: { S : 'string field' },
                data: { M : {
                  str : {S : 'string field'},
                  num : { N : '100' }
                }}
              }
             }
           },
           {
            PutRequest : {
             Item : {
               _id : { S : 'row-id.class-id.num'},
               searchKey: {S : 'class-id.num'},
               Nvalue: { N : '100' },
               data: { M : {
                 str : {S : 'string field'},
                 num : { N : '100' }
               }}
             }
            }
           }
          ]
        }
      };

      //console.log('***' + util.inspect(ddl, true, 1));
      var expectedData = {
        _id: 'row-id',
        _class: 'generic-class-id',
        _rev : 'rev00',
        str : 'string field',
        num : 100,
        strArr : ['hello', 'test'],
        map: {
          foo: 'string foo',
          bar: 100
        },
        mapArr: [ { fooA : 'barA'}],
        bool: true,
        null: null,
      };

      var actual = null;
      var batchStub = sinon.stub(ddl._dynamoDB, 'batchWriteItem', function(params, callback){
          //console.log('********');
          //console.log(util.inspect(params, true, null));
          //console.log('********');
          //console.log(util.inspect(expected, true, null));
          actual = params;
          callback(false, {});
      });
      var getStub = sinon.stub(ddl, 'get', function(id, classId){
        return Q(expectedData);
      });

      ddl.put('row-id', 'class-id', json, ['str','num'])
      .done(function(data){
        assert.ok(batchStub.calledOnce, 'batchWriteItem should be called');
        batchStub.restore();
        getStub.restore();
        assert.deepEqual(actual, expected);
        assert.equal(data, expectedData);
        done();
      }, function err(err){
        console.log(err);
        assert.ok(false);
      });


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
        done();
      }, function err(err){
        throw err;
        console.log(err);
        assert.ok(false);
      });

    });
  });

  describe('#query()', function(){
    it('should query index table', function(done){
      var expectedParams = {
        TableName: 'dd-idx-generic',
        IndexName: 'searchKey-Svalue-index',
        Select: 'ALL_PROJECTED_ATTRIBUTES',
        KeyConditionExpression: 'searchKey = :sk AND Svalue = :rangeValue',
        ExpressionAttributeValues: {
          ':sk': {
            S: 'generic-class-id.str'
          },
          ':rangeValue': {
            S: 'string value'
          }
        }
      };
      var actualParams = null;
      var stub = sinon.stub(ddl._dynamoDB, 'query', function(params, callback){
        actualParams = params;
        callback(false, {
          Items: [
            { _id : {S : 'row-id.generic-class-id.str'},
              data : { M : { foo : {S : 'bar' }}}
            }
          ]
        });
      });
      var expectedData = [
        { _id : 'row-id', data : { foo : 'bar'}, }
      ];

      ddl.query('generic-class-id', [{attr: 'str', op: '=', value: 'string value'}])
      .done(function(actualData){
        assert.ok(stub.calledOnce, 'updateItem should be called');
        assert.deepEqual(actualParams, expectedParams);
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
