//Add environment variables
process.env.AWS_REGION = 'unittest';
process.env.AWS_S3_BUCKET = 'unittest';

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
          data : { M : { str : { S : 'string field'}, num : { N : '100'} }}
        }}},
        { PutRequest: { Item: {
          lookupKey : { S : 'classId.num=0000000000000034'},
          id : {S : 'rowId'},
          data : { M : { str : { S : 'string field'}, num : { N : '100'} }}
        }}},
        { PutRequest: { Item: {
          lookupKey : { S : 'classId.strSorted=string field'},
          id : {S : 'rowId'},
          sortVal : { S : '0000000000000034'}, //base 32 of 100
          data : { M : { str : { S : 'string field'}, num : { N : '100'} }}
        }}},
      ];

      var previewData = { str: 'string field', num: 100};

      var actual = ddl._generateIndexItems('rowId', 'classId', json, previewData , { strSorted : {
          attr: 'str', sortAttr: 'num'
       } });

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
          'dd-idx-generic' : ddl._generateIndexItems('rowId', 'classId', json, previewData , { strSorted : {
              attr: 'str', sortAttr: 'num'
           } })
        }
      }; //end expected

      var ss = {
        id : sinon.stub(ddl, 'generateId', function(){ return 'rev100'}),
        batchWrite: sinon.stub(ddl, '_awsBatchWrite')
      };

      ss.batchWrite.onCall(0).returns(Q(true));

      ddl.put('rowId', 'classId', json,  { previewAttrs: ['str','num'], indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
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

  //this.update = function(id, classId, json, options)
  describe('#update()', function(){
    //check if all required attributes are present, including _rev; throw exception otherwise
    //update record, get old values back
    //determine attributes with changed values
    //generate delete requests for invalidated index records
    //generate put requests for new index records
    //insert index records
    it('should return rejected promise if required attributes are not present', function(done){
      var json = {
        _id : 'rowId1',
        str: 'new string'
      };
      ddl.update('rowId', 'classId', json, {})
      .done(function(){
        throw 'Should not be here';
      }, function(e){
        assert.ok(e.indexOf('err.ddl.missingrevattr') >= 0);
        done();
      });
    });
    it('should update the item and index records', function(done){
      //update record, get old values back
      //determine attributes with changed values
      //generate delete requests for invalidated index records
      //generate put requests for new index records
      //insert index records
      var ss = {
        id : sinon.stub(ddl, 'generateId', function(){ return 'rev100'}),
        update: sinon.stub(ddl, '_awsUpdate'),
        batchWrite: sinon.stub(ddl, '_awsBatchWrite')
      };
      ss.update.onCall(0).returns(Q({
        Attributes: ddl._Json2DynDB({
          _id : 'rowId',
          _clss : 'classId',
          _rev: 'rev0',
          _previewAttrs: ['str', 'num'],
          str: 'string field',
          num: 100,
          foo: 'not changed'
        })
      }));
      ss.batchWrite.onCall(0).returns(Q({}));

      var json = {
        _id : 'row-id',
        _class : 'generic-class-id',
        _rev : 'rev0',
        str : 'new string',
        num: 200
      };
      var expectedUpdateArgs = {
        Key : {
          _id : { S : 'rowId'}
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
          ':_rev' : { S : 'rev100'},
          ':_class' : {S : 'classId'},
          ':_oldRev' : { S : 'rev0'}
        },
        UpdateExpression: 'SET #rev = :_rev,#str = :str,#num = :num',
        ConditionExpression: '#class = :_class AND #rev = :_oldRev',
        ReturnValues: 'ALL_OLD'
      };

      var expectedBatchWriteArgs = {
        RequestItems: {
          'dd-idx-generic' : [
            { DeleteRequest: { Key: {
              lookupKey : { S : 'classId.str=string field'},
              id : {S : 'rowId'}
            }}},
            { DeleteRequest: { Key: {
              lookupKey : { S : 'classId.num=0000000000000034'},
              id : {S : 'rowId'}
            }}},
            { DeleteRequest: { Key: {
              lookupKey : { S : 'classId.strSorted=string field'},
              id : {S : 'rowId'}
            }}},
            { PutRequest: { Item: {
              lookupKey : { S : 'classId.str=new string'},
              id : {S : 'rowId'},
              data : { M : { str : { S : 'new string'}, num : { N : '200'} }}
            }}},
            { PutRequest: { Item: {
              lookupKey : { S : 'classId.num=0000000000000068'},
              id : {S : 'rowId'},
              data : { M : { str : { S : 'new string'}, num : { N : '200'} }}
            }}},
            { PutRequest: { Item: {
              lookupKey : { S : 'classId.strSorted=new string'},
              id : {S : 'rowId'},
              sortVal : { S : '0000000000000068'}, //base 32 of 100
              data : { M : { str : { S : 'new string'}, num : { N : '200'} }}
            }}},
          ]
        }
      };

      var expectedResults = {
        _id : 'rowId',
        _clss : 'classId',
        _rev: 'rev100',
        _previewAttrs: ['str', 'num'],
        str : 'new string',
        num: 200,
        foo: 'not changed'
      };

      ddl.update('rowId', 'classId', json, {indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
      .done(function(results){
        assert.ok(ss.update.calledOnce, 'update called');
        assert.deepEqual(ss.update.getCall(0).args, [expectedUpdateArgs]);
        assert.ok(ss.batchWrite.calledOnce, 'batchWriteItem called');
        assert.deepEqual(ss.batchWrite.getCall(0).args, [expectedBatchWriteArgs]);
        assert.deepEqual(results, expectedResults, 'returns new item');

        restoreAll(ss);
        done();
      }, function(e){
        throw e;
      });

    });

  });

  describe('#delete()', function(){
    it('should throw an exception if item cannot be found', function(done){
      var ss = {
        get : sinon.stub(ddl, 'get')
      };
      ss.get.onCall(0).returns(Q(null));

      ddl.delete('rowId', 'classId')
      .done(function(){
        throw 'should not be here';
      }, function(e){
        assert.deepEqual(ss.get.getCall(0).args, ['rowId', 'classId']);
        assert.ok(e.indexOf('(err.get.idnotfound)')>=0);
        restoreAll(ss);
        done();
      })
    });

    it('should throw an exception if class does not match', function(done){
      var ss = {
        get : sinon.stub(ddl, 'get')
      };
      ss.get.onCall(0).returns(Q({
        _id : 'rowId',
        _class : 'anotherClassId'
      }));

      ddl.delete('rowId', 'classId')
      .then(function(){
        throw 'should not be here';
      }, function(e){
        assert.deepEqual(ss.get.getCall(0).args, ['rowId', 'classId']);
        assert.ok(e.indexOf('(err.get.invalidclass)')>=0);
        restoreAll(ss);
        done();
      })
    });

    it('should delete item and index records', function(done){
      var ss = {
        get : sinon.stub(ddl, 'get'),
        batchWrite: sinon.stub(ddl, '_awsBatchWrite')
      };
      ss.get.onCall(0).returns(Q({
        _id : 'rowId',
        _class : 'classId',
        str: 'string field',
        num: 100,
        strArr : ['hello', 'test'],
        bool: true
      }));
      ss.batchWrite.onCall(0).returns(Q(true));

      var expectedBatchWriteArgs = {
        RequestItems: {
          'dd-generic' : [
            { DeleteRequest: { Key: {
              _id : {S : 'rowId'}
            }}},
          ],
          'dd-idx-generic' : [
            { DeleteRequest: { Key: {
              lookupKey : { S : 'classId.str=string field'},
              id : {S : 'rowId'}
            }}},
            { DeleteRequest: { Key: {
              lookupKey : { S : 'classId.num=0000000000000034'},
              id : {S : 'rowId'}
            }}},
            { DeleteRequest: { Key: {
              lookupKey : { S : 'classId.strSorted=string field'},
              id : {S : 'rowId'}
            }}}
          ]
        }
      };

      ddl.delete('rowId', 'classId', {indexOptions: { strSorted : {
          attr: 'str', sortAttr: 'num'
       } } })
      .done(function(results){
        assert.deepEqual(ss.get.getCall(0).args, ['rowId', 'classId']);
        assert.deepEqual(ss.batchWrite.getCall(0).args, [expectedBatchWriteArgs]);
        assert.equal(results, 'rowId');
        restoreAll(ss);
        done();
      }, function(e){
        throw e;
      });

    });

  }); //end ddl.delete

  describe('#query()', function(){
    it('should reject if require fields are missing', function(done){
      ddl.query('classId', {}, null)
      .done(function(){
        throw 'should not be here';
      }, function(e){
        assert.ok(e.indexOf('err.query.missingrequiredparams'));
        done();
      });
    });

    it('should execute a query and return with data with sort attr', function(done){
      var ss = {
        query: sinon.stub(ddl, '_awsQuery')
      };
      ss.query.onCall(0).returns(Q({
        Items: [
          ddl._Json2DynDB({id : 'rowId1', data: {foo: 'bar1'}}),
          ddl._Json2DynDB({id : 'rowId2', data: {foo: 'bar2'}}),
        ]
      }));

      var expectedQueryParams = {
        TableName: 'dd-idx-generic',
        IndexName: 'sortVal-idx',
        KeyConditionExpression: 'lookupKey = :lk',
        ExpressionAttributeValues: {
          ':lk' : { S : 'classId.strSorted=string value'}
        },
        ScanIndexForward: true,
        Select: 'ALL_PROJECTED_ATTRIBUTES'
      };

      var expectedResults = [
        {_id : 'rowId1', data: {foo: 'bar1'}},
        {_id : 'rowId2', data: {foo: 'bar2'}}
      ];

      ddl.query('classId', {indexName: 'strSorted', value: 'string value', sort: 'A'}, null)
      .done(function(results){
        assert.deepEqual(ss.query.getCall(0).args, [expectedQueryParams]);
        assert.deepEqual(results, expectedResults);

        restoreAll(ss);
        done();
      }, function(e){
        throw e;
      });

    });

  });





});
