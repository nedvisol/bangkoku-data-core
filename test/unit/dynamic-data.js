process.env.AWS_REGION = 'unittest';
process.env.AWS_S3_BUCKET = 'unittest';

var assert = require('assert');
var sinon = require('sinon');
var awsAdapter = require('../../lib/adapter/aws.js');
var DD = require('../../lib/dynamic-data.js');
var Q = require('q'),
  util = require('util'),
  jsep = require('jsep'),
  _ = require('underscore');

var mockAdapter = {
  DB: sinon.stub()
};
var mockCollection = {
  read:  sinon.stub(),
  create: sinon.stub(),
  update: sinon.stub(),
  delete: sinon.stub(),
  withCondition: sinon.stub()
}

var dateNowStub = sinon.stub(Date, 'now');

function resetStubs() {
  mockAdapter.DB.reset();
  mockCollection.read.reset();
  mockCollection.create.reset();
  mockCollection.update.reset();
  mockCollection.delete.reset();
  mockCollection.withCondition.reset();
  dateNowStub.reset();
}



describe('Dynamic Data', () => {
  describe('#getContext()', () => {
    it('should return a new context object with paritition Id, and the right adapter', () => {
      DD.useAdapter('foo-adapter');
      var ctx = DD.getContext('partition-id');

      assert.equal(ctx._partitionId, 'partition-id');
      assert.equal(ctx._adapter, 'foo-adapter');
    });
  });

  describe('Context', () => {


    describe('#getItemId()', ()=>{
      it('should return combined id and partition id', ()=>{
        var ctx = DD.getContext('partitionid');
        var itemId = ctx.getItemId('itemid');
        assert.equal(itemId, 'itemid@partitionid-i');
      });
    });

    describe('#generateId', ()=>{
      it('should returns base-36 random ID with 25 digit lenghth',()=>{
        var ctx = DD.getContext('partitionid');
        var id1 = ctx.generateUuid();
        var id2 = ctx.generateUuid();
        assert.ok(id1 != id2, 'two IDs should not be the same');
        assert.equal(id1.length, 25, '25 digits fixed');

      });
    });

    describe('#get()', ()=>{
      it('should return a promise with execute read op, data only include id and rev',(done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.onCall(0).returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q({str: 'foo', num: 100, _id : 'itemid@paritionid-i'
          , _rev : 'rev', _txn: 'txn', _class: 'classid'}));


        var ctx = DD.getContext('partitionid');
        ctx.get('itemid').promise()
        .done((results) => {
          //console.log(JSON.stringify(results));
          assert.deepEqual(mockAdapter.DB.getCall(0).args, [ 'dd-collection' ]);
          assert.deepEqual(mockCollection.read.getCall(0).args, [{"_id":"itemid@partitionid-i", "_range" : "0"}]);
          assert.deepEqual(results, {"str":"foo","num":100, _id : 'itemid', _rev : 'rev', _class : 'classid'});

          mockAdapter.DB.reset();
          mockCollection.read.reset();
          done();
        });
      });
    }); // end describe('#get()', ()=>{

    describe('#create', ()=>{
      it('should return rejected promise if json contains _attr', (done)=>{

        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.onCall(0).returns(mockCollection);
        mockAdapter.DB.onCall(1).returns(mockCollection);
        mockCollection.create.onCall(0).returns(Q('itemid'));

        var ctx = DD.getContext('partitionid');
        ctx.create({_id: 'foo', _class: 200})
        .as('classid')
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'invalid.parameters');
          done();
        });
      });

      it('should return failed promise if class is not provided', (done)=>{

        resetStubs();
        DD.useAdapter(mockAdapter);
        mockAdapter.DB.onCall(0).returns(mockCollection);
        mockAdapter.DB.onCall(1).returns(mockCollection);
        mockCollection.create.onCall(0).returns(Q('itemid'));
        mockCollection.create.onCall(1).returns(Q('indexitemid'));

        var ctx = DD.getContext('partitionid');
        ctx.create({str: 'foo', num: 200})
        .promise()
        .done( (results)=>{
          console.log(`here? ${results}`);
          assert.fail('should not be here');

          done();
        },(err)=>{
          assert.equal(err, 'missing.parameters');
          done();
        });

      });
      it('should insert item and class index and returns Q(id)', (done)=>{

        resetStubs();
        DD.useAdapter(mockAdapter);
        mockAdapter.DB.onCall(0).returns(mockCollection);
        mockAdapter.DB.onCall(1).returns(mockCollection);
        mockCollection.create.onCall(0).returns(Q('itemid'));
        mockCollection.create.onCall(1).returns(Q('indexitemid'));
        dateNowStub.returns(100);

        var ctx = DD.getContext('partitionid');

        var genIdStub = sinon.stub(ctx, 'generateUuid', ()=>{ return 'id'});

        ctx.create({str: 'foo', num: 200})
        .as('classid').promise()
        .done( (results)=>{
          //console.log(JSON.stringify(mockCollection.create.getCall(0).args));
          //console.log(JSON.stringify(mockCollection.create.getCall(1).args));
          assert.deepEqual(mockAdapter.DB.getCall(0).args, [ 'dd-collection' ]);
          assert.deepEqual(mockCollection.create.getCall(0).args,
            [{"str":"foo","num":200,"_id":"id@partitionid-i", "_range" : "0","_class":"classid","_rev":"id","_createdTime":"100"}]
          );
          assert.deepEqual(mockAdapter.DB.getCall(1).args, [ 'index-collection' ]);
          assert.deepEqual(mockCollection.create.getCall(1).args,
            [[{"lookupKey":"classid@partitionid/c","id":"id@partitionid-i","sortVal":"0000000000000034"}]]
          );
          assert.equal(results, 'id');
          done();
        },(err)=>{
          assert.fail(err);
        });

      });
      it('should insert index items if index options are provided', (done)=>{

        resetStubs();
        DD.useAdapter(mockAdapter);
        mockAdapter.DB.onCall(0).returns(mockCollection);
        mockAdapter.DB.onCall(1).returns(mockCollection);
        mockCollection.create.onCall(0).returns(Q('itemid'));
        mockCollection.create.onCall(1).returns(Q('indexitemid1'));
        mockCollection.create.onCall(2).returns(Q('indexitemid2'));
        mockCollection.create.onCall(3).returns(Q('indexitemid3'));
        dateNowStub.returns(100);

        var ctx = DD.getContext('partitionid');

        var genIdStub = sinon.stub(ctx, 'generateUuid', ()=>{ return 'id'});

        ctx.create({str: 'foo', num: 200})
        .as('classid')
        .index([
          {indexName: 'str', attrs: ['str']},
          {indexName: 'strnum', attrs: ['str','num']}
        ])
        .promise()
        .done( (results)=>{
          //console.log(JSON.stringify(mockCollection.create.getCall(1).args));
          assert.deepEqual(mockAdapter.DB.getCall(0).args, [ 'dd-collection' ]);
          assert.deepEqual(mockCollection.create.getCall(0).args,
            [{"str":"foo","num":200,"_id":"id@partitionid-i", "_range" : "0","_class":"classid","_rev":"id","_createdTime":"100"}]
          );
          assert.deepEqual(mockAdapter.DB.getCall(1).args, [ 'index-collection' ]);
          assert.deepEqual(mockCollection.create.getCall(1).args,
            [[{"lookupKey":"classid@partitionid/c","id":"id@partitionid-i","sortVal":"0000000000000034"}
            ,{"lookupKey":"str.classid@partitionid=[\"foo\"]","id":"id@partitionid-i","sortVal":"100"}
            ,{"lookupKey":"strnum.classid@partitionid=[\"foo\",200]","id":"id@partitionid-i","sortVal":"100"}]]
          );
          assert.equal(results, 'id');
          done();
        },(err)=>{
          assert.fail(err);
        });
      });
    }); //end describe #create()

    describe('#update()', ()=>{
      it('should reject if any of _id _rev and _class are not provided', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);

        var ctx = DD.getContext('partitionid');
        ctx.update({_rev: 'rev', _class: 200})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'missing.parameters');
        });

        ctx.update({_id: 'id', _class: 200})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'missing.parameters');
        });

        ctx.update({_id: 'id', _rev: 'rev'})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'missing.parameters');
          done();
        });

      }); //end it('should reject if any of _id _rev and _class are not provided', (done)=>{});

      it('should reject if json includes any attrs with _ other than id rev class', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);

        var ctx = DD.getContext('partitionid');
        ctx.update({_id: 'id', _rev: 'rev', _class:'class', _txn: 'txn id', _foo: 'foo'})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'invalid.parameters');
          done();
        });
      }); //end it('should reject if json includes any attrs with _ other than id rev class', (done)=>{});


      it('should reject if item id does not exist', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q(null));

        var ctx = DD.getContext('partitionid');
        ctx.update({_id: 'id', _rev: 'rev', _class:'class'})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'item.not.exists');
          done();
        });

      }); //end it('should reject if item id does not exist', (done)=>{});

      it('should reject if _rev does not match old rev', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q({_id: 'id1', _rev: 'rev2', _class: 'class', str: 'foo', num: 100}));

        var ctx = DD.getContext('partitionid');
        ctx.update({_id: 'id1', _rev: 'rev1', _class:'class', str: 'foo', num: 100})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'invalid.revision');
          done();
        });
      }); //it('should reject if _rev does not match old rev', (done)=>{});

      it('should reject if _class does not match existing class', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q({_id: 'id1', _rev: 'rev1', _class: 'class', str: 'foo', num: 100}));

        var ctx = DD.getContext('partitionid');
        ctx.update({_id: 'id1', _rev: 'rev1', _class:'class2', str: 'foo', num: 100})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
          //done();
        },(err)=>{
          assert.equal(err, 'invalid.class');
          done();
        });
      }); //it('should reject if _rev does not match old rev', (done)=>{});


      it('should add parition id to itemId and make update call', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q({_id: 'id1', _rev: 'rev1', _class: 'class', str: 'foo', num: 100}));
        mockCollection.update.onCall(0).returns(Q(true));
        mockCollection.withCondition.onCall(0).returns(mockCollection);

        var ctx = DD.getContext('partitionid');
        
        var genIdStub = sinon.stub(ctx, 'generateUuid');
        genIdStub.onCall(0).returns('id1');
        genIdStub.onCall(1).returns('id2');        
        
        ctx.update({_id: 'id1', _rev: 'rev1', _class:'class', str: 'bar', num: 200})
        .promise()
        .done( (results)=>{
          //assert.fail('should not be here');
          //console.log(JSON.stringify(mockCollection.withCondition.getCall(0).args));
          assert.deepEqual(mockCollection.update.getCall(0).args,
            [{"_id":"id1@partitionid-i","_range":"0"},{"str":"bar","num":200, _rev: 'id1'}]
          );
          assert.deepEqual(mockCollection.withCondition.getCall(0).args,
            [{"expr":"#oldrev = :oldrev","attrs":{"#oldrev":"_rev"},"values":{":oldrev":"rev1"}}]
          );
          done();
        },(err)=>{
          console.log(err);
          assert.fail('should not be here');
        });
      }); //end it('should add parition id to itemId and make update call', (done)=>{});

      it('should reject if _rev = oldRev check fails', (done)=>{
        resetStubs();
        DD.useAdapter(mockAdapter);

        mockAdapter.DB.returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q({_id: 'id1', _rev: 'rev1', _class: 'class', str: 'foo', num: 100}));
        mockCollection.update.onCall(0).returns(Q.reject({
          errCode: 'ConditionalCheckFailedException'
        }));
        mockCollection.withCondition.onCall(0).returns(mockCollection);

        var ctx = DD.getContext('partitionid');
        var genIdStub = sinon.stub(ctx, 'generateUuid');
        genIdStub.onCall(0).returns('id1');
        genIdStub.onCall(1).returns('id2');        
        
        
        ctx.update({_id: 'id1', _rev: 'rev1', _class:'class', str: 'bar', num: 200})
        .promise()
        .done( (results)=>{
          assert.fail('should not be here');
        },(err)=>{
          //console.log(JSON.stringify(mockCollection.withCondition.getCall(0).args));
          assert.deepEqual(mockCollection.update.getCall(0).args,
            [{"_id":"id1@partitionid-i","_range":"0"},{"str":"bar","num":200, _rev: 'id1'}]
          );
          assert.deepEqual(mockCollection.withCondition.getCall(0).args,
            [{"expr":"#oldrev = :oldrev","attrs":{"#oldrev":"_rev"},"values":{":oldrev":"rev1"}}]
          );
          assert.equal(err, 'optimisti-lock.failed');
          done();
        });

      }); //end it('should reject if _rev = oldRev check fails', (done)=>{});

      it('should update index items if index options are provided', (done)=>{
        done();

      }); //end it('should update index items if index options are provided', (done)=>{});


    }); //end describe('#update()', ()=>{});
  });
});
