process.env.AWS_REGION = 'unittest';
process.env.AWS_S3_BUCKET = 'unittest';

var assert = require('assert');
var sinon = require('sinon');
var awsAdapter = require('../../lib/adapter/aws.js');
var DD = require('../../lib/dynamic-data.js');
var Q = require('Q'),
  util = require('util'),
  jsep = require('jsep'),
  _ = require('underscore');

var mockAdapter = {
  collection: sinon.stub()
};
var mockCollection = {
  read:  sinon.stub(),
  create: sinon.stub()
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
      it('should return a promise with execute read op',(done)=>{
        DD.useAdapter(mockAdapter);

        mockAdapter.collection.onCall(0).returns(mockCollection);
        mockCollection.read.onCall(0).returns(Q({str: 'foo', num: 100}));


        var ctx = DD.getContext('partitionid');
        ctx.get('itemid').promise()
        .done((results) => {
          //console.log(JSON.stringify(results));
          assert.deepEqual(mockAdapter.collection.getCall(0).args, [ 'dd-colection' ]);
          assert.deepEqual(mockCollection.read.getCall(0).args, [{"_id":"itemid@partitionid-i"}]);
          assert.deepEqual(results, {"str":"foo","num":100});

          mockAdapter.collection.reset();
          mockCollection.read.reset();
          done();
        });
      });

      describe('#create', ()=>{
        it('should return failed promise if class is not provided', (done)=>{
          DD.useAdapter(mockAdapter);
          mockAdapter.collection.onCall(0).returns(mockCollection);
          mockAdapter.collection.onCall(1).returns(mockCollection);
          mockCollection.create.onCall(0).returns(Q('itemid'));
          mockCollection.create.onCall(1).returns(Q('indexitemid'));

          var ctx = DD.getContext('partitionid');
          ctx.create({str: 'foo', num: 200})
          .promise()
          .done( (results)=>{
            console.log(`here? ${results}`);
            assert.fail('should not be here');

            mockAdapter.collection.reset();
            mockCollection.read.reset();
            mockCollection.create.reset();
            done();
          },(err)=>{
            assert.equal(err, 'missing.parameters');
            done();
          });

        });
        it('should insert item and class index', (done)=>{

          DD.useAdapter(mockAdapter);
          mockAdapter.collection.onCall(0).returns(mockCollection);
          mockAdapter.collection.onCall(1).returns(mockCollection);
          mockCollection.create.onCall(0).returns(Q('itemid'));
          mockCollection.create.onCall(1).returns(Q('indexitemid'));

          var ctx = DD.getContext('partitionid');
          ctx.create({str: 'foo', num: 200})
          .as('classid').promise()
          .done( (results)=>{
            console.log(JSON.stringify(mockCollection.create.getCall(0).args));
            //console.log(JSON.stringify(mockCollection.create.getCall(1).args));
            assert.deepEqual(mockAdapter.collection.getCall(0).args, [ 'dd-colection' ]);
            assert.deepEqual(mockCollection.create.getCall(0).args, [{"_id":"itemid@partitionid-i"}]);
            assert.deepEqual(mockAdapter.collection.getCall(1).args, [ 'index-colection' ]);
            assert.deepEqual(mockCollection.create.getCall(1).args, [{"_id":"itemid@partitionid-i"}]);
            assert.eqaul(results, true);

            mockAdapter.collection.reset();
            mockCollection.read.reset();
            mockCollection.create.reset();
            done();
          },(err)=>{
            assert.fail(err);
          });

        });
        it('should insert index items if index options are provided', (done)=>{

          done();
        });
      });
    });
  });
});
