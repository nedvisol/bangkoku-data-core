process.env.AWS_REGION = 'unittest';
process.env.AWS_S3_BUCKET = 'unittest';

var assert = require('assert');
var sinon = require('sinon');
var awsAdapter = require('../../../lib/adapter/aws.js');
var Q = require('q'),
  util = require('util'),
  jsep = require('jsep'),
  _ = require('underscore');


function restoreAll(stubs) {
  _.each(stubs, function(stub){
    stub.restore();
  });
}

describe('AWS Adapter', function(){
  describe('#normalizeKeyName', ()=>{
    it('should remove non-alphanumeric chars', ()=>{
      var collection = awsAdapter.DB('foo');
      assert.equal('foobar', collection.normalizeKeyName('foo bar'));
      assert.equal('foobar', collection.normalizeKeyName('foo bar ! @#$%^&*()[]/'));
    });
  });

  describe('#create', function(){
    it('should invoke DynamoDB PutRequest', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q('id'));

      awsAdapter.DB('foo').create({str: 'bar', num: 100, bool: true, null: null })
      .done(function(id) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          assert.deepEqual(stub.getCall(0).args,
            ["putItem",{"Item":{"str":{"S":"bar"},"num":{"N":"100"},"bool":{"BOOL":true},"null":{"NULL":true}},"TableName":"foo"}]
          );
          stub.restore();
          done();
      });
    });

    it('should invoke DynamoDB PutRequest with check expression', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q('id'));

      awsAdapter.DB('foo').withCondition('attribute_not_exists(str)').create({str: 'bar', num: 100, bool: true, null: null })
      .done(function(id) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          assert.deepEqual(stub.getCall(0).args,
            ["putItem",{"Item":{"str":{"S":"bar"},"num":{"N":"100"},"bool":{"BOOL":true},"null":{"NULL":true}},"TableName":"foo","ConditionExpression":"attribute_not_exists(str)"}]
          );
          stub.restore();
          done();
      });
    });

    it('should invoke DynamoDB BatchWriteItem for JSON array', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q(['id1','id2']));

      awsAdapter.DB('foo').create([
        {str: 'foo1', num: 100},
        {str: 'foo2', num: 200}
      ])
      .done(function(id) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          assert.deepEqual(stub.getCall(0).args,
            ["batchWriteItem",{"RequestItems":{"foo":[{"PutRequest":{"Item":{"str":{"S":"foo1"},"num":{"N":"100"}}}}
            ,{"PutRequest":{"Item":{"str":{"S":"foo2"},"num":{"N":"200"}}}}]}}]
          );
          stub.restore();
          done();
      });
    });
  });

  describe('#read', function(){
    it('should invoke DynamoDB Get', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      var mockData = {"Item":{"str":{"S":"bar"},"num":{"N":"100"},"bool":{"BOOL":true},"null":{"NULL":true}}};
      stub.onCall(0).returns(Q(mockData));

      awsAdapter.DB('foo').read({_id : 'item-id'})
      .done(function(data) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          //console.log(JSON.stringify(data));
          assert.deepEqual(stub.getCall(0).args,
            ["getItem",{"Key":{"_id":{"S":"item-id"}},"TableName":"foo"}]
          );
          assert.deepEqual(data, {"str":"bar","num":100,"bool":true,"null":null});
          stub.restore();
          done();
      });
    });

    it('should return null if record is not found', (done)=>{
      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q({}));

      awsAdapter.DB('foo').read({_id : 'item-id'})
      .done(function(data) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          //console.log(JSON.stringify(data));
          assert.deepEqual(stub.getCall(0).args,
            ["getItem",{"Key":{"_id":{"S":"item-id"}},"TableName":"foo"}]
          );
          assert.deepEqual(data, null);
          stub.restore();
          done();
      });
    }); //end it('should return null if record is not found', (done)=>{});

  });

  describe('#delete', function(){
    it('should invoke DynamoDB DeleteItem', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q(true));

      awsAdapter.DB('foo').delete({_id : 'item-id'})
      .done(function(data) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          //console.log(JSON.stringify(data));
          assert.deepEqual(stub.getCall(0).args,
            ["delete",{"Key":{"_id":{"S":"item-id"}},"TableName":"foo"}]
          );
          assert.equal(data, true);
          stub.restore();
          done();
      });
    });

    it('should invoke DynamoDB batchWriteItem for bulk delete', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q(true));

      awsAdapter.DB('foo').delete([{_id : 'item-id1'}, {_id : 'item-id2'}])
      .done(function(data) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          //console.log(JSON.stringify(data));
          assert.deepEqual(stub.getCall(0).args,
            ["batchWriteItem",{"RequestItems":{"foo":[{"DeleteRequest":{"Key":{"_id":{"S":"item-id1"}}}}
            ,{"DeleteRequest":{"Key":{"_id":{"S":"item-id2"}}}}]}}]
          );
          assert.equal(data, true);
          stub.restore();
          done();
      });
    });
  });

  describe('#update()', ()=>{
    it('should invoke updateItem', (done)=>{

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q(true));

      awsAdapter.DB('foo').update({hash: 'id', range: 'range'}, {str: 'foo', num: 100, 'key name' : 'val'})
      .done(function(data) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          //console.log(JSON.stringify(data));
          assert.deepEqual(stub.getCall(0).args,
            ["updateItem",{"Key":{"hash":{"S":"id"},"range":{"S":"range"}},"TableName":"foo","UpdateExpression":"SET #str = :UPDstr, #num = :UPDnum, #keyname = :UPDkeyname"
            ,"ExpressionAttributeNames":{"#str":"str","#num":"num","#keyname":"key name"}
            ,"ExpressionAttributeValues":{":UPDstr":{"S":"foo"},":UPDnum":{"N":"100"},":UPDkeyname":{"S":"val"}}}]
          );
          assert.equal(data, true);
          stub.restore();
          done();
      });
    }); //end it('should invoke updateItem', (done)=>{

    it('should include check expression if provided', (done)=>{
      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q(true));

      awsAdapter.DB('foo')
      .withCondition({
        expr: '#oldid = :oldid',
        attrs: { '#oldid' : '_id'},
        values: {':oldid' : 'oldidvalue'}
      })
      .update({hash: 'id', range: 'range'}, {str: 'foo', num: 100, 'key name' : 'val'})
      .done(function(data) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          //console.log(JSON.stringify(data));
          assert.deepEqual(stub.getCall(0).args,
            ["updateItem",{"Key":{"hash":{"S":"id"},"range":{"S":"range"}}
            ,"TableName":"foo","UpdateExpression":"SET #str = :UPDstr, #num = :UPDnum, #keyname = :UPDkeyname","ExpressionAttributeNames":{"#str":"str","#num":"num","#keyname":"key name","#oldid":"_id"}
            ,"ExpressionAttributeValues":{":UPDstr":{"S":"foo"},":UPDnum":{"N":"100"},":UPDkeyname":{"S":"val"},":oldid":{"S":"oldidvalue"}},"ConditionExpression":"#oldid = :oldid"}]
          );
          assert.equal(data, true);
          stub.restore();
          done();
      });
    }); //it('should include check expression if provided', (done)=>{});


  });



});
