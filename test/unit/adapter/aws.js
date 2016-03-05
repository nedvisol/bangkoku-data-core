process.env.AWS_REGION = 'unittest';
process.env.AWS_S3_BUCKET = 'unittest';

var assert = require('assert');
var sinon = require('sinon');
var awsAdapter = require('../../../lib/adapter/aws.js');
var Q = require('Q'),
  util = require('util'),
  jsep = require('jsep'),
  _ = require('underscore');


function restoreAll(stubs) {
  _.each(stubs, function(stub){
    stub.restore();
  });
}

describe('AWS Adapter', function(){
  describe('#create', function(){
    it('should invoke DynamoDB PutRequest', function(done){

      var stub = sinon.stub(awsAdapter._ext, 'invokeDb');
      stub.onCall(0).returns(Q('id'));

      awsAdapter.DB('foo').create({str: 'bar', num: 100, bool: true, null: null })
      .done(function(id) {
          //console.log(JSON.stringify(stub.getCall(0).args));
          assert.deepEqual(stub.getCall(0).args,
            ["put",{"Item":{"str":{"S":"bar"},"num":{"N":"100"},"bool":{"BOOL":true},"null":{"NULL":true}},"TableName":"foo"}]
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
            ["put",{"Item":{"str":{"S":"bar"},"num":{"N":"100"},"bool":{"BOOL":true},"null":{"NULL":true}},"TableName":"foo","ConditionExpression":"attribute_not_exists(str)"}]
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
            ["get",{"Key":{"_id":{"S":"item-id"}},"TableName":"foo"}]
          );
          assert.deepEqual(data, {"str":"bar","num":100,"bool":true,"null":null});
          stub.restore();
          done();
      });
    });
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


});
