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
            ["put",{"PutRequest":{"Item":{"str":{"S":"bar"},"num":{"N":"100"},"bool":{"BOOL":true},"null":{"NULL":true}}}}]);
          stub.restore();
          done();
      });
    });
  });
});
