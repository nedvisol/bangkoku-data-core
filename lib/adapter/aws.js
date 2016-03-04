'use strict';

var _ = require('underscore'),
  AWS = require('aws-sdk'),
  Q = require('q'),
  util = require('util');

/****

db('collection-name')

create(data) => Q(id)
read(id) => Q(data)
update(data) => Q(data)
delete(id) => Q(true)
query(attribute, op, value) => Q(data)
****/

if (_.isUndefined(process.env.AWS_REGION)) {
  throw "AWS_REGION ENV is not defined";
}
if (_.isUndefined(process.env.AWS_S3_BUCKET)) {
  throw "AWS_S3_BUCKET ENV is not defined";
}

AWS.config.update({
  region: process.env.AWS_REGION
});

var  dynamoDB = new AWS.DynamoDB();

function dynamoDbInvoke(operation, params) {
  return Q.Promise(function(resolve, reject, notify) {
    var count = 0;
    var callback = function(err, awsData) {
      //console.log('** done ! ');
      //console.log(util.inspect(awsData, true, null));
      if (err) {
        reject([operation, err, awsData, params]);
      } else {
        if (!_.isUndefined(awsData.UnprocessedItems) && !_.isUndefined(awsData.UnprocessedItems.RequestItems)) {
          count++;
          if (count > 10) {
            reject([operation+' (too.many.retries) ', err, awsData, params]);
          }
          //wait for a little, then re-process
          setTimeout(function(){
            dynamoDB[operation](awsData.UnprocessedItems, callback);
          }, 500);
        } else {
          resolve(awsData);
        }
      }
    };
    //console.log(util.inspect(params, true, null));
    dynamoDB[operation](params, callback);
  });
}

function Json2DynDB(json){
  var dynObj = {};
  var that = this;
  _.each(json, function(value, key) {
    if (_.isString(value)) {
      dynObj[key] = { S : value };
    } else if (_.isNumber(value)) {
      var sv = value.toString();
      dynObj[key] = { N : sv};
    } else if (_.isBoolean(value)) {
      dynObj[key] = { BOOL : value };
    } else if (_.isNull(value)) {
      dynObj[key] = { NULL : true };
    } else if (_.isArray(value)) {
      var idx0 = value[0];
      if (value.length === 0) {
        dynObj[key] = { SS : [] }; //empty array assumed to be string
      } else if (_.isString(idx0)) {
        dynObj[key] = { SS : value };
      } else if (_.isObject(idx0)) {
        var mapArr = [];
        _.each(value, function(v){
          mapArr.push({ M : that._Json2DynDB(v)});
        });
        dynObj[key] = { L : mapArr };
      }
    } else if (_.isObject(value)) {
      var o = that._Json2DynDB(value);
      dynObj[key] = { M : o };
    }
  });
  return dynObj;
}

function DynDB2Json(dynDB) {
  var json = {};
  var that = this;
  _.each(dynDB, function(value, key){
    var jVal = null;
    if (!_.isUndefined(value.S)) {
      jVal = value.S;
    } else if (!_.isUndefined(value.N)) {
      jVal = Number(value.N);
    } else if (!_.isUndefined(value.SS)) {
      jVal = value.SS;
    } else if (!_.isUndefined(value.M)) {
      jVal = that._DynDB2Json(value.M);
    } else if (!_.isUndefined(value.BOOL)) {
      jVal = value.BOOL;
    } else if (!_.isUndefined(value.NULL)) {
      jVal = null;
    } else if (!_.isUndefined(value.L)) {
      jVal = [];
      _.each(value.L, function(item){
        if (!_.isUndefined(item.M)) {
          jVal.push(that._DynDB2Json(item.M));
        }
      });
    }
    json[key] = jVal;
  });

  return json;
}

var ext = {
  invokeDb: dynamoDbInvoke
};

class Collection {
  constructor(c) {
    this._collection = c;
    this._params = {};
  }

  checkExpression(exp) {
    this._params.checkExpression = exp;
  }

  create(data) {
    var ddbData = Json2DynDB(data);
    var params = {
      Item: ddbData,
      TableName: this._collection
    };
    if (this._params['checkExpression'] != null) {
      //params.
    }
    return ext.invokeDb('put', params);
  },

  read(id) {
    var params = {
      Key: {
        _id : { S : id }
      },
      TableName: this._baseTable
    };
  }

}


module.exports = {
  _db : dynamoDB,
  _ext : ext,
  DB : function(collection) {
    return new Collection(collection);
  }
};