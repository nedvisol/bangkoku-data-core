'use strict';

var _ = require('underscore'),
  AWS = require('aws-sdk'),
  Q = require('q'),
  util = require('util');

/****

db('collection-name')

create(json) => Q(true)
  withCondition(expression) => db
create([json, ...]) => Q([true, ...]);

read({key: value}) => Q(data)

update(kv, data) => Q(data)
withCondition(condition).update(kv, data) => Q(data)
  condition = {
    expr: 'string exp',
    attrs: { ref : 'attr name', ... },
    values: { ref : 'value', ...}
  }

delete(kv) => Q(true)
delete([kv,..]) = Q(true)

withCondition(condition).query(idxName) => Q([data,...])
	condition = {
		hashKey: { key: value},
		rangeKey: { key: value },
		rangeOp: '=' //or >, <
	}

DEBUG:
var db = require('./lib/adapter/aws.js');
db._initTables(1,1, 'dd-collection','index-collection').done((results)=>{console.log(results)}, (err)=>{console.log(`err ${util.inspect(err, true, null)}`)});
var col = db.DB('dd-generic');
col.read({_id: 'rev'}).done((results)=>{console.log(util.inspect(results))}, (err)=>{console.log(util.inspect(err))});


##SHELL INIT
export AWS_REGION="us-west-2"
export "AWS_S3_BUCKET"="bucket"

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
        reject({
          operation: operation,
          errCode: err.code,
          awsError: err,
          awsData: awsData,
          awsParams: params
        });

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
    //console.log(util.inspect(dynamoDB));
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

  normalizeKeyName(key) {
    return key.replace(/[^A-Za-z0-9]/g, '');
  }


  withCondition(exp) {
    this._params.checkExpression = exp;
    return this;
  }

  create(data) {
    if (_.isArray(data)) {
      return this.bulkCreate(data);
    }
    var ddbData = Json2DynDB(data);
    var params = {
      Item: ddbData,
      TableName: this._collection
    };
    if (this._params['checkExpression'] != null) {
      params.ConditionExpression = this._params['checkExpression'];
    }
    this._params = {};
    return ext.invokeDb('putItem', params)
    .then((results)=>{
      return true;
    });
  }

  bulkCreate(jsonArray) {
    var params = {
      RequestItems: {

      }
    };

    var putRequests = [];
    for(var json of jsonArray) {
      putRequests.push({PutRequest: { Item : Json2DynDB(json)}});
    }
    params.RequestItems[this._collection] = putRequests;

    this._params = {};
    return ext.invokeDb('batchWriteItem', params);
  }


  read(kv) {
    var params = {
      Key: Json2DynDB(kv),
      TableName: this._collection
    };

    this._params = {};
    return ext.invokeDb('getItem', params)
    .then(function(results){
      if (_.isUndefined(results.Item)) {
        return null;
      }
      var json = DynDB2Json(results.Item);
      return json;
    });
  }

  delete(kv) {
    if (_.isArray(kv)) {
      return this.bulkDelete(kv);
    }
    var params = {
      Key: Json2DynDB(kv),
      TableName: this._collection
    };

    this._params = {};
    return ext.invokeDb('delete', params)
    .then(function(results){
      return true;
    });
  }

  bulkDelete(kvArray) {
    var params = {
      RequestItems: {

      }
    };

    var delRequests = [];
    for(var kv of kvArray) {
      delRequests.push({DeleteRequest: { Key : Json2DynDB(kv)}});
    }
    params.RequestItems[this._collection] = delRequests;

    this._params = {};
    return ext.invokeDb('batchWriteItem', params)
    .then(()=>{ return true; });
  }

  update(kv, json) {
    var updateExp = 'SET ';
    var attrNames = {};
    var attrValues = {};
    var conditionExp = null;
    _.each(json, (value, key)=>{
      var nKey = this.normalizeKeyName(key);
      updateExp += `#${nKey} = :UPD${nKey}, `;
      attrNames[`#${nKey}`] = key;
      attrValues[`:UPD${nKey}`] = json[key];
    });
    updateExp = updateExp.substring(0, updateExp.length -2);

    if (this._params.checkExpression != null) {
      var checkExp = this._params.checkExpression;
      var attrs = checkExp.attrs;
      var values = checkExp.values;
      conditionExp = checkExp.expr;

      attrNames = _.extend(attrNames, attrs);
      attrValues = _.extend(attrValues, values);
    }

    var params = {
      Key: Json2DynDB(kv),
      TableName: this._collection,
      UpdateExpression: updateExp,
      ExpressionAttributeNames: attrNames,
      ExpressionAttributeValues: Json2DynDB(attrValues)
    };
    if (conditionExp != null) {
      params.ConditionExpression = conditionExp;
    }


    this._params = {};

    return ext.invokeDb('updateItem', params)
    .then((results)=>{
      return true;
    });
  }

}


//****** AWS initialize data tables ****

function AWSCollectionsInit(readCap, writeCap, dataCollection, indexCollection){
  if (_.isUndefined(dataCollection)) {
    dataCollection = 'dd-collection';
    indexCollection = 'index-collection';
  }
  var dataTable = {
    AttributeDefinitions: [
      { AttributeName: '_id', AttributeType: 'S'},
      { AttributeName: '_range', AttributeType: 'S'},
    ],
    KeySchema: [
      { AttributeName: '_id', KeyType: 'HASH' },
      { AttributeName: '_range', KeyType: 'RANGE' }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: readCap,
      WriteCapacityUnits: writeCap
    },
    TableName: dataCollection
  };

  var indexTable = {
    AttributeDefinitions: [
      { AttributeName: 'lookupKey', AttributeType: 'S'}, //** primary hash key
      { AttributeName: 'id', AttributeType: 'S'}, //** primary range key
      { AttributeName: 'sortVal', AttributeType: 'S'}, //** local secondary index range key
    ],
    KeySchema: [
      { AttributeName: 'lookupKey', KeyType: 'HASH' },
      { AttributeName: 'id', KeyType: 'RANGE' },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: readCap,
      WriteCapacityUnits: writeCap
    },
    TableName: indexCollection,
    LocalSecondaryIndexes: [
      {
          IndexName: 'sortVal-idx',
          KeySchema: [
            { AttributeName: 'lookupKey', KeyType: 'HASH' },
            { AttributeName: 'sortVal', KeyType: 'RANGE'}
          ],
          Projection: { ProjectionType: 'INCLUDE'
          , NonKeyAttributes: ['data']},
      }
    ]
  };

  return Q.Promise(function(resolve, reject, notify) {
    console.log('Creating data table');
    dynamoDB.createTable(dataTable, function(err, awsData) {
      if (err) {
        reject(["createTable-dataTable", err, awsData, dataTable]);
      } else {
        console.log('data table created');
        resolve(awsData);
      }
    });
  })
  .delay(2000)
  .then(function(){
    return Q.Promise(function(resolve, reject, notify) {
      console.log('Creating index table');
      dynamoDB.createTable(indexTable, function(err, awsData) {
        if (err) {
          reject(["createTable-indexTable", err, awsData, indexTable]);
        } else {
          console.log('index table created');
          resolve(awsData);
        }
      });
    })
  });
}

//**************************************



module.exports = {
  _db : dynamoDB,
  _ext : ext,
  _initTables : AWSCollectionsInit,
  DB : function(collection) {
    return new Collection(collection);
  }
};
