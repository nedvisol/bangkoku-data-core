var _ = require('underscore'),
  AWS = require('aws-sdk'),
  Q = require('q'),
  util = require('util'),
  uuid = require('node-uuid');

if (_.isUndefined(process.env.AWS_REGION)) {
  throw "AWS_REGION ENV is not defined";
}

AWS.config.update({
  region: process.env.AWS_REGION
});

var  _dynamoDB = new AWS.DynamoDB();
var  _s3 = new AWS.S3();

module.exports = function DynamicData(appName) {
  this._appName = appName;
  this._baseTable = 'dd-'+appName;
  this._indexTable = 'dd-idx-'+appName;
  this._dynamoDB = _dynamoDB; //backdoor for unit testing
  this._s3 = _s3; //backdoor for unit testing
  this._s3Bucket = appName+'-data';
  this.initializeDataStore = function(readCap, writeCap){
    var dataTable = {
      AttributeDefinitions: [
        { AttributeName: '_id', AttributeType: 'S'},
        { AttributeName: '_class', AttributeType: 'S'}
      ],
      KeySchema: [
        { AttributeName: '_id', KeyType: 'HASH' }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: readCap,
        WriteCapacityUnits: writeCap
      },
      TableName: this._baseTable,
      GlobalSecondaryIndexes: [
        {
            IndexName: 'class-index',
            KeySchema: [
              {AttributeName: '_class', KeyType: 'HASH'}
            ],
            Projection: { ProjectionType: 'KEYS_ONLY'},
            ProvisionedThroughput: {
              ReadCapacityUnits: readCap,
              WriteCapacityUnits: writeCap
            },
        }
      ]
    };

    var indexTable = {
      AttributeDefinitions: [
        { AttributeName: '_id', AttributeType: 'S'},
        { AttributeName: 'searchKey', AttributeType: 'S'},
        { AttributeName: 'Svalue', AttributeType: 'S'},
        { AttributeName: 'Nvalue', AttributeType: 'N'}
      ],
      KeySchema: [
        { AttributeName: '_id', KeyType: 'HASH' }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: readCap,
        WriteCapacityUnits: writeCap
      },
      TableName: this._indexTable,
      GlobalSecondaryIndexes: [
        {
            IndexName: 'searchKey-Svalue-index',
            KeySchema: [
              {AttributeName: 'searchKey', KeyType: 'HASH'},
              {AttributeName: 'Svalue', KeyType: 'RANGE'}
            ],
            Projection: { ProjectionType: 'INCLUDE'
            , NonKeyAttributes: ['data']},
            ProvisionedThroughput: {
              ReadCapacityUnits: readCap,
              WriteCapacityUnits: writeCap
            },
        },
        {
            IndexName: 'searchKey-Nvalue-index',
            KeySchema: [
              {AttributeName: 'searchKey', KeyType: 'HASH'},
              {AttributeName: 'Nvalue', KeyType: 'RANGE'}
            ],
            Projection: { ProjectionType: 'INCLUDE'
            , NonKeyAttributes: ['data']},
            ProvisionedThroughput: {
              ReadCapacityUnits: readCap,
              WriteCapacityUnits: writeCap
            },
        }
      ]
    };

    var that = this;
    return Q.Promise(function(resolve, reject, notify) {
      that._dynamoDB.createTable(dataTable, function(err, awsData) {
        if (err) {
          reject(["createTable-dataTable", err, awsData, dataTable]);
        } else {
          resolve(awsData);
        }
      });
    })
    .then(function(){
      return Q.Promise(function(resolve, reject, notify) {
        that._dynamoDB.createTable(indexTable, function(err, awsData) {
          if (err) {
            reject(["createTable-indexTable", err, awsData, indexTable]);
          } else {
            resolve(awsData);
          }
        });
      })
    });
  }; // END this.initializeDataStore

  this.generateId = function() {
    var buffer = new Buffer(16);
    uuid.v4(null, buffer);
    return buffer.readUIntLE(0, 4).toString(36) +
      buffer.readUIntLE(4, 4).toString(36) +
      buffer.readUIntLE(8, 4).toString(36) +
      buffer.readUIntLE(12, 4).toString(36);
    //return buffer.toString('hex');
  } //END this._generateId

  this._dynamoDBInvoke = function(operation, params) {
    var that = this;
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
              that._dynamoDB[operation](awsData.UnprocessedItems, callback);
            }, 500);
          } else {
            resolve(awsData);
          }
        }
      };
      //console.log(util.inspect(params, true, null));
      that._dynamoDB[operation](params, callback);
    });
  }, // END _dynamoDBInvoke
  this._Json2DynDB = function(json){
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
  }; // END this._Json2DynDB

  this._DynDB2Json = function(dynDB) {
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
  };

  this._generateIndexItems = function(id, classId, json, idxData) {
    var indexPuts = [];
    var that = this;
    _.each(json, function(value, key){
      var idxid = id+'.'+classId+'.'+key;
      var sk = classId+'.'+key;
      if (_.isString(value)) {
        indexPuts.push({
          PutRequest: {
            Item: that._Json2DynDB({ _id: idxid, searchKey: sk, Svalue: value, data : idxData })
          }
        });
      } else if (_.isNumber(value)) {
        indexPuts.push({
          PutRequest: {
            Item: that._Json2DynDB({ _id: idxid, searchKey: sk, Nvalue: value, data : idxData })
          }
        });
      }
    });
    return indexPuts;
  }
  /***
  indexHint: ['attr','attr',...],
  indexOptions: [
    ['attr1','attr2', ...],
  ]
  **/
  this.put = function(id, classId, json, indexHint, indexOptions) {
    var that = this;
    var dataJson = _.extend({_id: id, _class: classId, _rev: that.generateId(), _indexHint : indexHint }, json);
    var dataPut = {
      PutRequest: {
        Item : this._Json2DynDB(dataJson)
      }
    };

    var idxData = {};

    //build index hint data
    _.each(indexHint, function(item){
      idxData[item] = json[item];
    });
    var indexPuts = this._generateIndexItems(id, classId, json, idxData);

    var batchWriteData = {
      RequestItems : {

      }
    };
    batchWriteData.RequestItems[this._baseTable] = [dataPut];
    batchWriteData.RequestItems[this._indexTable] = indexPuts;

    return this._dynamoDBInvoke('batchWriteItem', batchWriteData)
    .then(function(){
      return that.get(id, classId);
    });

  }; //END this.put

  this.get = function(id, classId) {
    var params = {
      Key: {
        _id : { S : id }
      },
      TableName: this._baseTable
    };
    var that = this;
    return this._dynamoDBInvoke('getItem', params)
    .then(function(results){
      //console.log('**** getItem');
      //console.log(util.inspect(results, true, null));
      if (_.isUndefined(results.Item)) {
        return null;
      }
      var json = that._DynDB2Json(results.Item);
      //console.log(json._class + ' = '+classId);
      if (json._class !== classId) {
        throw "Mismatched class ID (err.ddl.mismatchedclassid)";
      }
      return json;
    });
  }; //END this.get


  this.update = function(id, classId, json) {
    //_rev must be provided
    if (_.isUndefined(json._rev)) {
      throw '_rev attribute is required for update (err.ddl.missingrevattr)';
    }
    var updateData = _.extend({}, json);
    delete updateData._id;
    delete updateData._class;
    delete updateData._indexHint;
    updateData._rev = this.generateId();

    var attrUpdates = {};
    var attrNames = {};
    var updateExpr = '';
    _.each(updateData, function(value, key){
      attrUpdates[':'+key] = value;

      if (key === '_rev') return;
      attrNames['#'+key] = key;
      if (updateExpr.length > 0) {
        updateExpr = updateExpr + ',';
      }
      updateExpr = updateExpr + '#'+key + ' = :'+key;
    });
    var exprAttrValues = this._Json2DynDB(attrUpdates);
    exprAttrValues[':_class'] = {S : classId};
    exprAttrValues[':_oldRev'] = {S : json._rev };


    var updateParams = {
      Key : {
        _id : { S : id }
      },
      TableName: this._baseTable,
      ExpressionAttributeValues: exprAttrValues,
      ExpressionAttributeNames: _.extend({
        '#rev' : '_rev',
        '#class' : '_class'
      }, attrNames),
      UpdateExpression: 'SET #rev = :_rev,'+updateExpr,
      ConditionExpression: '#class = :_class AND #rev = :_oldRev',
      ReturnValues: 'ALL_NEW'
    };

    var batchWriteParams = {
      RequestItems: {
      }
    };

    var that = this;
    var returnedData = null;
    return this._dynamoDBInvoke('updateItem', updateParams)
    .then(function(results){
      //console.log(util.inspect(results, true, null));

      returnedData = that._DynDB2Json(results.Attributes);
      //check if rev has changed (update successful)


      delete updateData._rev;
      var idxData = {};

      //build index hint data
      _.each(returnedData._indexHint, function(item){
        idxData[item] = updateData[item];
      });

      var idxRequests = that._generateIndexItems(id, classId, updateData, idxData);
      if (idxRequests.length > 0) {
        batchWriteParams.RequestItems[that._indexTable] = idxRequests;
        return that._dynamoDBInvoke('batchWriteItem', batchWriteParams);
      } else {
        return null;
      }
    })
    .then(function(){
      return returnedData;
    });
  }; //END this.update

  this.query = function(classId, queryCondition) {
    //queryCondition = [{ attr: 'name', op: '=', value: 'value'}, ..]
    //all conditions are AND'd.
    var qs = [];
    var that = this;
    _.each(queryCondition, function(qc){
      var indexName = 'searchKey-Svalue-index';
      var rangeValue  = { S : qc.value.toString() };
      var rangeAttrName = 'Svalue';
      if (_.isNumber(qc.value)) {
        indexName = 'searchKey-Nvalue-index';
        rangeValue  = { N: qc.value.toString() };
        rangeAttrName = 'Nvalue';
      }
      var queryParams = {
        TableName: that._indexTable,
        IndexName: indexName,
        Select: 'ALL_PROJECTED_ATTRIBUTES',
        KeyConditionExpression: 'searchKey = :sk AND ' + rangeAttrName + ' = :rangeValue',
        ExpressionAttributeValues: {
          ':sk': {
            S: classId + '.' + qc.attr
          },
          ':rangeValue': rangeValue
        }
      };
      qs.push(that._dynamoDBInvoke('query', queryParams));
    });
    return Q.all(qs)
    .then(function(results){
      //console.log('*** query results: '+util.inspect(results, true, null));

      //console.log('****results');
      //console.log(util.inspect(results, true,null));
      var idToDataMap = {};
      var idSets = [];
      _.each(results, function(result){
        var idSet = [];
        _.each(result.Items, function(item) {
          var idRegex = /([^.]+)\.([^.]+)\.([^.]+)/g; //format [item-id].[class].[attr]
          var json = that._DynDB2Json(item);

          //extract info from _id column
          //console.log('****query json');
          //console.log(util.inspect(json, true,null));
          var match = idRegex.exec(json._id);
          //console.log('****match');
          //console.log(util.inspect(match, true,null));
          json._id = match[1];
          var idxClassId = match[2];
          var idxAttr = match[3];

          if (idxClassId !== classId) {
            return;
          }
          delete json.searchKey;
          delete json.Svalue;
          delete json.Nvalue;

          idToDataMap[json._id] = json;
          idSet.push(json._id);
        });
        idSets.push(idSet);
      });

      //console.log('****idToDataMap');
      //console.log(util.inspect(idToDataMap, true,null));
      //console.log('****idSets');
      //console.log(util.inspect(idSets, true,null));

      //intersect all idSets
      var selectedIds = idSets[0];
      _.each(idSets, function(idSet){
        selectedIds = _.intersection(selectedIds, idSet);
      });
      //console.log('****selectedIds');
      //console.log(util.inspect(selectedIds, true,null));

      var selectedData = [];
      _.each(selectedIds, function(id){
        selectedData.push(idToDataMap[id]);
      });
      return selectedData;
    });
  }; //END this.query

  this.delete = function(id, classId) {
    var that = this;
    return this.get(id, classId)
    .then(function(json){
      if (_.isUndefined(json._id)) {
        throw "Item does not exist (err.ddl.deleteinvalidid)";
      }
      if (json._class !== classId) {
        throw "Invalid class (err.ddl.deleteinvalidclass)";
      }
      var params = {
        RequestItems: { }
      };
      params.RequestItems[that._baseTable] = [{
        DeleteRequest: {
          Key : {
            _id : { S : id }
          }
        }
      }];
      var idxRequests = [];
      _.each(json, function(value, key){
        if(key.indexOf('_') === 0) {
          //internal attrs are not indexed
          return;
        }
        idxRequests.push({
          DeleteRequest : {
            Key : {
              _id : { S : id+'.'+classId+'.'+key }
            }
          }
        });
      });
      params.RequestItems[that._indexTable] = idxRequests;
      return that._dynamoDBInvoke('batchWriteItem', params);
    });
  }; //END this.delete

  this.uploadDocument = function(buffer) {
    var params = {
      Bucket: this._s3Bucket,
      Key: this.generateId(),
      ACL: 'public-read',
      Body: buffer
    };

    var that = this;
    return Q.Promise(function(resolve, reject, notify) {
      that._s3.putObject(params, function(err, awsData) {

        if (err) {
          reject(["S3.PutObject", err, awsData, params]);
        } else {
          //convert to DDS structure
          resolve(params.Key);
        }
      });
    });
  }; //END this.uploadDocument

  this.getDocumentUrl = function(id){
    return 'https://s3.amazonaws.com/'+this._s3Bucket+'/'+id;
  }; //END this.getDocumentUrl

}
