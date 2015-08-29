var _ = require('underscore'),
  AWS = require('aws-sdk'),
  Q = require('q'),
  util = require('util'),
  uuid = require('node-uuid'),
  bigInt = require('big-integer'),
  mmHash = require('./murmur-hash.js'),
  jsep = require('jsep');

if (_.isUndefined(process.env.AWS_REGION)) {
  throw "AWS_REGION ENV is not defined";
}
if (_.isUndefined(process.env.AWS_S3_BUCKET)) {
  throw "AWS_S3_BUCKET ENV is not defined";
}

AWS.config.update({
  region: process.env.AWS_REGION
});

var  _dynamoDB = new AWS.DynamoDB();
var  _s3 = new AWS.S3();

module.exports = function SimpleStoreAWS(appName) {
  this._appName = appName;
  this._baseTable = 'dd-'+appName;
  this._indexTable = 'dd-idx-'+appName;
  this._dynamoDB = _dynamoDB; //backdoor for unit testing
  this._s3 = _s3; //backdoor for unit testing
  this._s3Bucket = process.env.AWS_S3_BUCKET;

  //Internal functions...

  /**
  Initialize AWS DynamoDB tables. Creates 2 tables: core data (dd-[appName])
  and index table (dd-idx-[appName]).
  @param {Number} readCap - DynamoDB read capacity
  @param {Number} writeCap - DynamoDB write capacity
  @returns {Q.promise} function(success); success = TRUE/FALSE

  **/
  this.initializeDataStore = function(readCap, writeCap){
    var dataTable = {
      AttributeDefinitions: [
        { AttributeName: '_id', AttributeType: 'S'},
      ],
      KeySchema: [
        { AttributeName: '_id', KeyType: 'HASH' }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: readCap,
        WriteCapacityUnits: writeCap
      },
      TableName: this._baseTable
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
      TableName: this._indexTable,
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

    var that = this;
    return Q.Promise(function(resolve, reject, notify) {
      console.log('Creating table '+that._baseTable);
      that._dynamoDB.createTable(dataTable, function(err, awsData) {
        if (err) {
          reject(["createTable-dataTable", err, awsData, dataTable]);
        } else {
          resolve(awsData);
        }
      });
    })
    .delay(2000)
    .then(function(){
      return Q.Promise(function(resolve, reject, notify) {
        console.log('Creating table '+that._indexTable);
        that.oDB.createTable(indexTable, function(err, awsData) {
          if (err) {
            reject(["createTable-indexTable", err, awsData, indexTable]);
          } else {
            resolve(awsData);
          }
        });
      })
    });
  }; // END this.initializeDataStore

  /***
  Generate random unique ID
  @returns {string} String representation of UUID v4, using base-36
  **/
  this.generateId = function() {
    var buffer = new Buffer(16);
    uuid.v4(null, buffer);
    //return buffer.readUIntLE(0, 4).toString(36) +
      //buffer.readUIntLE(4, 4).toString(36) +
      //buffer.readUIntLE(8, 4).toString(36) +
      //buffer.readUIntLE(12, 4).toString(36);
    //return buffer.toString('hex');
    var id = bigInt(buffer.readUIntLE(12, 4)).shiftLeft(96)
      .plus(bigInt(buffer.readUIntLE(8, 4)).shiftLeft(64))
      .plus(bigInt(buffer.readUIntLE(4, 4)).shiftLeft(32))
      .plus(bigInt(buffer.readUIntLE(0, 4))).toString(36);
    return id;
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
  }; // END _dynamoDBInvoke

  this._awsPut = function(params) {
    return this._dynamoDBInvoke('putItem', params);
  };
  this._awsGet = function(params) {
    return this._dynamoDBInvoke('getItem', params);
  };
  this._awsDelete = function(params) {
    return this._dynamoDBInvoke('deleteItem', params);
  };
  this._awsUpdate = function(params) {
    return this._dynamoDBInvoke('updateItem', params);
  };
  this._awsBatchWrite = function(params) {
    return this._dynamoDBInvoke('batchWriteItem', params);
  };



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

  /**
  Generates PutRequest items for indexed attributes/combo.
  @param {String} id - String ID of the item
  @param {String} classId - Class ID of the item
  @param {Object} json - item object
  @param {Object} previewData - preview data to be stored with index record
  @param {Object} idxOptions - index details

  idxOptions = {
    sortAttr: 'attribute name to be used as sort value'
  }

  **/
  function convertToBase32String(num) {
    var str = '0000000000000000'+num.toString(32);
    str = str.substr(str.length-16);

    return str;
  }

  this._generateIndexItems = function(id, classId, json, previewData, idxOptions) {

    var indexPuts = [];
    var that = this;
    _.each(json, function(value, key){
      //var idxid = id+'.'+classId+'.'+key;

      if (!_.isNumber(value) && !_.isString(value)) {
        return;
      }

      if (_.isNumber(value)) {
        //must be integer
        if (value % 1 !== 0) {
          return;
        }
        //covert to base 32 with leading zeros
        value = convertToBase32String(value);
      }

      var lookupKey = classId+'.'+key+'='+value;
      var idxJson = { lookupKey: lookupKey, id: id};

      if (idxOptions && idxOptions.sortAttr) {
        var sortVal = json[idxOptions.sortAttr];
        //ignore sort val if it's not a number
        if (_.isNumber(sortVal)) {
          idxJson['sortVal'] = convertToBase32String(sortVal);
        }
      }

      if (previewData) {
        idxJson['data'] = previewData;
      }

      indexPuts.push({
        PutRequest: {
          Item: that._Json2DynDB(idxJson)
        }
      });
    });

    return indexPuts;
  }
  /***
  Insert an item into data store
  @param id {String} - item ID
  @param classId {String} - class ID
  @param json {Object} - JSON document to be stored
  @param options {Object} - additional options
  @returns {Q} A promise

  Options = {

 }

  **/
  this.put = function(id, classId, json, previewAttrs, options) {
    var that = this;

    //attribute names with leading underscore is not allowed
    var underscore = false;
    _.each(json, function(value, key){
      if (key[0]=='_') {
        console.log('*** '+key);
        underscore = true;
      }
    });
    if (underscore) return Q.reject('Attribute names with underscore is not allowed');

    var dataJson = _.extend({_id: id, _class: classId, _rev: that.generateId(), _previewAttrs : previewAttrs }, json);
    var dataPut = {
      PutRequest: {
        Item : this._Json2DynDB(dataJson)
      }
    };

    var idxData = {};

    //build index hint data
    var previewData = _.pick(json, previewAttrs);

    var indexOptions = {};
    if (!_.isUndefined(options)) {
      if (options.indexOptions) {
        indexOptions = options.indexOptions;
      }
    }
    var indexPuts = this._generateIndexItems(id, classId, json, previewData, indexOptions);

    var batchWriteData = {
      RequestItems : {

      }
    };
    batchWriteData.RequestItems[this._baseTable] = [dataPut];
    batchWriteData.RequestItems[this._indexTable] = indexPuts;

    return this._awsBatchWrite(batchWriteData)
    .then(function(){
      return dataJson;
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


  /**
  Runs a query based on parameters
  @param {String} classId - Class ID to be queried
  @param {?} queryParams - pameters:
          Array<Map> - previous version of queryParams {attr: 'attr', op: '=', value: 'value'}
          Map - new query parameters
          {
            conditions: 'expression', //Javascript-style expression
            indexOptions: {} //MAP of index options (see #put())
          }
          expression may contain functions:
           comboIndexMatch(['attr','attr',..],['value',100,...])
  **/
  this.query = function(classId, queryParams) {
    if (_.isArray(queryParams)) {
      return this._query(classId, queryParams); //call the "old" version
    } else if (_.isObject(queryParams)) {
      return this._queryExpression(classId, queryParams);
    }
  };


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
  https://s3-us-west-2.amazonaws.com/good-neighbor-media/a79tm0eqn0pvfs8curvavdd1i
  this.getDocumentUrl = function(id){
    return 'https://s3-'+ process.env.AWS_REGION +'.amazonaws.com/'+this._s3Bucket+'/'+id;
  }; //END this.getDocumentUrl

}
