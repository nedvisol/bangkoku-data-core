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
  this._awsBatchGet = function(params) {
    return this._dynamoDBInvoke('batchGetItem', params);
  };
  this._awsQuery = function(params) {
    //console.log('query param '+util.inspect(params,true,null));
    return this._dynamoDBInvoke('query', params);
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


  function convertToBase32String(num) {
    var str = '0000000000000000'+num.toString(32);
    str = str.substr(str.length-16);

    return str;
  }

  /**
  Generate put/delete requests for index records
  @param id {String} Id of the item
  @param classId {String} class of the item
  @param json {Object} object to be indexed
  @param previewData {Object} preview data (object) to be inserted with the index
  @param idxOptions {Object} - index details
  @param deleteIndex {Boolean} optional; TRUE will create DeleteRequest, FALSE will create PutRequest; default is FALSE

  idxOptions = {
    'idx-name' : {
      attr: 'attr name to be indexed',
      sort: 'attr name used for sorting'
    }
    sortAttr: 'attribute name to be used as sort value'
  }

  **/
  this._generateIndexItems = function(id, classId, json
    , previewData, idxOptions, deleteIndex) {

    if (_.isUndefined(deleteIndex)) {
      deleteIndex = false;
    }

    function genIdxRecord(key, value, sortAttr) {
      if (!_.isNumber(value) && !_.isString(value)) {
        return null;
      }
      if (key[0]=='_') {
        //internal attr
        return null;
      }

      if (_.isNumber(value)) {
        //must be integer
        if (value % 1 !== 0) {
          return null;
        }
        //covert to base 32 with leading zeros
        value = convertToBase32String(value);
      }

      var lookupKey = classId+'.'+key+'='+value;
      var idxJson = { lookupKey: lookupKey, id: id};

      if (!deleteIndex && sortAttr) {
        var sortVal = json[sortAttr];
        //ignore sort val if it's not a number
        if (_.isNumber(sortVal)) {
          idxJson['sortVal'] = convertToBase32String(sortVal);
        } else if (_.isString(sortVal)) {
          idxJson['sortVal'] = sortVal;
        }
      }

      if (!deleteIndex && previewData) {
        idxJson['data'] = previewData;
      }

      return idxJson;
    }

    var indexPuts = [];
    var req = (deleteIndex)?'DeleteRequest':'PutRequest';
    var keyName = (deleteIndex)?'Key':'Item';

    _.each(json, function(value, key){
      var idxJson = genIdxRecord(key, value, null);
      if (idxJson == null) return;
      var item = {};
      item[req] = {};
      item[req][keyName] = this._Json2DynDB(idxJson);
      indexPuts.push(item);
    }.bind(this));

    if (idxOptions) {
      _.each(idxOptions, function(option, idxName) {
        var idxJson = genIdxRecord(idxName, json[option.attr], option.sortAttr);
        if (idxJson == null) return;
        var item = {};
        item[req] = {};
        item[req][keyName] = this._Json2DynDB(idxJson);
        indexPuts.push(item);
      }.bind(this));
    }
    return indexPuts;
  };






  /***
  Insert an item into data store
  @param id {String} - item ID
  @param classId {String} - class ID
  @param json {Object} - JSON document to be stored
  @param options {Object} - additional options
  @returns {Q} A promise

  Options = {
  previweAttrs : [ 'attr','attr'], //Attribute names to be included in previewData for index
  indexOptions : {

   }
 }

  **/
  this.put = function(id, classId, json, options) {
    var that = this;
    var previewAttrs  = [];
    if (options && options.previewAttrs) {
      previewAttrs = options.previewAttrs;
    }

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

  var getFromArray = function(ids, classId) {
    if (ids.length > 100) {
      return Q.reject('Too many IDs (limit 100)');
    }
    var params = {
      RequestItems: {}
    };
    var keys = [];
    _.each(ids, function(id){
      keys.push(this._Json2DynDB({_id : id}));
    }.bind(this));
    params.RequestItems[this._baseTable] = {
      Keys : keys,
      ConsistentRead: true
    };
    return this._awsBatchGet(params)
    .then(function(results){
      var rows = [];
      if (results.Responses && results.Responses[this._baseTable]) {
        var items = results.Responses[this._baseTable];
        _.each(items, function(item){
          rows.push(this._DynDB2Json(item))
        }.bind(this));
        return rows;
      } else {
        return [];
      }
    }.bind(this));
  }.bind(this);

  this.get = function(id, classId) {
    if (_.isArray(id)) {
      return getFromArray(id, classId);
    }
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


  /****
  Update item
  @param id {String} ID of the item to be updated
  @param classId {String} class of the item to be updated
  @param json {Object} data to be updated, will only update attributes in this parameter
  @param options {Object} options including index options
  @returns {Q} A promise fulfilled with new values
  Options = {
  indexOptions : {
    // see generateIndexItems
   }
  }
  **/
  this.update = function(id, classId, json, options) {
    //check if all required attributes are present, including _rev; throw exception otherwise
    if (!json._rev) {
      return Q.reject('_rev attribute is required for update (err.ddl.missingrevattr)');
    }
    //update record, get old values back
    var updateData = _.extend({}, json);
    var newItem = null;

    return Q(0)
    .then(function(){
      //var updateData = _.extend({}, json);
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
        ReturnValues: 'ALL_OLD'
      };

      return this._awsUpdate(updateParams);
    }.bind(this))
    //determine attributes with changed values
    .then(function(response){
      //expect old values in response, convert to JSON
      var oldValues = this._DynDB2Json(response.Attributes);
      //pick changed attributes from oldValues
      var selectedOldValues = _.pick(oldValues, _.keys(updateData));

      //determine new(updated) item
      newItem = _.extend({}, oldValues, updateData);

      //pick preview data from new Item
      var previewData = _.pick(newItem, oldValues._previewAttrs);

      //remove unchanged attributes from selectedOldValues - no need to update index
      var selectedNewValues = _.extend({}, updateData);
      _.each(updateData, function(value, key) {
        var oldVal = selectedOldValues[key];
        //console.log('comparing '+value+'='+oldVal);
        if (oldVal == value) {
          //skip it!
          //console.log('remove');
          delete selectedNewValues[key];
          delete selectedOldValues[key];
        }
      });

      var idxOptions = {};
      if (options && options.indexOptions) {
        idxOptions = options.indexOptions;
      }

      var idxDeletes = this._generateIndexItems(id, classId, selectedOldValues, previewData, idxOptions, true);
      var idxPuts = this._generateIndexItems(id, classId, selectedNewValues, previewData, idxOptions, false);
      var idxRequests = _.union(idxDeletes, idxPuts);

      if (idxRequests.length == 0) {
        //nothing to do...
        return null;
      }

      var batchWriteParams = {
        RequestItems: {
        }
      };
      batchWriteParams.RequestItems[this._indexTable] = idxRequests;
      return this._awsBatchWrite(batchWriteParams);

    }.bind(this))
    .then(function(){
      return newItem;
    }.bind(this));
    //generate delete requests for invalidated index records
    //generate put requests for new index records
    //insert index records




  };



  /**
  Runs a query based on parameters
  @param {string} classId - Class ID to be queried
  @param {object} queryParams - query parameters
  @param {object} options
  @returns {Q} a Promise which will be fulfilled with array of objects (id and preview data)

  queryParams = {
    indexName : 'attr or index name',
    value: 'value of the index to be queried',
    sort: 'A or D' //ascending or descending
  }


  **/
  this.query = function(classId, queryParams, options) {
    //throw an exception if indexName or value is missing
    //query data store
    //return
    return Q(0)
    .then(function(){
      if (!queryParams || !queryParams.indexName || !queryParams.value) {
        return Q.reject('Missing required parameters (err.query.missingrequiredparams)');
      }

      var value = queryParams.value;
      if (_.isNumber(value)) {
        value = convertToBase32String(value);
      }

      var lookupKey = classId+'.'+queryParams.indexName+'='+value;
      var params = {
        TableName: this._indexTable,
        KeyConditionExpression: 'lookupKey = :lk',
        ExpressionAttributeValues: {
          ':lk' : { S : lookupKey}
        },
      };
      if (queryParams.sort) {
        params['IndexName'] = 'sortVal-idx';
        params['ScanIndexForward'] = (queryParams.sort == 'A')?true:false;
        params['Select'] = 'ALL_PROJECTED_ATTRIBUTES';
        if (queryParams.sortOp && queryParams.sortVal) {
          params['KeyConditionExpression'] = 'lookupKey = :lk AND sortVal '+queryParams.sortOp+' :sv';
          var sv = queryParams.sortVal;
          if (_.isNumber(queryParams.sortVal)) {
            sv = convertToBase32String(sv);
          }
          params['ExpressionAttributeValues'][':sv'] = { S : sv };
        }
      }
      //console.log('query params ****'+util.inspect(params, true, null));
      return this._awsQuery(params);
    }.bind(this))
    .then(function(results){
      var queryResult = [];
      _.each(results.Items, function(item){
        item = this._DynDB2Json(item);
        item['_id'] = item['id'];
        delete item.id;
        queryResult.push(item);
      }.bind(this));
      return queryResult;
    }.bind(this));
  };

  /**
  Delete an item and its index records
  @param id {String} ID of the item
  @param classId {String} class of the item
  @param options {Object} options including indexOptions
  @returns {Q} Promise which will be fulfilled with row ID if successful
  **/
  this.delete = function(id, classId, options) {
    return Q(0)
    //get an item from data store
    .then(function(){
      return this.get(id, classId);
    }.bind(this))
    //throws an exception if not found or class does not match
    .then(function(response){

      if (response == null) {
        return Q.reject('Item not found (err.get.idnotfound)');
      }
      if (!response._id) {
        return Q.reject('Item not found (err.get.idnotfound)');
      }
      if (response._class != classId) {
        return Q.reject('Invalid class (err.get.invalidclass)');
      }
      //generate index records to be deleted
      var idxOptions = {};
      if (options && options.indexOptions) {
        idxOptions = options.indexOptions;
      }

      //function(id, classId, json
        //, previewData, idxOptions, deleteIndex)
      var idxDeletes = this._generateIndexItems(id, classId, response, null, idxOptions, true);

      var params = {
        RequestItems: { }
      };
      params.RequestItems[this._baseTable] = [{
        DeleteRequest: {
          Key : {
            _id : { S : id }
          }
        }
      }];
      //delete item and index records
      params.RequestItems[this._indexTable] = idxDeletes;

      return this._awsBatchWrite(params);
    }.bind(this))
    //return row ID
    .then(function(results){
      return id;
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
