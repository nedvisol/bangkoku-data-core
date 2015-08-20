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

  this._singleQuery = function _singleQuery(classId, queryCondition) {
    var indexName = 'searchKey-Svalue-index';
    var rangeValue  = { S : queryCondition.value.toString() };
    var op = queryCondition.op;
    var rangeAttrName = 'Svalue';
    if (_.isNumber(queryCondition.value)) {
      indexName = 'searchKey-Nvalue-index';
      rangeValue  = { N: queryCondition.value.toString() };
      rangeAttrName = 'Nvalue';
    }
    var queryParams = {
      TableName: this._indexTable,
      IndexName: indexName,
      Select: 'ALL_PROJECTED_ATTRIBUTES',
      KeyConditionExpression: 'searchKey = :sk AND ' + rangeAttrName + ' '+ op +' :rangeValue',
      ExpressionAttributeValues: {
        ':sk': {
          S: classId + '.' + queryCondition.attr
        },
        ':rangeValue': rangeValue
      }
    };
    return this._dynamoDBInvoke('query', queryParams);
  }

  this._query = function(classId, queryConditions) {
    //queryCondition = [{ attr: 'name', op: '=', value: 'value'}, ..]
    //all conditions are AND'd.
    var qs = [];
    var that = this;
    _.each(queryConditions, function(qc){
      qs.push(that._singleQuery.apply(that, [classId, qc]));
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
  } //END function _query()

  this._processQueryFunctions = function(parseTree) {

    var fnName = parseTree.callee.name;

    if (fnName == 'comboIndexMatch') {
      //expect 2 args
      if (parseTree.arguments.length < 2) {
        throw 'comboIndexMatch requires 2 parameters';
      }
      var attrs = [];

      _.each(parseTree.arguments[0].elements, function(e){
        attrs.push(e.value);
      });
      var vals = [];
      _.each(parseTree.arguments[1].elements, function(e){
        vals.push(e.value);
      });
      return {attr: attrs, op: 'comboIndexMatch', value: vals};
    }
    throw 'Unsupposed function '+fnName;
  }

  this._queryExpressionGetQueries = function(parseTree, queries) {

    if (_.isUndefined(queries)) {
      queries = [];
    }

    var comparisonOps = ['==','>','<','>=','<=','!='];
    var logicalOps = ['&&','||'];
    switch(parseTree.type){
      case 'LogicalExpression':
        if (_.contains(logicalOps, parseTree.operator)) {
          this._queryExpressionGetQueries(parseTree.left, queries);
          this._queryExpressionGetQueries(parseTree.right, queries);
          return queries;
        }
        break;
      case 'BinaryExpression':
        if (!_.contains(comparisonOps, parseTree.operator)) {
          throw 'Unsupported operation: '+parseTree.operator;
        }
        if (parseTree.left.type !== 'Identifier'
            && parseTree.right.type !== 'Literal') {
          throw 'Left side must be attribue name, right side must be literal';
        }
        parseTree.queryIndex = queries.length;
        var op = parseTree.operator;
        if (op == '==') { op = '='}; //use single equal sign
        queries.push({attr: parseTree.left.name, op: op
          ,value: parseTree.right.value});
        return queries;
        break;
      case 'UnaryExpression':
        throw 'Unsupported operation: '+parseTree.operator;
        break;
      case 'CallExpression':
        //return resolveFn(parseTree.callee, parseTree.arguments, scope);
        if (parseTree.callee.type != 'Identifier') {
          throw 'Unsupported caller type';
        }
        var functionName = parseTree.callee.name;
        parseTree.queryIndex = queries.length;
        queries.push(this._processQueryFunctions(parseTree));
        break;
      default:
        //console.log('**** '+util.inspect(parseTree));
        throw 'Unknown operator type: '+parseTree.type;
    }
    return queries;
  } //END this._queryExpressionGetQueries;

  this._queryExpressionProcess = function(parseTree, results) {
    switch(parseTree.type){
      case 'LogicalExpression':
        var lr = this._queryExpressionProcess(parseTree.left, results);
        var rr = this._queryExpressionProcess(parseTree.right, results);
        switch(parseTree.operator) {
          case '&&': return _.intersection(lr, rr); break;
          case '||': return _.union(lr, rr); break;
        }
        break;
      case 'BinaryExpression':
      case 'CallExpression':
        var resultIdx = parseTree.queryIndex;
        var result = results[resultIdx];
        var ids = _.pluck(result, '_id');
        return ids;
        break;
      default:
      return null;
    }
    return queries;
  }; //END this._queryExpressionProcess

  this._queryExpression = function(classId, queryParams) {
    var conditions = queryParams.conditions;
    var idxOptions = queryParams.indexOptions;

    var parseTree = jsep(conditions);
    var queries = this._queryExpressionGetQueries(parseTree, []);

    var promises = [];
    var that = this;
    _.each(queries, function(query) {
      var promise = that._singleQuery(classId, query);
      promises.push(promise);
    });

    var resultsSet = null;
    return Q.all(promises)
    .then(function(results){
      resultsSet = results;
      return that._queryExpressionProcess(parseTree, results);
    })
    .then(function(ids){

      //*** TODO May need to improve this algorithm
      var combinedResults = {};
      _.each(resultsSet, function(results) {
        _.each(results, function(r){
          combinedResults[r._id] = r;
        });
      });
      //console.log(util.inspect(resultsSet));
      //console.log(util.inspect(combinedResults));
      return _.values(_.pick(combinedResults, ids));
    });

  } //END this._queryExpression


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

  /**
  Generates PutRequest items for indexed attributes/combo.
  @param {String} id - String ID of the item
  @param {String} classId - Class ID of the item
  @param {Object} json - item object
  @param {Object} idxData - preview data to be stored with index record
  @param {Object} idxOptions - index details

  idxOptions = {
    "SearchKey_name" : {
      type : "combo|???",
      attributes: ['attr','attr',...]
    }
  }

  **/
  this._generateIndexItems = function(id, classId, json, idxData, idxOptions) {
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

    _.each(idxOptions, function(value, skName){
      var idxid = id+'.'+classId+'.'+skName;
      var sk = classId+'.'+skName;
      if (value.type == 'combo') {
        //sort attributes
        var sortedAttrs = _.sortBy(value.attributes, function(item){return item;});
        var sortedValues = _.map(sortedAttrs, function(attr){ return json[attr];});
        var hash = mmHash(JSON.stringify(sortedValues), 100);
        //console.log('**** '+ util.inspect(sortedValues));

        indexPuts.push({
          PutRequest: {
            Item: that._Json2DynDB({ _id: idxid, searchKey: sk, Nvalue: hash, data : idxData })
          }
        });

      } else {
        throw 'Index type not supported: '+value.type;
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
  this.put = function(id, classId, json, indexHint, options) {
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
    var indexOptions = {};
    if (!_.isUndefined(options)) {
      if (options.indexOptions) {
        indexOptions = options.indexOptions;
      }
    }
    var indexPuts = this._generateIndexItems(id, classId, json, idxData, indexOptions);

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

  this.getDocumentUrl = function(id){
    return 'https://s3.amazonaws.com/'+this._s3Bucket+'/'+id;
  }; //END this.getDocumentUrl

}
