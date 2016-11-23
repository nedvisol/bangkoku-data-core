'use strict';
var util = require('util'),
uuid = require('uuid'),
bigInt = require('big-integer'),
_ = require('underscore'),
Q = require('q')
;
/****

Backend data store requirements:

1. Data Table
 - Fields:
    _id
    _created
    _rev
    _class
    _txn

Hash Key = _id
Sort Key = _range


Item Key for Items
_id = [id]@[partition-id]-i
_range = 0

Item Key for Relationships
_id = [id]@[partition-id]-r
_range = [relationship-id]-[timestamp]-[related id]


2. Index Table
Fields:
  lookupKey (Hash)
  id (Range)
  sortVal

Primary key: lookupKey,id
Secondary local index: lookupKey,sortVal

For data item
lookupKey = [index-name].[class id]@[partition id]=[value]

For class index
lookupKey = [classid]@[partitionid]/c

//Init****
var DD = require('./lib/dynamic-data.js');
var DB = require('./lib/adapter/aws.js');
var util = require('util');
DD.useAdapter(DB);
var ctx = DD.getContext('global-partition');

ctx.create({email: 'ned@njv.us', name: 'Ned'}).as('user').promise().done((results)=>{console.log(results)}, (err)=>{console.log(`err ${util.inspect(err, true, null)}`)});
var itemId='1604bzcdlgeqoo7moh204iljs';
ctx.get(itemId).promise().done((results)=>{console.log(results)}, (err)=>{console.log(`err ${util.inspect(err, true, null)}`)});
var updateJson = {email: 'ned-update@njv.us', name: 'Ned-update', _id : itemId, _rev: '2bd7t1d6b3pvuipqnmqikp1iz', _class: 'user'};
ctx.update(updateJson).promise().done((results)=>{console.log(results)}, (err)=>{console.log(`err ${util.inspect(err, true, null)}`)});

DD.getContext('partition-id') => Q(ctx)

ctx.get('id').promise() => Q(json)
ctx.create(json).as('class').index(indexOptions).promise() => Q(id)
ctx.update(json).index(indexOptions).promise() => Q(json)
ctx.delete('id').promise() => Q(true)
ctx.link(id, id).as('relationship id').promise()
ctx.unlink(id, id).as('relationship id').promise()
ctx.findLinks(id).newerFirst().promise()    // returns all relationships
ctx.findLinks(id).as('relationship id').olderFirst().promise()    // returns specific relationships

ctx.newTxn().expiresIn(3000) => ctx
ctx.commit()
ctx.abort()



***/

var __adapter = null;
var __dataCollection = 'dd-collection';
var __indexCollection = 'index-collection';

var __maxIdDigits = 25;

function convertToBase32String(num) {
  var str = '0000000000000000'+num.toString(32);
  str = str.substr(str.length-16);

  return str;
}

class Context {
  constructor(adapter, partitionId) {
    this._adapter = adapter;
    this._partitionId = partitionId;
    return this;
  }

  getItemId(id) {
    return `${id}@${this._partitionId}-i`;
  }

  generateUuid() {
    var buffer = new Buffer(16);
    uuid.v4(null, buffer);
    var id = bigInt(buffer.readUIntLE(12, 4)).shiftLeft(96)
      .plus(bigInt(buffer.readUIntLE(8, 4)).shiftLeft(64))
      .plus(bigInt(buffer.readUIntLE(4, 4)).shiftLeft(32))
      .plus(bigInt(buffer.readUIntLE(0, 4))).toString(36);

    //prepend 0s if needed
    for(var cnt = id.length; cnt < __maxIdDigits; cnt++) {
      id = '0'+id;
    }
    return id;
  }

  get(id) {
    return new GetOperation(this, id);
  }

  create(json) {
    return new CreateOperation(this, json);
  }

  update(json) {
    return new UpdateOperation(this, json);
  }

}

class GetOperation {
  constructor(ctx, id) {
    this._ctx = ctx;
    this._id = id;
  }

  promise() {
    var itemId = this._ctx.getItemId(this._id);
    return this._ctx._adapter.DB(__dataCollection).read({ _id: itemId, _range: '0'})
    .then((results)=>{

      //strip all attrs begining with _
      var attrs = _.pick(results, ['_id','_rev','_class']);
      for(var key of _.keys(results)) {
        if (key[0]=='_') {
          delete results[key];
        }
      }
      var id = attrs._id;
      id = id.substring(0, id.indexOf('@')); //get item id from actual id
      attrs._id = id;

      //add id rev and class back in
      return _.extend(results, attrs);
    });
  }
}

class CreateOperation {
  /**
  Insert item and associated indices
  1. insert data item
  2. insert index item for class
  3. insert index items specified in index options

  JSON data must not include any attribute with _ as first char

  IndexOptions = array of Index:
  [
  {
    indexName: 'name',
    attrs: [attrName,...]
  }
  ,...]

  **/
  constructor(ctx, json) {
    this._ctx = ctx;
    this._json = json;
    this._classId = null;
    this._idxOptions = null;
  }

  as(classId) {
    this._classId = classId;
    return this;
  }

  index(idxOptions) {
    this._idxOptions = idxOptions;
    return this;
  }

  promise() {

    if (this._classId == null) {
      return Q.reject('missing.parameters');
    }

    for(var key of _.keys(this._json)){
      if (key[0]=='_') {
        return Q.reject('invalid.parameters');
      }
    }

    var dataCollection = this._ctx._adapter.DB(__dataCollection);
    var indexCollection = this._ctx._adapter.DB(__indexCollection);
    var json = this._json;
    var itemId = this._ctx.generateUuid();
    var timestamp = Date.now();
    var fullItemId = this._ctx.getItemId(itemId)
    json = _.extend(json, {
      _id : fullItemId,
      _range: '0',
      _class : this._classId,
      _rev: this._ctx.generateUuid(),
      _createdTime : timestamp
    });

    //index JSON data for class
    var idxItems = [];
    var classIdx = {
      lookupKey :`${this._classId}@${this._ctx._partitionId}/c`,
      id: fullItemId,
      sortVal : convertToBase32String(timestamp)
    };
    idxItems.push(classIdx);

    if (this._idxOptions != null) {
      _.each(this._idxOptions,(idxOption)=>{
        //console.log(util.inspect(idxOption,true,null));
        var valArr = [];
        _.each(idxOption.attrs, (attr)=>{
          valArr.push(this._json[attr]);
        });
        var val = JSON.stringify(valArr);
        var idxJson = {
          lookupKey : `${idxOption.indexName}.${this._classId}@${this._ctx._partitionId}=${val}`,
          id: fullItemId,
          sortVal: timestamp
        };
        idxItems.push(idxJson);
      });
    }

    return dataCollection.create(json)
    .then((id)=> {
      return indexCollection.create(idxItems)
    })
    .then((id)=>{
      return itemId;
    });

  }
}


class UpdateOperation {
  /**
  Update an item and associated indices
  0. read existing item
  1. update data item
  2. re-create index items specified in index options

  JSON data must not include any attribute with _ as first char, except for _id and _rev

  IndexOptions = array of Index:
  [
  {
    indexName: 'name',
    attrs: [attrName,...]
  }
  ,...]

  **/
  constructor(ctx, json) {
    this._ctx = ctx;
    this._json = json;
    this._idxOptions = null;
  }

  index(idxOptions) {
    this._idxOptions = idxOptions;
    return this;
  }

  promise() {

    var dataCollection = this._ctx._adapter.DB(__dataCollection);
    var indexCollection = this._ctx._adapter.DB(__indexCollection);

	//it should reject if id,rev, or class are not provided
    var json = this._json;
    if (_.isUndefined(json._id)  || _.isUndefined(json._rev) || _.isUndefined(json._class)) {
      return Q.reject('missing.parameters');
    }

	//extract key attributes from JSON
    var keyAttrs = _.pick(json, ['_id','_rev','_class']);
    json = _.omit(json, ['_id','_rev','_class']);

	//it should reject if json includes attrs with _ other than id, rev, and class
    for(var key of _.keys(json)){
      if (key[0]=='_') {
        return Q.reject('invalid.parameters');
      }
    }
    
    //inject new _rev
    json._rev = this._ctx.generateUuid();

	//first read the item first to make sure they exist
    var itemId = this._ctx.getItemId(keyAttrs._id);
    return dataCollection.read({_id: itemId, _range: '0'})
    .then((results)=>{
      if (results == null) {
        //item not found
        return Q.reject('item.not.exists');
      }      
      if(results._rev != keyAttrs._rev) {
        return Q.reject('invalid.revision');
      }
      if(results._class != keyAttrs._class) {
        return Q.reject('invalid.class');
      }

      //update the record
      var condition = {
        expr: '#oldrev = :oldrev',
        attrs: { '#oldrev' : '_rev'},
        values: { ':oldrev' : keyAttrs._rev}
      };
      var kv = {
        _id : itemId,
        _range: '0'
      };

      return dataCollection
      .withCondition(condition)
      .update(kv, json)
      .then((results)=>{
        return true;
      }, (err)=>{
        if (err.errCode == 'ConditionalCheckFailedException') { //hard-coded AWS error
          return Q.reject('optimisti-lock.failed');
        } else {
          return Q.reject(err);
        }
      });
    });
  }
}



module.exports = {
  useAdapter: function(adapter) {
    __adapter = adapter;
  },
  getContext: function(partitionId) {
    return new Context(__adapter, partitionId);
  }
};
