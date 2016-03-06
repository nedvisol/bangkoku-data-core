'use strict';
var util = require('util'),
uuid = require('node-uuid'),
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
lookupKey = [classid].[attr]@[partition id]=[value]

For class index
lookupKey = [classid]@[partitionid]/c




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

}

class GetOperation {
  constructor(ctx, id) {
    this._ctx = ctx;
    this._id = id;
  }

  promise() {
    var itemId = this._ctx.getItemId(this._id);
    return this._ctx._adapter.collection(__dataCollection).read({ _id: itemId});
  }
}

class CreateOperation {
  /**
  Insert item and associated indices
  1. insert data item
  2. insert index item for class
  3. insert index items specified in index options

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
  }

  as(classId) {
    this._classId = classId;
    return this;
  }

  promise() {

    if (this._classId == null) {
      return Q.reject('missing.parameters');
    }

    var dataCollection = this._ctx._adapter.collection(__dataCollection);
    var indexCollection = this._ctx._adapter.collection(__indexCollection);
    var json = this._json;
    var itemId = this._ctx.generateUuid();
    var timestamp = Date.now();
    json = _.extend(json, {
      _id : this._ctx.getItemId(itemId),
      _class : this._classId,
      _rev: this._ctx.generateUuid(),
      _createdTime : timestamp
    });

    //index JSON data for class
    var idxJson = {
      lookupKey :`${this._classId}@${this._ctx._partitionId}/c`,
      id: itemId,
      sortVal : timestamp
    };

    return dataCollection.create(json)
    .then((id)=> {
      return indexCollection.create(idxJson)
    })
    .then((id)=>{
      return true;
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
