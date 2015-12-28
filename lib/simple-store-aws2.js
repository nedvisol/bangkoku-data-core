'use strict';
//ES6
var _appName = 'generic';
class SimpleStoreAWS2 {

  static setAppName(appName) {
    _appName = appName;
  }
  static put(className) {
    return new SimpleStoreAWS2('put', className);
  }
  static get(className) {
    return new SimpleStoreAWS2('get', className);
  }
  static query(className) {
    return new SimpleStoreAWS2('query', className);
  }
  static update(className) {
    return new SimpleStoreAWS2('update', className);
  }
  static delete(className) {
    return new SimpleStoreAWS2('delete', className);
  }

  constructor(op, classId) {
    this._op = op;
    this._classId = classId;
  }

  indexes(indexOptions) {
    this._indexOptions = indexOptions;
  }


}

/***
BDC.setAppName('app');
BDC.put('user').indexes(indexOptions).data(json).promise()
.then(result => {})

BDC.query('user').indexes(indexOptions).when('key = value').promise()
.then(results => {})

BDC.get('user').id('user-id-123').promise()
.then(result => {})

BDC.update('user').indexes(indexOptions).set('key', 'value').promise()
.set('key', 'value').data(json)
.then(result => {})

BDC.delete('user').id('user-id-123').promise()
.then(result => {})

**/

module.exports = SimpleStoreAWS2;
