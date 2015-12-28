var simpleStoreAws = require('./simple-store-aws.js');
var simpleStoreAws2 = require('./simple-store-aws2.js');

module.exports = function(store){
  if (store == 'aws') {
    return simpleStoreAws;
  } else if (store == 'aws2') {
    return simpleStoreAws2;
  }
}
