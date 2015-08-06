var simpleStoreAws = require('./simple-store-aws.js');

module.exports = function(store){
  if (store == 'aws') {
    return simpleStoreAws;
  }
}
