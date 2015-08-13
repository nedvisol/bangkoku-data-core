var _ = require('underscore'),
    utility = require('./utility.js');

/***
Entity Meta data
{
  attrs : {
    'attribute-name' : {
      label: 'display label in default language',
      label_LOCALE: 'display label in different locale',
      formula: 'EXPRESSION', //automatically calculates attr based on other attr
      validation: {
        expr: 'EXPRESSION', //must evaluated to TRUE to be considered valid, optional
        required: true, //or false, optional
        regex: 'PATTERN', //regex pattern, optional
      }
    }
  }
}

EXPRESSION ::=
  expr
  | (expr)

expr ::=
  operand
  | operand operation operand
  | function(expr)


**/

var Entity = {
  _metaData : null,
  _data: null,

  constructor: function(metaData) {
    this._metaData = metaData;
  },

  set: function(attr, value) {
    if (_.isObject(attr)) {
      this._data = _.extend(this._data, attr);
    } else {
      this._data[attr] = value;
    }
  }

};

Entity.extend = utility.extend;

module.exports = Entity;
