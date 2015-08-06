var _ = require('underscore');

module.exports = {
  extend : function(props) {
    var parent = this;
    var child = null;
    if (props && _.has(prop, 'constructor')) {
      child = props.constructor;
    } else {
      child = function(){ return parent.apply(this, arguments); }
    }

    _.extend(child, parent);

    // Set the prototype chain to inherit from `parent`, without calling
    // `parent` constructor function.
    var Surrogate = function(){ this.constructor = child; };
    Surrogate.prototype = parent.prototype;
    child.prototype = new Surrogate;

    // Add prototype properties (instance properties) to the subclass,
    // if supplied.
    if (props) _.extend(child.prototype, props);

    // Set a convenience property in case the parent's prototype is needed
    // later.
    child.__super__ = parent.prototype;

    return child;
  };
};
