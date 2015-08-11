var jsep = require('jsep'),
  _ = require('underscore'),
  util = require('util');

function resolve(parseTree, scope) {
  switch(parseTree.type){
    case 'Literal':
      return parseTree.value;
      break;
    case 'Identifier':
      return scope[parseTree.name];
      break;
    case 'MemberExpression':
      var prop = (parseTree.property.type == 'Literal')?
              parseTree.property.value:parseTree.property.name;
              //console.log('memexpr '+scope[parseTree.object.name][prop]);
      return scope[parseTree.object.name][prop];
      break;
    case 'BinaryExpression':
      return resolveOp(parseTree.operator,
        parseTree.left, parseTree.right, scope);
      break;
    case 'UnaryExpression':
    return resolveOp(parseTree.operator,
      parseTree.argument, null, scope);
      break;
    case 'CallExpression':
      return resolveFn(parseTree.callee, parseTree.arguments, scope);
      break;
    default:
      throw "Unknown operator type";
  }
}

function resolveFn(callee, args, scope) {
    //console.log('** callee **'+util.inspect(callee, true, null));
  var fn = resolve(callee, scope);
    //console.log('** fn '+fn);
  var obj = scope[callee.object.name];
  var resolvedArgs = [];
  _.each(args, function(a){
    resolvedArgs.push(resolve(a, scope));
  });
  return fn.apply(obj, resolvedArgs);
}

function resolveOp(operator, left, right, scope) {
  var leftVal = resolve(left, scope);
  var rightVal = resolve(right, scope);
  switch (operator) {
    case '+': return leftVal + rightVal; break;
    case '-': return leftVal - rightVal; break;
    case '*': return leftVal * rightVal; break;
    case '/': return leftVal / rightVal; break;
    case '&': return leftVal & rightVal; break;
    case '|': return leftVal | rightVal; break;
    case '>': return leftVal > rightVal; break;
    case '<': return leftVal < rightVal; break;
    case '>=': return leftVal >= rightVal; break;
    case '<=': return leftVal <= rightVal; break;
    case '==': return leftVal == rightVal; break;
    case '!=': return leftVal != rightVal; break;
    case '&&': return leftVal && rightVal; break;
    case '||': return leftVal || rightVal; break;
    case '!': return !leftVal; break;
  };
}

module.exports = {

  eval : function(expr, scope) {
    if (_.isString(expr)) {
      expr = jsep(expr);
    }
    return resolve(expr, scope);
  }
};
