var jsep = require('jsep');

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
      throw new Exception('Unknown operator type');
      break;
  }
}

function resolveFn(callee, args, scope) {

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
    case '&&': return leftVal && rightVal; break;
    case '||': return leftVal || rightVal; break;
    case '!': return !leftVal; break;
  };
}

module.exports = {

  eval : function(expr, scope) {}
};
