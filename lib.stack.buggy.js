var _ 				= require('underscore');

function stack() {
	this.reset();
}
stack.prototype.reset = function() {
	this.stack 		= [];
	this.current 	= 0;
	this.done		= 0;
}
stack.prototype.add = function(item, params) {
	this.stack.push({
		fn:		item,
		params:	params
	});
}
stack.prototype.process = function(callback) {
	var scope = this;
	var i;
	this.callback 		= callback;
	for (i=0;i<this.stack.length;i++) {
		this.stack[this.current].fn(this.stack[this.current].params,function() {
			//console.log("DONE: ",scope.done,"/",scope.stack.length-1);
			if (scope.done == scope.stack.length-1) {
				//console.log("DONE!!");
				callback();
			}
			scope.done++;
		});
		this.current++;
	}
}

exports.stack = stack;