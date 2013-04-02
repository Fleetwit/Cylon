var _ 				= require('underscore');

function stack() {
	this.reset();
}
stack.prototype.reset = function() {
	this.stack 		= [];
	this.current 	= 0;
}
stack.prototype.add = function(item) {
	this.stack.push(item);
}
stack.prototype.process = function(callback) {
	this.callback 		= callback;
	this.processNext();
}
stack.prototype.processNext = function(callback) {
	var scope = this;
	if (this.current == this.stack.length-1) {
		console.log("processNext: END");
		//scope.callback();
		return true;
	}
	console.log("processNext: ",scope.current);
	scope.current++;
		scope.processNext();
	/*
	this.stack[this.current](function() {
		scope.current++;
		scope.processNext();
	});*/
}

exports.stack = stack;