var http 				= require('http');
var uuid 				= require('./lib.uuid');
var _ 					= require('underscore');
var qs 					= require('querystring');

function logger(options) {
	var scope = this;
	this.color = {
		'reset': 		'\033[0m',
		'bold': 		'\033[1m',
		'italic': 		'\033[3m',
		'underline': 	'\033[4m',
		'blink': 		'\033[5m',
		'black': 		'\033[30m',
		'red': 			'\033[31m',
		'green': 		'\033[32m',
		'yellow': 		'\033[33m',
		'blue': 		'\033[34m',
		'magenta': 		'\033[35m',
		'cyan': 		'\033[36m',
		'white': 		'\033[37m'
	};
	
	this.options = _.extend({
		label:		"LOGGER",
		color:		"red"
	},options);
	
	
}
logger.prototype.log = function(){
	console.log(this.color[this.options.color]+this.options.label);
	for (i in arguments) {
		console.log(this.color['reset'], arguments[i],this.color['reset']);
	}
};
logger.prototype.info = function(){
	console.log(this.color[this.options.color]+this.options.label);
	for (i in arguments) {
		console.log(this.color['blue'], arguments[i],this.color['reset']);
	}
};
logger.prototype.error = function(){
	console.log(this.color[this.options.color]+this.options.label);
	for (i in arguments) {
		console.log(this.color['red'],this.color['blink'], arguments[i],this.color['reset']);
	}
};

exports.logger = logger;