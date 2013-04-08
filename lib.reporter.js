var simpleclient 	= require('./lib.simpleclient').simpleclient;
var _ 				= require('underscore');

function reporter(options) {
	var scope 		= this;
	this.options 	= _.extend({
		label:			"<unknown>",
		host: 			"209.59.172.80",	// 209.59.172.80
		port: 			8016,
		onRequest:		function() {}
	},options);
	
	this.clients = new simpleclient(this.options.host,this.options.port,{
		onConnect:	function(instance) {
			
		},
		onFail:	function(instance) {
			
		},
		onClose:	function(instance, code) {
			
		},
		onReceive:	function(instance, message) {
			if (message.update) {
				var date = new Date();
				instance.send({
					dd:		date.getDate(),
					mm:		date.getMonth()+1,
					yyyy:	date.getFullYear(),
					h:		date.getHours(),
					m:		date.getMinutes(),
					s:		date.getSeconds(),
					label:	scope.options.label,
					data:	scope.options.onRequest()
				});
			}
		}
	});
	
}


exports.reporter = reporter;


