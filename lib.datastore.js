var mysql				= require('mysql');
var http 				= require('http');
var fs 					= require('fs');
var mongodb 			= require('mongodb');
var qs 					= require('querystring');
var _ 					= require('underscore');

function datastore(options) {
	this.options = _.extend({
		host:		"127.0.0.1",
		port:		27017,
		database:	"fleetwit"
	},options);
	
	this.collections = {};
}
datastore.prototype.init = function(callback) {
	var scope 		= this;
	
	this.server 	= new mongodb.Server(this.options.host, this.options.port, {});
	this.db			= new mongodb.Db(this.options.database, this.server, {w:1});
	this.db.open(function (error, client) {
		if (error) {
			throw error;
		}
		scope.instance = client;
		callback();
	});
}
datastore.prototype.open = function(collectionName, callback) {
	var scope 		= this;
	if (!this.collections[collectionName]) {
		this.collections[collectionName] = new mongodb.Collection(this.instance, collectionName);
	}
	callback(this.collections[collectionName]);
}
datastore.prototype.getUser = function(collectionName, uid, callback) {
	var scope 		= this;
	this.open(collectionName, function(collection) {
		collection.find({
			uid:	uid
		}, {
			limit:1
		}).toArray(function(err, docs) {
			console.dir(docs);
			if (docs.length == 0) {
				// No userdata, we create it.
				collection.insert({
					uid:			uid,
					surveydata:		{},
					facebookdata:	{},
					twitterdata:	{}
				}, function(err, docs) {
					callback(collection);
				});
			} else {
				callback(collection);
			}
			
		});
	});
}

datastore.prototype.set = function(collection, label, value, callback) {
	var scope 		= this;
	var criteria 	= {};
	criteria[label]	= label;
	var buffer		= {};
	buffer.value	= value;
	collection.update(criteria, {$set: buffer}, {upsert:true}, callback);
}

datastore.prototype.incr = function(collection, criteria, value, callback) {
	var scope 		= this; 
	var buffer		= {};
	buffer.value	= value;
	
	
	collection.findAndModify(criteria, [['_id','asc']], {$set:{updated:true}, $inc: {value: value}}, {}, function(err, data) {
		console.log("findAndModify :: ",err, data);
		if (err) {
			// couldn't update, let's create
			collection.update(_.extend({value:value},criteria), {$set: buffer}, {upsert:true}, function(err2, data2) {
				scope.get(collection, criteria, function(err3, data3) {
					callback(data3)
				});
			});
			/*scope.set(collection, 'value', value, function(err2, data2) {
				scope.get(collection, 'value', function(data3) {
					callback(data3)
				});
			});*/
		} else {
			//console.log("data:",data);
			callback(data);
		}
	});
}

datastore.prototype.get = function(collection, label, callback) {
	var scope 		= this;
	var criteria 	= {};
	criteria[label]	= label;
	collection.findOne(criteria, function(err, data) {
		callback(data);
	});
}

exports.datastore = datastore;