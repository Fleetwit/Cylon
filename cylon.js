var _cluster		= require('cluster');
var _os				= require('os');
var _ 				= require('underscore');
var _qs 			= require('querystring');
var _logger 		= require('./lib.logger').logger;
var _datastore 		= require('./lib.datastore').datastore;
var _server 		= require('./lib.simpleserver').simpleserver;
var _stack 			= require('./lib.stack').stack;
var _redis 			= require("redis");
var _mysql			= require('mysql');
var reporter 		= require('./lib.reporter').reporter;

var client 			= require('./node.awsi').client;
var server 			= require('./node.awsi').server;

var debug_mode		= true;

function cylon() {
	var scope 		= this;
	
	this.port		= 8024;
	this.logger 	= new _logger({label:'Cylon:'+process.pid});
	this.raceData	= {};		// Data on all the races
	this.count		= {};		// Number of users per race and per level
	this.inc		= {};		// Number of users per race and per level
	
	this.clients	= {};		// Active clients on this process (with circular reference)
	this.interval	= {
		refresh:		1000,		// Refresh of race data
		update:			500		// Refresh of online count
	};
	this.init();
	
};

cylon.prototype.init = function() {
	var scope = this;
	scope.mongo = new _datastore({
		database:	"fleetwit"
	});
	scope.mongo.init(function() {
		scope.mysql = _mysql.createConnection({
			host     : 'localhost',
			user     : 'root',
			password : '',
			database : 'fleetwit'
		});
		scope.mysql.connect(function(err) {
			// Get the levels for the races
			scope.refreshData(function() {
				
				scope.updateCount(function() {
					scope.serverInit();
					// Start the intervals
					setInterval(function() {
						scope.refreshData(function() {});
					}, scope.interval.refresh);
					
					setInterval(function() {
						scope.updateCount(function() {});
					}, scope.interval.update);
				});
			});
		});
	});
};
cylon.prototype.processArgs = function() {
	var i;
	var args 	= process.argv.slice(2);
	var output 	= {};
	for (i=0;i<args.length;i++) {
		var l1	= args[i].substr(0,1);
		if (l1 == "-") {
			if (args[i+1] == "true") {
				args[i+1] = true;
			}
			if (args[i+1] == "false") {
				args[i+1] = false;
			}
			if (!isNaN(args[i+1]*1)) {
				args[i+1] = args[i+1]*1;
			}
			output[args[i].substr(1)] = args[i+1];
			i++;
		}
	}
	return output;
};
cylon.prototype.serverInit = function() {
	var scope = this;
	this.logger.error("Server Starting on port "+this.port);
	
	this.server = new server({
		port:		this.port,
		onConnect:	function(wsid) {
			//console.log("connect",wsid);
			
		},
		onReceive:	function(wsid, data, flag) {
			var rqt	= new Date().getTime();
			if (data.raceToken) {
				scope.auth(data.raceToken, function(success, user) {
					//console.log(">>",new Date().getTime()-rqt+"ms");
					if (success) {
						// register user
						scope.registerUser(wsid, user);
						
						// Give the user the current online count + milliseconds before the race starts
						var response = {
							online:	scope.count,
							timer:	new Date(scope.raceData[user.rid].start_time*1000).getTime()-new Date().getTime()
						};
						if (data.ask_id) {
							response.response_id = data.ask_id;
						}
						if (data.send_time) {
							response.send_time = data.send_time;
						}
						scope.server.send(wsid, response);
					} else {
						var response = {invalidRaceToken: true};
						if (data.ask_id) {
							response.response_id = data.ask_id;
						}
						if (data.send_time) {
							response.send_time = data.send_time;
						}
						scope.server.send(wsid, response);
					};
				});
			} else if (data.level) {
				scope.userSetLevel(wsid, data.level, data.ask_id);
			} else if (data.getUpdate) {
				var response = {online: scope.count};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(wsid, response);
			} else if (data.scores) {
				scope.saveScore(wsid, data.scores, data.ask_id);
				var response = {sent: true};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(wsid, response);
			} else if (data.crashtest) {
				crashnow();
			} else if (data.saveLevel) {
				scope.saveLevelData(wsid, data);
			} else {
				var response = {failed: true};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(wsid, response);
			}
		},
		onClose:	function(wsid) {
			scope.deleteUser(wsid);
		}
	});
	
	/*
	this.reporter = new reporter({
		label:		"cylon",
		onRequest:	function() {
			return {
				cpu: 	_os.cpus()[0].times,
				totalmem:	_os.totalmem(),
				freemem:	_os.freemem(),
				mem:	process.memoryUsage(),
				count:	scope.server.count,
				ocount:	scope.server.ocount
			};
		}
	});*/
};
cylon.prototype.refreshData = function(callback) {
	var i;
	var j;
	var l;
	var scope = this;
	// get the races data
	this.mongo.open("cylon", function(collection) {
		scope.mysql.query("select * from races", function(err, rows, fields) {
			if (rows.length > 0 && rows[0].id > 0) {
				var l = rows.length;
				// save the races data
				for (i=0;i<l;i++) {
					scope.raceData[rows[i].id] = rows[i];
				}
				// Load the number of level per race
				scope.mysql.query("select count(g.id) as levels, r.id from races as r, races_games_assoc as g where r.id=g.rid group by r.id", function(err, rows, fields) {
					if (rows.length > 0 && rows[0].id > 0) {
						l = rows.length;
						// save the races data
						for (i=0;i<l;i++) {
							scope.raceData[rows[i].id].levels	= rows[i].levels;
						}
						callback();
					}
				});
			}
		});
	});
};
cylon.prototype.updateCount = function(callback) {
	var i;
	var j;
	var l;
	var scope = this;
	// get the races data
	this.mongo.open("cylon", function(collection) {
		var stack = new _stack();
		
		// Update count
		for (i in scope.inc) {
			stack.add(function(params, onFinish) {
				/*params.collection.findAndModify({count: true, race:params.i}, [['_id','asc']], {$inc: scope.inc[params.i]}, {upsert:true}, function(err, data) {
					scope.count[params.i] = data;
					// remove data we don't need
					delete scope.count[params.i]["_id"];
					delete scope.count[params.i]["count"];
					delete scope.count[params.i]["race"];
					// reset the inc counter
					scope.inc[params.i] = {};
					
					onFinish();
				});*/
				params.collection.update({count: true, race:params.i}, {$inc: scope.inc[params.i]}, {upsert:true}, function(err1, data1) {
					params.collection.find({count: true, race:params.i}, {
						limit:1
					}).toArray(function(err, docs) {
						if (docs.length > 0) {
							data = docs[0];
							scope.count[params.i] = data;
							// remove data we don't need
							delete scope.count[params.i]["_id"];
							delete scope.count[params.i]["count"];
							delete scope.count[params.i]["race"];
							// reset the inc counter
							scope.inc[params.i] = {};
						}
						onFinish();
					});
				});
			}, _.extend({},{i:i,collection:collection}));
		}
		// everything is updated
		stack.process(function() {
			if (scope.server) {
				scope.server.broadcast({
					online:		scope.count,
					players:	scope.players
				});
			}
			callback();
		});
		
	});
};
/*
{
	raceid: {
		levelid: {
			uid: (userdata)
		}
	}
}
*/


cylon.prototype.registerUser = function(wsid, user) {
	var scope = this;
	
	// Register in the list of clients
	this.clients[wsid]	= {		// register by uid
		client:	wsid,
		data:	_.extend({level: 0},user)
	};
	
	if (!scope.inc[user.rid]) {
		scope.inc[user.rid] = {};
	}
	if (!scope.inc[user.rid][0]) {
		scope.inc[user.rid][0] = 0;
	}
	scope.inc[user.rid][0]++;
};
cylon.prototype.userSetLevel = function(wsid, level, ask_id) {
	var scope 			= this;
	var user 			= this.clients[wsid].data;
	var currentLevel 	= user.level;

	// Update the online count
	if (!scope.inc[user.rid]) {
		scope.inc[user.rid] = {};
	}
	if (!scope.inc[user.rid][level]) {
		scope.inc[user.rid][level] = 0;
	}
	if (!scope.inc[user.rid][level-1]) {
		scope.inc[user.rid][level-1] = 0;
	}
	scope.inc[user.rid][level]++;
	scope.inc[user.rid][level-1]--;
	
	// update the level locally
	this.clients[wsid].data.level = level;
	
	var response = {level: level};
	if (ask_id) {
		response.response_id = ask_id;
	}
		
	scope.server.send(wsid, response);
};

cylon.prototype.deleteUser = function(wsid) {
	var scope 			= this;
	// We only count authentified users...
	if (this.clients[wsid]) {
		var user 		= this.clients[wsid].data;
		
		// Update online count
		if (!scope.inc[user.rid]) {
			scope.inc[user.rid] = {};
		}
		if (!scope.inc[user.rid][user.level]) {
			scope.inc[user.rid][user.level] = 0;
		}
		scope.inc[user.rid][user.level]--;
		
		delete scope.clients[wsid];				// Remove from the list of clients
	}
}

cylon.prototype.auth = function(raceToken, onAuth) {
	var scope = this;
	// Check if the raceToken is valid
	scope.mysql.query("select u.email,u.firstname,u.lastname,u.id,r.rid from races_scores as r, users as u where u.id=r.uid and r.racetoken='"+raceToken+"'", function(err, rows, fields) {
		if (rows.length > 0 && rows[0].id > 0) {
			rows[0].raceToken = raceToken;
			onAuth(true, rows[0]);
		} else {
			onAuth(false);
		}
	});
};
cylon.prototype.saveLevelData = function(wsid, data) {
	var scope 			= this;
	var user 			= this.clients[wsid].data;
	var levelData		= data.saveLevel;
	var levelIndex		= data.gameIndex;
	
	this.mongo.open("scores", function(collection) {
		
		if (!levelData.score) {
			levelData.score = 0;
		}
		
		// Log the score and the data for this game
		collection.update(
			{
				uid:			user.id,
				race:			user.rid,
				level:			levelIndex
			},{
				$set: {
					data:	levelData
				}
			},{
				upsert:true
			}, function(err, docs) {
				var response = {
					saveLevel: true
				};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(wsid, response);
			}
		);
		
		// Sum the score for that race
		collection.update(
			{
				uid:			user.id,
				race:			user.rid,
				type:			"race"
			},{
				$inc: {
					score:	levelData.score
				}
			},{
				upsert:true
			}, function(err, docs) {
				
			}
		);
		
		// Sum the score for the general ranking
		collection.update(
			{
				uid:			user.id,
				type:			"overall"
			},{
				$inc: {
					score:	levelData.score
				}
			},{
				upsert:true
			}, function(err, docs) {
				
			}
		);
		
	});
};

var instance = new cylon();

/************************************/
/************************************/
/************************************/
// Process Monitoring
setInterval(function() {
	process.send({
		memory:		process.memoryUsage(),
		process:	process.pid
	});
}, 1000);

// Crash Management
if (!debug_mode) {
	process.on('uncaughtException', function(err) {
		console.log("uncaughtException",err);
	});
}


