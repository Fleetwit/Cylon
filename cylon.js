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

var debug_mode		= true;

function cylon() {
	var scope 		= this;
	
	this.port		= 8080;
	this.logger 	= new _logger({label:'Cylon:'+process.pid});
	this.raceData	= {};		// Data on all the races
	this.count		= {};		// Number of users per race and per level
	this.incr		= {};		// Increment on the number of users per race and per level since last update
	this.players	= {};		// List of players online per race and per level
	this.playerAdd	= {};		// List of players to add per race and per level
	this.playerDel	= {};		// List of players to remove per race and per level
	this.clients	= {};		// Active clients on this process (with circular reference)
	this.interval	= {
		refresh:		5000000,	// Refresh of race data
		update:			10000		// Refresh of online count
	};
	this.init();
};

cylon.prototype.init = function() {
	var scope = this;
	scope.redis = _redis.createClient();
	scope.redis.select(0, function() {
		scope.mongo = new _datastore({
			database:		"fleetwit"
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
						scope.logger.log("\n updateCount ONLINE\n");
						scope.serverInit();
					});
				});
				
				// Start the intervals
				setInterval(function() {
					scope.refreshData(function() {});
				}, scope.interval.refresh);
				
				setInterval(function() {
					scope.updateCount(function() {
						//scope.logger.error(scope.players);
					});
				}, scope.interval.update);
			});
		});
	});
};
cylon.prototype.serverInit = function() {
	var scope = this;
	this.logger.error("Server Starting");
	this.server = new _server(this.port, {
		logger:		this.logger,
		onConnect:	function(client) {
			//scope.logger.log("client",client);
		},
		onReceive:	function(client, data) {
			if (data.raceToken) {
				scope.logger.log("Receiving:",data.raceToken);
				scope.auth(data.raceToken, function(success, user) {
					if (success) {
						// register user
						scope.registerUser(client, user);
						
						// Give the user the current online count + milliseconds before the race starts
						scope.server.send(client.uid, {
							online:	scope.count[user.rid],
							timer:	new Date(scope.raceData[user.rid].start_time*1000).getTime()-new Date().getTime()
						});
					} else {
						scope.server.send(client.uid, {invalidRaceToken: true});
					}
					scope.logger.error("user online",user);
				});
			}
			if (data.level) {
				scope.userSetLevel(client, data.level);
			}
		},
		onQuit:	function(client) {
			var user 			= scope.clients[client.uid].data;
			var currentLevel 	= user.level;
			scope.incr[user.rid][currentLevel]		-= 1;
			scope.count[user.rid][currentLevel]		-= 1;
			delete scope.players[user.rid][currentLevel][user.id];
			
		}
	});
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
							// Create the objects if they don't exist yet
							if (!scope.count[rows[i].id]) {
								scope.count[rows[i].id] = {};
							}
							if (!scope.incr[rows[i].id]) {
								scope.incr[rows[i].id] = {};
							}
							if (!scope.count[rows[i].id].total) {
								scope.count[rows[i].id].total = 0;
							}
							if (!scope.incr[rows[i].id].total) {
								scope.incr[rows[i].id].total = 0;
							}
							if (!scope.players[rows[i].id]) {
								scope.players[rows[i].id] = {};
							}
							if (!scope.playerAdd[rows[i].id]) {
								scope.playerAdd[rows[i].id] = {};
							}
							if (!scope.playerDel[rows[i].id]) {
								scope.playerDel[rows[i].id] = {};
							}
							/*if (!scope.clients[rows[i].id]) {
								scope.clients[rows[i].id] = {};
							}*/
							for (j=0;j<=rows[i].levels;j++) {
								if (!scope.count[rows[i].id][j]) {
									scope.count[rows[i].id][j] = 0;
								}
								if (!scope.incr[rows[i].id][j]) {
									scope.incr[rows[i].id][j] = 0;
								}
								if (!scope.players[rows[i].id][j]) {
									scope.players[rows[i].id][j] = {};
								}
							}
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
		//scope.logger.log("MONGO OPENED");
		var stack = new _stack();
		
		// Update the number of users online
		for (i in scope.raceData) {
			for (j=0;j<=scope.raceData[i].levels;j++) {
				// Update the var
				stack.add(function(params, onFinish) {
					var x = params.i;
					var y = params.j;
					//console.log("INCR",{count:true, rid: scope.raceData[x].id, level: y});
					scope.mongo.incr(collection, {count:true, rid: scope.raceData[x].id, level: y}, scope.incr[scope.raceData[x].id][y], function(data) {
						// update the count
						scope.count[scope.raceData[x].id][y] = data.value;
						// reset the incr value
						scope.incr[scope.raceData[x].id][y]	= 0;
						onFinish();
					});
				}, _.extend({},{i:i,j:j}));
			}
		}
		
		// Update the client list
		// Add players
		
		//console.log("scope.playerAdd:: ",scope.playerAdd);
		for (i in scope.playerAdd) {
			stack.add(function(params, onFinish) {
				params.collection.update({players: true, rid:params.i}, {$set: scope.playerAdd[params.i]}, {upsert:true}, function(err, data) {
					//scope.logger.error("********doc.list********",doc.list);
					scope.playerAdd[params.i] = {}; // reset the list
					onFinish();
				});
			}, _.extend({},{i:i,collection:collection}));
		}
		// Remove players
		for (i in scope.playerDel) {
			stack.add(function(params, onFinish) {
				params.collection.update({players: true, rid:params.i}, {$unset: scope.playerDel[params.i]}, {upsert:true}, function(err, data) {
					scope.playerDel[params.i] = {}; // reset the list
					onFinish();
				});
			}, _.extend({},{i:i,collection:collection}));
		}
		// Remove players
		
		for (i in scope.players) {
			stack.add(function(params, onFinish) {
				params.collection.find({players: true, rid:params.i}, {}).toArray(function(err, doc) {
					if (!doc.list) {
						doc.list = {};
					}
					//scope.logger.error("********doc.list********",err,doc.list);
					scope.players[doc.rid] = doc.list;
					onFinish();
				});
			}, _.extend({},{i:i,collection:collection}));
		}
		
		// everything is updated
		stack.process(function() {
			if (scope.server) {
				scope.server.broadcast({
					count:	scope.count,
					incr:	scope.incr,
					players:{
						players:	scope.players,
						playerAdd:	scope.playerAdd,
						playerDel:	scope.playerDel
					}
				});
			}
			
			console.log("DONE");
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


cylon.prototype.registerUser = function(client, user) {
	var scope = this;
	
	// Register in the list of clients
	scope.clients[client.uid]	= {		// register by uid
		client:	client,
		data:	_.extend({level: 0},user)
	};
	
	// Register the user to his race
	scope.players[user.rid][0][user.id]	= user;
	
	// Add to the push list
	scope.playerAdd[user.rid]["0."+user.id]	= user;
	
	// remove the user from the Delete list, in case he was there (case: disconnect-reconnect in less than 500ms)
	delete scope.playerDel[user.rid]["0."+user.id];
	
	// Increment the online count
	scope.incr[user.rid][0]		+= 1;
	scope.count[user.rid][0]	+= 1;
	
	scope.server.send(client.uid, {
		players:	scope.players,
		playerAdd:	scope.playerAdd,
		playerDel:	scope.playerDel
	});
};
cylon.prototype.userSetLevel = function(client, level) {
	var scope 			= this;
	var user 			= this.clients[client.uid].data;
	var currentLevel 	= user.level;
	
	var userObject		= _.extend({},scope.players[user.rid][currentLevel][user.id]);
	
	// Remove the user from the current level
	delete scope.playerAdd[user.rid][currentLevel+"."+user.id];
	delete scope.playerDel[user.rid][currentLevel+"."+user.id];
	delete scope.players[user.rid][currentLevel][user.id];
	
	// Remove the online reference
	scope.playerDel[user.rid][currentLevel+"."+user.id] = userObject;
	
	// Register to the new level
	scope.playerAdd[user.rid][level+"."+user.id] 	= userObject;
	scope.players[user.rid][level][user.id] 		= userObject;
	
	// Decrement the online count for previous level
	scope.incr[user.rid][currentLevel]		-= 1;
	scope.count[user.rid][currentLevel]		-= 1;
	
	// Increment the online count for the new level
	scope.incr[user.rid][level]				+= 1;
	scope.count[user.rid][level]			+= 1;
	
	// update the level locally
	this.clients[client.uid].data.level = level;
	
	scope.server.send(client.uid, {
		players:	scope.players,
		playerAdd:	scope.playerAdd,
		playerDel:	scope.playerDel,
		userObject:	userObject
	});
};
/*
cylon.prototype.deleteUser = function(client, user) {
	var scope = this;
	delete scope.players[user.rid][user.level];				// Remove from the list of players
	delete scope.playerAdd[user.rid]["list."+user.id];		// Remove from the list of new players
	delete scope.clients[user.rid][user.id];				// Remove from the list of clients
	scope.playerDel[user.rid]["list."+user.id]	= user;		// Remove from the online store
}*/
cylon.prototype.auth = function(raceToken, onAuth) {
	var scope = this;
	// Check if the raceToken is valid
	scope.mysql.query("select u.email,u.firstname,u.lastname,u.id,r.rid from races_scores as r, users as u where u.id=r.uid and r.racetoken='"+raceToken+"'", function(err, rows, fields) {
		if (rows.length > 0 && rows[0].id > 0) {
			onAuth(true, rows[0]);
		} else {
			onAuth(false);
		}
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


