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
	
	this.port		= 8024;
	this.logger 	= new _logger({label:'Cylon:'+process.pid});
	this.raceData	= {};		// Data on all the races
	this.count		= {};		// Number of users per race and per level
	this.players	= {};		// List of players online per race and per level
	this.playerAdd	= {};		// List of players to add per race and per level
	this.playerDel	= {};		// List of players to remove per race and per level
	this.clients	= {};		// Active clients on this process (with circular reference)
	this.interval	= {
		refresh:		60*5000,	// Refresh of race data
		update:			1000		// Refresh of online count
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
	});
};
cylon.prototype.serverInit = function() {
	var scope = this;
	this.logger.error("Server Starting on port "+this.port);
	this.server = new _server(this.port, {
		logger:		this.logger,
		onConnect:	function(client) {
			//scope.logger.log("client",client);
		},
		onReceive:	function(client, data) {
			if (data.raceToken) {
				scope.auth(data.raceToken, function(success, user) {
					if (success) {
						// register user
						scope.registerUser(client, user);
						
						// Give the user the current online count + milliseconds before the race starts
						var response = {
							online:	scope.count,
							timer:	new Date(scope.raceData[user.rid].start_time*1000).getTime()-new Date().getTime()
						};
						if (data.ask_id) {
							response.response_id = data.ask_id;
						}
						scope.server.send(client.uid, response);
					} else {
						var response = {invalidRaceToken: true};
						if (data.ask_id) {
							response.response_id = data.ask_id;
						}
						scope.server.send(client.uid, response);
					};
				});
			} else if (data.level) {
				scope.userSetLevel(client, data.level, data.ask_id);
			} else if (data.getUpdate) {
				var response = {online: scope.count};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(client.uid, response);
			} else if (data.scores) {
				scope.saveScore(client, data.scores, data.ask_id);
				var response = {sent: true};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(client.uid, response);
			} else if (data.crashtest) {
				crashnow();
			} else if (data.saveLevel) {
				scope.saveLevelData(client, data);
			} else {
				var response = {failed: true};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(client.uid, response);
			}
		},
		onQuit:	function(client) {
			
			scope.deleteUser(client);
			
			
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
							if (!scope.players[rows[i].id]) {
								scope.players[rows[i].id] = {};
							}
							if (!scope.playerAdd[rows[i].id]) {
								scope.playerAdd[rows[i].id] = {};
							}
							if (!scope.playerDel[rows[i].id]) {
								scope.playerDel[rows[i].id] = {};
							}
							for (j=0;j<=rows[i].levels;j++) {
								if (!scope.count[rows[i].id][j]) {
									scope.count[rows[i].id][j] = 0;
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
		var stack = new _stack();
		
		// Add new players to their levels
		for (i in scope.playerAdd) {
			stack.add(function(params, onFinish) {
				params.collection.update({players: true, rid:params.i}, {$set: scope.playerAdd[params.i]}, {upsert:true}, function(err, data) {
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
		
		// Update players list
		for (i in scope.players) {
			stack.add(function(params, onFinish) {
				params.collection.find({players: true, rid:params.i}, {}).toArray(function(err, doc) {
					if (doc.length > 0) {
						//console.log("doc",doc);
						if (!doc[0].list) {
							doc[0].list = {};
						}
						scope.players[params.i] = doc[0].list;
						onFinish();
					}
				});
			}, _.extend({},{i:i,collection:collection}));
		}
		
		// Reformat the object
		stack.add(function(params, onFinish) {
			var i;
			var j;
			for (i in scope.raceData) {
				// Create the objects if they don't exist yet
				if (!scope.players[scope.raceData[i].id]) {
					scope.players[scope.raceData[i].id] = {};
				}
				for (j=0;j<=scope.raceData[i].levels;j++) {
					if (!scope.players[scope.raceData[i].id][j]) {
						scope.players[scope.raceData[i].id][j] = {};
					}
				}
			}
			
			onFinish();
		}, {});
		
		// Recalculate the number of player per level
		stack.add(function(params, onFinish) {
			scope.computeCount(function() {
				onFinish();
			});
		}, {});
		
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


cylon.prototype.registerUser = function(client, user) {
	var scope = this;
	
	// Register in the list of clients
	this.clients[client.uid]	= {		// register by uid
		client:	client,
		data:	_.extend({level: 0},user)
	};
	
	// Register the user to his race, at level #0 (start screen)
	if (scope.players[user.rid] && scope.players[user.rid][0]) {
		scope.players[user.rid][0][user.id]	= user;
	}
	
	// Add to the push list
	scope.playerAdd[user.rid]["list."+"0."+user.id]	= user;
	
	// remove the user from the Delete list, in case he was there (case: disconnect-reconnect in less than 500ms)
	delete scope.playerDel[user.rid]["list."+"0."+user.id];
	
	scope.server.send(client.uid, {
		players:	scope.players,
		playerAdd:	scope.playerAdd,
		playerDel:	scope.playerDel
	});
};
cylon.prototype.userSetLevel = function(client, level, ask_id) {
	var scope 			= this;
	var user 			= this.clients[client.uid].data;
	var currentLevel 	= user.level;
	
	if (!scope.players[user.rid]) {
		scope.players[user.rid] = {};
	}
	if (!scope.players[user.rid][level]) {
		scope.players[user.rid][level] = {};
	}
	
	var userObject		= _.extend({},scope.players[user.rid][currentLevel][user.id]);
	
	// Remove the user from the current level
	delete scope.playerAdd[user.rid]["list."+currentLevel+"."+user.id];
	delete scope.playerDel[user.rid]["list."+currentLevel+"."+user.id];
	delete scope.players[user.rid][currentLevel][user.id];
	
	// Remove the online reference
	scope.playerDel[user.rid]["list."+currentLevel+"."+user.id] = userObject;
	
	// Register to the new level
	scope.playerAdd[user.rid]["list."+level+"."+user.id] 	= userObject;
	
	// update the level locally
	this.clients[client.uid].data.level = level;
	
	var response = {level: level};
	if (ask_id) {
		response.response_id = ask_id;
	}
		
	scope.server.send(client.uid, response);
};

cylon.prototype.deleteUser = function(client) {
	var scope 			= this;
	// We only count authentified users...
	if (this.clients[client.uid]) {
		var user 		= this.clients[client.uid].data;
		delete scope.players[user.rid][user.level];				// Remove from the list of players
		delete scope.playerAdd[user.rid]["list."+user.level+"."+user.id];		// Remove from the list of new players
		delete scope.clients[client.uid];				// Remove from the list of clients
		scope.playerDel[user.rid]["list."+user.level+"."+user.id]	= user;		// Remove from the online store
	}
}
cylon.prototype.saveScore = function(client, score, ask_id) {
	var scope 			= this;
	var user 			= this.clients[client.uid].data;
	var totalscore 		= 0;
	for (i in score) {
		if (!isNaN(score[i].score)) {
			totalscore += score[i].score;
		}
	}
						
	scope.mysql.query("update races_scores set end_time="+Math.round(new Date().getTime()/1000)+", log='"+JSON.stringify(score)+"', score="+totalscore+" where racetoken='"+user.raceToken+"'", function(err, rows, fields) {
		var response = {score: totalscore};
		if (ask_id) {
			response.response_id = ask_id;
		}
		scope.server.send(client.uid, response);
	});
}
cylon.prototype.computeCount = function(callback) {
	var scope = this;
	var i;
	var j;
	var k;
	for (i in this.players) {
		if (!this.count[i]) {
			this.count[i] = {};
		}
		for (j in this.players[i]) {
			if (!this.count[i][j]) {
				this.count[i][j] = 0;
			}
			this.count[i][j] = Object.keys(this.players[i][j]).length;
		}
	}
	callback();
};
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
cylon.prototype.saveLevelData = function(client, data) {
	var scope 			= this;
	var user 			= this.clients[client.uid].data;
	var levelData		= data.saveLevel;
	var levelIndex		= data.gameIndex;
	
	scope.mongo.getUser("datastore",user.id, function(collection, docs) {
		
		var buffer = {};
		buffer['race.'+user.rid+"."+levelIndex]	= levelData;
		
		collection.update(
			{
				uid:			user.id
			},{
				$set: buffer
			}, function(err, docs) {
				var response = {
					saveLevel: true
				};
				if (data.ask_id) {
					response.response_id = data.ask_id;
				}
				scope.server.send(client.uid, response);
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


