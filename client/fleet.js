var fleet = {};

/*
	WEBSOCKET JQUERY EXTENDER
*/
fleet.ws = function (url, options) {
	
	var scope  = this;
	
	this.options = $.extend({
		onOpen: function(e){
			console.log("onOpen",e);
		},
        onClose: function(e){
			console.log("onClose",e);
			console.info("Connection to the server lost.");
		},
        onReceive: function(data){
			console.log("onReceive",data);
		},
        onFail: function(){
			console.log("onFail","This browser is not compatible.");
		},
        onError: function(e){
			console.log("onError",e);
		},
        onConnectionLost: function(e){
			console.log("onConnectionLost",e);
		}
	},options);
	
	try {
		// Init the WebSocket
		if (window['MozWebSocket']) {
			this.ws = new MozWebSocket(url, []);
		} else if (window['WebSocket']) {
			this.ws = new WebSocket(url, []);
		} else {
			this.ws = false;
		}
	} catch (e) {
		this.options.onConnectionLost(e);
	}
	
	// If Websocket
	if (this.ws) {
		$(this.ws).bind('open', 	this.options.onOpen);
		$(this.ws).bind('close', 	this.options.onClose);
		$(this.ws).bind('message', 	function(e) {
			try {
				var data = JSON.parse(e.originalEvent.data);
				scope.options.onReceive(data);
			} catch(e) {
				scope.options.onError(e);
			}
		});
		
		$(window).unload(function(){
			scope.ws.close();
			scope.ws = null;
		});
		
	}
	
}
fleet.ws.prototype.send = function(message) {
	var scope = this;
	message = JSON.stringify(message);
	if (!this.ws) {
		this.options.onFail();
	}
	console.log("sending",message);
	this.ws.send(message);
}
fleet.ws.prototype.close = function(message) {
	var scope = this;
	this.ws.close();
}







/*
	FLEET PROTOCOL HANDLER
*/
fleet.protocol = function(url) {
	this.url 		= url;
	this.reset();
}
fleet.protocol.prototype.reset = function() {
	this.authtoken 	= false;
	this.suid		= false;
	this.ocount		= 0;	// Number of users online
};
fleet.protocol.prototype.connect = function(onConnect) {
	
	var scope = this;
	
	scope.onConnect = onConnect;
	
	this.ws = new fleet.ws(this.url, {
		onFail: function() {
			alert("Your browser is not compatible");
		},
		onReceive: function(data) {
			console.log(">> Receiving:",data);
			if (data instanceof Array) {
				var i;
				var l = data.length;
				for (i=0;i<l;i++) {
					scope.processData(data[i]);
				}
			} else {
				scope.processData(data);
				//console.log("error","Data is not an array");
			}
		}
	});
}
fleet.protocol.prototype.processData = function(data) {
	
	var scope = this;
	
	switch (data) {
		// Simple non-JSON data
		case "auth":
			// auth request
			if (this.authtoken) {
				this.ws.send({
					auth:{
						authtoken:	this.authtoken
					}
				});
			} else {
				console.log("error","Authtoken required");
			}
		break;
		// Complex/JSON data
		default:
			// Invalid token
			if (data.invalid_token) {
				console.info("Your access token is invalid!");
				alert("Your Access Token has timed out. \nThe page will now reload.");
				document.location = document.location;
			}
			// Update on the number of online players
			if (data.online) {
				this.ocount = data.online;
				console.log("Online:",this.ocount+" users");
				$('[data-dom="players_count"]').html(this.ocount);
				console.log("data-dom",$('[data-dom="players_count"]'));
			}
			// Receiving our UID (UUIDv4)
			if (data.uid) {
				this.suid = data.uid;
				console.log("Our UUIDv4:",this.suid);
				// Starting the game
				scope.onConnect();
			}
			// Server Change (default server was full)
			if (data.serverchange) {
				// disconnect first
				this.ws.close();
				// Now we reset and reconnect to the new server
				this.url 		= "ws://"+data.serverchange.host+":"+data.serverchange.port;
				this.reset();
				this.connect();
			}
		break;
	}
}

fleet.protocol.prototype.start = function() {
	
	var scope = this;
	
	this.gameloader 		= new fleet.gameloader();
	this.gameloader.games 	= this.games;
	this.gameloader.init();
}






/*
	FLEET GAME LOADER
*/
fleet.gameloader = function(url) {
	this.url 		= url;
	this.reset();
}
fleet.gameloader.prototype.init = function() {
	console.log("Our games: ",this.games);
};