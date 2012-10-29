/**
 * Basic app to read every line of a file and push them into a queue
 * Mostly copied from http://docs.cloudfoundry.com/services/rabbitmq/nodejs-rabbitmq.html
 */

var http = require('http');
var amqp = require('amqp');
var URL = require('url');
var htmlEscape = require('sanitizer/sanitizer').escape;
var fs = require('fs');
var lazy = require('lazy');

var EXCHANGE_NAME = 'node-tree-demo-exchange'

function rabbitUrl() {
	if (process.env.VCAP_SERVICES) {
		conf = JSON.parse(process.env.VCAP_SERVICES);
		return conf['rabbitmq-2.4'][0].credentials.url;
	}
	else {
		return "amqp://localhost";
	}
}

var port = process.env.VCAP_APP_PORT || 3000;

var messages = [];

function setup() {

	var exchange = conn.exchange(EXCHANGE_NAME, {
		'type': 'fanout', 
		durable: false
	}, function() {
		// create queue
		var queue = conn.queue('', {
			durable: false, 
			exclusive: true
		},
		function() {
			// when queue is created, setup listeners
			queue.subscribe(function(msg) {
				console.log("Read a message from queue");
				var data = msg.body;
				console.log(data.domain + "_" + data.stub);
			});
			// now bind the queue to our exchange
			queue.bind(exchange.name, '');
		});
		queue.on('queueBindOk', function() {
			console.log("Successfully bound to queue")
			readFileAndPush(exchange);
		});
	});
}

function readFileAndPush(exchange) {
	// read every line of the file
	new lazy(fs.createReadStream('../clicks/redir-2012-10-22-15-00-click-redir-16-0c0e100b-48a3-49a6-bd29-f1dc57a65f1d.log'))
	.lines
	.forEach(function(line){
		console.log("Reading line from file")
		var fields = line.toString().split("\t");
		var msg = {
			'domain': fields[0],
			'stub': fields[1],
			'created': fields[2],
			'updated': fields[3],
			'ip': fields[4],
			'referrer': fields[5],
			'user_agent': fields[6],
			'language': fields[7],
			'snowball_id': fields[8],
			'campaign_id': fields[9],
			'parent_id': fields[10],
			'domain_id': fields[12],
			'original_url_id': fields[13],
			'redirection_id': fields[14],
			'project_id': fields[15],
			'channel': fields[16],
			'tool': fields[17],
			'sharer_id': fields[18],
			'bot_flag': fields[19],
			'user_id': fields[20],
			'notes': fields[21]
		}
		console.log("Pushing to queue")
		exchange.publish('', {
			body: msg
		});
	});	
}

// setup rabbit and start rest of the app
console.log("Starting ... AMQP URL: " + rabbitUrl());
var conn = amqp.createConnection({
	url: rabbitUrl()
});
conn.on('ready', setup);