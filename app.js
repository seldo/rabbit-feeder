/**
 * Basic app to read every line of a file and push them into a queue
 * Mostly copied from http://docs.cloudfoundry.com/services/rabbitmq/nodejs-rabbitmq.html
 */

var amqp = require('amqp');
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
	//new lazy(fs.createReadStream('../clicks/redir-2012-10-22-15-59-click-redir-22-java-a225e958-bad8-4e50-b7e6-5f6d84138e9d.log'))
	var filename = '../clicks/fake-clicks-2.tsv';
	if (process.argv[2]) {
		filename = process.argv[2];
	}
	new lazy(fs.createReadStream(filename))
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
			'domain_id': fields[11],
			'original_url_id': fields[12],
			'redirection_id': fields[13],
			'project_id': fields[14],
			'channel': fields[15],
			'tool': fields[16],
			'sharer_id': fields[17],
			'bot_flag': fields[18],
			'user_id': fields[19],
			'notes': fields[20]
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