var restify = require('restify');
var config = require('config');
var kafka = require('./kafka');

var Producers = {};
config.clusters.forEach(function(v, i, a){
  console.log('%s %s', v.name, v.broker_list);
//  Producers[v.name] = kafka.make_producer(v.broker_list);
});

function publish(req, res, next) {
  var topic = req.params.topic || "default";
  var key = req.params.key || null;
  var messages = req.params.messages;

  Producers['cluster1'].sendSync(new kafka.KeyedMessage(topic, key, messages));
  res.send('OK\n');
  next();
}

var cluster = require('cluster');
var http = require('http');
var workers = config.workers || require('os').cpus().length;

if (cluster.isMaster) {
  for (var i = 0; i < workers; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
  } else {
    var server = restify.createServer();
    //server.use(restify.authorizationParser());
    server.use(restify.queryParser());
    server.use(restify.bodyParser());
    server.use(restify.conditionalRequest());

    server.get('/publish', publish);
    server.post('/publish', publish);
    server.get('/publish/:topic', publish);
    server.post('/publish/:topic', publish);

    server.listen(config.http_port, function() {
      console.log('%s listening at %s', server.name, server.url);
    });
}
