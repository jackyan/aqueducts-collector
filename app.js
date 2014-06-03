var restify = require('restify');
var config = require('config');
var kafka = require('./kafka');

var Producers = {};
config.clusters.forEach(function(v, i, a){
//  console.log('%s %s', v.name, v.broker_list);
  Producers[v.name] = kafka.make_producer(v.broker_list);
});

function selectProducer(message) {
  if ("topic_list" == config.load_balancer) {
    config.clusters.forEach(function(v, i, a) {
      if (v.topic_list.indexOf(message.topic) > -1) {
        console.log("topic_list, %s", v.name);
        return Producers[v.name];
      }
    });
  } else {
    config.clusters.forEach(function(v, i, a) {
      if (v.idc_list.indexOf(message.idc) > -1) {
        console.log("broker_list, %s", v.name);
        return Producers[v.name];
      }
    });
  }
  return null;
}

function publish(req, res, next) {
  var message = JSON.parse(req.params.message);
  var key = req.params.key || message.key || null;
  var topic = message.topic;
  var producer = selectProducer(message);

  if (null == message || null == topic || null == producer)
    res.send("Wrong format");
  else {
    producer.sendSync(new kafka.KeyedMessage(topic, key, req.params.message));
    res.send('OK\n');
  }

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

    server.listen(config.http_port, function() {
      console.log('%d listening at %s', worker.process.pid, server.url);
    });
}
