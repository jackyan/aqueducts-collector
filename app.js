var restify = require('restify');
var config = require('config');
var kafka = require('./kafka');

var Producers = {};
config.clusters.forEach(function(v, i, a){
  Producers[v.name] = kafka.make_producer(v.broker_list);
});

function selectProducer(message) {
  var target = null;
  if ("topic_list" == config.load_balancer) {
    config.clusters.forEach(function(v, i, a) {
      if (v.topic_list.indexOf(message.topic) > -1) {
        target = v.name;
      }
    });
  } else {
    config.clusters.forEach(function(v, i, a) {
      if (v.idc_list.indexOf(message.idc) > -1) {
        target = v.name;
      }
    });
  }
  return Producers[target];
}

function publish(msg) {
  var message = JSON.parse(msg || null);
  var key = message.key || null;
  var topic = message.topic;
  var producer = selectProducer(message);

  if (null == message || null == topic || null == producer)
    return false;
  else {
    producer.sendSync(new kafka.KeyedMessage(topic, key, req.params.message));
    return true;
  }
}

function httpPublish(req, res, next) {
  if (publish(req.params.message))
    res.send("Wrong format");
  else
    res.send('OK\n');

  next();
}

var cluster = require('cluster');
var http = require('http');
var dgram = require("dgram");
var workers = config.workers || require('os').cpus().length;

if (cluster.isMaster) {
  for (var i = 0; i < workers; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
  } else {
    // http restify server
    var httpServer = restify.createServer();
    httpServer.use(restify.queryParser());
    httpServer.use(restify.bodyParser());
    httpServer.use(restify.conditionalRequest());

    httpServer.get('/publish', httpPublish);
    httpServer.post('/publish', httpPublish);

    httpServer.listen(config.http_port, function() {
      console.log('Process ID: ' + process.pid + ' HTTP Server listening on %s', server.url);
    });

    // udp server
    var udpServer = dgram.createSocket(config.udp_version, function(msg, rinfo) {
      publish(msg);
    });

    udpServer.on('listening', function () {
        var address = server.address();
        console.log('Process ID: ' + process.pid  + ' UDP Server listening on ' + address.address + ":" + address.port);
    });

    udpServer.bind(config.udp_port);
}
