var restify = require('restify');
var config = require('config');
var kafka = require('./kafka');

var Producers = {};
config.clusters.forEach(function(v, i, a){
  Producers[v.name] = kafka.make_producer(v.broker_list);
});

function selectProducer(message) {
  var target = "default";
  try {
    if ("topic_list" == config.load_balancer) {
      config.clusters.forEach(function(v, i, a) {
        var topic = message.product + "_" + message.service + "_topic";
        if (v.topic_list.indexOf(topic) > -1) {
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
  } catch (err) {}
  return Producers[target];
}

function check(msg) {
  return true;
}

function publish(msg) {
  try {
    var message = JSON.parse(msg || null);
    var key = message.key || null;
    var topic = message.product + "_" + message.service + "_topic";
    var producer = selectProducer(message);

    if (null == message || null == message.product || null == message.service || null == producer)
      return "product && service must be provided";
    else {
      if (!check(message))
        return "please register first";

      msg.event_time = new Date().getTime();
      msg.topic = topic;

      producer.sendSync(new kafka.KeyedMessage(topic, key, msg.toString()));
      return "ok";
    }
  } catch (err) {
    return "invalid format";
  }
}

function httpPublish(req, res, next) {
  //var result = {}
  //result.response = publish(req.params.message);
  //res.send(JSON.stringify(result));

  res.send({reponse : publish(req.params.message) });
  next();
}

var cluster = require('cluster');
var workers = config.workers || require('os').cpus().length;

if (cluster.isMaster) {
  for (var i = 0; i < workers; i++) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
} else {
    // process setting
    process.on('uncaughtException', function(err) {
      console.error(err);
      // process.exit(1);
    });

    // http restify server
    var httpServer = restify.createServer();
    httpServer.use(restify.queryParser());
    httpServer.use(restify.bodyParser());
    httpServer.use(restify.conditionalRequest());

    httpServer.get('/publish', httpPublish);
    httpServer.post('/publish', httpPublish);

    httpServer.listen(config.http_port, function() {
      console.log('Process ID: ' + process.pid + ' HTTP Server listening on %s', httpServer.url);
    });

    // tcp server
    net = require('net');
    net.createServer(function (socket) {
      socket.on('data', function (data) {
        // publish
        publish(data.toString());
      });
    }).listen(8089);

    // udp server
    var dgram = require("dgram");
    var udpServer = dgram.createSocket(config.udp_version, function(msg, rinfo) {
      publish(msg);
    });

    udpServer.on('listening', function () {
      var address = udpServer.address();
      console.log('Process ID: ' + process.pid  + ' UDP Server listening on ' + address.address + ":" + address.port);
    });

    udpServer.bind(config.udp_port);
}
