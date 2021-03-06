var restify = require('restify');
var config = require('config');
var kafka = require('./kafka');
var rest = require('restler')

var Producers = {};
config.clusters.forEach(function(v, i, a){
  Producers[v.name] = kafka.make_producer(v.broker_list);
});

function selectProducer(message) {
  var target = "default";
  try {
    if ("topic_list" == config.load_balancer) {
      config.clusters.forEach(function(v, i, a) {
        product = message.product || message.username || message.organization
        var topic = product + "_" + message.service + "_topic";
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

function publish(msg) {
  try {
    var message = JSON.parse(msg || null);
    var key = message.key || null;
    product = message.product || message.username || message.organization
    var topic = product + "_" + message.service + "_topic";
    var producer = selectProducer(message);

    if (null == message || null == product || null == message.service || null == producer)
      return "product && service must be provided";
    else {
      message.event_time = new Date().getTime();
      message.topic = topic;

      producer.sendSync(new kafka.KeyedMessage(topic, key, JSON.stringify(message)));
      console.log(topic)
      console.log(message)
      return "ok";
    }
  } catch (err) {
    return "invalid format";
  }
}

function publishBulk(msg) {
  var bulkMsgs = msg.toString();                                                                                                                                                                    
  var lines = null;
  if (bulkMsgs.indexOf("\n") > -1) {                                                                                                                                                                
    lines = bulkMsgs.split("\n");                                                                                                                                                             
  } else {                                                                                                                                                                                             
    lines = [ bulkMsgs ] ;                                                                                                                                                                    
  }   
  lines.forEach(function(line) {
    publish(line);
  });
}

function checkToken(auth_token, params, res) {
  rest.get(config.token_api_url + "?token=" + auth_token).on('complete', function(data, response) {
    if (response.statusCode == 200) {
      res.send({response : publish(params.message)})
    } else {
      res.send({response : "worng token"})
    }
  });
}

function httpPublish(req, res, next) {
  if (token = req.headers.token) {
    checkToken(token, req.params, res)
  } else {
    res.send({response : "don't have token"})
  }
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

  /*
  // tcp server
  var net = require('net');
  var tcpServer = net.createServer(function (socket) {
      var data = '';
      socket.setEncoding('utf8');
      socket.on('data', function(chunk) {
        publishBulk(chunk);
      });

      socket.on('end', function() {
        publishBulk(data);
    });
  });

  tcpServer.listen(config.tcp_port, function() {
    var address = tcpServer.address();
    console.log('Process ID: ' + process.pid + ' TCP Server listening on ' + address.address + ":" + address.port);
  });

  // udp server
  var dgram = require("dgram");
  var udpServer = dgram.createSocket(config.udp_version, function(msg, rinfo) {
    publishBulk(msg);
  });

  udpServer.on('listening', function () {
    var address = udpServer.address();
    console.log('Process ID: ' + process.pid  + ' UDP Server listening on ' + address.address + ":" + address.port);
  });

  udpServer.bind(config.udp_port);

  // relp tcp server
  var relp = require('relp');
  var relpServer = new relp.Server({ port: '8091' });
  console.log('Process ID: ' + process.pid  + ' relp tcp  Server listening on ' +  "*:" + '8091');

  relpServer.on('message', function (message) {
    publish(message.body);
    relpServer.ack(message);
  });
  */
}

