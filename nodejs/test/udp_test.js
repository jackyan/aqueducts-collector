var cluster = require('cluster');
var workers = require('os').cpus().length;

if (cluster.isMaster) {
  for (var i = 0; i < workers; i++) {
    cluster.fork();
  }
} else {
  process.on('uncaughtException', function(err) {
    console.error(err);
  });

  var dgram = require('dgram');
  var client = dgram.createSocket('udp4'); // ipv4
  var message = new Buffer("{\"topic\" : \"test_topic_4_dba\", \"a\" : 1, \"idc\" : \"tc\"}");
  var start = new Date().getTime();
  for (var i = 0; i < 20000; i++){
    client.send(message, 0, message.length, 8090, "tc-aqueducts-dev00.tc.baidu.com", function(err, bytes) {
      if (err) throw err;
      // console.log('UDP message sent to ' + HOST +':'+ PORT);
      client.close();
    });
  }
  var diff = new Date().getTime() - start;
  console.log(process.pid + " udp interface consumed:" + diff);
}