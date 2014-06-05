var Client = require('node-rest-client').Client;

var client = new Client();

var args = {
  data: {"topic" : "test_topic_4_dba", "a":1, "idc": "tc"},
  headers:{"Content-Type": "application/json"} 
};

client.registerMethod("http://tc-aqueducts-dev00.tc.baidu.com:8080/publish", "POST");

var cluster = require('cluster');
var workers = require('os').cpus().length;

if (cluster.isMaster) {
  for (var i = 0; i < workers; i++) {
    cluster.fork();
  }
} else {
  // process setting
  process.on('uncaughtException', function(err) {
    console.error(err);
    // process.exit(1);
  });

  var start = new Date().getTime();
  for (var i = 0; i < 100000; i++){
    client.methods.postMethod(args, function(data,response){});
  }
  var diff = new Date().getTime() - start;
  console.log(process.pid + " http restful interface consumed:" + diff);
}
