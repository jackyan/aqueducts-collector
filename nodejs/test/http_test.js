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

  var requestify = require('requestify');
  var start = new Date().getTime();
  for (var i = 0; i < 20000; i++){
    requestify.post('http://tc-aqueducts-dev00.tc:8080/publish', {
      message : "{\"topic\" : \"test_topic_4_dba\", \"a\" : 1, \"idc\" : \"tc\"}",
      dataType : 'form-url-encoded',
      headers : {
        //'Content-Length' : Buffer.byteLength(message)
      }
    }).then(function(response) {
  //    console.log(response.getBody().toString());
    });
  }
  var diff = new Date().getTime() - start;
  console.log(process.pid + " http restful interface consumed:" + diff);
}

// apache benchmark test
// dbl-aqueducts-mysql00.dbl01
// ./bin/ab -n 1000000 -c 1000 -p message.txt -T 'application/x-www-form-urlencoded'  http://tc-aqueducts-dev00.tc:8080/publish
// result: 8k qps
