var java = require("java");

java.classpath.push("./lib/kafka_2.8.0-0.8.1.1.jar");
java.classpath.push("./lib/scala-library-2.8.0.jar");
java.classpath.push("./lib/log4j-1.2.17.jar");
java.classpath.push("./lib/metrics-core-2.2.0.jar");
java.classpath.push("./lib/slf4j-api-1.7.7.jar");

var KeyedMessage = java.import('kafka.producer.KeyedMessage');

function make_producer(broker_list) {

  var properties = java.newInstanceSync("java.util.Properties");
  properties.putSync("metadata.broker.list", broker_list);
  properties.putSync("request.required.acks", "0");
  properties.putSync("producer.type", "async");
  properties.putSync("serializer.class", "kafka.serializer.StringEncoder");
  
  var producerConfig = java.newInstanceSync('kafka.producer.ProducerConfig', properties);

  return java.newInstanceSync('kafka.javaapi.producer.Producer', producerConfig);
}

exports.KeyedMessage = KeyedMessage;
exports.make_producer = make_producer;
