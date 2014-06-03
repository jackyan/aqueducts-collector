1. `npm install java restify config`
1. `curl -X POST -d 'topic=nodejs_test_topic&messages={"a",1}' http://localhost:8080/publish`
1. `curl -X POST -d 'messages={"a",1}' http://localhost:8080/publish/nodejs_test_topic`
