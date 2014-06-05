1. `npm install`
1. edit ./config/default.json
1. `node start`
1. `curl -X POST -d "message={\"topic\" : \"test_topic_4_dba\", \"a\":1, \"idc\": \"tc\"}" http://localhost:8080/publish`
1. `echo "{\"topic\" : \"test_topic_4_dba\", \"a\":1, \"idc\": \"tc\"}" | nc -w 1 -u localhost 8090`
