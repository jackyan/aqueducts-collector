## install & config
1. `npm install`
1. edit ./config/default.json
1. `node start`

## usage
### http server
1. performance: 8000 qps
1. `curl -H "token: user_token" -X POST -d "message={\"product\" : \"nodejs\", \"service\": \"kafka\", \"idc\": \"tc\"}" http://localhost:8080/publish`
1. `curl -H "token: user_token" -X POST -d 'message={"product" : "nodejs", "service": "kafka", "idc": "tc"}' http://localhost:8080/publish`

### tcp server
1. performance: 
1. `echo "{\"product\" : \"nodejs\", \"service\": \"kafka\", \"idc\": \"tc\"}" | nc -w 1 -t localhost 8089`

### udp server
1. `echo "{\"product\" : \"nodejs\", \"service\": \"kafka\", \"idc\": \"tc\"}" | nc -w 1 -u localhost 8090`

