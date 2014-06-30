# aqueducts-collector tcp

## usage
1. `cd src && go get && cd ..`
1. `cd bin && go build ../src/tcp2kafka.go`
1. `cd bin && nohup ./tcp2kafka ../default.json > tcp2kafka.log 2>&1 &`

## logstash client
