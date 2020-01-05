#https://www.m3db.io/openapi/

per rob https://m3db.slack.com/archives/CKARJDKT2/p1577442957085600
make it like https://www.vskills.in/certification/tutorial/big-data/apache-cassandra/nodetool/

try to get the curl's and operational tasks from this page into a tool
https://m3db.github.io/m3/how_to/kubernetes/

^Cbmcqueen-mn2:m3db bmcqueen$ kubectl port-forward svc/m3dbnode-persistent-cluster 9003

bmcqueen-mn2:m3db bmcqueen$ curl -v localhost:9003/health
*   Trying 127.0.0.1...
* TCP_NODELAY set
* Connected to localhost (127.0.0.1) port 9003 (#0)
> GET /health HTTP/1.1
> Host: localhost:9003
> User-Agent: curl/7.64.1
> Accept: */*
> 
< HTTP/1.1 200 OK
< Content-Type: application/json
< Date: Sat, 04 Jan 2020 03:42:30 GMT
< Content-Length: 26
< 
{"ok":true,"status":"up"}
* Connection #0 to host localhost left intact
* Closing connection 0
bmcqueen-mn2:m3db bmcqueen$ curl -v localhost:9003/health


initialize placement
list placements

#https://m3db.github.io/m3/operational_guide/namespace_configuration/
#https://www.m3db.io/openapi/#tag/M3DB-Namespace
list namespaces
creating a namespace
delete namespace

db placements
#https://m3db.github.io/m3/operational_guide/placement_configuration/#removing-a-node
#https://www.m3db.io/openapi/#tag/M3DB-Placement
adding a node
removing a node
replacing a node
