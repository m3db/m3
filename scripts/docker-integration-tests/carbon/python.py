import pipes
rawstring = r's=$(date +%s); curl -sSfg "http://localhost:7201/api/v1/query_range?start=$(( s - 300))&end=$s&step=10&query={__graphite0__='foo',__graphite1__='bar',__graphite2__='baz'}" | jq ".data.result[0].values[][1]==\"1\"" | grep -m 1 true'

print pipes.quote(rawstring)