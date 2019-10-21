#!/usr/bin/env bash

set -ex
COMPARATOR=$GOPATH/src/github.com/m3db/m3/scripts/comparator
source $COMPARATOR/docker-setup.sh
source $COMPARATOR/grafana/generate-dash.sh
CI=${CI:-true}

QUERIES=(
	'rate(quail[5m]):15s' 
	'rate(quail[5m]):15s' 
)
	# 'quail:5m'
	# 'quail:1m'

DASH_QUERY=""
for query in "${QUERIES[@]}"
do
	DASH_QUERY=$(echo $DASH_QUERY $query)
done

echo "generating grafana dashboard"
generate_dash $DASH_QUERY

echo "setting up containers"
$COMPARATOR/setup.sh

echo "setting up docker"
setup_docker $CI

comparator=$COMPARATOR/compare.out
go build -o $comparator $COMPARATOR/compare.go
function defer {
	rm $comparator
	if [[ "$CI" == "true" ]]
	then
		teardown_docker $CI
	fi
}
trap defer EXIT

for query in "${QUERIES[@]}"
do
	$comparator -query=$query
done
