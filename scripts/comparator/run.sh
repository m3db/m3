#!/usr/bin/env bash

set -ex
export COMPARATOR=$GOPATH/src/github.com/m3db/m3/scripts/comparator
source $COMPARATOR/docker-setup.sh

export REVISION=$(git rev-parse HEAD)

CI=${CI:-true}
RUN_ONLY=${RUN_ONLY:-false}

export QUERY_FILE=$COMPARATOR/queries.json

export END=${END:-$(date +%s)}
export START=${START:-$(( $END - 10800 ))}

function generate_dash {
	GRAFANA_PATH=$COMPARATOR/grafana

	TEMPLATE=$GRAFANA_PATH/dashboard.tmpl
	OUTPUT=$GRAFANA_PATH/dash.json.out

	GENERATOR=$GRAFANA_PATH/generate_dash.go
	
	go run $GENERATOR \
		-r=$REVISION \
		-q=$QUERY_FILE \
		-o=$OUTPUT \
		-t=$TEMPLATE \
		-s=$START \
		-e=$END
}

if [[ "$RUN_ONLY" == "false" ]]
then
	DASH_QUERY=""
	for query in "${QUERIES[@]}"
	do
		DASH_QUERY=$(echo $DASH_QUERY "$query")
	done

	echo "generating grafana dashboard"
	generate_dash

	echo "setting up containers"
	$COMPARATOR/setup.sh

	echo "setting up docker"
	setup_docker $CI
fi

comparator=$COMPARATOR/compare.out
go build -o $comparator $COMPARATOR/compare.go
function defer {
	rm $comparator
	if [[ "$CI" == "true" ]]
	then
		teardown_docker $CI
	fi
}

if [[ "$RUN_ONLY" == "false" ]]
then
	trap defer EXIT 
fi

while read query
do
	$comparator -query="$query" -s=$START -e=$END
done < $QUERY_FILE
