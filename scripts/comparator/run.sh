#!/usr/bin/env bash

set -ex
export COMPARATOR=$GOPATH/src/github.com/m3db/m3/scripts/comparator
source $COMPARATOR/docker-setup.sh

export REVISION=$(git rev-parse HEAD)

CI=${CI:-true}
RUN_ONLY=${RUN_ONLY:-false}

export QUERY_FILE=$COMPARATOR/basic_queries/queries.json
export REGRESSION_DIR=$COMPARATOR/regression_data
export GRAFANA_PATH=$COMPARATOR/grafana
export DASHBOARD=$GRAFANA_PATH/dash.json.out

export END=${END:-$(date +%s)}
export START=${START:-$(( $END - 10800 ))}
# TODO: make this a bit less hacky in the future; e.g. take from config.
export COMPARATOR_WRITE="localhost:9001"

function generate_dash {
	TEMPLATE=$GRAFANA_PATH/dashboard.tmpl

	GENERATOR=$GRAFANA_PATH/generate_dash.go
	
	go run $GENERATOR \
		-r=$REVISION \
		-q=$QUERY_FILE \
		-o=$DASHBOARD \
		-t=$TEMPLATE \
		-s=$START \
		-e=$END
}

if [[ "$RUN_ONLY" == "false" ]]
then
	if [[ ! "$CI" == "true" ]]
	then
		echo "generating grafana dashboard"
		generate_dash
	fi

	echo "setting up containers"
	$COMPARATOR/setup.sh

	echo "setting up docker"
	setup_docker $CI
fi

comparator=$COMPARATOR/compare.out
go build -o $comparator $COMPARATOR/.
function defer {
	rm $comparator
	if [[ "$CI" == "true" ]]
	then
		teardown_docker $CI
	else
		if [[ "$RUN_ONLY" == "false" ]]
		then
			rm $DASHBOARD
		fi
	fi
}

if [[ "$RUN_ONLY" == "false" ]]
then
	trap defer EXIT 
fi

# Run PromQL testdata tests
go test -v -timeout 300s -tags=compatibility -count=1 github.com/m3db/m3/src/query/test/compatibility/

# Run comparator tests AFTER PromQL testdata tests to keep seeded data, useful
# for inspecting it visually. This is necessary to do in this order as the
# PromQL testdata tests clear all seeded data as part of execution.
$comparator -input=$QUERY_FILE \
-s=$START \
-e=$END \
-comparator=$COMPARATOR_WRITE \
-regressionDir=$REGRESSION_DIR
