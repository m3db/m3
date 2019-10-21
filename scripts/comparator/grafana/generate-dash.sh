#!/usr/bin/env bash

GRAFANA_PATH=$GOPATH/src/github.com/m3db/m3/scripts/comparator/grafana
GENERATED=$GRAFANA_PATH/dash.json
FRAGMENT=$GRAFANA_PATH/m3db_overlaid_dashboard_fragment
REVISION=$(git rev-parse HEAD)

function generate_dash {
  INIT=2
  ID=$INIT
  QUERIES=$@
  cat $GRAFANA_PATH/m3db_overlaid_dashboard_head>$GENERATED
  for Q in $QUERIES
  do
    if [ ! $ID -eq $INIT ]
    then
      echo ",">>$GENERATED
    fi

    QUERY=$(echo $Q | cut -d ":" -f1)
    INTERVAL=$(echo $Q | cut -d ":" -f2)

    awk '{gsub(/\${QUERY}/,"'$QUERY'");print}' $FRAGMENT |
    awk '{gsub(/\${INTERVAL}/,"'$INTERVAL'");print}' |
    awk '{gsub(/\${ID}/,"'$ID'");print}'>>$GENERATED
    
    ID=$(( $ID + 1 ))
  done

  awk '{gsub(/\${REVISION}/,"'$REVISION'");print}' \
  $GRAFANA_PATH/m3db_overlaid_dashboard_tail>>$GENERATED
}
