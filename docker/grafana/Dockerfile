FROM grafana/grafana:latest

COPY ./docker/grafana/datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml
COPY ./docker/grafana/dashboards.yaml /etc/grafana/provisioning/dashboards/all.yaml

RUN mkdir -p /tmp/grafana_dashboards
COPY ./integrations/grafana/m3query_dashboard.json /tmp/grafana_dashboards/m3query_dashboard.json
COPY ./integrations/grafana/m3coordinator_dashboard.json /tmp/grafana_dashboards/m3coordinator_dashboard.json
COPY ./integrations/grafana/m3db_dashboard.json /tmp/grafana_dashboards/m3db_dashboard.json
COPY ./integrations/grafana/temporal_function_comparison.json /tmp/grafana_dashboards/temporal_function_comparison.json
COPY ./integrations/grafana/m3aggregator_dashboard.json /tmp/grafana_dashboards/m3aggregator_dashboard.json
COPY ./integrations/grafana/m3aggregator_end_to_end_details.json /tmp/grafana_dashboards/m3aggregator_end_to_end_details.json
COPY ./scripts/development/m3_prom_remote_stack/prom_remote_demo_dashboard.json /tmp/grafana_dashboards/prom_remote_demo_dashboard.json.json


# Need to replace datasource template variable with name of actual data source so auto-import
# JustWorksTM. Use a temporary directory to host the dashboards since the default
# directory is owned by root.
COPY ./integrations/grafana/m3db_overlaid_dashboard.json /tmp/m3db_overlaid_dashboard.json
RUN awk '{gsub(/DS_PROMETHEUS/,"Prometheus");print}' /tmp/m3db_overlaid_dashboard.json \
  | awk '{gsub(/DS_M3QUERY/,"M3Query - Prometheus");print}' > /tmp/grafana_dashboards/m3db_overlaid_dashboard.json
