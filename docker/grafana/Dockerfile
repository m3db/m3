FROM grafana/grafana:latest

COPY ./docker/grafana/datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml
COPY ./docker/grafana/dashboards.yaml /etc/grafana/provisioning/dashboards/all.yaml

# Need to replace datasource template variable with name of actual data source so auto-import
# JustWorksTM. Use a temporary directory to host the dashboards since the default
# directory is owned by root.
COPY ./integrations/grafana/m3db_dashboard.json /tmp/m3db_dashboard.json
RUN mkdir -p /tmp/grafana_dashboards
RUN awk '{gsub(/\${DS_PROMETHEUS}/,"Prometheus");print}' /tmp/m3db_dashboard.json > /tmp/grafana_dashboards/m3db_dashboard.json
