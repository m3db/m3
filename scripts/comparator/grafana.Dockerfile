FROM grafana/grafana:latest

COPY ./datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml
COPY ./dashboards.yaml /etc/grafana/provisioning/dashboards/all.yaml

# RUN mkdir -p /tmp/grafana_dashboards
# COPY ./integrations/grafana/m3query_dashboard.json /tmp/grafana_dashboards/m3query_dashboard.json
# COPY ./integrations/grafana/m3coordinator_dashboard.json /tmp/grafana_dashboards/m3coordinator_dashboard.json
# COPY ./integrations/grafana/m3db_dashboard.json /tmp/grafana_dashboards/m3db_dashboard.json
# COPY ./integrations/grafana/temporal_function_comparison.json /tmp/grafana_dashboards/temporal_function_comparison.json