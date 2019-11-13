FROM grafana/grafana:latest

COPY ./datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml
COPY ./dashboards.yaml /etc/grafana/provisioning/dashboards/all.yaml

RUN mkdir -p /tmp/grafana_dashboards
COPY ./dash.json.out /tmp/grafana_dashboards/dashboard.json
