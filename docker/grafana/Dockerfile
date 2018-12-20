FROM grafana/grafana:latest

COPY ./docker/grafana/datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml
COPY ./docker/grafana/dashboards.yaml /etc/grafana/provisioning/dashboards/all.yaml

# Need to replace datasource template variable with name of actual data source so auto-import
# JustWorksTM. Need to chown the file because the /var/lib/grafana/dashboards directory is
# only writable by root by default.
COPY --chown=grafana:grafana ./integrations/grafana/m3db_dashboard.json /var/lib/grafana/dashboards/m3db_dashboard.json
RUN sed -i 's/${DS_PROMETHEUS}/Prometheus/g' /var/lib/grafana/dashboards/m3db_dashboard.json
