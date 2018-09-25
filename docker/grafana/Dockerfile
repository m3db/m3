FROM grafana/grafana:latest

COPY ./docker/grafana/datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml
COPY ./docker/grafana/dashboards.yaml /etc/grafana/provisioning/dashboards/all.yaml
COPY ./integrations/grafana/m3db_dashboard.json /var/lib/grafana/dashboards/m3db_dashboard.json
# Need to replace datasource template variable with name of actual data source so auto-import
# JustWorksTM. Need to switch users because the /var/lib/grafana/dashboards directory is
# only writable by root.
USER root
RUN sed -i 's/${DS_PROMETHEUS}/Prometheus/g' /var/lib/grafana/dashboards/m3db_dashboard.json
# Switch back to grafana user.
USER grafana