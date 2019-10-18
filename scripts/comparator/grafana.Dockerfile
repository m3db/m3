FROM grafana/grafana:latest

COPY ./datasource.yaml /etc/grafana/provisioning/datasources/datasource.yaml