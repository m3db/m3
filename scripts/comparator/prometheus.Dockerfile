# The config is baked in rather than bind-mounted: in CI the compose commands
# run against a docker-in-docker sidecar that cannot see the checkout, so a
# bind mount of prometheus.yml resolves to an empty directory there.
FROM prom/prometheus:v2.31.2
COPY prometheus.yml /etc/prometheus/prometheus.yml
