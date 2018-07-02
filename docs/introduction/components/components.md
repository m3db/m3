# Components

## M3Coordinator

M3Coordinator is a coordination service that sits in between Prometheus and M3DB. It acts as a bridge for both reads and writes so that users can access all of the benefits of M3DB such as long term storage and multi DC setup without migrating off of Prometheus. See [this presentation](https://schd.ws/hosted_files/cloudnativeeu2017/73/Integrating%20Long-Term%20Storage%20with%20Prometheus%20-%20CloudNativeCon%20Berlin%2C%20March%2030%2C%202017.pdf) for more on long term storage in Prometheus.
