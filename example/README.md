## WARNING: This is Alpha software and not intended for use until a stable release.

# Running M3DB on GCP

Setup GCP for m3db:

    1. Download necessary packages
        $ sudo apt-get install golang golint make git golang-glide
    2. Configure git
        $ git config --global user.email "test@example.com"
        $ git config --global user.name "First Last"
    3. Set GOPATH:
        $ export GOPATH=/home/<user_name>/code
    4. Create m3db directory
        $ mkdir -p /home/<user_name>/code/src/github.com/m3db
    5. cd into m3db directory and git clone m3db
        $ git clone https://github.com/m3db/m3db
    6. Checkout `braskin/static_topology` (until this is landed in master)
        $ git checkout origin/braskin/static_topology
    7. Build m3db
        $ git submodule update --init --recursive
        $ glide install
        $ make services
    8. There are three config files in this directory (`config1.yaml`, `config2.yaml`, `config3.yaml`) - you will use one config per m3db node
    9. Update config on each host to reflect the correct IP addresses of the GCP instances and the names of the m3db servers.
        - You should only need to update the IP addresses for the topology under the config section. e.g.:
            ```
            config:
              static:
                  topology:
                      shards: 64
                      hosts:
                          - host: m3db_server_1
                          listenAddress: "10.142.0.6:9000"
                          - host: m3db_server_2
                          listenAddress: "10.142.0.7:9000"
                          - host: m3db_server_3
                          listenAddress: "10.142.0.9:9000"
                  namespaces:
                      - name: metrics
                          options:
                          retention:
                          retentionPeriod: 24h
                          blockSize: 4h
                          bufferPast: 2h
            ```
    10. Run m3db:
        $ sudo ./bin/m3dbnode -f config[1-3].yaml
