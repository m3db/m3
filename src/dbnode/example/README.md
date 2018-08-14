## WARNING: This is Alpha software and not intended for use until a stable release.

# Running a single node of M3DB on GCP

Setup GCP for m3db:

    1. Download necessary packages
        $ sudo apt-get install golang golint make git golang-glide
    2. Set GOPATH:
        $ export GOPATH=$HOME/code
    3. Create m3db directory
        $ mkdir -p $HOME/code/src/github.com/m3db
    4. cd into m3db directory and git clone m3db
        $ git clone https://github.com/m3db/m3
    5. Build m3db
        $ git submodule update --init --recursive
        $ glide install
        $ make services
    8. Run m3db:
        $ sudo ./bin/m3dbnode -f example/m3db-node-config.yaml
