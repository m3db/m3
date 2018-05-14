FROM debian:9

# Install build tools 
RUN apt-get update
RUN apt-get -y install golint make git golang-glide curl

# Setup GO environment
ENV GOPATH=/root/code
ENV GOROOT=/usr/local/go
RUN curl -Ss -o /tmp/go1.9.1.linux-amd64.tar.gz https://storage.googleapis.com/golang/go1.9.1.linux-amd64.tar.gz
RUN cd /tmp/ && tar zxf /tmp/go1.9.1.linux-amd64.tar.gz && mv go /usr/local/go
RUN ln -s /usr/local/go/bin/go /usr/local/bin/go

# Determine if `--build-arg GITSHA` was used , if not find the GITSHA of
# HEAD and checkout that version. 
RUN git clone https://github.com/m3db/m3db /root/code/src/github.com/m3db/m3db/
ENV DEFAULT_GITSHA master
RUN DEFAULT_GITSHA=$(cd /root/code/src/github.com/m3db/m3db/ && git rev-parse HEAD)
ARG GITSHA
ENV GITSHA ${GITSHA:-$DEFAULT_GITSHA}

# Build m3dbnode binary 
RUN cd /root/code/src/github.com/m3db/m3db/ && \
    git checkout $GITSHA && \
    git submodule update --init --recursive && \
    glide install && \
    make m3dbnode-linux-amd64

ENTRYPOINT ['/root/code/src/github.com/m3db/m3db/bin/m3dbnode -f /root/code/src/github.com/m3db/m3db/example/m3db-node-config.yaml']
