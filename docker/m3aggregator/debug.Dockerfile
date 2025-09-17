FROM golang:1.21-alpine AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install delve debugger - use v1.23.1 which supports Go 1.21
RUN go install github.com/go-delve/delve/cmd/dlv@v1.23.1

# Use alpine with go runtime for debugging
FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install ca-certificates for HTTPS requests and glibc compatibility for delve
RUN apk add --no-cache ca-certificates libc6-compat

# Expose application ports and debug port
EXPOSE 5000/tcp 6000/tcp 6001/tcp 40000/tcp

# Copy the delve debugger
COPY --from=builder /go/bin/dlv /bin/dlv

# Copy the m3aggregator binary (built with debug symbols)
ADD ./m3aggregator /bin/m3aggregator
ADD ./config/m3aggregator.yml /etc/m3aggregator/m3aggregator.yml

# Create directory for source code mount
RUN mkdir -p /go/src/github.com/m3db/m3

# Set working directory
WORKDIR /go/src/github.com/m3db/m3

# Create startup script that starts the application with delve
RUN echo '#!/bin/sh' > /bin/start-debug.sh && \
    echo 'echo "Starting m3aggregator with delve debugger..."' >> /bin/start-debug.sh && \
    echo 'echo "Debug port: ${M3_DEBUG_PORT:-40000}"' >> /bin/start-debug.sh && \
    echo 'echo "Application will start and wait for debugger connection."' >> /bin/start-debug.sh && \
    echo 'exec /bin/dlv --listen=0.0.0.0:${M3_DEBUG_PORT:-40000} --headless=true --api-version=2 --accept-multiclient --check-go-version=false exec /bin/m3aggregator -- -f /etc/m3aggregator/m3aggregator.yml' >> /bin/start-debug.sh && \
    chmod +x /bin/start-debug.sh

ENTRYPOINT ["/bin/start-debug.sh"]
