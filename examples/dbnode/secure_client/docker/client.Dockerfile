FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Add m3dbclient binary
ADD ./m3db-client /bin/m3db-client
RUN chmod -R 755 /bin
