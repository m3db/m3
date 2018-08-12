# Dockerfile for building docs is stored in a separate dir from the docs,
# otherwise the generated site will unnecessarily contain the Dockerfile

FROM python:3.5-alpine
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

WORKDIR /m3db
EXPOSE 8000

# mkdocs needs git-fast-import which was stripped from the default git package
# by default to reduce size
RUN pip install mkdocs==0.17.3 mkdocs-material==2.7.3 && \
    apk add --no-cache git-fast-import openssh-client
ENTRYPOINT [ "/bin/ash", "-c" ]
