# Dockerfile for building docs is stored in a separate dir from the docs,
# otherwise the generated site will unnecessarily contain the Dockerfile

FROM python:3.5-alpine
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

WORKDIR /m3db
EXPOSE 8000

# mkdocs needs git-fast-import which was stripped from the default git package
# by default to reduce size
RUN pip install \
    nltk==3.4.5 \
    mkdocs-material==5.5.3
RUN apk add --no-cache git-fast-import openssh-client
ENTRYPOINT [ "/bin/ash", "-c" ]
