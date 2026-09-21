FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Expect a prebuilt static Linux binary in ./bin relative to this Dockerfile
COPY ./bin/m3consumer .

EXPOSE 9000

CMD ["./m3consumer"]
