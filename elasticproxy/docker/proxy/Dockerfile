FROM golang:1.20-alpine AS build
RUN apk add --no-cache --update gcc g++ git
ARG SRCROOT=.
ENV GOPATH /app
RUN mkdir -p /app/output
WORKDIR /app/src

COPY ./ /app/src/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildmode=exe -o /app/output/ $SRCROOT/elasticproxy/cmd/proxy

FROM alpine:latest

HEALTHCHECK --start-period=5s --interval=1m --timeout=3s --retries=3 CMD wget --no-verbose --tries=1 --spider http://localhost:9200/sneller/version || exit 1

EXPOSE 9200

ARG SRCROOT=.
ENV PROXY_CONFIG_PATH="/root/proxy-config.json"
ENV PROXY_VERBOSE=""
ENV ELASTIC_ENDPOINT=""
ENV ELASTIC_ENDPOINT_IGNORE_CERT="false"
ENV ELASTIC_PASSWORD=""
ENV ELASTIC_INDEX=""
ENV SNELLER_ENDPOINT=""
ENV SNELLER_TOKEN=""
ENV SNELLER_DATABASE=""
ENV SNELLER_TABLE=""
ENV IGNORE_TOTAL_HITS="false"
ENV DIAG_LOG_FOLDER=""
ENV DIAG_LOG_QUERIES="false"
ENV DIAG_COMPARE_ELASTIC="false"

COPY $SRCROOT/elasticproxy/docker/proxy/docker-run.sh /usr/local/bin/
COPY --from=build /app/output/proxy /usr/local/bin

RUN mkdir -p /root && chmod 755 /root && chmod 755 /usr/local/bin/docker-run.sh /usr/local/bin/proxy

CMD /usr/local/bin/docker-run.sh
