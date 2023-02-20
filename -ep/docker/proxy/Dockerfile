FROM alpine:latest

HEALTHCHECK --start-period=5s --interval=1m --timeout=3s --retries=3 CMD wget --no-verbose --tries=1 --spider http://localhost:9200/sneller/version || exit 1

EXPOSE 9200

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

COPY docker-run.sh /usr/local/bin/
COPY proxy /usr/local/bin/
RUN chmod 755 /usr/local/bin/*
RUN mkdir -p /root && chmod 755 /root

CMD /usr/local/bin/docker-run.sh
