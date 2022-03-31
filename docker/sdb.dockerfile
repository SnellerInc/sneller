FROM alpine:latest
COPY ./output/sdb /usr/local/bin
ENTRYPOINT ["/usr/local/bin/sdb"]
