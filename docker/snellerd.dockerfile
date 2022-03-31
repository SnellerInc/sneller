FROM alpine:latest
COPY ./output/snellerd /usr/local/bin
COPY ./output/k8s-peers /usr/local/bin
ENTRYPOINT ["/usr/local/bin/snellerd"]
