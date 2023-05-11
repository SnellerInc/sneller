FROM golang:1.20-alpine AS build
RUN apk add --no-cache --update gcc g++
ENV GOPATH /app
RUN mkdir -p /app/output
WORKDIR /app/oss
COPY ./ /app/oss/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildmode=exe -o /app/output/ ./cmd/snellerd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildmode=exe -o /app/output/ ./cmd/k8s-peers

FROM alpine:latest
COPY --from=build /app/output/snellerd /usr/local/bin
COPY --from=build /app/output/k8s-peers /usr/local/bin
ENTRYPOINT ["/usr/local/bin/snellerd"]
