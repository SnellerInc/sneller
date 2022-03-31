#!/bin/sh
mkdir -p output
for binary in sdb snellerd k8s-peers
do
    echo Compiling $binary
    CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -trimpath -buildmode=exe -o "./output/$binary" "../cmd/$binary"
done
for image in snellerd sdb
do
    echo Creating docker image $image
    docker build -f $image.dockerfile -t sneller/$image:latest .
done
