#!/bin/sh
mkdir -p output

for CMD in proxy
do
  echo Compiling Elastic proxy: $CMD
  CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -trimpath -buildmode=exe -o $CMD/$CMD ../cmd/$CMD

  echo Creating docker image snellerinc/elastic-$CMD
  (cd $CMD; docker build -t snellerinc/elastic-$CMD:latest .)
done
