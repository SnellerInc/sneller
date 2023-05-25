#!/bin/sh
cd ../..
echo Creating docker image snellerinc/elasticproxy
docker build -f elasticproxy/docker/proxy/Dockerfile -t snellerinc/elasticproxy:latest .
