#!/bin/bash
docker-compose down
rm -rf flights news proxy-config.json elastic-proxy minio output proxy/proxy
for v in docker_elastic-certs docker_elastic-data docker_minio; do docker volume rm $v; done
