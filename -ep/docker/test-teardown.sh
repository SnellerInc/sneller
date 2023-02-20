#!/bin/bash

set -e

docker-compose down
for volume in docker_elastic-certs docker_elastic-data docker_minio
do
    docker volume rm -f ${volume}
done

echo "Cleaning up 'elastic-proxy'"
sudo rm -rf elastic-proxy/
