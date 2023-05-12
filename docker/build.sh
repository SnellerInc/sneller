#!/bin/sh
cd ..
for image in snellerd sdb
do
    echo Creating docker image $image
    docker build -f docker/$image.dockerfile -t snellerinc/$image:latest .
done
