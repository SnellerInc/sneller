#!/bin/bash
export SNELLER_REPO
export SNELLERD_PORT
export SNELLERD_INTER_PORT
export SNELLERAPI_PORT
export MINIO_IMAGE=quay.io/minio/minio:latest
export ACCESS_KEY_ID=minio
export SECRET_ACCESS_KEY=minio123
source .env

envsubst < sneller-daemon.yaml > sneller-daemon.exp.yaml
envsubst < sneller-api.yaml > sneller-api.exp.yaml
kubectl -n minio apply -f sneller-daemon.exp.yaml
kubectl -n minio apply -f sneller-api.exp.yaml
