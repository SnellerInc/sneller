#!/bin/sh
AWS_ACCESS_KEY_ID=AKI`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1`
AWS_SECRET_ACCESS_KEY=SAK`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-40} | head -n 1`
SNELLER_TOKEN=ST`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1`
> .env cat <<EOF
MINIO_IMAGE=quay.io/minio/minio:latest
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
SNELLER_BUCKET=s3://test/
SNELLER_TOKEN=$SNELLER_TOKEN
SNELLER_INDEX_KEY=4AiJmIzLvMAP8A/1XdmbuzdwDduxHdu4hVRO7//7vd8=
SNELLER_REGION=us-east-1
S3_ENDPOINT=http://minio:9100
CACHESIZE=1G
EOF
echo "Refreshed the .env file with random values."
echo
echo "Minio ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "Minio SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
echo "Sneller bearer token: $SNELLER_TOKEN"
