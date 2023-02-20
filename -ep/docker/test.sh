#!/bin/bash
set -e
if [ -f .env ]; then
    . .env
else
    echo -n "Generating secrets..."
    AWS_REGION=us-east-1
    AWS_ACCESS_KEY_ID=AKI`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1`
    AWS_SECRET_ACCESS_KEY=SAK`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-40} | head -n 1`
    SNELLER_ENDPOINT=http://snellerd:9180/
    SNELLER_BUCKET=s3://test
    SNELLER_TOKEN=ST`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1`
    SNELLER_INDEX_KEY=4AiJmIzLvMAP8A/1XdmbuzdwDduxHdu4hVRO7//7vd8=
    SNELLER_REGION=us-east-1
    SNELLER_DATABASE=test
    SNELLER_TABLE1=sample_flights
    SNELLER_TABLE2=news
    ELASTIC_ENDPOINT=http://elastic:9200
    ELASTIC_PASSWORD=EP`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1`
    ELASTIC_INDEX1=kibana_sample_data_flights
    ELASTIC_INDEX2=news
    DOCKER_UID=$(id -u)
    DOCKER_GID=$(id -g)
    S3_ENDPOINT=http://minio:9100
    CACHESIZE=1G
    DIAG_LOG_FOLDER=/var/log/proxy
    > .env cat <<EOF
    AWS_REGION=$AWS_REGION
    AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
    SNELLER_ENDPOINT=$SNELLER_ENDPOINT
    SNELLER_BUCKET=$SNELLER_BUCKET
    SNELLER_TOKEN=$SNELLER_TOKEN
    SNELLER_INDEX_KEY=$SNELLER_INDEX_KEY
    SNELLER_REGION=$SNELLER_REGION
    SNELLER_DATABASE=$SNELLER_DATABASE
    SNELLER_TABLE1=$SNELLER_TABLE1
    SNELLER_TABLE2=$SNELLER_TABLE2
    ELASTIC_ENDPOINT=$ELASTIC_ENDPOINT
    ELASTIC_PASSWORD=$ELASTIC_PASSWORD
    ELASTIC_INDEX1=$ELASTIC_INDEX1
    ELASTIC_INDEX2=$ELASTIC_INDEX2
    DOCKER_UID=$DOCKER_UID
    DOCKER_GID=$DOCKER_GID
    S3_ENDPOINT=$S3_ENDPOINT
    CACHESIZE=$CACHESIZE
    DIAG_LOG_FOLDER=$DIAG_LOG_FOLDER
EOF
    echo "Created the .env file with random values."
    echo
    echo "Minio ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
    echo "Minio SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
    echo "Sneller bearer token: $SNELLER_TOKEN"
    echo "Elastic password: $ELASTIC_PASSWORD"
fi

# Build Docker image
./build-docker.sh

# Write out the Elastic Proxy configuration
> proxy-config.json cat <<HEREDOC
{
    "*": {
        "elastic": {
            "endpoint": "$ELASTIC_ENDPOINT",
            "user": "elastic",
            "password": "$ELASTIC_PASSWORD"
        },
        "sneller": {
            "endpoint": "$SNELLER_ENDPOINT",
            "token": "$SNELLER_TOKEN"
        },
        "mapping": {
            "$ELASTIC_INDEX1": {
                "database": "$SNELLER_DATABASE",
                "table": "$SNELLER_TABLE1",
                "ignoreTotalHits": true
            },
            "$ELASTIC_INDEX2": {
                "database": "$SNELLER_DATABASE",
                "table": "$SNELLER_TABLE2",
                "ignoreTotalHits": false
            }
        },
        "logFolder": "$DIAG_LOG_FOLDER",
        "compareWithElastic": false
    }
}
HEREDOC

# Start the Docker images
docker-compose up -d

# Create the test bucket
echo "Creating test bucket"
if docker run \
    --rm --net sneller-network --env-file .env \
    amazon/aws-cli --endpoint http://minio:9100 \
    s3 mb "s3://test"
then
    echo "Test bucket created"
else
    exit 5
fi

# Download the Sample flight data (requires S3 access)
mkdir -p flights
wget -c -P flights/ https://sneller-download.s3.amazonaws.com/sample_data_flights.json.gz

# Copy the data to the test bucket
if docker run \
    --rm --net sneller-network --env-file .env -v "$(pwd)/flights/:/data/" \
    amazon/aws-cli --endpoint http://minio:9100 \
    s3 sync /data s3://test/source/$SNELLER_DATABASE/$SNELLER_TABLE1/
then
    echo "Copied flight data to Minio"
else
    exit 6
fi

mkdir -p news
wget -c -P news/ https://sneller-download.s3.amazonaws.com/news.json.gz

# Copy the data to the test bucket
if docker run \
    --rm --net sneller-network --env-file .env -v "$(pwd)/news/:/data/" \
    amazon/aws-cli --endpoint http://minio:9100 \
    s3 sync /data s3://test/source/$SNELLER_DATABASE/$SNELLER_TABLE2/
then
    echo "Copied news data to Minio"
else
    exit 6
fi

# Create table definition in Minio bucket
TEMPFILE=$(mktemp)
cat > $TEMPFILE <<HEREDOC
{
    "name": "$SNELLER_TABLE1",
    "input": [
        {
            "pattern": "s3://test/source/$SNELLER_DATABASE/$SNELLER_TABLE1/*.json.gz",
            "format": "json.gz",
            "hints": {
                "OriginLocation.lat": "number",
                "OriginLocation.lon": "number",
                "DestLocation.lat": "number",
                "DestLocation.lon": "number"
            }
        }
    ]
}
HEREDOC
if docker run \
    --rm --net sneller-network --env-file .env -v "$TEMPFILE:/data/definition.json" \
    amazon/aws-cli --endpoint http://minio:9100 \
    s3 cp /data/definition.json s3://test/db/$SNELLER_DATABASE/$SNELLER_TABLE1/definition.json
then
    rm $TEMPFILE
else
    rm $TEMPFILE
    exit 7
fi

# Create table definition in Minio bucket
TEMPFILE=$(mktemp)
cat > $TEMPFILE <<HEREDOC
{
    "name": "$SNELLER_TABLE2",
    "input": [
        {
            "pattern": "s3://test/source/$SNELLER_DATABASE/$SNELLER_TABLE2/*.json.gz",
            "format": "json.gz"
        }
    ]
}
HEREDOC
if docker run \
    --rm --net sneller-network --env-file .env -v "$TEMPFILE:/data/definition.json" \
    amazon/aws-cli --endpoint http://minio:9100 \
    s3 cp /data/definition.json s3://test/db/$SNELLER_DATABASE/$SNELLER_TABLE2/definition.json
then
    rm $TEMPFILE
else
    rm $TEMPFILE
    exit 7
fi

# Ingest data into Sneller
echo "Ingesting data..."
for t in $SNELLER_TABLE1 $SNELLER_TABLE2
do
    if docker run --rm --net sneller-network --env-file .env snellerinc/sdb -v sync $SNELLER_DATABASE $t
    then
	echo "Table $t ingested"
    else
	exit 8
    fi
done

# Query the number of items via Sneller
echo "Obtaining counts via Sneller..."
for t in $SNELLER_TABLE1 $SNELLER_TABLE2; do
    echo Table $t
    curl -s -G -H "Authorization: Bearer $SNELLER_TOKEN" \
        --data-urlencode "database=$SNELLER_DATABASE" \
        --data-urlencode "json" \
        --data-urlencode "query=SELECT COUNT(*) FROM $t" \
        'http://localhost:9180/executeQuery'
done

# Obtain the number of items via the Elastic proxy
echo "Obtaining count via Elastic proxy"
for i in $ELASTIC_INDEX1 $ELASTIC_INDEX2; do
    curl -s -u "elastic:$ELASTIC_PASSWORD" "http://127.0.0.1:9243/$i/_count"
done

# Wait a while to make sure Elastic is ready
echo "Waiting for Elastic to get ready"
sleep 10

# Setup the index
echo "Configure mapping for indexes"
curl -s -u "elastic:$ELASTIC_PASSWORD" -X PUT "http://127.0.0.1:9200/$ELASTIC_INDEX1/" -H 'Content-Type: application/json' --data-binary @flightdata-settings.json; echo; sleep 1
curl -s -u "elastic:$ELASTIC_PASSWORD" -X PUT "http://127.0.0.1:9200/$ELASTIC_INDEX2/" -H 'Content-Type: application/json' --data-binary @news-settings.json; echo; sleep 1

# Ingest data into Elastic
for f in `find flights -name '*.json.gz' -type f`; do
    echo "Ingesting: $f"
    zcat $f | \
        sed "/^\s*$/d;s/^/{\"index\": {}}\n/" | \
        curl -s -u "elastic:$ELASTIC_PASSWORD" -X POST "http://127.0.0.1:9200/$ELASTIC_INDEX1/_bulk" -H "Content-Type:application/x-ndjson" --data-binary @- > /dev/null
done

# Ingest data into Elastic
for f in `find news -name '*.json.gz' -type f`; do
    echo "Ingesting: $f"
    zcat $f | split -l 1000 -
    for ff in x??; do
        cat $ff | \
            sed "/^\s*$/d;s/^/{\"index\": {}}\n/" | \
            curl -s -u "elastic:$ELASTIC_PASSWORD" -X POST "http://127.0.0.1:9200/$ELASTIC_INDEX2/_bulk" -H "Content-Type:application/x-ndjson" --data-binary @- > /dev/null
        rm $ff
    done
done

# Wait a while to make sure Elastic finished ingesting
sleep 3

# Obtain the number of items via Elastic
curl -s -u "elastic:$ELASTIC_PASSWORD" "http://127.0.0.1:9200/${ELASTIC_INDEX1}/_count"; echo
curl -s -u "elastic:$ELASTIC_PASSWORD" "http://127.0.0.1:9200/${ELASTIC_INDEX2}/_count"; echo

# Import the dashboard
echo "Importing dashboard"
for port in 5601 6601; do
    curl -s -u "elastic:$ELASTIC_PASSWORD" -X POST -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
        "http://127.0.0.1:$port/api/kibana/dashboards/import" --data-binary @dashboard.json > /dev/null
done

echo "Done"
