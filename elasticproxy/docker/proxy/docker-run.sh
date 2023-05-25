#!/bin/sh
echo "Using proxy configuration ${PROXY_CONFIG_PATH:=/root/proxy-config.json}"
if [ ! -f "$PROXY_CONFIG_PATH" ]; then
    > "$PROXY_CONFIG_PATH" cat  <<HEREDOC
{
    "*": {
        "elastic": {
            "endpoint": "$ELASTIC_ENDPOINT",
            "user": "elastic",
            "password": "$ELASTIC_PASSWORD",
            "ignoreCert": $ELASTIC_IGNORE_CERT
        },
        "sneller": {
            "endpoint": "$SNELLER_ENDPOINT",
            "token": "$SNELLER_TOKEN"
        },
        "mapping": {
            "$ELASTIC_INDEX": {
                "database": "$SNELLER_DATABASE",
                "table": "$SNELLER_TABLE",
                "ignoreTotalHits": $IGNORE_TOTAL_HITS
            }
        },
        "logFolder": "$DIAG_LOG_FOLDER",
        "compareWithElastic": $DIAG_COMPARE_ELASTIC
    }
}
HEREDOC
fi
cat "$PROXY_CONFIG_PATH"

if [ ! -z "$DIAG_LOG_FOLDER" ]; then
    mkdir -p $DIAG_LOG_FOLDER
fi

ARG_CONFIG="-config $PROXY_CONFIG_PATH"
ARG_VERBOSE=""
if [ ! -z "$PROXY_VERBOSE" ]; then
    ARG_VERBOSE="-v"
fi

/usr/local/bin/proxy -endpoint "0.0.0.0:9200" $ARG_CONFIG $ARG_VERBOSE
