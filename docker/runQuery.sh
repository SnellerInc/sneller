#!/bin/bash -e
curl -G -H "Authorization: Bearer token" --data-urlencode "database=database" --data-urlencode 'json' --data-urlencode 'query=SELECT * FROM table' 'http://localhost:9182/executeQuery'
