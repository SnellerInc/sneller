
# Export data from sample dashboards

```
$ elasticdump --input="https://elastic:Elastic123^@search-test5-cc5q6qmgq4rhnpdhdsg2lyem6m.us-east-1.es.amazonaws.com:443/kibana_sample_data_flights" --limit=10000 --type=data --sourceOnly --output=sample_data_flights.json
```

# Exporting queries

Enable loggin in: Logs / CloudWatch Logs / Search slow logs

See [Log all the searches going through Elasticsearch](https://jolicode.com/blog/log-all-the-searches-going-through-elasticsearch)

```
PUT kibana_sample_data_flights/_settings
{
  "index.search.slowlog.threshold.query.trace": "0s",
  "index.search.slowlog.level": "trace"
}
```
