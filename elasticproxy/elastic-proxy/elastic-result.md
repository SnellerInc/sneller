# Structure of an Elastic Result
Each Elastic result has the following structure:
```json
{
    "timed_out": false /* bool */,
    "hits": {
        "hits": [
            {
                "_score": 1,
                "_type": "",
                "_id": "",
                "_index": "index-name",
                "_source": {
                    /* actual record */
                }
            },
            /* ... */
        ],
        "total": {
            "relation": "eq",
            "value": 12345
        },
        "max_score": 1.0
    }
    "_shards:" {
	    "successful": 1,
	    "failed": 0,
	    "skipped": 0,
	    "total": 1,
    }
    "took": 0 /* duration */
    "aggregations": {
        "agg1": {
        },
        "agg2": {
        }
    }
}
```
There are two important parts and those are the `hits` and the `aggregations`.

## Hits
The hits contain the records that match the given input query and typically hold the full source data (unless a projection was given).

## Aggregations
Aggregations are much more complex, because there are different types of aggregations and there are generally three different structures:
1. Metric aggregations typically hold only a value and is denoted by the `*metricValue` type. This typically looks something like:
   ```json
   {"value": 123.45}
   ```
1. Bucket aggregations that always return a single bucket (i.e. `filter` aggregation). These aggregations always hold the `doc_count` value for the number of items matching the bucket aggregation, but can also hold sub-aggregations and is denoted by the `bucketSingleResult` type. It holds all the sub-aggregations and the document count.
   ```json
   {
    "doc_count": 123,
    "avg_price": {"value": 123.45}
   }
   ```
1. Bucket aggregations that may return one or more buckets (i.e. `terms` aggregation). Each aggregation holds a `bucket` field that is an array of keyed single bucket responses and looks like this:
   ```json
   [
    {
     "key": "t-shirts",
     "doc_count": 123,
     "avg_price": {"value": 25}
    },
    {
     "key": "jeans",
     "doc_count": 45,
     "avg_price": {"value": 123.45}
    }
    ,
    /* ... */
   ]
   ```