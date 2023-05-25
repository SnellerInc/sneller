# Converting Elastic queries into Sneller SQL
Elastic queries are JSON objects that have a lot of functionality and options. Most basic queries can be translated into Sneller SQL, but it sometimes requires multiple queries.

## Filter queries
Elastic supports a wide variety of query filters to reduce the amount of data that needs to be processed. The Elastic Proxy currently supports a subset of the queries and will throw an error if an unsupported query filter is used.

Effectively the query filter defines the `WHERE` clause of the SQL statement. It's important to filter the data to reduce the amount of data that needs to be scanned. Elastic also supports a Lucene-style query-string that is also supported.

## Aggregations
Elasticsearch organizes aggregations into three categories:

 * [Metric](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics.html) aggregations that calculate metrics, such as a sum or average, from field values.
 * [Bucket](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket.html) aggregations that group documents into buckets, also called bins, based on field values, ranges, or other criteria.
 * [Pipeline](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline.html) aggregations that take input from other aggregations instead of documents or fields.

Pipeline aggregations are currently not supported, so we'll focus on bucket and metric aggregations.

### Bucket aggregations
There are a lot of bucket aggregations in Elastic, but currently only the following aggregations are supported:

 * [Filter](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filter-aggregation.html) and [Filters](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filters-aggregation.html) aggregations.
 * [Terms](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html) and [Multi terms](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-multi-terms-aggregation.html) aggregations.

#### Filter and Filters aggregation
These aggregations result in a single bucket and create a bucket that contains the result of the aggregation on the filtered data.
```json
{
  "count": 0,
  "aggs": {
    "bucketTShirts": {
      "filter": { "term": { "type": "t-shirt" } }
    }
  }
}
```
translates to:
```sql
SELECT COUNT(*) FROM "table" WHERE "type" = 't-shirt'
```

The *filters* aggregation is functionally identical to the *filter* aggregation, but it creates a bucket for each filter, so:
```json
{
  "size": 0,
  "aggs" : {
    "messages" : {
      "filters" : {
        "filters" : {
          "errors" :   { "match" : { "body" : "error"   }},
          "warnings" : { "match" : { "body" : "warning" }}
        }
      }
    }
  }
}
```
translates to two SQL queries:
```sql
SELECT COUNT(*) FROM "table" WHERE "body" = 'error'
SELECT COUNT(*) FROM "table" WHERE "body" = 'warning'
```

#### Terms and Multi terms aggregations
Terms bucket aggregations are essentially the `GROUP BY` functionality. The most common bucket aggregation is a *terms* aggregation that returns a list of buckets for each value in the `GROUP BY`, so
```json
{
  "count": 0,
  "aggs": {
    "bucketOrigin": {
      "terms": { "field": "OriginCountry" }
    }
  }
}
```
translates to:
```sql
SELECT "OriginCountry", COUNT(*) FROM "table" GROUP BY "OriginCountry"
```
Note that Elastic always returns a document count for each bucket, so we always need to include `COUNT(*)` in the projection. The *multi terms* aggregation is functionally identical, but it allows to specify multiple fields to group by.

Bucket aggregations can be nested, so it's valid to use this:
```json
{
  "count": 0,
  "aggs": {
    "bucketOrigin": {
      "terms": { "field": "OriginCountry" },
      "aggs": {
        "bucketDest": {
          "terms": { "field": "DestCountry" }
        }
      }
    }
  }
}
```
This translates to the following two queries:
```sql
SELECT "OriginCountry", COUNT(*) FROM "table" GROUP BY "OriginCountry"
SELECT "OriginCountry", "DestCountry", COUNT(*) FROM "table" GROUP BY "OriginCountry", "DestCountry"
```
Note that this first query is obsolete, because the `OriginCountry` elements and the count per `OriginCountry` can be calculated during postprocessing, based on the second query. We'll get to these optimizations later.

Instead of using a nested *terms* aggregation, it would also have been possible to use a single *multi terms* aggregation instead. The difference is subtle, but the *multi terms* aggregation returns a single array of buckets. The nested *terms* aggregation also returns a nested array of buckets.

### Metric aggregations
There are a lot of metric aggregations in Elastic, but currently only the following aggregations are supported:

 * [Min](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html), [Max](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html) and [Avg](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html) return the min/max/average value.
 * [Value count](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html) returns the number of values. Note that this may be different than the `doc_count` value, because the *value count* counts the number of times that the value was actually in the data. If a field doesn't exist in a particular record, then the record is counted in `doc_count`, but the record won't increase the *value count*.
 * [Cardinality](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html) returns the number of *unique* items.

These metrics aggregations can be translated into SQL using the basic SQL functions: `MIN(field)` (min), `MAX(field)` (max), `AVG(field)` (avg), `COUNT(field)` (value count) and `COUNT(DISTINCT field)` (cardinality). Metric aggregations can be used as a top-level aggregation, such as:
```json
{
  "count": 0,
  "aggs": {
    "minPrice": { "min": { "field": "price" } },
    "avgPrice": { "avg": { "field": "price" } },
    "maxPrice": { "max": { "field": "price" } }
  }
}
```
translates to:
```sql
SELECT COUNT(*), MIN(price), AVG(price), MAX(price) FROM "table"
```
Note that `COUNT(*)` is always added to return the `doc_count` per bucket.

Metrics aggregations can be used as a nested aggregation, but it's not possible to nest an aggregation under a metrics aggregation. When nested, the translation is a bit different:
```json
{
  "count": 0,
  "aggs": {
    "bucketOrigin": {
      "terms": { "field": "product" },
      "aggs": {
        "minPrice": { "min": { "field": "price" } },
        "avgPrice": { "avg": { "field": "price" } },
        "maxPrice": { "max": { "field": "price" } }
      }
    }
  }
}
```
translates to:
```sql
SELECT "product", COUNT(*), MIN(price), AVG(price), MAX(price) FROM "table" GROUP BY "product"
```

Metric aggregations can be added to the bucket aggregation query, so it's fairly easy to implement.

#### Cardinality aggregation
The cardinality aggregation using `COUNT(DISTINCT field)` and (for now) can't be combined with other aggregations. Although the Sneller VM supports multiple hash-sets, it's currently not able to combine them with other metrics. So the following SQL query will return an error:
```sql
SELECT COUNT(*), COUNT(DISTINCT "price") FROM "table" GROUP BY "product"
```
For now, this is solved by creating two queries:
```sql
SELECT COUNT(*) FROM "table" GROUP BY "product"
SELECT COUNT(DISTINCT "price") FROM "table" GROUP BY "product"
```

# Optimizing query execution
The basic Elastic proxy translates all aggregations into separate queries. Consider the following Elastic query:
```json
{
  "count": 0,
  "aggs": {
    "categories": {
      "terms": { "field": "category" },
      "avgPrice": { "avg": { "field": "price" } },
      "aggs": {
        "products": {
          "terms": { "field": "product" },
          "aggs": {
            "minPrice": { "min": { "field": "price" } },
            "avgPrice": { "avg": { "field": "price" } },
            "maxPrice": { "max": { "field": "price" } },
            "prices": { "cardinality": { "field": "price" } }
          }
        }
      }
    }
  }
}
```
This translates to the following queries:
```sql
SELECT "category", COUNT(*)                           FROM "table" GROUP BY "category"
SELECT "category", AVG("price")                       FROM "table" GROUP BY "category"
SELECT "category", "product", COUNT(*)                FROM "table" GROUP BY "category", "product"
SELECT "category", "product", MIN("price")            FROM "table" GROUP BY "category", "product"
SELECT "category", "product", AVG("price")            FROM "table" GROUP BY "category", "product"
SELECT "category", "product", MAX("price")            FROM "table" GROUP BY "category", "product"
SELECT "category", "product", COUNT(DISTINCT "price") FROM "table" GROUP BY "category", "product"
```
Because most operators can be combined, this can be reduced to the following three queries:
```sql
SELECT "category", COUNT(*), AVG("price") FROM "table" GROUP BY "category"
SELECT "category", "product", COUNT(*), MIN("price"), AVG("price"), MAX("price") FROM "table" GROUP BY "category", "product"
SELECT "category", "product", COUNT(DISTINCT "price") FROM "table" GROUP BY "category", "product"
```
If the average price at the category level isn't required, then the first query can be completely eliminated and the `doc_count` per category could be determined from the output of the second aggregation.

The Sneller SQL engine is more efficient when all queries are executed in a single roundtrip. Instead of sending three separate queries, it will send the following query:
```sql
SELECT
 (SELECT "category", COUNT(*), AVG("price") FROM "table" GROUP BY "category") AS "q1",
 (SELECT "category", "product", COUNT(*), MIN("price"), AVG("price"), MAX("price") FROM "table" GROUP BY "category", "product") AS "q2",
 (SELECT "category", "product", COUNT(DISTINCT "price") FROM "table" GROUP BY "category", "product") AS "q3"
```
Another advantage of this approach is that the query runs with the same set of data during each query. When the queries are fired sequentially, then data might have been changed between two queries and give inconsistent results.

# Combining SQL results back to Elastic responses
The SQL results return all the required data, but it's not in the format that is used by the Elastic Proxy. The returned PartiQL data needs to be translated back into an Elastic response. To make sure that this is possible, all the SQL queries are annotated, so the results can be related to the original Elastic query.

Instead of using the following queries:
```sql
SELECT
 (SELECT "category", COUNT(*), AVG("price") FROM "table" GROUP BY "category") AS "q1",
 (SELECT "category", "product", COUNT(*), MIN("price"), AVG("price"), MAX("price") FROM "table" GROUP BY "category", "product") AS "q2",
 (SELECT "category", "product", COUNT(DISTINCT "price") FROM "table" GROUP BY "category", "product") AS "q3"
```
It will be sent like this:
```sql
SELECT
(
  SELECT "category" AS "$key:categories%0",
         COUNT(*) AS "doc_count",
         AVG("price") AS "avgPrice"
  FROM "table"
  GROUP BY "category"
) AS "$bucket:categories%0",
(
  SELECT "category" AS "$key:categories%0",
         "product" AS "$key:categories:products%0",
         COUNT(*) AS "doc_count",
         MIN("price") AS "minPrice",
         AVG("price") AS "avgPrice",
         MAX("price") AS "maxPrice"
  FROM "table"
  GROUP BY "category", "product"
) AS "$bucket:categories:products%0",
(
  SELECT "category" AS "$key:categories%0",
         "product" AS "$key:categories:products%0",
         COUNT(DISTINCT "price") AS "prices"
  FROM "table"
  GROUP BY "category", "product"
) AS "$bucket:categories:products%1"
```
Each query runs for a particular bucket aggregation, but a bucket aggregation can run multiple queries. Each `SELECT` statement is assigned the name `$bucket:<aggName>[:<aggName>]*%<index>`. If a bucket aggregation uses multiple queries, then they only differ by the index number.

The first step is to combine all results per bucket aggregation. In the example above, the data of `$bucket:categories:products%0` and `$bucket:categories:products%0` can be merged into a single "table". These tables are merged based on the `$key:<column>%<index>` values.

The second step is to fill produce "missing" data. The following Elastic query:
```json
{
  "count": 0,
  "aggs": {
    "categories": {
      "terms": { "field": "category" },
      "aggs": {
        "products": {
          "terms": { "field": "product" }
        }
      }
    }
  }
}
```
Translates to the following SQL query:
```sql
SELECT
(
  SELECT "category" AS "$key:categories%0",
         "product" AS "$key:categories:products%0",
         COUNT(*) AS "doc_count"
  FROM "table"
  GROUP BY "category", "product"
) AS "$bucket:categories:products%0"
```
The calculation of the `COUNT(*)` for the top-level aggregation was optimized, because it can be calculated from the nested query. So if the result processing detects a nested bucket *without* results for the bucket above, it will bottom-up produce the top-level data.

The last step is to convert the actual bucket data into the Elastic JSON response by combining the proper buckets and nested buckets.