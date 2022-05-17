# Vectorized SQL for JSON at scale: fast, simple, schemaless

Sneller is a high-performance vectorized SQL engine for JSON that runs directly on object storage. Sneller is optimized to handle large TB-sized JSON (and more generally, semi-structured data including deeply nested structures/fields) without needing a schema to be specified upfront or dedicated ETL/ELT/indexing steps. It is particularly well suited for the rapidly growing world of [event data](https://keen.io/blog/analytics-for-hackers-how-to-think-about-event-data/) such as data from Security, Observability, Ops, Product Analytics and Sensor/IoT data pipelines. Under the hood, Sneller operates on [ion](https://amzn.github.io/ion-docs/), a structure-preserving, compact binary representation of the original JSON data.

Sneller's query performance derives from pervasive use of SIMD, specifically AVX-512 [assembly](https://github.com/SnellerInc/sneller/blob/master/vm/evalbc_amd64.s) in its 250+ core primitives. The main engine is capable of processing many lanes in parallel per core for very high processing throughput. This eliminates the need to pre-process JSON data into an alternate representation - such as search indices (Elasticsearch and variants) or columnar formats like parquet (as commonly done with SQL-based tools). Combined with the fact that Sneller's main 'API' is SQL (with JSON as the primary output format), this greatly simplifies processing pipelines built around JSON data.

Sneller extends standard SQL syntax via [PartiQL](https://partiql.org) by supporting path expressions to reference nested fields/structures in JSON. For example, the `.` operator dereferences fields within structures. In combination with normal SQL functions/operators, this makes for a far more ergonomic way to query deeply nested JSON than non-standard SQL extensions. Additionally, Sneller implements a large (and growing!) number of [built-in functions](https://docs.sneller.io/sneller-SQL.html) from other SQL implementations.

Unlike traditional data stores, Sneller completely separates storage from compute, as it is foundationally built to use object storage such as [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage), [Azure Blob](https://azure.microsoft.com/en-us/services/storage/blobs/) or [Minio](https://min.io) as its _primary_ storage layer. There are no other dependencies, such as meta-databases or key/value stores, to install, manage and maintain. This means no complex redundancy-based architecture (HA) is needed to avoid data loss. It also means that scaling Sneller up or down is as simple as adding or removing compute-only nodes.

Here is a 50000 ft overview of what is essentially the complete Sneller pipeline for JSON -- you can also read our more detailed blog post [Introducing sneller](https://www.sneller.io/post/introducing-sneller).

![Sneller SQL for JSON](https://sneller-assets.s3.amazonaws.com/SnellerForJson.png)

## Build from source

Make sure you have Golang 1.18 installed, and build as follows:

```console
$ git clone https://github.com/SnellerInc/sneller
$ cd sneller
$ go build ./...
```

## AVX-512 support

Please make sure that your CPU has [AVX-512](https://en.wikipedia.org/wiki/AVX-512#CPUs_with_AVX-512) support. Also note that AVX-512 is widely available on all major cloud providers: for [AWS](https://aws.amazon.com/intel/) we recommend c6i (Ice Lake) or r5 (Skylake), for GCP we recommend N2, M2, or C2 instance types, or either Dv4 or Ev4 families on [Azure](https://azure.microsoft.com/en-us/blog/new-general-purpose-and-memoryoptimized-azure-virtual-machines-with-intel-now-available/).

## Quick test drive 

The easiest way to try out sneller is via the (standalone) `sneller` executable. (Note: this is more of a development tool, for application use see either the Docker or Kubernetes section below.)

We've made some sample data available in the `sneller-samples` bucket, based on the (excellent) [GitHub archive](https://www.gharchive.org). Here are some queries that illustrate what you can do with Sneller on [fairly complex](https://api.github.com/events) JSON event structures containing 100+ fields.

#### simple count
```console
$ go install github.com/SnellerInc/sneller/cmd/sneller@latest
$ aws s3 cp s3://sneller-samples/gharchive-1day.ion.zst .
$ du -h gharchive-1day.ion.zst
1.3G
$ sneller -j "select count(*) from 'gharchive-1day.ion.zst'"
{"count": 3259519}
```

#### search/filter _(notice SQL string operations such as LIKE/ILIKE on a nested field `repo.name`)_

```console
$ # all repos containing 'orvalds' (case-insensitive)
$ sneller -j "SELECT DISTINCT repo.name FROM 'gharchive-1day.ion.zst' WHERE repo.name ILIKE '%orvalds%'"
{"name": "torvalds/linux"}
{"name": "jordy-torvalds/dailystack"}
{"name": "torvalds/subsurface-for-dirk"}
```

#### standard SQL aggregations/grouping

```console
$ # number of events per type
$ sneller -j "SELECT type, COUNT(*) FROM 'gharchive-1day.ion.zst' GROUP BY type ORDER BY COUNT(*) DESC"
{"type": "PushEvent", "count": 1686536}
...
{"type": "GollumEvent", "count": 7443}
```

#### query custom payloads _(see `payload.pull_request.created_at` only for `type = 'PullRequestEvent'` rows)_

```console
$ # number of pull requests that took more than 180 days
$ sneller -j "SELECT COUNT(*) FROM 'gharchive-1day.ion.zst' WHERE type = 'PullRequestEvent' AND DATE_DIFF(DAY, payload.pull_request.created_at, created_at) >= 180"
{"count": 3161}
```

#### specialized operators like `TIME_BUCKET`

```console
$ # number of events per type per hour (date histogram)
$ sneller -j "SELECT TIME_BUCKET(created_at, 3600) AS time, type, COUNT(*) FROM 'gharchive-1day.ion.zst' GROUP BY TIME_BUCKET(created_at, 3600), type"
{"time": 1641254400, "type": "PushEvent", "count": 58756}
...
{"time": 1641326400, "type": "MemberEvent", "count": 316}
```

#### combine multiple queries

```console
# fire off multiple queries simultaneously as a single (outer) select
$ sneller -j "SELECT (SELECT COUNT(*) FROM 'gharchive-1day.ion.zst') AS query0, (SELECT DISTINCT repo.name FROM 'gharchive-1day.ion.zst' WHERE repo.name ILIKE '%orvalds%') as query1" | jq
{
  "query0": 3259519,
  "query1": [
    { "name": "torvalds/linux" },
    { "name": "jordy-torvalds/dailystack" },
    { "name": "torvalds/subsurface-for-dirk" }
  ]
}
```

If you're a bit more adventurous, you can grab the 1month object (contains **80M** rows at **29GB** compressed), here as tested on a c6i.32xlarge:
```console
$ aws s3 cp s3://sneller-samples/gharchive-1month.ion.zst .
$ du -h gharchive-1month.ion.zst 
29G
$ time sneller -j "select count(*) from 'gharchive-1month.ion.zst'"
{"count": 79565989}
real    0m4.892s
user    6m41.630s
sys     0m48.016s
$ 
$ time sneller -j "SELECT DISTINCT repo.name FROM 'gharchive-1month.ion.zst' WHERE repo.name ILIKE '%orvalds%'"
{"name": "torvalds/linux"}
{"name": "jordy-torvalds/dailystack"}
...
{"name": "IHorvalds/AstralEye"}
real    0m4.940s
user    7m11.080s
sys     0m28.268s
```

## Performance

Depending on the type of query, sneller is capable of processing GB/s of data per second per core, as shown in these benchmarks (measured on a c6i.12xlarge instance on AWS with an Ice Lake CPU):

```console
$ cd vm
$ # S I N G L E   C O R E
$ GOMAXPROCS=1 go test -bench=HashAggregate
cpu: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz
BenchmarkHashAggregate/case-0                  6814            170163 ns/op          6160.59 MB/s
BenchmarkHashAggregate/case-1                  5361            217318 ns/op          4823.83 MB/s
BenchmarkHashAggregate/case-2                  5019            232081 ns/op          4516.98 MB/s
BenchmarkHashAggregate/case-3                  4232            278055 ns/op          3770.13 MB/s
PASS
ok      github.com/SnellerInc/sneller/vm        6.119s
$
$ # A L L   C O R E S
$ go test -bench=HashAggregate
cpu: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz
BenchmarkHashAggregate/case-0-48             155818              6969 ns/op        150424.92 MB/s
BenchmarkHashAggregate/case-1-48             129116              8764 ns/op        119612.84 MB/s
BenchmarkHashAggregate/case-2-48             121840              9379 ns/op        111768.43 MB/s
BenchmarkHashAggregate/case-3-48             119640              9578 ns/op        109444.06 MB/s
PASS
ok      github.com/SnellerInc/sneller/vm        5.576s
```

The following chart shows the performance for a varying numbers of cores:

![Sneller Performance](https://sneller-assets.s3.amazonaws.com/SnellerHashAggregatePerformance.png)

Sneller is capable of scaling beyond a single server and for instance a medium-sized r6i.12xlarge cluster in AWS can achieve 1TB/s
in scanning performance, even running non-trivial queries.

## Spin up stack locally
It is easiest to spin up a local stack, comprising of just Sneller as the query engine and Minio as the S3 storage layer, by using Docker. Detailed instructions can be found [here](docker/README.md) using sample data from the GitHub archive (but swapping this out for your own data is trivial). Note that this setup is a single node install and therefore no-HA.

Once you have followed the instructions, you can interact with Sneller on port `localhost:9180` via curl, eg. as per:
```sh
$ curl -G -H "Authorization: Bearer $SNELLER_TOKEN" --data-urlencode "database=gha" \
    --data-urlencode 'json' --data-urlencode 'query=SELECT COUNT(*) FROM gharchive' \
    'http://localhost:9180/executeQuery'
{"count": 2141038}
$ curl -G -H "Authorization: Bearer $SNELLER_TOKEN" --data-urlencode "database=gha" \
    --data-urlencode 'json' --data-urlencode 'query=SELECT type, COUNT(*) FROM gharchive GROUP BY type ORDER BY COUNT(*) DESC' \
    'http://localhost:9180/executeQuery'
{"type": "PushEvent", "count": 1303922}
{"type": "CreateEvent", "count": 261401}
...
{"type": "GollumEvent", "count": 4035}
{"type": "MemberEvent", "count": 2644}
```

## Spin up sneller stack in the cloud

It is also possible to use Kubernetes to spin up a sneller stack in the cloud. You can either do this on AWS using S3 for storage or in another (hybrid) cloud that supports Kubernetes and potentially using an object storage such as Minio.

See the [Sneller on Kubernetes](https://docs.sneller.io/kubernetes.html) instructions for more details and an example of how to spin this up.

## Documentation

See the `docs` directory for more information (technical nature).

## Explore further 

See [docs.sneller.io](https://docs.sneller.io/index.html) for further information:
- [Quickstart](https://docs.sneller.io/quickstart.html)
- [SQL documentation](https://docs.sneller.io/sneller-SQL.html)
- [Sneller REST API](https://docs.sneller.io/api.html)

## Development 

See docs/DEVELOPMENT.

## Contribute

Sneller is released under the AGPL-3.0 license. See the LICENSE file for more information. 
