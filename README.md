# Fast vectorized SQL for JSON at scale

Sneller is a high-performance vectorized SQL engine that runs directly on top of object storage. Sneller is optimized to handle JSON data (including deeply nested structures/fields) without requiring a schema to be specified upfront. Sneller implements PartiQL, a superset of SQL. Under the hood, Sneller operates on [ion](https://amzn.github.io/ion-docs/), a compact binary representation of the original JSON data.

Sneller is able to process multiple lanes in parallel on each core through the use of AVX-512 instrutions. This is turn alleviates the need to pre-compute all sorts of indexes or transform the data into columnar structures, which is notoriously hard for flexible nested JSON sub-structures.

Sneller extends SQL by supporting "path expressions," which dynamically dereference sub-values of composite data-types. For example, the `.` operator dereferences fields within structures, as it does in many C-family programming languages. Additionally, sneller implements a large (and growing!) number of [built-in functions from other SQL implementations](https://sneller-docs-dev.s3.amazonaws.com/sneller-SQL.html).

The following diagram shows a basic positioning of sneller -- more information can be found in the blog post [Introducing sneller](https://blog.sneller.io/introducing-sneller).

![Sneller SQL for JSON](https://github.com/frank-sneller/sneller/blob/main/sneller-sql-on-json.png)

## Build from source

Make sure you have Golang 1.18 installed, and build as follows:

```console
$ git clone https://github.com/SnellerInc/sneller
$ cd sneller
$ go build ./...
```

## Quick test drive 

The easiest way to try out sneller is via the `sneller` executable, there is some sample data available in the `sneller-samples` S3 bucket, for example using some data from the (excellent) [gharchive.org](https://www.gharchive.org):
```console
$ cd cmd/sneller
$ aws s3 cp s3://sneller-samples/gharchive-1day.ion.zst .
$ du -h gharchive-1day.ion.zst
1.3G
$ ./sneller -j "select count(*) from 'gharchive-1day.ion.zst'"
{"count": 3259519}
$
$ # all repos containing 'orvalds' (case-insensitive)
$ ./sneller -j "SELECT DISTINCT repo.name FROM 'gharchive-1day.ion.zst' WHERE repo.name ILIKE '%orvalds%'"
{"name": "torvalds/linux"}
{"name": "jordy-torvalds/dailystack"}
{"name": "torvalds/subsurface-for-dirk"}
$
$ # number of events per type
$ ./sneller -j "SELECT type, COUNT(*) FROM 'gharchive-1day.ion.zst' GROUP BY type ORDER BY COUNT(*) DESC"
{"type": "PushEvent", "count": 1686536}
...
{"type": "GollumEvent", "count": 7443}
$ 
$ # number of pull requests that took more than 180 days
$ ./sneller -j "SELECT COUNT(*) FROM 'gharchive-1day.ion.zst' WHERE type = 'PullRequestEvent' AND DATE_DIFF(DAY, payload.pull_request.created_at, created_at) >= 180"
{"count": 3161}
$
$ # number of events per type per hour (date histogram)
$ ./sneller -j "SELECT TIME_BUCKET(created_at, 3600) AS time, type, COUNT(*) FROM 'gharchive-1day.ion.zst' GROUP BY TIME_BUCKET(created_at, 3600), type"
{"time": 1641254400, "type": "PushEvent", "count": 58756}
...
{"time": 1641326400, "type": "MemberEvent", "count": 316}
```

If you want to be a bit more adventurous, you can grab the 1month object (contains 80M rows at 29GB compressed), here as tested on a c6i.36xlarge:
```console
$ aws s3 cp s3://sneller-samples/gharchive-1month.ion.zst .
$ du -h gharchive-1month.ion.zst 
29G
$ time ./sneller -j "select count(*) from 'gharchive-1month.ion.zst'"
{"count": 79565989}
real    0m5.628s
user    8m34.520s
sys     0m35.691s
$ 
$ time ./sneller -j "SELECT DISTINCT repo.name FROM 'gharchive-1month.ion.zst' WHERE repo.name ILIKE '%orvalds%'"
{"name": "torvalds/linux"}
{"name": "jordy-torvalds/dailystack"}
...
{"name": "IHorvalds/AstralEye"}
real    0m5.936s
user    9m35.244s
sys     0m17.217s
```

## Performance

Depending on the type of query, sneller is capable of processing GB/s of data per second per core, as shown in these benchmarks (measured on a c6i.12xlarge instance on AWS):

```console
$ cd vm
$ # S I N G L E   C O R E
$ GOMAXPROCS=1 go test -bench=HashAggregate                                                                     
cpu: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz
BenchmarkHashAggregate/case-0                       6814            170163 ns/op          6160.59 MB/s
BenchmarkHashAggregate/case-1                       5361            217318 ns/op          4823.83 MB/s
BenchmarkHashAggregate/case-2                       5019            232081 ns/op          4516.98 MB/s
BenchmarkHashAggregate/case-3                       4232            278055 ns/op          3770.13 MB/s
PASS
ok      github.com/SnellerInc/sneller/vm        6.119s
$
$ # A L L   C O R E S
$ go test -bench=HashAggregate
cpu: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz
BenchmarkHashAggregate/case-0-48                  155818              6969 ns/op        150424.92 MB/s
BenchmarkHashAggregate/case-1-48                  129116              8764 ns/op        119612.84 MB/s
BenchmarkHashAggregate/case-2-48                  121840              9379 ns/op        111768.43 MB/s
BenchmarkHashAggregate/case-3-48                  119640              9578 ns/op        109444.06 MB/s
PASS
ok      github.com/SnellerInc/sneller/vm        5.576s
```

The following chart shows the performance for a varying numbers of cores:

![Sneller Performance](https://sneller-assets.s3.amazonaws.com/SnellerHashAggregatePerformance.png)

sneller is capable of scaling beyond a single server and for instance a medium-sized r6i.12xlarge cluster in AWS can achieve 1TB/s
in scanning performance, even running non-trivial queries.

## Spin up stack locally

It is easiest to spin up a local stack (non-HA/single node) via `docker compose`. Make sure you have `docker` and `docker-compose` installed. Follow the instructions below.

#### Spin up sneller and minio
```console
$ cd docker
$
$ # login to ECR (Elastic Container Repository on AWS)
$ aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 671229366946.dkr.ecr.us-east-1.amazonaws.com
Login Succeeded
$
$ # create unique credentials for sneller and minio (used as S3-compatible object store)
$ ./generate-env.sh
Minio ACCESS_KEY_ID: AKxxxxxxxxxxxxxxxxxxxxx
Minio SECRET_ACCESS_KEY: SAKyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy
Sneller bearer token: STzzzzzzzzzzzzzzzzzzzz
$
$ # spin up sneller and minio
$ docker-compose up
Attaching to docker-minio-1, docker-snellerd-1
docker-snellerd-1  | run_daemon.go:90: Sneller daemon 9a3d6ae-master listening on [::]:9180
docker-minio-1     | API: http://172.18.0.3:9100  http://127.0.0.1:9100 
...
docker-minio-1     | Finished loading IAM sub-system (took 0.0s of 0.0s to load data).
```

#### Copy input data into object storage
```console
$ # fetch some sample data
$ wget https://sneller-samples.s3.amazonaws.com/gharchive-1million.json.gz
$ 
$ # create bucket for source data
$ docker run --rm --net 'sneller-network' --env-file .env \ 
  amazon/aws-cli --endpoint 'http://minio:9100' s3 mb 's3://test'
...
make_bucket: test
$
$ # copy data into object storage
$ docker run --rm --net 'sneller-network' --env-file .env \
  -v "`pwd`:/data" amazon/aws-cli --endpoint 'http://minio:9100' s3 cp '/data/gharchive-1million.json.gz' 's3://test/gharchive/'
```

#### Create table definition and sync data
```console
$ # create table definition file (with wildcard pattern for input data)
$ cat << EOF > definition.json
{ "name": "gharchive", "input": [ { "pattern": "s3://test/gharchive/*.json.gz", "format": "json.gz" } ] }
EOF
$
$ # copy the definition.json file
$ docker run --rm --net 'sneller-network' --env-file .env \
  -v "`pwd`:/data" amazon/aws-cli --endpoint 'http://minio:9100' s3 cp '/data/definition.json' 's3://test/db/gharchive/gharchive/definition.json'
$
$ # sync the data
$ docker run --rm --net 'sneller-network' --env-file .env \
  671229366946.dkr.ecr.us-east-1.amazonaws.com/sneller/sdb -v sync gharchive gharchive
...
updating table gharchive/gharchive...
table gharchive: wrote object db/gharchive/gharchive/packed-G6JF75KRUAGREYAGJIJ5NCSLSQ.ion.zst ETag "40c7ffbf758c0782930a55a8ffe43a93-1"
update of table gharchive complete
```

#### Query the data with `curl`
```console
$ # query the data (replace STzzzzzzzzzzzzzzzzzzzz with SNELLER_TOKEN from .env file!)
$ curl -s -G --data-urlencode 'query=SELECT COUNT(*) FROM gharchive' -H 'Authorization: Bearer STzzzzzzzzzzzzzzzzzzzz' --data-urlencode 'json' --data-urlencode 'database=gharchive' http://localhost:9180/executeQuery
{"count": 1000}
$
$ # yet another query: number of events per type
$ curl -s -G --data-urlencode 'query=SELECT type, COUNT(*) FROM gharchive GROUP BY type ORDER BY COUNT(*) DESC' -H 'Authorization: Bearer STzzzzzzzzzzzzzzzzzzzz' --data-urlencode 'json' --data-urlencode 'database=gharchive' http://localhost:9180/executeQuery
{"type": "PushEvent", "count": 1234}
...
{"type": "GollumEvent", "count": 567}
```

See [Sneller on Docker](https://sneller-docs-dev.s3.amazonaws.com/docker.html) for more detailed instructions.

## Spin up sneller stack in the cloud

It is also possible to use Kubernetes to spin up a sneller stack in the cloud. You can either do this on AWS using S3 for storage or in another (hybrid) cloud that support Kubernetes and potentially using an object storage as Minio.

See the [Sneller on Kubernetes](https://sneller-docs-dev.s3.amazonaws.com/kubernetes.html) instructions for more details and an example of how to spin this up.

## Documentation

See the `docs` directory for more information (technical nature).

## Explore further 

See [docs.sneller.io](https://sneller-docs-dev.s3.amazonaws.com/index.html) for further information:
- [Quickstart](https://sneller-docs-dev.s3.amazonaws.com/quickstart.html)
- [SQL documentation](https://sneller-docs-dev.s3.amazonaws.com/sneller-SQL.html)
- [Sneller REST API](https://sneller-docs-dev.s3.amazonaws.com/api.html)

## Development 

See docs/DEVELOPMENT.

## Contribute

sneller is released under the AGPL-3.0 license. See the LICENSE file for more information. 
