## Become a test partner

Please reach out to frank@sneller.io if you are interested in becoming a test partner of our serverless cloud offering.

# SQL for JSON at scale: fast, simple, schemaless

Sneller is a high-performance SQL engine built to analyze
petabyte-scale un-structured logs and other event data.

Here are a couple major differentiators between Sneller and other SQL solutions:

 <!-- TODO: Add link to "explaining" blog post for next topic as well -->
 - Sneller is designed to use cloud object storage as its **only** backing store.
 - Sneller's SQL VM is [implemented in AVX-512 assembly](https://sneller.io/blog/sql-vm-in-avx-512/).
   Medium-sized compute clusters provide throughput in excess of **terabytes per second**.
 - Sneller is [completely schemaless](https://sneller.io/blog/why-schemaless/).
   No more ETL-ing your data! Heterogeneous JSON data can be ingested directly.
 - Sneller uses a [hybrid approach between columnar and row-oriented data layouts](https://sneller.io/blog/zion-format/)
   to provide lightweight ingest, low storage footprint, and super fast scanning speeds.

[Sneller Cloud](https://console.sneller.io/register) gives you access to a hosted version of the Sneller SQL engine
that runs directly on data stored entirely in **your S3 buckets**.
Our cloud platform offers excellent performance and is priced at an extremely competitive \$150 **per petabyte** of data scanned.

<!-- TODO: Grafana Demo -->

## Browser Demo

You can run queries **for free** against Sneller Cloud from your browser through our [playground](https://sneller.io/playground).
We've created [a public table containing about 1 billion rows](https://sneller.io/playground) from the [GitHub archive](https://www.gharchive.org) data set.
Additionally, you can create new ephemeral tables by uploading your own JSON data (but please don't upload anything sensitive!)

The Sneller playground is also usable directly with a local http client like `curl`:

[![asciicast](https://asciinema.org/a/580308.svg)](https://asciinema.org/a/580308)

## Local Demo

[![asciicast](https://asciinema.org/a/eOjVUwlA7ZYXTGtC6PpsupR2O.svg)](https://asciinema.org/a/eOjVUwlA7ZYXTGtC6PpsupR2O)

If you have `go` installed on a machine with AVX512, you can build tables
from JSON files and run the query engine locally:

```console
$ grep -q avx512 /proc/cpuinfo && echo "yes, I have AVX512"
yes, I have AVX512
$ # install the sdb tool (make sure $GOBIN is in your $PATH)
$ go install github.com/SnellerInc/sneller/cmd/sdb@latest
$ # pack a JSON object into a table that can be queried;
$ # here we're using some github archive JSON:
$ wget https://data.gharchive.org/2015-01-01-15.json.gz
$ sdb pack -o github.zion 2015-01-01-15.json.gz
$ # run a query, using JSON as the output format:
$ sdb query -v -fmt=json "select count(*), type from read_file('github.zion') group by type"
{"type": "CreateEvent", "count": 1471}
{"type": "PushEvent", "count": 5815}
{"type": "WatchEvent", "count": 1230}
{"type": "ReleaseEvent", "count": 60}
{"type": "PullRequestEvent", "count": 474}
{"type": "IssuesEvent", "count": 545}
{"type": "ForkEvent", "count": 355}
{"type": "GollumEvent", "count": 61}
{"type": "IssueCommentEvent", "count": 844}
{"type": "DeleteEvent", "count": 260}
{"type": "PullRequestReviewCommentEvent", "count": 136}
{"type": "CommitCommentEvent", "count": 73}
{"type": "MemberEvent", "count": 25}
{"type": "PublicEvent", "count": 2}
18874368 bytes (18.000 MiB) scanned in 1.475857ms 12.5GiB/s
```

See our [SQL reference](https://sneller.io/docs/sql-reference) for more information
on the Sneller SQL dialect.

If you don't have access to a physical machine with AVX512 support,
we recommend renting a VM from one of the major cloud providers with
one of these instance families:

 - AWS: c6i, m6i, r6i
 - GCP: N2, M2, C2, C3
 - Azure: Dv4, Ev4

## Sneller Cloud

Our [cloud platform](https://sneller.io/sign-up/) simplifies the Sneller SQL
user experience by giving you instant access to thousands of CPU cores to run your queries.
Sneller Cloud also provides automatic synchronization between your source data and your
SQL tables, so you don't have any batch processes to manage in order to keep your tables
up-to-date. Our cloud solution has a simple usage-based pricing model that depends entirely
on the amount of data your queries scan. (Since Sneller Cloud doesn't store any of your
data, there are no additional storage charges.)

## Performance

Sneller is generally able to provide end-to-end scanning performance in excess of 1GB/s/core
on high-core-count machines. The core SQL engine is typically able to saturate the memory
bandwidth of the machine; generally about half of the query execution time is spent
decompressing the source data, and the other half is spent in the SQL engine itself.
Scanning performance scales linearly with the number of CPU cores available,
so for example a 1000-CPU cluster would generally provide scanning performance
in excess of 1TB/s.

The `zion` [compression format that the SQL engine consumes is "bucketized"](https://sneller.io/blog/zion-format/) so that
queries that don't touch all of the fields in the source data consume fewer cycles
during decompression. Concretely, the top-level fields in each record are hashed
into one of 16 buckets, and each of these buckets is compressed separately.
The query planner determines which fields are referenced by each query, and at
execution time only the buckets that contain fields necessary to compute the final
query result are actually decompressed. (Strictly columnar formats like Parquet
stripe data into one bucket per column, with the restriction that the columns
and their types are known in advance. Since Sneller operates on un-structured
data, our solution needed to be completely agnostic to the structure of the data itself.)

<!-- FIXME: add a link to a blog post about the zion format -->

## License

Most of Sneller is released under the AGPL-3.0 license. See the LICENSE file for more information.

The `ion`, `iguana` and `date` packages are released under the Apache 2.0 license.
See LICENSE.apache for more information.
