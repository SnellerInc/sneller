# Running integration tests

The subdirectory `docker` contains two scripts `test.sh`
and `test-teardown.sh`.

The script `test.sh` spins up local docker containers, preload
both `snellerd` and EleasticSearch with sample data,
and runs simple queries to validate if everything is fine.
(Script is quite verbose.)

Then, the integration tests present in `elastic_proxy`
module are run with this setup.

To cleanup test environment, use `test-teardown.sh`.

A sample session may look like this:

```bash
$ cd elasticproxy/docker
$ ./test.sh
# a lot of output

$ cd ../elastic_proxy
$ go test

$ cd ../docker
$ ./test-teardown.sh
```

## Prerequisites

The following programs have to be installed.

* docker
* docker-compose
* wget, curl
* jq


# Querying snellerd, ElasticSearch or Elastic Proxy

Once `./test.sh` got run, then all services running on
the machine.  It's possible to send any query to these
services using for instance `curl`. The script contains
sample queries, that can be used as a base to custom
ones.
