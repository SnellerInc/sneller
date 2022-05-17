# Run Sneller on Docker
Running Sneller in a Docker environment is a great way to get to know Sneller in a non-intrusive way. It provides a complete infrastructure and uses [Minio](https://min.io/) to provide an S3-compatible object storage.

## Requirements
Sneller heavily depends on AVX-512 to achieve its high perfomance. So make sure your CPU [supports AVX-512](https://en.wikipedia.org/wiki/AVX-512#CPUs_with_AVX-512). If not, then you may want to use one of the major cloud providers (i.e. AWS) that support AVX-512 on all modern instance types. On AWS we recommend using C6i (compute optimized) or R5 (memory optimized) instance types.

 * Intel AVX-512 support
 * Docker is installed
 * Sufficient memory and storage (depends on your test-set)

Sneller caches data using `tmpfs` that is a memory-backed storage (RAM-disk). All cache data is stored in `/var/cache/sneller` and the default environment that is generated allocates 1GB of storage. You can change this amount in `.env` (after generation, but before spinning up the containers).
 
## Automatic run script
We have provided an automatic script `run.sh` that will perform all the steps (as described in the next chapter) and will execute a query on the Sneller engine.

## Manual steps

### Spin up the Docker containers
To be able to run standalone, you need to have two containers:

 1. `minio` provides the S3 compatible object storage, so you don't need an AWS account or store data in AWS S3.
 1. `snellerd` provides the actual Sneller daemon that parses, plans and executes queries.

Minio uses an *Access Key ID* and *Secret Access Key* to access to the object storage. Access to the Sneller query engine is granted using a bearer token. For security reasons, we require that you generate these variables and you can run [`./generate-env.sh`](generate-env.sh) to generate the `.env` file that holds all the relevant variables.
```sh
$ ./generate-env.sh 
Refreshed the .env file with random values.

Minio ACCESS_KEY_ID: AKxxxxxxxxxxxxxxxxxxxxx
Minio SECRET_ACCESS_KEY: SAKyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy
Sneller bearer token: STzzzzzzzzzzzzzzzzzzzz
```
In the examples we use these values, but you should replace the values with the values that you generated. Once this has been done, you're ready to spin up the containers.
```
$ docker-compose up -d
[+] Running 2/2
 ⠿ Container docker-minio-1     Started                                                                                                                                 0.7s
 ⠿ Container docker-snellerd-1  Started
```
It does expose the following ports:

 * `9100` is the S3 compatible port that is used to access the object storage.
 * `9101` is the Minio webconsole port (use the `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` for authentication).
 * `9180` is the Sneller API port that is used to execute queries.

If you prefer not to use docker-compose, you can also create and run the containers by using only the docker command.

### Add some data
Sneller uses S3 (or compatible) object storage to fetch the data. When running in AWS it's trivial to use AWS S3 for this purpose, but in this environment we are using Minio for this.

We use some sample data from the Github archive. You can choose how much data you want to ingest, but this will download the data for a single day:
```sh
$ mkdir data
$ wget -P data/ https://data.gharchive.org/2022-01-01-{0..23}.json.gz
```
or if you're feeling a bit more adventurous, the data for a single month:
```sh
$ mkdir data
$ wget -P data/ https://data.gharchive.org/2022-01-{01..31}-{0..23}.json.gz
```
Depending on your internet speed this download may take a while.

**NOTE:** In the following examples we are using the `amazon/aws-cli` docker image to copy the data. If you prefer to use the AWS CLI directly, then you can also use the CLI. Replace the endpoint `http://minio:9100` with `http://localhost:9100` in that case and make sure you set the AWS environment variables properly.

Use the following statement to create the source bucket:
```sh
$ docker run \
   --rm --net 'sneller-network' --env-file .env \
   amazon/aws-cli --endpoint 'http://minio:9100' \
   s3 mb 's3://test'
```
The `.env` file specifies that all Sneller data is stored inside the `s3://test` bucket, so if you want to use another name, then make sure you also update this file. Otherwise Sneller won't be able to find the data.

Use the following statement to copy the data to the Minio container.
```sh
$ docker run \
   --rm --net 'sneller-network' --env-file .env -v "`pwd`/data/:/data" \
   amazon/aws-cli --endpoint 'http://minio:9100' \
   s3 sync '/data' 's3://test/gharchive/'
```
### Create the table definition
Sneller is a schemaless database engine, but it does need to know which databases and tables exist. All Sneller data is stored in the `s3://test` bucket (defined in `.env`) and is always stored in the `db` folder. This folder holds the database, which holds the tables. So if we want to name the database `gha` and the table `gharchive`, then the full path is `s3://test/db/gha/gharchive`.

Now we need to create a definition for this customer table. Create a file named `definition.json` with the following content:
```json
{
    "name": "gharchive",
    "input": [
        {
            "pattern": "s3://test/gharchive/*.json.gz",
            "format": "json.gz"
        }
    ]
}
```
It specifies that the table is named `gharchive` and it will scan the `s3://test/gharchive` folder for all files with the `.json.gz` extension. Now upload this file to `s3://test/db/gha/gharchive/definition.json`:
```sh
$ docker run \
   --rm --net 'sneller-network' --env-file .env -v "`pwd`:/data" \
   amazon/aws-cli --endpoint 'http://minio:9100' \
   s3 cp '/data/definition.json' 's3://test/db/gha/gharchive/definition.json'
```
### Ingest the data
Now it's time to ingest the data. This is done using the `sdb` tool that has its own image. You can ingest the data for the table using:
```sh
$ docker run \
   --rm --net 'sneller-network' --env-file .env \
   snellerinc/sdb \
   -v sync gha gharchive
```

### Query the data
Now it's time to query the data. First we'll read the token information from the environment:
```sh
$ . .env
```
Now run a query using a simple CURL statement:
```sh
$ curl -G -H "Authorization: Bearer $SNELLER_TOKEN" \
    --data-urlencode "database=gha" \
    --data-urlencode 'json' \
    --data-urlencode 'query=SELECT COUNT(*) FROM gharchive' \
    'http://localhost:9180/executeQuery'
{"count": 2141038}
```
Or to obtain the number of items per type:
```sh
$ curl -G -H "Authorization: Bearer $SNELLER_TOKEN" \
    --data-urlencode "database=gha" \
    --data-urlencode 'json' \
    --data-urlencode 'query=SELECT type, COUNT(*) FROM gharchive GROUP BY type ORDER BY COUNT(*) DESC' \
    'http://localhost:9180/executeQuery'
{"type": "PushEvent", "count": 1303922}
{"type": "CreateEvent", "count": 261401}
{"type": "PullRequestEvent", "count": 159442}
{"type": "WatchEvent", "count": 111123}
{"type": "IssueCommentEvent", "count": 88850}
{"type": "DeleteEvent", "count": 72318}
{"type": "IssuesEvent", "count": 35029}
{"type": "ForkEvent", "count": 33686}
{"type": "PullRequestReviewEvent", "count": 29841}
{"type": "ReleaseEvent", "count": 13642}
{"type": "CommitCommentEvent", "count": 10727}
{"type": "PullRequestReviewCommentEvent", "count": 9020}
{"type": "PublicEvent", "count": 5358}
{"type": "GollumEvent", "count": 4035}
{"type": "MemberEvent", "count": 2644}
```

### Adding more data
If you have more data, then simply add it to the Minio source container and run the `sdb sync` again. It will ingest the new data and it will be available for processing when the synchronization has completed.
