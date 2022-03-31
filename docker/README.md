# Run Sneller on Docker
Running Sneller in a Docker environment is a great way to get to know Sneller in a non-intrusive way. It provides a complete infrastructure and uses [Minio](https://min.io/) to provide an S3-compatible object storage.

## Requirements
Sneller heavily depends on AVX-512 to achieve its high perfomance. So make sure your CPU [supports AVX-512](https://en.wikipedia.org/wiki/AVX-512#CPUs_with_AVX-512). If not, then you may want to use one of the major cloud providers (i.e. AWS) that support AVX-512 on all modern instance types. On AWS we recommend using C6i (compute optimized) or R5 (memory optimized) instance types.

 * Intel AVX-512 support
 * Docker is installed
 * Sufficient memory and storage (depends on your test-set)
 
## Login to ECR
The Sneller docker images are currently stored in a private AWS Elastic Container Repository (ECR), so you need read-only access to this repo to obtain the docker images. With the proper AWS credentials, you should be able to log in using:
```
$ aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 671229366946.dkr.ecr.us-east-1.amazonaws.com
Login Succeeded

Logging in with your password grants your terminal complete access to your account. 
For better security, log in with a limited-privilege personal access token. Learn more at https://docs.docker.com/go/access-tokens/
```

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
$ docker-compose up
Attaching to docker-minio-1, docker-snellerd-1
docker-snellerd-1    | run_daemon.go:130: Sneller daemon 1234567-master listening on [::]:9180
docker-minio-1       | API: http://172.18.0.4:9100  http://127.0.0.1:9100 
docker-minio-1       | 
docker-minio-1       | Console: http://172.18.0.4:9101 http://127.0.0.1:9101 
docker-minio-1       | 
docker-minio-1       | Documentation: https://docs.min.io
```
It does expose the following ports:

 * `9100` is the S3 compatible port that is used to access the object storage.
 * `9101` is the Minio webconsole port (use the `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` for authentication).
 * `9180` is the Sneller API port that is used to execute queries.

### Add some data
Sneller uses S3 (or compatible) object storage to fetch the data. When running in AWS it's trivial to use AWS S3 for this purpose, but in this environment we are using Minio for this.

We provide some sample data from s3://sneller-example-data/docker/test/sf1 that needs to be copied over to Minio. First download the sample data:
```sh
$ wget 'https://sneller-example-data.s3.amazonaws.com/docker/sf1/customer.json'
```
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
   --rm --net 'sneller-network' --env-file .env -v "`pwd`:/data" \
   amazon/aws-cli --endpoint 'http://minio:9100' \
   s3 cp '/data/customer.json' 's3://test/sf1/'
```
### Create the table definition
Sneller is a schemaless database engine, but it does need to know which databases and tables exist. All Sneller data is stored in the `s3://test` bucket (defined in `.env`) and is always stored in the `db` folder. This folder holds the database, which holds the tables. So if we want to name the database `sf1` and the table `customer`, then the full path is `s3://test/db/sf1/customer`.

Now we need to create a definition for this customer table. Create a file named `definiton.json` with the following content:
```json
{
    "name": "customer",
    "input": [
        {
            "pattern": "s3://test/sf1/*.json",
            "format": "json"
        }
    ]
}
```
It specifies that the table is named `customer` and it will scan the `s3://test/sf1` folder for all files with the `.json` extension. Now upload this file to `s3://test/db/sf1/customer/definition.json`:
```sh
$ docker run \
   --rm --net 'sneller-network' --env-file .env -v "`pwd`:/data" \
   amazon/aws-cli --endpoint 'http://minio:9100' \
   s3 cp '/data/definition.json' 's3://test/db/sf1/customer/definition.json'
```
### Ingest the data
Now it's time to ingest the data. This is done using the `sdb` tool that has its own image. You can ingest the data for the sf1/customer table using:
```sh
$ docker run \
   --rm --net 'sneller-network' --env-file .env \
   671229366946.dkr.ecr.us-east-1.amazonaws.com/sneller/sdb \
   -v sync sf1 customer
```

### Query the data
Now it's time to query the data:
```sh
$ curl -G -H "Authorization: Bearer STzzzzzzzzzzzzzzzzzzzz" \
    --data-urlencode "database=sf1" \
    --data-urlencode 'json' \
    --data-urlencode 'query=SELECT COUNT(*) FROM customer' \
    'http://localhost:9180/executeQuery'
{"count": 150000}
```