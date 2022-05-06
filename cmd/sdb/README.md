Sneller Database Tool
=====================

Options
-------

-   `-unsafe` allows for use of ion as an input format as well as use of
    the unsafe index signing key
-   `-v` enables verbose output to stderr

Create Command
--------------

Running `sdb create ...` will create a new table in a database and then
create a new index and synchronize it. (Note that `create` can be run
even when a definition already exists in order to overwrite the existing
definition.)

``` {.example}
$ cat nation-def.json
{
"name": "nation",
"input": [{"pattern": "s3://sneller-rdk/tpch/10n/sf1/nation/*.10n", "format": "unsafe-ion"}]
}
$ sdb -v -unsafe create s3://my-bucket mydb nation-def.json
```

Sync Command
------------

Running `sdb sync ...` will synchronize all the tables in a database in
parallel. If the patterns from the table schemas do not point to any new
objects, then no work is performed. Otherwise, new data is ionized and a
new index is written out.

(Note that we do not permit objects to be present twice as inputs to a
particular table. In other words, we disallow schema patterns that
select overlapping sets of files. We could relax this constraint in the
future by automatically deduplicating files.)

``` {.example}
localhost:~/sneller-core/cmd/sdb$ ./sdb -v -unsafe sync s3://sneller-rdk sf1
detected table at path "db/sf1/nation/"
using unsafe ion format for tpch/10n/sf1/nation/nation.10n
updating table nation...
table nation: wrote object db/sf1/nation/packed-1637693052.ion.zst ETag "45e5cd833bd2454f44c286da394b7097-1"
update of table nation complete
localhost:~/sneller-core/cmd/sdb$ curl -o - $(../../tinys3 sign s3://sneller-rdk/db/sf1/nation/index) |../../dump |jq
{
  "name": "nation",
  "created": "2021-11-23T18:44:14Z",
  "contents": [
    {
      "path": "db/sf1/nation/packed-1637693052.ion.zst",
      "etag": "\"45e5cd833bd2454f44c286da394b7097-1\"",
      "last-modified": "2021-11-23T18:44:14Z",
      "format": "blockfmt/compressed/v1",
      "original": [
        {
          "path": "s3://sneller-rdk/tpch/10n/sf1/nation/nation.10n",
          "etag": "\"3b144de81b35d3b3d061a146a463f664\"",
          "last-modified": "2021-04-13T17:35:17Z",
          "format": ".ion"
        }
      ]
    }
  ]
}
```
