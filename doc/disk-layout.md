# Sneller Table Storage Layout

## Packfiles

Row data is stored in the database backing store in objects called `packfile`s.
Each `packfile` is composed of one or more `block`s and each `block` is composed
of one or more `chunk`s. At the end of the packfile is a `trailer` that contains
metadata describing all of the blocks in the file.

```
+-----------------+---------....+------------+---------+
| block 0         | block 1     | block N-1  | trailer |
+----+----+----...+----+----... +----+---....+---------+
| C0 | C1 | C2 .. | C0 | C1 ... | C0 | C1 .. |
```

Each `chunk` is a compressed group of ion rows.
The compression format used for compressing chunks is indicated in the `trailer`.
Currently we use the semi-columnar ion-aware `zion` format for compressing rows.
The decompressed size of a `chunk` is always below an upper bound set in the `trailer`;
currently this boundary is 1MB.
The data in a `chunk` will always decompress into a sequence of `ion` records,
optionally prefixed with an `ion` symbol table.

A `block` is a sequence of chunks that have to be processed sequentially.
Since each `chunk` may contain inline symbol table updates, a consumer must
process all of the preceding symbol tables in the `chunk`s within each `block`
in order to produce the correct symbol table for any particular sequence of rows.
Blocks are typically composed of 50 to 100 chunks, which corresponds to between
50MB and 100MB of decompressed `ion` data.

Since `block`s can be processed independently (while `chunk`s cannot),
the `block` is the smallest unit of data we use for sparse indexing.

## Trailers

The `trailer` object lists the byte offset for each `block` in the associated
`packfile`, the compression format used for all of the `chunk`s within the blocks,
and a sparse index on the list of `block`s in the `packfile`.

The `trailer` object stored in each `packfile` is also stored in the `index`
object alongside the paths to each `packfile` comprising the table so that
the query planner does not need to read from any `packfile`s in order to
create a query plan.

The sparse index in the `trailer` contains two pieces of information used during query planning:

 1. An ordered index of `block`s with respect to timestamps that occur within the data.
 This lets the query planner convert timestamp bounds in a query into block ranges within packfiles.
 2. A key/value pair for each partition value associated with the packfile. These tags let the
 query planner shuffle data by partitions and also eliminate packfiles that do not match query predicates.

## Index Objects

The "root" object that describes a Sneller SQL table is called an `index`.

The `index` object contains the following information:

 - A list of `packfile` names that collectively contain all the rows in the table.
 (NOTE: in practice this list becomes a tree when the list of objects
 becomes very large.) Each `packfile` name is stored alongside the `trailer` for that packfile.
 - A B+-tree listing all of the source objects that have been inserted
 into the table. This tree is used to guarantee that object inserts
 are idempotent.

## Appends

Appending data to an existing `packfile` is fairly straightforward, as it is simply a
compressed stream of `ion` records.

A new `packfile` that appends new data to an old `packfile` is produced as follows:

 1. All the blocks except for the last block are copied verbatim into the new `packfile`.
 Typically this is done with an S3 "server-side copy."
 2. All the chunks except for the last chunk in the last block are copied verbatim into
 the new `packfile`, taking care to decode symbol tables in each `chunk` so that the symbol
 table for the final `chunk` is available in memory.
 3. New data is written into the `chunk` stream with the current symbol table, taking care to
 flush data at the appropriate `chunk` and `block` boundaries as usual, followed by a new `trailer`.

Note in the steps above that we are able to avoid downloading and uploading all but
the final `block` of data, and we are able to avoid re-compressing all but the final `chunk` of data,
while still ensuring that every `chunk` and `block` is completely full before it is flushed.

## Execution

For each table referenced in a SQL query, the query planner determines the list of `block`s within
every `packfile` in an `index` that match any predicates associated with the table.
(The query planner also computes the set of referenced columns so that dereference-aware compression
like `zion` can use this information to elide certain rows as the `chunk`s are decompressed.)
Since the `block` metadata is available inside the `trailer` for each `packfile` in the `index`,
the query planner typically only has to perform 1 read operation (to fetch the `index`) in order
to produce a query plan.

At execution time, each `block` is deterministically assigned an `ETag` based on the `packfile` `ETag` plus the offset
of the `block` within the file. The computed `ETag` for each `block` is used to partition blocks onto
machines deterministically via Rendezvous Hashing.
