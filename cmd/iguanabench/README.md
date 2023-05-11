# Iguana Benchmark Tool

Just run `iguanabench <file>`.

The `-t` flag indicates the size reduction threshold at which entropy coding is used.
Providing `-t 0` disables entropy coding entirely, and `-t 1` enables it unconditionally as long as it results in any size reduction at all.

## Results

These are the results we get on a Xeon Gold 5320 with `lz4 v1.9.4` and `zstd v1.5.2` with the [Silesia compression corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia):

| Program | Ratio | Decompression Speed |
|---------|-------|---------------------|
| zstd -b3 | 3.186 | 943.9 MB/s |
| zstd -b9 | 3.574 | 1015.8 MB/s |
| zstd -b18 | 3.967| 910.6 MB/s |
| lz4 -b1 | 2.101 | 3493.8 MB/s |
| lz4 -b5 | 2.687 | 3323.5 MB/s |
| lz4 -b9 | 2.721 | 3381.5 MB/s |
| iguana -t=0 | 2.58 | 4450 MB/s |
| iguana -t=1 | 3.11 | 2260 MB/s |

As you can see, `iguana` with entropy coding enabled (`-t 1`) has a similar
compression ratio to `zstd -3`, but it decompresses more than twice as quickly.
With entropy coding disabled (`-t 0`), `iguana` has a compression ratio roughly
equivalent to `lz4 -5` and decompresses about 33% faster.
