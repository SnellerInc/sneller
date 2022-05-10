# json2ion CLI tool

This tool converts JSON into the Amazon Ion binary format.
It is primarily useful for generating test data.

The tool reads data from stdin and writes to stdout.

Please also check [iondump](https://github.com/SnellerInc/iondump)
that performs the reverse operation.

## Usage

```bash
$ ./json2ion < input.json > output.ion
```
