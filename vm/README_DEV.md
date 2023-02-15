# Test queries

The test application has two extra options:

- `-symlink` (default: on) - enables crawling symlinks
  when looking for test (`TestQueries`) or benchmark files
  (`BenchamrkTestQueries`).

- `-trace` (default: off) - prints on the standard output
  bytecode being executed by tests `TestQueries`.


# Code generators

The `vm` package contains three major code generators
invoked with the `go generate` command:

1. `genrewrite_main.go` produces SSA simplification
   rules based on `simplify.rules` and creates
   file `simplify1.go`.

2. `_generate/genops.go` scans assembly files for
   definitions of opcode functions (`bc{name}`) and
   creates constants in `ops_gen.go`, `ops_gen_amd64.s`
   and `ops_mask.h`.

3. `_generate/genconst.go` scans assembly files for
   used constants and produces `bc_constant_gen.h`.

The code generator `_generate/strcase.go` generates
lookup tables needed by `tolower` & `toupper` opcodes.
It should be run manaully when unicode version shipped
with Go changes. The command is:

```bash
$ go run _generate/strcase.go -o evalbc_strcase_constant.h
```


# Constant extraction

The script `genconst.go` scans all assembly files
and gathers used constants.

Constants are expected to be in format:

* `CONST{type}_{integer_value}()` or
* `CONST{type}_{name}()`.

Note the parenthesis at the end.

**Type** is one of `Q` (uint64), `D` (uint32), `B` (byte) or `F64` (float64).
For example:

* `CONSTD_0xfff()` is `uint32(0x00000fff)`,
* `CONSTQ_42` is `uint64(42)`,
* `CONSTB_0b1011` is `byte(11)`,
* `CONSTF64_360` is `float64(360.0)`.

**Integer value** is any unsigned number that is a valid macro
name and is properly parsed by `strconv.ParseUint`. It's allowed
to use prefixes `0b`, `0x` as well as the `_` to separate digits.

In the case of non-numeric part of name, it's required to **define**
constant in a comment, using one of the following syntaxes:

```
CONST{type}_{name}() = unsigned integer value
CONST{type}_{name}() = floating-point value
CONSTF64_{name}() = uint64(64-bit hex number)
```

For example:

```
CONSTD_NEG_1 = 0xffffffff
CONSTF64_HALF = 0.5
CONSTF64_SIGN_BIT = uint64(0x8000000000000000)
```


# Aggregates at the assembly level

Each aggregate function should have assigned a distinct `AggregateOpFn`
value. Through the method `dataSize()` we inform the execution engine
how many bytes of storage the aggregation procedure needs. This is
a static buffer, allocated once per query execution. (NB. most aggregates
use a const-sized buffer, the only exception is approximate count).

The buffer assigned to an aggregate can be freely used, it's not altered
by the execution engine.

Aggregates are invoked in two contexts:

- regular aggregation, like `SELECT SUM(x) FROM ...`,
- hash aggregations, like `SELECT x, SUM(x) FROM ... GROUP BY x`.

A bytecode instruction gets **aggregate slot**, a 32-bit offset.
Depending on the context, we get address of buffer and update
it in two ways.

## Regular aggregation

The base pointer for all aggregates is passed in R10, we need to add
it to the slot value of our aggregate. That's it.

```
  // aggregate slot argument is at 0
  BC_UNPACK_RU32(0, OUT(DX))    // DX - 32-bit aggregate slot

  // calculate address
  ADDQ  R10, DX                 // DX - a pointer to aggregate buffer
```

Once we the have address to our data we update buffer as needed.  We have 16
inputs + buffer. For instance most simple aggregates use 16-byte buffers. The
lower 8 bytes is used to store aggregation of new 16 inputs (interpreted either
as `int64` or `float64`), the higher 8 bytes is total number of rows processed
(interpreted as `uint64`); for integer addition we execute something like
that:

```go
    bytes []byte        // located at address DX
    values [16]int64    // VIRT_VALUES + slot

    tmp := int64(binary.LittleEndian.Uint64(bytes[0:]))
    count := binary.LittleEndian.Uint64(bytes[8:])
    for i := range 0..15 { // loop = usually a single AVX512 instruction
        if i-th bit of mask is set {
            tmp += values[i]
        }
    }

    binary.LittleEndian.PutUint64(bytes[0:], uint64(tmp))
    binary.LittleEndian.PutUint64(bytes[0:], uint64(count + popcount(mask)))
```

## Hash aggregation

In the case of hash aggregation, each distinct value ("key from group by") has
its own buffer for aggregates, stored in a radix tree. Obtaining its address
requires the following steps:

```
  // aggregate slot argument is at 0
  BC_UNPACK_RU32(0, OUT(R15))            // R15 - 32-bit aggregate slot

  ADDQ $const_aggregateTagSize, R15      // add const offset
  ADDQ radixTree64_values(R10), R15      // add the base pointer
                                         // R15 - buffer pointer
```

It's not the end.  Upon calling an opcode for hash aggregation, an
external code fills `bytecode.bucket`: it is an array of sixteen
32-bit slots (**they may repeat**), and these buckets has to be updated.

In a Go-code terms it looks like this:

```go
    var bc bytecode
    // ...
    for i := range bc.bucket { // 16 buckets = 16 iterations
        if i-th bit of mask is set {
            /* 1 */ var bucket uint32  = bc.bucket[i]
            /* 2 */ var x float64      = i-th input value
            /* 3 */ var buffer []byte  = R15 + bucket     // the aggregate buffer

            at this point **a single value** x and buffer are used
            to perfom aggregation procedure
        }
    }
```

Note that currently all aggregates use vectorized code to perform
the above loop. Please see for instance macro `BC_AGGREGATE_SLOT_MARK_OP`.

Below are shown the code snippets showing how to obtain certain
values.  If they were placed in a real loop, then we would split
them - for instance load base address once, and then increase
pointers.  But for better readability, they were put as code sequences.

Reading i-th bucket (**1**).

```
  LEAQ bytecode_bucket(VIRT_BCPTR), DX      // &bc.bucket[0]
  ADDQ $(4 * i), DX                         // &bc.bucket[i]
  MOVL (DX), CX                             // CX - 32-bit number (bucket[i])
```

Reading i-th input value (**2**).

```
  BC_UNPACK_SLOT(4, OUT(BX))                // BX - 16-bit slot (at offset 4)
  LEAQ 0(VIRT_VALUES)(BX*1), BX             // BX - input values ptr
  ADDQ $(8 * i), BX                         // BX - ptr to i-th 64-bit value
  MOVQ (BX), X0                             // X0 - 64-bit value
```

Obtaining buffer pointer (**3**).

```
  // inputs:
  // R15 - `radixTree64.values` pointer (calculated at the beginning)
  // CX  - i-th bucket (calculated in #1)
  ADDQ    R15, CX
```
