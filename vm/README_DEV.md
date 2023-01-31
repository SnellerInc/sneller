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
