# The Documentation of ION Subset Used by Sneller

## Introduction

The Sneller SQL query engine consumes and outputs Amazon ION binary encoded data.
Data in the ION format is decoded on-the-fly by the execution engine and it's
also the format that the execution engine outputs. For more information about
binary ION, please check out the following links:

  - https://amazon-ion.github.io/ion-docs/index.html (Overview)
  - https://amazon-ion.github.io/ion-docs/docs/binary.html (Binary ION)

The ION data format allows for multiple binary representations
of the same value. Sneller, however, requires that each value has only a single
representation, which is called a canonical representation. This document
describes which representations are allowed in binary ION data that the Sneller
execution engine both consumes and outputs.

## Terms

Terms used in this document and in Sneller source code:

  - `value.tlv` - the first byte of a value, which contains value `type` and `L` field
    encoded as `(type << 4) | L`.
  - `value.type` - value type - 4 bits (compatible with ION).
  - `value.L` - `L` field encoded in `value.tlv` byte.
  - `value.[Length]` - optional Length field encoded as VarUInt that follows `tlv` byte.
  - `value.hLen` - header length - the number of bytes required to store `tlv` byte and
    `[Length]`.
  - `value.hdr32` - first 32 bits of a value data, which are enough to decode any
    value. The first byte of hdr32 is `tlv` byte and the rest is either value content
    or bytes that represent `value.[Length]`.
  - `value.hdr64` - first 64 bits of a value data.

## Value Types Supported by Sneller

  - `0x0` - NULL value.
  - `0x1` - Boolean value.
  - `0x2` - Positive integer or zero.
  - `0x3` - Negative integer, never zero.
  - `0x4` - Floating point.
  - `0x5` - Decimal (NOT SUPPORTED).
  - `0x6` - Timestamp.
  - `0x7` - Symbol.
  - `0x8` - String.
  - `0x9` - Clob (NOT SUPPORTED).
  - `0xA` - Blob (NOT SUPPORTED in data, but used internally).
  - `0xB` - List.
  - `0xC` - Sexp (NOT SUPPORTED).
  - `0xD` - Struct.
  - `0xE` - Annotation (NOT SUPPORTED in data, only in ION header).
  - `0xF` - Reserved (NOT SUPPORTED).

## Encoding Restrictions

  - NULL value - ION supports NULL of every value type. The NULL is then represented by
    `value.L == 0xF`. Sneller only supports `NULL` as a value encoded as `tlv == 0x0F`,
    which describes a `null.null` value. This means that all other value types never
    have `L` field set to `0xF`.

  - `Positive` and `Negative` integers are always encoded by using the shortest ION
    representation. For example integer zero is encoded as `0x20`, where `type == 0x2`
    and `L == 0` - no other representation of zero is allowed.

  - `Floating point` values are always encoded as `float64`. Floating point values that
    have no fractional part and can be encoded as integers are always encoded as integers.
    This means that `float64(1)` is encoded as `[0x21, 0x01]` (positive integer having
    `L == 1` and a single content byte `0x01`, which represents `1`). This also implies
    that a floating point `tlv` byte is always encoded as `0x48` (where `4` means floating
    point and `8` means 8 bytes of data).

  - `Timestamp` values do not use timezone offsets (this value is always zero).
    Additionally, timestamp values always have `[year, month, day, hour, minute, second]`
    parts, plus an optional fractional second component that always has `fraction_exponent`
    equal to `-6` (i.e. microsecond precision).

  - `String` is always UTF-8 encoded, invalid UTF-8 sequences are not allowed.

  - `Struct` never uses `tlv == 0xD1` representation. The `L` field is decoded like any
    other field and is never equal to `1` as `1` is not enough to encode a symbol id and
    value. ION specification uses `tlv == 0xD1` (`L == 1`) byte to describe a struct where
    all symbols are sorted. Sneller never uses this representation and stores all symbols
    sorted by design (implicitly).

## Encoding Limitations

  - Positive and negative numbers can represent at most 64-bit integers. Integers that
    overflow 64 bits are stored as `float64` with a possible precision loss.

  - The maximum length of `[Length]` field is 3 bytes, which means that values that have
    content length of up to `(1 << 21) - 1` bytes can be stored in a single row.

  - The maximum symbol size is `3` bytes, thus the maximum number of symbols in a single
    ION data chunk is `(1 << 21) - 1`.

## Decoding Notes

  - The Sneller SQL engine always loads at least 4 bytes to decode an ION value. These 4
    bytes are named `hdr32` and are enough to decode the value type and its content length,
    which is either stored in `L` field (as a part of TLV byte) or varuint-encoded `[Length]`
    field that follows TLV byte. Since `[Length]` can occupy at most 3 bytes these 4 bytes
    are enough to decode any ION value.

  - The decoder uses `value.type > 1` condition to decide whether to trust `L` field as a
    length. If the value type is greater than `1` (thus it's not null or bool) the `L` field
    is used by the decoder, if the `L > 1` condition doesn't pass the `L` field is treated as
    zero, thus the value is interpreted as having a zero content length, which is true for
    null and bool values - true is encoded as `0x11`, which has `L` field set to `1`, but
    it's just to store the boolean bit, not the length.
