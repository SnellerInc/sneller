# Sneller SQL User's Guide

## Introduction

## SQL Support

Sneller SQL is a flavor of SQL that is generally
compatible with standard SQL, with modifications
to allow it to efficiently execute queries over
hierarchical data without a schema.

Sneller SQL queries are written in ordinary
UTF8-encoded text. These query text strings
are typically sent as HTTP request bodies or
URL-encoded query strings.

Sneller SQL only supports part of the "DQL" portion of
standard SQL (i.e. `SELECT-FROM-WHERE` statements, etc.).
Sneller does not currently use SQL to perform database insert/update operations,
although we may support those at some point in the future.

<!-- TODO: link to API docs when we have them -->

Sneller SQL extends the concept of SQL "rows" and "columns"
to "records" and "values" In other words, each "row" of
data is a record of values, and records themselves are
also values. A "table" is an *un-ordered* collection of records.

Instead of projecting "columns," a Sneller SQL query projects record fields.

For example, the query

```sql
SELECT 1 AS x, 2 AS y, (SELECT 'z' AS z, NULL AS bar) AS sub
```

evaluates to

```json
{"x": 1, "y": 2, "sub": {"z": "z", "bar": null}}
```

Sneller SQL can handle tables that have records
with wildly different schemas, as it does not
assume that the result of a particular field selection
must produce a particular datatype (or even any value at all).

### Execution Model

Since Sneller is designed to run as a "hosted" multi-tenant
product, the query engine and query planner are designed so
that queries will execute within a (generous) fixed memory limit
and a linear amount of time with respect to the size of the input.
In other words, any query that is accepted by the query planner will touch each
row of each table referenced in each individual `FROM` clause no more than once,
and query operations that need to buffer rows (e.g. `ORDER BY`, `GROUP BY`, etc.)
will not buffer indefinitely.

### Identifiers

Sneller SQL supports both quoted and un-quoted identifiers
(i.e. `foo` and `"foo"`). Double-quotes are used to quote identifiers
that would conflict with SQL keywords or would not otherwise be
legal identifiers. (For example, `""` is a valid identifier, but it
is impossible to write that identifier without quoting.)

In general, users should prefer double-quoted identifiers, as SQL
has a large number of keywords that conflict with commonly-used
attribute names.

### Core Types

#### Floats

By default, the arithmetic operators like `+`
operate on IEEE-754 double-precision floating-point numbers.
When tables are created from parsing text formats like `JSON`,
any numbers that have non-zero fractional components (or cannot
fit in the native integer representation) are implicitly converted
to double-precision floats.

#### Integers

Numbers without fractional decimal components are stored
and operated upon as 64-bit signed integers.
Integers are implicitly converted to double-precision floats
when an operation would intermix numbers of different types.

#### Strings

Internally, strings are UTF8-encoded.
Functions and operators that operate on strings
are UTF8-aware.

#### Timestamps

Sneller SQL supports native operations
on timestamps with microsecond-level precision.

#### Structures

Structures are a collection of name-and-value pairs
like you would see in a `JSON` object.
(Every row of data in a table is a structure object.)

Sneller SQL path expression like `x.y.z` can be used
to navigate through nested structures.

#### Null

The value `NULL` is its own atom distinct
from the absence of a value.
The value `NULL` can be detected with the `IS NULL` expression.

#### Missing

`MISSING` is the notation for the absence of a value.
Since Sneller SQL has functions and operator that only
operate on certain data-types, some operations may not
return a meaningful result. For example, the result
of the expression `3 + 'foo'` is `MISSING`, since we
cannot add the integer 3 to the string `'foo'`.
Similarly, the result of a path expression `foo.bar`
where the value `foo` is not a structure (or `foo` is a structure
without any field called `bar`) is also `MISSING`.

When projecting columns for output, the Sneller SQL engine
will omit labels for columns that produce `MISSING`.
In other words, an expression that evaluates to
`{'x': 'foo', 'y': MISSING}` is output as `{'x': 'foo'}`.

As a general rule, functions and operators that receive
arguments that are `MISSING` or are ill-typed will yield `MISSING`.
An obvious exception to this rule is `IS [NOT] MISSING`, which
can be used to detect whether a value is missing.

#### Lists

Lists are ordered sequences of any supported datatype.
List elements can be dereferenced using indexing path expressions
like `tags[0]`.

### Literals

#### Literal Strings

Literal strings are wrapped in the single-quote (`'`) character.
Literal strings are allowed to contain conventional ASCII escape sequences
(`\t\n\a\r\b\n\v\f`, etc.), as well as the Unicode escape sequence forms
`\u0000` and `\U00000000`.

#### Literal Timestamps

Literal timestamps are enclosed in the back-tick (```) character
and formatted in RFC3339 representation with microsecond precision,
i.e. six decimal digits in the "seconds" component of the timestamp.

For example:
```
`2022-05-01T12:35:01.000111Z`
```

#### Literal Numbers

Literal numbers (integers and floating-point numbers) are
written in decimal format. Floating-point numbers may have
an exponent written in scientific notation (i.e. `1e20` or `1E-20`).

### Grammar

The following EBNF grammar approximately
describes the grammar that is accepted by
the SQL parser.

```ebnf
query = cte_clause* sfw_query ;

identifier = raw_id | quoted_id ;

raw_id = letter { letter | number | '_' } ;
quoted_id = '"' { char } '"' ;

cte_clause = WITH identifier 'AS' '(' sfw_query ')' { ',' identifier 'AS' '(' sfw_query ')' } ;

binding_list = expr [ 'AS' identifier ] { ',' expr [ 'AS' identifier ] } ;

expression_list = expr { ',' expr } ;

sfw_query = 'SELECT' [ 'DISTINCT' ['ON' '(' expression_list ')'] ] ('*' | binding_list) [ from_clause ] [ where_clause ] [ group_by_clause ] [ order_by_clause ] [ limit_clause ] ;

from_clause = 'FROM' path_expr [ 'AS' identifier]  { ',' path_expr [ 'AS' identifier] } ;

where_clause = 'WHERE' expr ;

group_by_clause = 'GROUP BY' binding_list ;

order_column = expr [('ASC' | 'DESC')] [('NULLS FIRST' | 'NULLS LAST')] ['AS' identifier] ;
order_by_clause = 'ORDER BY' order_column { ',' order_column } ;

limit_clause = 'LIMIT' integer ['OFFSET' integer];

path_expr = identifier { (('.' identifier) | ('[' integer ']')) } ;

integer = ... ; // decimal integer literal
float = ... ; // decimal floating-point literal
string = ''' (unescaped_char | escaped_char) ''' ;
timestamp = '`' rfc3339-timestamp '`' ; // See RFC3339
list = '[' expr { ', ' expr } ']' ;
structure = '{' string ':' expr { ',' string ':' expr } '}' ;

expr = compare_expr | arith_expr | in_expr | case_expr | like_expr |
       regex_expr | is_expr | not_expr | function_expr | subquery_expr |
       between_expr | path_expr |
       integer | string | float | timestamp;

subquery_expr = '(' sfw_query ')' ;
like_expr = expr ('LIKE' | '~~' | 'ILIKE' | '~~*') string ['ESCAPE' string] ;
regex_expr = expr ('SIMILAR TO' | '~' | '~*') string ;
compare_expr = expr ('<' | '<=' | '=' | '<>' | '>=' | '>') expr ;
is_expr = expr 'IS' [ 'NOT' ] ( 'NULL' | 'MISSING' | 'TRUE' | 'FALSE' ) ;
not_expr = ('!' | 'NOT') expr ;
in_expr = expr 'IN' ( subquery_expr | '(' { expr } ')' ) ;

// note: currently the bounds are restricted
// to paths and literal datums due to the shift-reduce
// conflict with ordinary AND
between_expr = expr BETWEEN expr AND expr ;

arith_expr = expr ('+' | '-' | '/' | '*' | '%') expr;

function_name = ... ; // see list of built-in functions
function_expr = function_name '(' arg { ',' args } ')' ;

case_expr = 'CASE' [ expr ] { 'WHEN' expr 'THEN' expr } [ 'ELSE' expr ] 'END' ;
```

### General Limitations

#### JOIN restrictions

The Sneller SQL query engine supports
"un-nesting" cross joins and inner joins,
but neither use the `JOIN` SQL keyword explicitly.
The query engine does not yet support other kinds of SQL joins.

##### Unnesting

The `,` operator in the `FROM` position
can be used to `CROSS JOIN` all of the
rows in a table with an array element
that occurs in each row.

For example, if we have a table with the following rows:
```JSON
{"array": [{"y": 3}, {"y": 4}], "x": "first"}
{"array": [{"y": 5}, {"y": 6}], "x": "second"}
```

Then the following query
```SQL
select outer.x, inner.y
from table as outer, outer.array as inner
```

would produce
```JSON
{"x": "first", "y": 3}
{"x": "first", "y": 4}
{"x": "second", "y": 5}
{"x": "second", "y": 6}
```

##### Correlated Sub-queries

A "correlated" sub-query is one that uses
a binding from an outer query to determine
the result of an inner query.
Correlated sub-queries can be used to compute
results that are similar to a traditional SQL
"inner join." The query engine will reject correlated
sub-queries that cannot be optimized to run in linear time.
(Also, please read the subsequent section on general
restrictions applied to sub-queries.)

For example, if we have a table `inner` that looks like this:
```JSON
{"x": "foo", "y": "first row"}
{"x": "bar", "y": "second row"}
```

and a table `outer` that looks like this:
```JSON
{"z": "first outer", "x": "foo"}
{"z": "second outer", "x": "bar"}
```

Then the following query
```SQL
SELECT z, (SELECT inner.y FROM inner AS inner WHERE inner.x = outer.z) AS y
FROM outer AS outer
```

would produce this result:
```JSON
{"z": "first outer", "y": "first row"}
{"z": "second outer", "y": "second row"}
```

#### Subquery restrictions

Since the query engine implements
sub-queries by buffering the intermediate
query results, sub-queries are not allowed
to have arbitrarily large result-sets.

The query planner will reject sub-queries
that do not meet *ONE* of the following conditions:

 - The query has a `LIMIT` clause with
 a value of less than 10,000.
 - The query has a `SELECT DISTINCT` or `GROUP BY` clause.
 - The query is an aggregation with
 no corresponding `GROUP BY` clause (and thus has
 a result-set size of 1).

Additionally, the query execution engine will
fail queries that produce too many intermediate results.
(Currently this limit is 10,000 items.)

##### Correlated subqueries

Correlated sub-queries (sub-queries that refer to
variable bindings defined in the outer query) are
subject to additional restrictions. The query engine
only supports correlated sub-queries that meet *ALL* of
the following conditions:

 - The sub-query is equivalent to a comparator-based
 `JOIN` using only equality comparisons in the join
 predicate (also known as an equi-join).
 - The sub-query contains only one reference to a
 variable binding defined in the outer query.
 - The sub-query does not refer to the correlated
 variable binding in the `SELECT` clause.

Correlated sub-queries that do not meet the above
conditions will be rejected by the query engine.

#### Ordering Restriction

The `ORDER BY` clause may not operate on
an unlimited number of rows, as it would require
that the query engine buffer an unlimited number of rows
in order to sort them.

The query engine will reject an `ORDER BY` clause
that occurs without *at least one* of the following:

 - A `LIMIT` clause of 10000 elements or fewer
 - A `GROUP BY` clause

#### Implicit Subquery Scalar Coercion

In order to maintain compatibility with standard
SQL, the Sneller SQL query planner will automatically
convert certain sub-queries into scalar results.
However, sub-queries are also allowed to produce
non-scalar results, since in general the result
of any query can be represented as a list of structures.

The rules for scalar coercion are as follows:

 - If a sub-query occurs on either side of a binary
 infix operator (a comparison, an arithmetic operator,
 `IS`, etc.), then the result is coerced to a scalar.
 The query planner will return an error if the query
 has more than one column in its final projection or
 if the query could return more than 1 row. (In other words,
 queries that are converted to scalars should have either
 an explicit `LIMIT 1` clause or an aggregation operation
 with no `GROUP BY` clause.)
 - If a sub-query occurs on the right-hand-side of an `IN` expression,
 the result of the query is coerced to a list. The query planner will
 return an error if the query projects more than one column.
 - If the sub-query has an explicit `LIMIT 1` or is an aggregation
 with no `GROUP BY` clause, *and* the query produces one output column,
 the result is coerced to a scalar.
 - If the sub-query has one result row (see the previous bullet point)
 and the query produces *more than one* output column, then the result
 is coerced to a structure.
 - Otherwise, the result is kept as a list of structures.

#### Dedicated Time functions

Since the result of a Sneller SQL sub-expression
can be any of the supported data-types, there are some
limitations on the amount of overloading that certain
operators can support.

For aggregating timestamp values, we have the built-in
aggregation operations `EARLIEST` and `LATEST`, which
perform the equivalent of `MIN` and `MAX` operations
on timestamp values, respectively.

#### Grouping Types

If the grouping columns in a `GROUP BY` clause
evaluate to structures (or lists of structures),
the grouping may generate erroneous results for those buckets.
(The query engine does not support comparing structures
for equality across data blocks, so a single logical structure
may result in more than one grouping bucket.)
Grouping on lists is supported as long as those lists
do not contain structures.

<!--
TODO: should we check for structures
and make them MISSING in the query engine?
Might be friendlier behavior to make it fail 100% of the time
rather than just some of the time.
-->

### Path Expressions

Path expressions are used to dereference sub-values
of composite data-types like structures and lists.
The `.` operator dereferences fields within structures,
and the `[index]` operator indexes into lists.
Currently only integer constants are permitted in indexing expressions.

For example, `foo.bar[3]` selects the field `bar` from
the struct value `foo` and then indexes into the fourth
list element of the field `bar`. If any of the intermediate
steps in that operation can't be performed (because `foo`
is not a struct, or `bar` is not a list with at least four elements),
then the result is `MISSING`.

### Binding Precedence

The `WITH`, `SELECT`, `GROUP BY`, and `ORDER BY` clauses
can all create new variable bindings with the `AS identifier` syntax.
Variable bindings are evaluated in left-to-right order within
each clause, and variable bindings across clauses are evaluated
beginning with `WITH`, followed by `GROUP BY` and `SELECT`, and then
finally `ORDER BY`.

For example:

```sql
SELECT COUNT(*) AS count, count/100, group
FROM table
GROUP BY TRIM(name) AS group
ORDER BY count DESC
```

The binding `group` produced in `GROUP BY` is used
in `SELECT` rather than repeating `TRIM(name)`, and
the binding `count` produced in `SELECT` is used
in `ORDER BY` rather than repeating `COUNT(*)`.
The `count` binding produced in the first part of `SELECT`
is also used in the subsequent `count/100` output column.
Bindings can be used to avoid repeating complicated
expressions in multiple places within the same query.

## Operators

### Composite Constructors

#### Structure Expressions

Sneller SQL supports structure literal expressions.

For example, the following query produces records
with one fields called `rec` that is itself a record
of two fields (`x` and `y`):
```SQL
SELECT {'x': fields.x, 'y': z} AS rec
FROM table
```

In other words, the results of the query above might look like:
```json
{"rec": {"x": 3, "y": "foo"}}
{"rec": {"x": 2, "y": "bar"}}
```

If a field in a structure expression evaluates to `MISSING`,
then the field will be omitted from the structure representation.

#### List Expressions

Comma-separated expression wrapped in square brackets
(i.e. `[3, foo, bar]`) are evaluated as lists.

For example, the following query produces a single
field called `lst` that is a list:
```SQL
SELECT [x, y] AS lst
FROM table
```

If a field within a list expression evaluates to `MISSING`,
it will be omitted from the list.

### Aggregations

#### `COUNT`

As `COUNT(*)`, returns an integer count
of the total number of rows that reach the aggregation clause.

As `COUNT(expr)`, returns an integer count
of the total number of rows for which `expr` evaluates to a
value that is not `MISSING`.

For example, the following two queries are equivalent:
```SQL
SELECT COUNT(*)
FROM table
WHERE x > 3
```

```SQL
SELECT COUNT(CASE WHEN x > 3 THEN TRUE ELSE MISSING END)
FROM table
```

#### `COUNT(DISTINCT)`

`COUNT(DISTINCT expr)` counts the number of distinct
results produced by evaluating `expr` for each row.

Current limitations: `COUNT(DISTINCT expr)` is not allowed
to occur inside a `GROUP BY` query.

#### `MIN` and `MAX`

`MIN(expr)` and `MAX(expr)` produce the largest
and smallest numeric value, respectively, that reach
the aggregation clause. If `expr` never evaluates to
a numeric value, then these expressions yield `NULL`.

#### `EARLIEST` and `LATEST`

`EARLIEST(expr)` and `LATEST(expr)` produce the earliest
and latest timestamp value, respectively, that reach
that aggregation clause. Like `MIN` and `MAX`, these
aggregations yield `NULL` when the aggregation expression
never returns a timestamp value.

#### `SUM`

`SUM(expr)` accumulates the sum of `expr` for
all of the rows that reach the aggregation expression.
If `expr` never evaluates to a number, `SUM(expr)` yields `NULL`.

#### `AVG`

`AVG(expr)` accumulates the average of `expr`
for all the rows that reach the aggregation expression.
If `expr` never evaluates to a number, `AVG(expr)` yields `NULL`.

#### `BIT_AND`

`BIT_AND(expr)` computes bitwise AND of all results produced by
evaluating `expr` for each row. If `expr` never evaluates to a number,
`BIT_AND(expr)` yields `NULL`.

#### `BIT_OR`

`BIT_OR(expr)` computes bitwise OR of all results produced by
evaluating `expr` for each row. If `expr` never evaluates to a number,
`BIT_OR(expr)` yields `NULL`.

#### `BIT_XOR`

`BIT_XOR(expr)` computes bitwise XOR (exclusive OR) of all results
produced by evaluating `expr` for each row. If `expr` never evaluates
to a number, `BIT_XOR(expr)` yields `NULL`.

#### `BOOL_AND` and `EVERY`

`BOOL_AND(expr)` computes bitwise AND of all results produced by
evaluating `expr` for each row coerced to a boolean type. If `expr`
never evaluates to a boolean, `BOOL_AND(expr)` yields `NULL`.

`EVERY(expr)` is an alias of `BOOL_AND(expr)`.

#### `BOOL_OR`

`BOOL_OR(expr)` computes bitwise OR of all results produced by
evaluating `expr` for each row coerced to a boolean type. If `expr`
never evaluates to a boolean, `BOOL_OR(expr)` yields `NULL`.

#### `APPROX_COUNT_DISTINCT`

`APPROX_COUNT_DISTINCT(expr)` counts the approximate number of
distinct results produced by evaluating `expr` for each row.

`APPROX_COUNT_DISTINCT(expr, precision)` allows to set the
precision. The precision is given as number from 4 to 16. The
default precision is 11.

The table below shows relative error for each precision value.

| precision | error |
| --------- | ------|
| 4         | 0.520 |
| 5         | 0.465 |
| 6         | 0.425 |
| 7         | 0.393 |
| 8         | 0.368 |
| 9         | 0.347 |
| 10        | 0.329 |
| 11        | 0.314 |
| 12        | 0.300 |
| 13        | 0.288 |
| 14        | 0.278 |
| 15        | 0.269 |
| 16        | 0.260 |

This aggregate is faster than `COUNT(DISTINCT expr)`, and it
does not have the same limitations regarding the cardinality
of the input expression.

Example

```sql
SELECT
    COUNT(id) AS exact,
    APPROX_COUNT_DISTINCT(id, 4) AS approx4,
    APPROX_COUNT_DISTINCT(id, 5) AS approx5,
    APPROX_COUNT_DISTINCT(id, 6) AS approx6,
    APPROX_COUNT_DISTINCT(id, 7) AS approx7,
    APPROX_COUNT_DISTINCT(id, 8) AS approx8,
    APPROX_COUNT_DISTINCT(id, 9) AS approx9,
    APPROX_COUNT_DISTINCT(id, 10) AS approx10,
    APPROX_COUNT_DISTINCT(id, 11) AS approx11,
    APPROX_COUNT_DISTINCT(id, 12) AS approx12,
    APPROX_COUNT_DISTINCT(id, 13) AS approx13,
    APPROX_COUNT_DISTINCT(id, 14) AS approx14,
    APPROX_COUNT_DISTINCT(id, 15) AS approx15,
    APPROX_COUNT_DISTINCT(id, 16) AS approx16
FROM sample_input
```

The result for sample data:

```json
{
    "exact"   : 150000,
    "approx4" : 305163, -- diff: 155163, relative error: 103.442%
    "approx5" : 221944, -- diff:  71944, relative error:  47.963%
    "approx6" : 191157, -- diff:  41157, relative error:  27.438%
    "approx7" : 168042, -- diff:  18042, relative error:  12.028%
    "approx8" : 166567, -- diff:  16567, relative error:  11.045%
    "approx9" : 161878, -- diff:  11878, relative error:   7.919%
    "approx10": 154556, -- diff:   4556, relative error:   3.037%
    "approx11": 154406, -- diff:   4406, relative error:   2.937%
    "approx12": 151081, -- diff:   1081, relative error:   0.721%
    "approx13": 149152, -- diff:    848, relative error:   0.565%
    "approx14": 149845, -- diff:    155, relative error:   0.103%
    "approx15": 149775, -- diff:    225, relative error:   0.150%
    "approx16": 149630  -- diff:    370, relative error:   0.247%
}

```

#### `SNELLER_DATASHAPE`

`SNELLER_DATASHAPE(*)` is an aggregate that collects unique
fields present in a query and gathers their data types
(see also the `TYPE_BIT` function).

The function returns a structure having the following fields:

* `total` - the total number of rows fetched from the table;
* `fields` - dictionary of fully qualified paths associated with
  type names; each type has the number of fields having given type;
* `error` (optional) - error message when the number of fields
  is greater than the limit (10,000).

Example:

```sql
SELECT sneller_datashape(*) FROM employees
```

The result might be like this:

```json
{
    "total": 1000
    "fields": {
        "name": {
            "string": 1000,
            "string-min-length": 3,
            "string-max-length": 15
        },
        "surname": {
            "string": 1000,
            "string-min-length": 5,
            "string-max-length": 41
        },
        "contract": {
            "bool": 1000
        },
        "age": {
            "int": 900,
            "int-min-value": 21,
            "int-max-value": 69,
            "null": 100,
        }
        "address": {
            "string": 51,
            "string-min-length": 24,
            "string-max-length": 119,
            "struct":  624
        }
        "address.street" {
            "string": 624
        }
        "address.number" {
            "int": 473,
            "null": 96,
            "string": 55
        }
        "address.postcode" {
            "string": 591,
            "null": 89
        }
    }
}
```

There are following type names:

* `null`,
* `bool`,
* `int`,
* `float`,
* `decimal`,
* `timestamp`,
* `string`,
* `list`,
* `struct`,
* `sexp`,
* `clob`,
* `blob`,
* `annotation`.

For `string` types there are also available min and max string lengths (keys
"string-min-length" and "string-max-length"). For `int` and `float` there are
available min and max values (keys "int-min-value", "int-max-value",
"float-min-value" and "float-max-value").

For `list` field, there's an artificial child "$items" that presents
a union of all values found in the given list.

**Current limitations**: the `SNELLER_DATASHAPE` aggregate can be the
only one present in a query. Mixing it with other aggregates is not supported.


### Filtered aggregates

All aggregate functions accept an optional filter clause, which causes
an aggregate to consume only the rows matching the given condition.
Note that such conditions are applied **after** filtering rows with
the main `WHERE` clause.

The syntax of filter is:

```sql
aggregate FILTER (WHERE condition)
```

Example:

```sql
SELECT SUM(x) FILTER (WHERE x > 0) AS sum_positive,
       MIN(x) FILTER (WHERE x < 0) AS min_negative,
       COUNT(*) FILTER (WHERE x = 0) AS zero_count
FROM table
```

See also [Postgres Aggregate Expressions](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES)

### Infix Operators

#### `+`, `-`, `*`, `/`, `%`

Conventional infix arithmetic operators
yield a number from two input numbers.
Arithmetic operators yield `MISSING` if
one or more of the input values is not
a number value.

#### `&`, `|`, `^`, `<<`, `>>`, `>>>`

Bitwise operations yield an integer from two input integers
or `MISSING` if one or more of the input values is not an
integer.

  * `x & y` - bitwise AND of `x` and `y`
  * `x | y` - bitwise OR of `x` and `y`
  * `x ^ y` - bitwise XOR (exclusive OR) of `x` and `y`
  * `x << y` - bitwise shift left of `x` by `y`, if `y`
    is greater than 63 the result becomes zero
  * `x >> y` - arithmetic shift right of `x` by `y`,
    shifting in the most significant bit (sign bit) of `x`
  * `x >>> y` - logical shift right of `x` by `y`,
    shifting in zeros


#### `LIKE` (`~~`) and `ILIKE` (`~~*`)

The `LIKE` operator matches a string value
against a pattern. The pattern on the right-hand-side
of `LIKE` must be a literal string. A `LIKE` expression pattern
can include a set of pattern-patching metacharacters.

* `%` denotes zero or more Unicode points.
* `_` denotes exactly one Unicode point.

Note that all other characters in the like expression match only themselves.

For example:

```sql
SELECT * FROM table
WHERE message_body LIKE '%foo%'
```

The query above will return all the records
from `table` that have a string-typed `message_body`
field that contains the sub-string `'foo'`.

The metacharacters `%` and `_` can be escaped by any Unicode character
provided with the `ESCAPE` keyword. Any character preceded with the escape
characters will only match itself. No default escape character is
assumed, and the escape character cannot equal either metacharacter
`%` and `_`. Note that the escape character will not be part of the
matching string and only serves as a metacharacter in the like expression.

For example:

```sql
SELECT * FROM table
WHERE message_body LIKE '%100#%%' ESCAPE '#'
```

The query above will return all the records
from `table` that have a string-typed `message_body`
field that contains the sub-string `100%`.

The `ILIKE` operator works identically to `LIKE`,
except that individual character matches are case-insensitive.
(Since Sneller SQL is Unicode-aware, characters are compared
using Unicode "Simple Case Folding" rules.)

#### `SIMILAR TO`

The `SIMILAR TO` operator matches a string value against
a regular expression pattern. The pattern on the right-hand-side
of `SIMILAR TO` must be a literal string. The expression pattern
can include a set of pattern-matching metacharacters, including
the two supported by the `LIKE` operator.

* `%` denotes zero or more Unicode point.
* `_` denotes exactly one Unicode point.
* `|` denotes alternation (either of two alternatives).
* `*` denotes repetition of the previous item zero or more times.
* `+` denotes repetition of the previous item one or more times.
* `?` denotes repetition of the previous item zero or one time.
* `{m}` denotes repetition of the previous item exactly m times.
* `{m,}` denotes repetition of the previous item m or more times.
* `{m,n}` denotes repetition of the previous item at least m and not more than n times.
* Parentheses `()` can be used to group items into a single logical item.
* A bracket expression `[`...`]` specifies a character class, just as in POSIX regular expressions.

Note that the period `.` is *not* a metacharacter for `SIMILAR TO`.

The pattern will match from the beginning of the string value until the end of the pattern.

For example:

```sql
SELECT * FROM table
WHERE message_body SIMILAR TO 'a{2,5}b|c'
```

The query above will match with sequence of 2 upto 5 'a' characters
followed by either an `b` or `c`. Any other characters before or after will
that do not match the pattern will prevent a match. For example, the string
value 'xaaab' does not match.

#### POSIX-Regex `~` and case-insensitive POSIX-Regex `~*`

The POSIX-Regex `~` operator matches a string value against a
POSIX-regex pattern. A non-exhaustive list of metacharacters:

* `.` denotes exactly one Unicode point.
* `|` denotes alternation (either of two alternatives).
* `*` denotes repetition of the previous item zero or more times.
* `+` denotes repetition of the previous item one or more times.
* `?` denotes repetition of the previous item zero or one time.
* `{m}` denotes repetition of the previous item exactly m times.
* `{m,}` denotes repetition of the previous item m or more times.
* `{m,n}` denotes repetition of the previous item at least m and not more than n times.
* `^` start-of-line anchor
* `$` end-of-line anchor

Note that the `LIKE` metacharacters `%` and `_` are not metacharacters for `~`
and `~*`, but that the period `.` is.

The parsing of the regex pattern is handled by `Go`, and thus all features
supported by the Go regex compiler are also supported by Sneller. A description
of the Go regex syntax can be found in https://github.com/google/re2/wiki/Syntax.

Contrary to `SIMILAR TO` that matches from the beginning of a string, the
POSIX regex will match starting from any position in the string. Stated
differently, a POSIX regex has an implicit `.*` at the beginning of a pattern,
while the `SIMILAR TO` has an implicit start-of-line anchor `^`. Similar situation
hold on how matching the end of the string is handled: the POSIX regex does
not need to match the end of the string value while `SIMILAR TO` does. The
POSIX regex has an implicit `.%` at the end of the pattern while `SIMILAR TO` an
implicit end-of-line anchor `$`. The wildcards `.*` are thus redundant in regex
pattern `'.*Biden.*'`

For example:

```sql
SELECT * FROM table
WHERE message_body ~ '(?i)biden[[:digit:]]'
```

The query above will match with any string that contains the case-insensitive string
`biden` followed by one digit (0-9).

The case-insensitive POSIX-Regex `~*` operator works identically
to the POSIX-Regex `~`, except that individual characters matches
are case-insensitive.

#### `IN`

The `IN` operator matches a value against a list of values.

For example:
```sql
SELECT * FROM table WHERE val IN (3, 'foo', NULL)
```
The query above returns all the rows in `table`
for which the field `val` is the number 3, the string `'foo'`,
or the value `NULL`.

The `IN` operator can also accept a sub-query
that can be coerced to a list of scalars on the right-hand-side:

```sql
WITH top5_attrs AS (SELECT COUNT(*), attr
                    FROM table
                    GROUP BY attr
                    ORDER BY COUNT(*) DESC
                    LIMIT 5)
SELECT *
FROM table
WHERE attr IN (SELECT attr FROM top5_attrs)
```

The query above selects all the rows in `table` where
the value `attr` is a member of the set of the top 5
most frequently occurring unique `attr` values in `table`.

### Unary Operators

#### `!` or `NOT`

The `NOT` operator inverts booleans.
In other words, `NOT TRUE` yields `FALSE`,
and `NOT FALSE` yields `TRUE`.
The `NOT` operator applied to non-boolean values
yields `MISSING`.

#### `IS`

`IS` is used to compare values to one of the atoms
`TRUE`, `FALSE`, `NULL`, or `MISSING`.

Testing a value with `expr IS MISSING` is the
idiomatic way of determining whether a value is `MISSING`.

For `IS TRUE`, `IS FALSE`, and `IS NULL`,
the behavior of `IS` differs subtly from the `=` operator
in that the expression always evaluates to a boolean,
even if the left-hand-side of the operator is `MISSING`.
In other words, `MISSING IS FALSE` yields `FALSE`,
whereas `MISSING = FALSE` yields `MISSING`.

<!--
FIXME: explain behavior of IS NOT NULL
w.r.t. MISSING, since our behavior is
that MISSING IS NOT NULL -> FALSE,
which is helpful in terms of writing queries
but not the most intuitive...
-->

### Conditionals

#### `COALESCE`

`COALESCE` accepts one or more expressions
as arguments and yield the result of the
first expression that is neither `NULL` nor `MISSING`.
If none of the expressions produce a non-`NULL` value,
then `NULL` is returned.

`COALESCE(x, y)` is exactly equivalent to
`CASE WHEN x IS NOT NULL THEN x WHEN y IS NOT NULL THEN y ELSE NULL`.

#### `CASE`

`CASE` evaluates a series of conditional expressions
and returns the body following the first conditional
expression evaluating to `TRUE`.

Each arm of a `CASE` expression has the general form
`WHEN condition THEN consequent`, where `condition` and `consequent`
are the conditional and consequent expressions to be evaluated, respectively.

A `CASE` expression may end with an `ELSE` clause to indicate
the value to produce when none of the `condition` expressions evaluate to `TRUE`.

When no explicit `ELSE` clause is present in a `CASE`,
an implicit `ELSE MISSING` is inserted.

#### Simplified `CASE`

`CASE` variant given in the following format:

```sql
CASE expr
    WHEN val1 THEN result1
    WHEN val2 THEN result2
    ...
    WHEN valN THEN resultN
    ELSE default    -- optional
END
```

A case expression evaluates to the k-th result when `expr` equals
to k-th value. It is equivalent to a generic case:

```sql
CASE
    WHEN expr = val1 THEN result1
    WHEN expr = val2 THEN result2
    ...
    WHEN expr = valN THEN resultN
    ELSE default    -- optional
END
```

See [Postgres Conditional Expressions](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-CASE)

#### `NULLIF`

`NULLIF(a, b)` is exactly equivalent to
`CASE WHEN a = b THEN NULL ELSE a`.

### Bit Manipulation

#### `BIT_COUNT`

`BIT_COUNT(expr)` returns the number of bits set of `expr` casted to a
64-bit signed integer, or `MISSING` if `expr` is not of numeric type.

### Math Constants

#### `PI`

`PI()` returns the value of `π` as a double precision floating point.

### Math Functions

#### `ABS`

`ABS(expr)` returns the absolute value
of the expression `expr` if `expr` evaluates to a number;
otherwse, it returns `MISSING`.

#### `CBRT`

`CBRT(expr)` computes the cube root of its argument `expr`.

NOTE: This function is more precise than `POW(expr, 1.0 / 3.0)`.

#### `EXP`

`EXP(expr)` computes Euler's number raised to the given power `expr`.

#### `EXPM1`

`EXPM1(expr)` computes Euler's number raised to the given power `expr - 1`.

#### `EXP2`

`EXP2(expr)` computes `2` raised to the given power `expr`.

#### `EXP10`

`EXP10(expr)` computes `10` raised to the given power `expr`.

#### `HYPOT`

`HYPOT(xExpr, yExpr)` computes the square root of the sum of the squares of
`xExpr` and `yExpr`.

NOTE: this functions is more precise than `SQRT(xExpr * xExpr + yExpr * yExpr)`.

#### `LN`

`LN(expr)` computes the natural logarithm of `expr`.

#### `LN1P`

`LN1P(expr)` computes the natural logarithm of `expr + 1`.

#### `LOG`

`LOG()` function has two variants:

  - `LOG(expr)` computes the base-10 logarithm of `expr`
  - `LOG(baseExpr, numExpr)` computes `baseExpr` logarithm of `numExpr`

Compatibility notice: `LOG(expr)` (without a base) is a synonym of `LOG10(expr)`.
This is compatible with Postgres and SQLite, but incompatible with MySQL and others,
which compute natural logarithm instead. We recommend the explicit use of either
`LN(expr)` to compute the natural logarithm of `expr` or `LOG10(expr)` to compute the
base-10 logarithm of `expr`.

In addition, some SQL dialects have the order of `LOG(base, n)` arguments reversed.
For example SQL server uses `LOG(n, base)` instead. So always check the order of
the arguments when porting an existing SQL code to Sneller.

NOTE: At the moment `LOG(base, n)` is equivalent to `LOG2(n) / LOG2(base)`.

#### `LOG2`

`LOG2(expr)` computes the base-2 logarithm of `expr`.

#### `LOG10`

`LOG10(expr)` computes the base-10 logarithm of `expr`.

#### `POW` or `POWER`

`POW(baseExpr, expExpr)` computes the value of `baseExpr` raised to the given
power `expExpr`.

NOTE: `POWER(baseExpr, expExpr)` is a synonym of `POW(baseExpr, expExpr)`.

#### `SIGN`

`SIGN(expr)` returns -1 if `expr` evaluates
to a negative number, 0 if `expr` evaluates to 0,
1 if `expr` evaluates to a positive number, and
`MISSING` otherwise.

#### `SQRT`

`SQRT(expr)` returns the square root of
`expr` as long as `expr` evaluates to a number.
Otherwise, `SQRT(expr)` evaluates to `MISSING`.

### Trigonometric Functions

#### `DEGREES`

`DEGREES(expr)` converts radians in `expr` to degrees.

NOTE: at the moment the computation is equivalent to `(expr) * (180.0 / PI())`.

#### `RADIANS`

`RADIANS(expr)` converts degrees in `expr` to radians.

NOTE: at the moment the computation is equivalent to `(expr) * (PI() / 180.0)`.

#### `SIN`

`SIN(expr)` computes sine of `expr`.

#### `COS`

`COS(expr)` computes cosine of `expr`.

#### `TAN`

`TAN(expr)` computes tangent of `expr`.

#### `ASIN`

`ASIN(expr)` computes arc-sine of `expr`.

#### `ACOS`

`ACOS(expr)` computes arc-cosine of `expr`.

#### `ATAN`

`ATAN(expr)` computes arc-tangent of `expr`.

#### `ATAN2`

`ATAN2(yExpr, xExpr)` computes the angle in the plane between the positive
x-axis and the ray from `(0, 0)` to the point `(xExpr, yExpr)`.

### Rounding Functions

#### `ROUND`

The `ROUND(num)` function rounds a number to the nearest integer.
When `num` is exactly halfway between two integers, `ROUND` rounds
to the largest-magnitude integer.

Examples:

```sql
ROUND(42.4) -> 42
ROUND(42.8) -> 43
ROUND(-42.4) -> -42
ROUND(-42.8) -> -43
```

See [Postgres Math Functions](https://www.postgresql.org/docs/current/functions-math.html)

#### `ROUND_EVEN`

The `ROUND_EVEN(num)` function rounds a number to the nearest integer,
taking care to use the parity of the integer component of `num` to break
ties when `num` is exactly halfway between two integers.

```sql
ROUND_EVEN(1.5) -> 2 -- note: same result as ROUND()
ROUND_EVEN(2.5) -> 2 -- note: ROUND(2.5) -> 3
```

See [Postgres Math Functions](https://www.postgresql.org/docs/current/functions-math.html)

#### `TRUNC`

The `TRUNC(num)` function truncates a number to the
next-lowest-magnitude integer.

Examples:

```sql
TRUNC(42.4) -> 42
TRUNC(42.8) -> 42
TRUNC(-42.4) -> -42
TRUNC(-42.8) -> -42
```

See [Postgres Math Functions](https://www.postgresql.org/docs/current/functions-math.html)

#### `FLOOR`

The `FLOOR(num)` function rounds a number down to the
next integer less than or equal to `num`.

Examples:

```sql
FLOOR(42.4) -> 42
FLOOR(42.8) -> 42
FLOOR(-42.4) -> -43
FLOOR(-43.8) -> -43
```

See [Postgres Math Functions](https://www.postgresql.org/docs/current/functions-math.html)

#### `CEIL` or `CEILING`

The `CEIL(num)` function rounds a number to the next integer greater than or equal to `num`.

Examples:

```sql
CEIL(42.4) -> 43
CEIL(42.8) -> 43
CEIL(-42.4) -> -42
CEIL(-42.8) -> -42
```

NOTE: `CEILING(num)` is a synonym of `CEIL(num)`.

See [Postgres Math Functions](https://www.postgresql.org/docs/current/functions-math.html)

### GEO Functions

#### `GEO_DISTANCE`

`GEO_DISTANCE(lat1, long1, lat2, long2)` calculates the distance between
two latitude and longitude points and returns the result in meters. The
result is an approximation that uses a Haversine formula, which determines
the great-circle distance between two points on a sphere.

External Resources:

  - [Haversine formula](https://en.wikipedia.org/wiki/Haversine_formula)

#### `GEO_HASH`

`GEO_HASH(lat, long, num_chars)` encodes a string representing a geo-hash
of the latitude `lat` and longitude `long` having `num_chars` characters.
Each `GEO_HASH` character encodes 5 bits of interleaved latitude and
longitude. When the number of characters is even the count of latitude
and longitude bits is the same; when it's odd, latitude has one bit less
than longitude.

`GEO_HASH()` is just a hash calculated from scaled latitude and longitude
coordinates; it doesn't project the coordinates in any way.

The `num_chars` parameter's range is 1 to 12. Out of range parameters are
automatically clamped to a valid range. For example `GEO_HASH(a, b, 100)`
would produce the same result as `GEO_HASH(a, b, 12)`.

Forwards compatibility notice: At the moment the maximum precision of
`GEO_HASH()` is 12 characters, which represents 60 bits of interleaved
latitude and longitude values. We may increase the range of `num_chars` in
the future, so please always specify the precision and do not rely on
parameter clamping.

External resources:

  - https://en.wikipedia.org/wiki/Geohash provides insight into geo-hash
    encoding

#### `GEO_TILE_X` and `GEO_TILE_Y`

`GEO_TILE_X(long, precision)` and `GEO_TILE_Y(lat, precision)` functions
calculate the corresponding X and Y tiles for the given `lat`, `long`
coordinates and the specified `precision`. The `precision` is sometimes
called zoom and specifies the number of bits.

The latitude and longitude coordinates are first projected by using Mercator
function and then X and Y cell indexes are calculated by quantizing the
projected coordinates into the given `precision`, which specifies the
number of bits of each value. For example precision of 8 bits would produce
values within a [0, 255] range.

The `precision` parameter will be clamped into a [0, 32] range, where 0
means 0 bits (both output tiles will be 0/0) and 32 means 32 bits for
both X and Y, which describes a tile around 3x3 cm.

Forwards compatibility notice: At the moment the maximum precision of
`GEO_TILE_X()` and `GEO_TILE_Y()` is 32 bits, which is slightly more
than ElasticSearch, which limits the precision to 29 bits. We may
increase the range of `precision` in the future, so please always
specify the precision and do not rely on parameter clamping.

External resources:

  - https://en.wikipedia.org/wiki/Mercator_projection provides insight
    into Mercator projection

  - https://en.wikipedia.org/wiki/Tiled_web_map provides insight into
    geo tiling, our implementation is desiged to be compatible

#### `GEO_TILE_ES`

`GEO_TILE_ES(lat, long, precision)` does the same projection as the
`GEO_TILE_X(long. precision)` and `GEO_TILE_Y(lat, precision)` functions.
The `precision` has the same restriction and the output X and Y coordinates
are the same. What `GEO_TILE_ES()` does differently is the output encoding.

`GEO_TILE_ES(lat, long, precision)` encodes a string representing a cell of
a map tile in a "precision/x/y" format, which is compatible with ElasticSearch
geotile aggregation.

See `GEO_TILE_X()` and `GEO_TILE_Y()` functions for more details.

### Built-in Functions

#### `DATE_ADD`

`DATE_ADD(part, num, time)` adds `num` of the unit `part`
to the timestamp `time`.

`part` can be one of the following keywords:

 - `MICROSECOND` or `MICROSECONDS`
 - `MILLISECOND` or `MILLISECONDS`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `WEEK`
 - `MONTH`
 - `QUARTER`
 - `YEAR`

See [Presto Timestamp functions](https://prestodb.io/docs/0.217/functions/datetime.html)

#### `DATE_DIFF`

`DATE_DIFF(part, from, to)` determines the difference
between `from` and `to` in terms of the date interval `part`.

`part` can be one of the following keywords:

 - `MICROSECOND` or `MICROSECONDS`
 - `MILLISECOND` or `MILLISECONDS`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `WEEK`
 - `MONTH`
 - `QUARTER`
 - `YEAR`

See [Presto Timestamp functions](https://prestodb.io/docs/0.217/functions/datetime.html)

#### `DATE_TRUNC`

`DATE_TRUNC(part, expr)` truncates a timestamp to the specified precision.

`part` can be one of the following keywords:

 - `MICROSECOND` or `MICROSECONDS`
 - `MILLISECOND` or `MILLISECONDS`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `WEEK(SUNDAY|MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY)`
 - `MONTH`
 - `QUARTER`
 - `YEAR`

`DATE_TRUNC()` returns a timestamp that contains only
the components of the timestamp `expr` that are less precise
than the precision given by `part`. In other words, `DATE_TRUNC(SECOND, x)`
truncates the timestamp `x` down to the nearest second.

`DATE_TRUNC()` also allows to truncate a date to a particular day of week.
Use `DATE_TRUNC(WEEK(WEEKDAY))` to truncate a date to `SUNDAY`, `MONDAY`,
`TUESDAY`, `WEDNESDAY`, `THURSDAY`, `FRIDAY`, or `SATURDAY`.

(It can be useful to use the result of a `DATE_TRUNC()` expression
as a group value in `GROUP BY` in order to build a histogram
with buckets corresponding to calendar dates.)

#### `EXTRACT`

`EXTRACT(part FROM expr)` extracts part of a date from a timestamp.

`part` can be one of the following keywords:

 - `MICROSECOND` or `MICROSECONDS` (the result includes seconds)
 - `MILLISECOND` or `MILLISECONDS` (the result includes seconds)
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `DOW` (day of week in [0-6] range where 0 represents Sunday)
 - `DOY` (day of year in [1-366] range)
 - `MONTH`
 - `QUARTER`
 - `YEAR`

`EXTRACT` yields the integer corresponding to the requested
date part, or `MISSING` if `expr` does not evaluate to a timestamp.

#### `UTCNOW`

`UTCNOW()` evaluates to the timestamp value
representing the time at which the query was parsed.

(During query parsing, the `UTCNOW()` expression
is simply replaced with a timestamp literal containing the current time.)

#### `LEAST` and `GREATEST`

`LEAST(x, ...)` and `GREATEST(x, ...)` accept one or more
numeric arguments and yields the smallest (largest)
of their arguments. If none of the arguments
are numbers, `MISSING` is returned.

#### `WIDTH_BUCKET`

The expression `WIDTH_BUCKET(num, lo, hi, count)`
takes the number `num` and assigns it to one of `count` (integer)
buckets in an equidepth histogram along the range from
the numbers `lo` to `hi`. The behavior is intended to
match the `width_bucket()` function from Postgres.

A typical use of `WIDTH_BUCKET` is to produce a
bucket value for use in a `GROUP BY` clause.

See [Postgres Math Functions](https://www.postgresql.org/docs/current/functions-math.html)

#### `TIME_BUCKET`

The expression `TIME_BUCKET(time, interval)`
takes the timestamp `time` and assigns it a bucket
based on the integer `interval`, which specifies a bucket
width in seconds. The returned bucket is an integer representing
the seconds elapsed since the Unix epoch for the associated bucket.

The expression `TIME_BUCKET(time, interval)` is mathematically equivalent to
`TO_UNIX_EPOCH(time) - (TO_UNIX_EPOCH(time) % interval)`.

A typical use of `TIME_BUCKET` is to produce a
bucket value for use in a `GROUP BY` clause.

#### `TO_UNIX_EPOCH`

`TO_UNIX_EPOCH(expr)` converts a timestamp value
into a signed integer representing the number of
seconds elapsed since the Unix epoch, or `MISSING`
if `expr` is not a timestamp.

#### `TO_UNIX_MICRO`

`TO_UNIX_MICRO(expr)` converts a timestamp value
into a signed integer representing the number
of microseconds elapsed since the Unix epoch,
or `MISSING` if `expr` is not a timestamp.

#### `TRIM`, `LTRIM`, and `RTRIM`

The `TRIM` function has two forms.
The single-argument form `TRIM(str)` yields a substring
of `str` with leading and trailing spaces removed.

*Known limitations: only ASCII white-space characters
are considered: ' ', '\t', '\r', '\n', '\v', and '\f'. Other
non-ASCII white-spaces such as U+0085 (next line) are not
considered.*

The two-argument form `TRIM(str, cutset)` yields
a substring of `str` with characters in the string `cutset`
removed from the leading and trailing characters in `str`.

The `LTRIM` and `RTRIM` variants of the `TRIM` function
trim only from the left- or right-hand-side of the input string,
respectively. Both `LTRIM` and `RTRIM` support the single- and
two-argument forms of `TRIM`.

There is also support for more verbose `TRIM` syntax available
in other SQL engines:

- `TRIM(cutset FROM str)` or `TRIM(BOTH cutset FROM str)` are both
  equivalent to `TRIM(str, cutset)`;
- `TRIM(LEADING cutset FROM str) is equivalent to `LTRIM(str, cutset);
- `TRIM(TRAILING cutset FROM str) is equivalent to `RTRIM(str, cutset).

*Known limitations: the `cutset` string must be a constant
string of four or fewer ASCII characters.*

Examples:
```sql
-- one-argument form
TRIM(' xyz ') -> 'xyz'
RTRIM(' xyz ') -> ' xyz'
LTRIM(' xyz ') -> 'xyz '

-- two-argument form
TRIM('\r\nline\r\n', '\r\n') -> 'line'
RTRIM('\r\nline\r\n', '\r\n') -> '\r\nline'
LTRIM('\r\nline\r\n', '\r\n') -> 'line\r\n'
```

#### `SIZE`

`SIZE(expr)` returns one of the following:

 - If `expr` is a list, the length of that list as an integer
 - If `expr` is a struct, the number of fields in that struct
 as an integer
 - Otherwise, `MISSING`

#### `CHAR_LENGTH` or `CHARACTER_LENGTH`

`CHAR_LENGTH(str)` (or, alternatively, `CHARACTER_LENGTH(str)`)
returns the number of Unicode points in a string as an integer.

#### `LOWER` and `UPPER`

`LOWER(str)` and `UPPER(str)` changes case of letters from the
input string.

Examples:

```sql
LOWER('SnElLeR') -- returns 'sneller'
UPPER('SnElLeR') -- returns 'SNELLER'
```

#### `EQUALS_CI`

`EQUALS_CI(str, constant_str)` compares case-sensitive
a string expression with a **constant** string.

Example:

```sql
SELECT * FROM table WHERE EQUALS_CI(status, 'IDLE')
```

Note: `EQUALS_CI` is an optimized implementation of
expression `LOWER(str) == LOWER(constant_str)`.

#### `SUBSTRING`

`SUBSTRING` extracts a substring from the input string.
The function accepts two forms:

1. `SUBSTRING(str, start, length)` - substring is
   described with the starting position (counted from 1)
   and length.

2. `SUBSTRING(str, start)` - substring is denoted
   with the starting position and spans to the string's end.

If `start` is negative or is larger than the length of `str`,
then the result is an empty string.

If `start` + `length` is larger than the length of `str`,
then the output is trimmed to the length of `str`. Likewise,
when `length` is zero or negative.

Examples:

```sql
SUBSTRING('kitten', 1)      -- returns 'kitten'
SUBSTRING('kitten', 3)      -- returns 'ten'

SUBSTRING('kitten', 3, 2)   -- returns 'tt'
SUBSTRING('kitten', 3, -1)  -- returns 'ten'
SUBSTRING('kitten', -1, 20) -- returns ''
```

#### `SPLIT_PART`

The expression `SPLIT_PART(str, sep, n)`
computes the `n`th substring of `str` by
splitting `str` on `sep`. The index `n` is 1-indexed.

For example, `SPLIT_PART('foo\nbar\n', '\n' 1)`
evaluates to `'foo'`, and `SPLIT_PART('foo\nbar\n', '\n', 2)`
evaluates to `'bar'`.

If the index `n` exceeds the number of substrings
produced by splitting `str` on `sep`, then `''` is returned.
If `n` evaluates to an integer less than or equal to zero,
then `MISSING` is returned.

*Known limitation: the separator string `sep`
must be a single-character ASCII string constant excluding
the NUL ASCII character*

See [Postgres string functions](https://www.postgresql.org/docs/current/functions-string.html).

#### `IS_SUBNET_OF`

The `IS_SUBNET_OF` function has two forms;
the three-argument form `IS_SUBNET_OF(start, end, str)`
returns a boolean indicating if `str` is an IPv4 address
in dotted notation that fits in the range from `start` to `end`,
and the two-argument form `IS_SUBNET_OF(cidr, str)` returns
a boolean indicating if `str` is an IPv4 address that belongs
to the subnet `cidr` in CIDR address notation.

Examples:
```sql
-- three-argument form
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.3') -> TRUE
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.4') -> TRUE
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.5') -> TRUE
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.6') -> FALSE

-- two-argument form
IS_SUBNET_OF('128.1.2.3/24', '128.1.2.4') -> TRUE
IS_SUBNET_OF('128.1.2.3/24', '128.1.2.3') -> TRUE
IS_SUBNET_OF('128.1.2.3/24', '128.1.3.0') -> FALSE
```

*Known limitation: the `start` and `end` strings in the three-argument form
and the `cidr` string in the two-argument form must be constant strings.*

#### `EQUALS_FUZZY`, `EQUALS_FUZZY_UNICODE`
Fuzzy String Matching using
[Damerau-Levenshtein distance](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance)
calculation. The `EQUALS_FUZZY` function determines whether a data string equals a provided string literal
if the data string can be transformed into the string literal with less or equal number
of edits. Stated differently, the distance between the data and the string literal
is the minimal number of edits, and if the number of edits does not exceed some
threshold, the two strings are considered to have a *fuzzy match*.
The Damerau–Levenshtein distance considers *insertions*, *deletions*, *substitutions* of
single characters, and *transpositions* of two adjacent characters.

The unicode variant `EQUALS_FUZZY_UNICODE` treats strings as UTF8 strings, while the
`EQUALS_FUZZY` treats strings as byte sequences. Substitutions of
equal *ASCII* characters but with different casing have an edit distance of zero, but
substitutions of unicode code-points with different casing have an edit distance of one.

Examples:
```sql
-- string literal 'cache' is treated a byte sequence
EQUALS_FUZZY('Cash', 'cache', 1) -> FALSE
EQUALS_FUZZY('Cash', 'cache', 2) -> TRUE
EQUALS_FUZZY('Cash', 'cache', 3) -> TRUE

-- string literal 'Straße' is treated a unicode sequence
EQUALS_FUZZY_UNICODE('strasse', 'Straße', 1) -> FALSE
EQUALS_FUZZY_UNICODE('strasse', 'Straße', 2) -> TRUE

-- string literal 'Straße' is treated a byte sequence
-- (note that `ß` is a 2-byte sequence)
EQUALS_FUZZY('strasse', 'Straße', 1) -> FALSE
EQUALS_FUZZY('strasse', 'Straße', 2) -> TRUE
```

The calculated *edit distance* between two strings is an *estimation* of the true
Damerau–Levenshtein distance. A benefit of this estimation is that the `EQUALS_FUZZY` has a
runtime complexity comparable to a case-insensitive string compare; a downside is that some
distances are overestimated.

The following pseudocode illustrates how estimations are obtained. Two strings (`DATA`, `NEEDLE`)
are compared from left to right three bytes (or unicodes) at the time.
While comparing, the number of edits is accumulated, and once this number exceed a
provided threshold, the function yields false. The first three characters `D0`, `D1`, and `D2` from `DATA`,
and the first three characters `N0`, `N1`, and `N2` from `NEEDLE` are compared in the following fashion.
If either data or needle does not have 1, 2 or 3 characters, take surrogate values `0xFF`, or
`0xFFFFFFFF` for the unicode variant.

With increasing complexity, first the 1 character approximation, 2 character and finally 3 character
approximation. Note that if the estimation function considers only one character, that is,
`D0` and `N0` the estimation would implement a Manhattan distance function:

Estimate Damerau–Levenshtein distance with *one* characters lookahead
``` text
WHILE (DATA not empty) OR (NEEDLE not empty) DO
    D0 := DATA[0]
    N0 := NEEDLE[0]

    IF (D0==N0) // the first characters match
    THEN editDistance += 0; advanceData += 1; advanceNeedle += 1
    ELSE // substitution in all remaining situations:
        editDistance += 1; advanceData += 1; advanceNeedle += 1
    ENDIF
ENDDO
```

If the estimation function considers two characters, that is `D0`, `D1`, `N0`
and `N1`, the estimation allows single insertions, deletions, and transpositions
but would still estimate some edit distances wrongly. For example, two consecutive
deletions would not be recognized but would be considered two
substitutions which may result in a full mismatch of the remaining string.

Estimate Damerau–Levenshtein distance with *two* characters lookahead
```
WHILE (DATA not empty) OR (NEEDLE not empty) DO
    D0 := DATA[0]
    N0 := NEEDLE[0]

    IF (D0==N0) // the first characters match
    THEN editDistance += 0; advanceData += 1; advanceNeedle += 1
    ELSE
        D1 := DATA[1]
        N1 := NEEDLE[1]

        // character is deleted in data
        IF (D1!=N0) && (D0==N1)
        THEN editDistance += 1; advanceData += 1; advanceNeedle += 2
        ENDIF

        // character is inserted in data
        IF (D1==N0) && (D0!=N1)
        THEN editDistance += 1; advanceData += 2; advanceNeedle += 1
        ENDIF

        // characters are transposed in data
        IF (D1==N0) && (D0==N1)
        THEN editDistance += 1; advanceData += 2; advanceNeedle += 2
        ENDIF

        // all remaining situations: character is substituted in data
        IF (D1!=N0) && (D0!=N1)
        THEN editDistance += 1; advanceData += 1; advanceNeedle += 1
        ENDIF
    ENDIF
ENDDO
```
Finally, estimation of Damerau–Levenshtein distance in the fuzzy matcher
uses *three* character lookahead.

```
WHILE (DATA not empty) OR (NEEDLE not empty) DO
    D0 := DATA[0]
    N0 := NEEDLE[0]

    IF (D0==N0) // the first characters match
    THEN editDistance += 0; advanceData += 1; advanceNeedle += 1
    ELSE
        D1 := DATA[1]
        D2 := DATA[2]
        N1 := NEEDLE[1]
        N2 := NEEDLE[2]

        // two characters are transposed in data
        IF (N0!=D0) && (N0==D1) && (N1==D0)
        THEN editDistance += 1; advanceData += 2; advanceNeedle += 2
        ENDIF

        // one character is deleted in data
        IF (N0!=D0) && (N0!=D1) && (N1==D0) && (N2==D1)
        THEN editDistance += 1; advanceData += 1; advanceNeedle += 2
        ENDIF

        // two characters are deleted in data:
        IF (N0!=D0) && (N1!=D1) && (N2!=D2) && (N2==D0) && (N0!=D1) && (N1!=D2) && (N1!=D0) && (N2!=D1) && (N0!=D2)
        THEN editDistance += 2; advanceData += 1; advanceNeedle += 3
        ENDIF

        // one character is inserted in data
        IF (N0!=D0) && (N0==D1) && (N1==D2) && (N1!=D0)
        THEN editDistance += 1; advanceData += 2; advanceNeedle += 1
        ENDIF

        // two characters are insearted in data
        IF (N0!=D0) && (N1!=D1) && (N2!=D2) && (N2!=D0) && (N0!=D1) && (N1!=D2) && (N1!=D0) && (N2!=D1) && (N0==D2)
        THEN editDistance += 2; advanceData += 3; advanceNeedle += 1
        ENDIF

        // transposition and insertion ab -> bca:
        IF (N0!=D0) && (N1!=D1) && (N2!=D2) && (N2!=D0) && (N0!=D1) && (N1!=D2) && (N1==D0) && (N2!=D1) && (N0==D2)
        THEN editDistance += 2;; advanceData += 3; advanceNeedle += 2
        ENDIF

        // substitution in all remaining situations:
        ELSE editDistance += 1; advanceData += 1; advanceNeedle += 1
    ENDIF
ENDDO
```

This method calculates either the true Damerau–Levenshtein distance or the method overestimates.
The estimation is the result of a three character horizon: the method
cannot look beyond these three characters, and cannot foresee which edit to choose such that the
*smallest* edit distance is found. In the following example the above method chooses a substitution
and this would give an edit distance of 6, while a deletion at position 0, 1, and 2 gives a smaller
edit distance of 3. A seven character horizon would have been needed to foresee that, in this
specific situation, deletions should be preferred over substitutions.

 Example of an overestimation of distance 6 while true Damerau–Levenshtein is 3
```
data:   "aaaaaa"
needle: "bbbaaaaaa"
```

#### `CONTAINS_FUZZY`, `CONTAINS_FUZZY_UNICODE`

The `CONTAINS_FUZZY` function is similar to the `EQUALS_FUZZY` function.
Instead of determining whether a string *equals* a provided string literal
with less (or equal) number of edits, `CONTAINS_FUZZY` determines whether
a string *contains* a provided string literal with less (or equal) number of
edits. The complexity is comparable to a case-insensitive string contains function.

```sql
CONTAINS_FUZZY('The quick brown foks jums over the lazy dog', 'Fox Jumps', 3) -> TRUE
```

#### `CAST`

`CAST` allows to convert an arbitrary expression into
equivalent expression of given type. The syntax is

```sql
CAST(expr AS type)
```

Known types are:

* `MISSING` (forcibly removes a column from the result),
* `NULL`,
* `STRING`,
* `INTEGER`,
* `FLOAT`,
* `BOOLEAN`,
* `TIMESTAMP`,
* `STRUCT`,
* `LIST`,
* `DECIMAL`,
* `SYMBOL`.

The only implemented conversions are:

* `INTEGER` -> `BOOLEAN`;
* `INTEGER` -> `FLOAT`;
* `INTEGER` -> `STRING`;
* `FLOAT` -> `INTEGER`;
* `FLOAT` -> `BOOLEAN`;
* `BOOLEAN` -> `INTEGER`;
* `BOOLEAN` -> `FLOAT`.
* `BOOLEAN` -> `STRING`.

Any other conversions yield `MISSING`.

#### `TYPE_BIT`

The `TYPE_BIT` function produces an integer
that represents the JSON type of its argument.
Each bit in the resulting integer is reserved
for one specific type, which allows the results
of `TYPE_BIT` to be aggregated together with
`BIT_OR` to produce a bitmask representing all
the possible types of a value.
`TYPE_BIT` produces `0` if its argument is `MISSING`.

The return values for `TYPE_BIT` are as follows:

 - 0 : Missing
 - 1 : Null
 - 2 : Boolean
 - 4 : Number
 - 8 : Timestamp
 - 16 : String
 - 32 : List
 - 64 : Struct

#### `TABLE_GLOB` and `TABLE_PATTERN`

`TABLE_GLOB(path)` and `TABLE_PATTERN(path)` can be
used in the `FROM` position of a `SELECT` statement to
execute a query across multiple tables with names
matching the given shell pattern or regular expression
respectively.

Path segments used as patterns will generally need to
be enclosed in double-quotes to avoid special
characters causing syntax errors, for example:
```sql
SELECT * FROM TABLE_GLOB("table[0-9][0-9]")
SELECT * FROM TABLE_PATTERN("(access|error)_logs")
```
`TABLE_GLOB` and `TABLE_PATTERN` can be used to search
within a particular database by including the database
name as the first segment of the path argument, for
example:
```sql
-- searching within a database named "db"
SELECT * FROM TABLE_GLOB(db."*_logs")
```

*Note: `TABLE_GLOB` and `TABLE_PATTERN` cannot be used
to match the database portion of the path, only the
table name.*
