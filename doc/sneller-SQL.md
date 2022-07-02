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

sfw_query = 'SELECT' [ 'DISTINCT' ] ('*' | binding_list) [ from_clause ] [ where_clause ] [ group_by_clause ] [ order_by_clause ] [ limit_clause ] ;

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

expr = compare_expr | arith_expr | in_expr | case_expr | like_expr |
       is_expr | not_expr | function_expr | subquery_expr |
       between_expr | path_expr |
       integer | string | float | timestamp;

subquery_expr = '(' sfw_query ')' ;
like_expr = expr ('LIKE' | 'ILIKE') string ;
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

case_expr = 'CASE' { 'WHEN' expr 'THEN' expr } [ 'ELSE' expr ] 'END' ;
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

In particular, the ordering operators `<`, `>`, etc.
only operate on numbers, not on timestamps as they do
in many SQL systems. We don't overload these operators
in order to avoid having to resolve the situation where
two supported values (a number and a timestamp) are present
on either side of the comparison, but the result is still
`MISSING` because there isn't a good way to compare those two values.

For ordering timestamp values, we have a built-in function
called `BEFORE()` that returns whether its arguments are
strictly ordered in time with respect to one another.
In other words, `BEFORE(a, b)` returns `TRUE` if `a` is before `b`,
and `FALSE` otherwise (or `MISSING` if one of the arguments isn't a timestamp).

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

Path expressions are used to dereferenced sub-values
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
to occur alongside any other aggregation expressions in
a `SELECT` clause. We implement `COUNT(DISTINCT expr)`
by rewriting the query into a compound query that uses
`SELECT DISTINCT` and `COUNT`.

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

### `BOOL_AND`

`BOOL_AND(expr)` computes bitwise AND of all results produced by
evaluating `expr` for each row coerced to a boolean type. If `expr`
never evaluates to a boolean, `BOOL_AND(expr)` yields `NULL`.

### `BOOL_OR`

`BOOL_OR(expr)` computes bitwise OR of all results produced by
evaluating `expr` for each row coerced to a boolean type. If `expr`
never evaluates to a boolean, `BOOL_OR(expr)` yields `NULL`.

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


#### `LIKE` and `ILIKE`

The `LIKE` operator matches a string value
against a pattern. The pattern on the right-hand-side
of `LIKE` must be a literal string.
The `%` character within the pattern matches
zero or more characters, and the `_` character
matches exactly one Unicode point. All other characters
in the pattern string match only themselves.

For example:

```sql
SELECT *
FROM table
WHERE message_body LIKE '%foo%'
```

The query above will return all the records
from `table` that have a string-typed `message_body`
field that contains the sub-string `'foo'`.

The `ILIKE` operator works identically to `LIKE`,
except that individual character matches are case-insensitive.
(Since Sneller SQL is Unicode-aware, characters are compared
using Unicode "Simple Case Folding" rules.)

#### `IN`

The `IN` operator matches a value against a list of values.

For example:
```
SELECT * FROM table WHERE val IN (3, 'foo', NULL)
```
The query above returns all the rows in `table`
for which the field `val` is the number 3, the string `'foo'`,
or the value `NULL`.

The `IN` operator can also accept a subquery
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
  - `LOG(baseExpr, numExpr)` combutes `baseExpr` logarithm of `numExpr`

Compatibility notice: `LOG(expr)` (without a base) is a synonym of `LOG10(expr)`.
This is compatible with Postgres and SQLite, but incompatible with MySQL and others,
which compute natural logarithm instead. We recommend the explicit use of either
`LN(expr)` to compute the natural logaritm of `expr` or `LOG10(expr)` to compute the
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

`ASIN(expr)` computes arcsine of `expr`.

#### `ACOS`

`ACOS(expr)` computes arccosine of `expr`.

#### `ATAN`

`ATAN(expr)` computes arctangent of `expr`.

#### `ATAN2`

`ATAN2(yExpr, xExpr)` computes the angle in the plane between the positive
x-axis and the ray from `(0, 0)` to the point `(xExpr, yExpr)`.

### Rounding Functions

#### `ROUND`

The `ROUND(num)` function rounds a number to the nearest integer.
When `num` is exactly half way between two integers, `ROUND` rounds
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

```
ROUND_EVEN(1.5) -> 2 # note: same result as ROUND()
ROUND_EVEN(2.5) -> 2 # note: ROUND(2.5) -> 3
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

#### `CEILING`

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

 - `MICROSECOND`
 - `MILLISECOND`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `MONTH`
 - `YEAR`

See [Presto Timestamp functions](https://prestodb.io/docs/0.217/functions/datetime.html)

#### `DATE_DIFF`

`DATE_DIFF(part, from, to)` determines the difference
between `from` and `to` in terms of the date interval `part`.

`part` can be one of the following keywords:

 - `MICROSECOND`
 - `MILLISECOND`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `MONTH`
 - `YEAR`

See [Presto Timestamp functions](https://prestodb.io/docs/0.217/functions/datetime.html)

#### `DATE_TRUNC`

`DATE_TRUNC(part, expr)` truncates a timestamp to the specified precision.

`part` can be one of the following keywords:

 - `MICROSECOND`
 - `MILLISECOND`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `MONTH`
 - `YEAR`

`DATE_TRUNC()` returns a timestamp that contains only
the components of the timestamp `expr` that are less precise
than the precision given by `part`. In other words, `DATE_TRUNC(SECOND, x)`
truncates the timestamp `x` down to the nearest second.

(It can be useful to use the result of a `DATE_TRUNC()` expression
as a group value in `GROUP BY` in order to build a histogram
with buckets corresponding to calendar dates.)

#### `EXTRACT`

`EXTRACT(part FROM expr)` extracts part of a date from a timestamp.

`part` can be one of the following keywords:

 - `MICROSECOND`
 - `MILLISECOND`
 - `SECOND`
 - `MINUTE`
 - `HOUR`
 - `DAY`
 - `MONTH`
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

See [Postgres Math Functions](https://www.postgresql.org/docs/9.1/functions-math.html)

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

#### `BEFORE`

`BEFORE(x, ...)` accepts one or more expressions
that evaluate to timestamps and returns `TRUE` if
the expressions yield times that are ordered.
If the arguments to `BEFORE` are out-of-order, then
it returns `FALSE`. If one or more of the arguments
are not timestamps, then it returns `MISSING`.

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

The two-argument form `TRIM(str, cutset)` yields
a substring of `str` with characters in the string `cutset`
removed from the leading and trailing characters in `str`.

The `LTRIM` and `RTRIM` variants of the `TRIM` function
trim only from the left- or right-hand-side of the input string,
respectively. Both `LTRIM` and `RTRIM` support the single- and
two-argument forms of `TRIM`.

*Known limitations: the `cutset` string must be a constant
string of four or fewer characters.*

Examples:
```
# one-argument form
TRIM(' xyz ') -> 'xyz'
RTRIM(' xyz ') -> ' xyz'
LTRIM(' xyz ') -> 'xyz '

# two-argument form
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

<!---
Document me!
-->

#### `EQUALS_CI`

<!---
Document me!
-->

#### `SUBSTRING`

<!---
Document me!
-->

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
must be a single-character string constant.*

See [Postgres string functions](https://www.postgresql.org/docs/9.1/functions-string.html).

#### `IS_SUBNET_OF`

The `IS_SUBNET_OF` function has two forms;
the three-argument form `IS_SUBNET_OF(start, end, str)`
returns a boolean indicating if `str` is an IPv4 address
in dotted notation that fits in the range from `start` to `end`,
and the two-argument form `IS_SUBNET_OF(cidr, str)` returns
a boolean indicating if `str` is an IPv4 address that belongs
to the subnet `cidr` in CIDR address notation.

Examples:
```
# three-argument form
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.3') -> TRUE
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.4') -> TRUE
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.5') -> TRUE
IS_SUBNET_OF('128.1.2.3', '128.1.2.5', '128.1.2.6') -> FALSE

# two-argument form
IS_SUBNET_OF('128.1.2.3/24', '128.1.2.4') -> TRUE
IS_SUBNET_OF('128.1.2.3/24', '128.1.2.3') -> TRUE
IS_SUBNET_OF('128.1.2.3/24', '128.1.3.0') -> FALSE
```

*Known limitation: the `start` and `end` strings in the three-argument form
and the `cidr` string in the two-argument form must be constant strings.*

#### `CAST`

<!---
Document me!
-->

#### `TABLE_GLOB` and `TABLE_PATTERN`

`TABLE_GLOB(path)` and `TABLE_PATTERN(path)` can be
used in the `FROM` position of a `SELECT` statement to
execute a query across multiple tables with names
matching the given shell pattern or regular expression
respectively.

Path segments used as patterns will generally need to
be enclosed in double-quotes to avoid special
characters causing syntax errors, for example:
```
SELECT * FROM TABLE_GLOB("table[0-9][0-9]")
SELECT * FROM TABLE_PATTERN("(access|error)_logs")
```
`TABLE_GLOB` and `TABLE_PATTERN` can be used to search
within a particular database by including the database
name as the first segment of the path argument, for
example:
```
# searching within a database named "db"
SELECT * FROM TABLE_GLOB(db."*_logs")
```

*Note: `TABLE_GLOB` and `TABLE_PATTERN` cannot be used
to match the database portion of the path, only the
table name.*
