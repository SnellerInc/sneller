// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

/*
Package sort contains low-level procedures that implement `ORDER BY`
queries execution.


Overview

Sorting handles different sorting directions ('ASC' or 'DESC') as well
as placing NULL/MISSING values in output ('NULLS FIRST', 'NULLS LAST').

Sorting follows the PartiQL spec, section "12.2 The PartiQL order-by
less-than function" (https://partiql.org/assets/PartiQL-Specification.pdf).
Data types are ordered as follows:

* false,
* true,
* numeric (precision does not matter),
* timestamp,
* string,
* binary data (blob/clob),
* array,
* struct.


Limitations

1. Sorting requires that symbol tables in all input chunks are exactly the same.

2. Sorting is meant to be the last step of a whole query. Sorting, if present,
is executed exactly once. This is the planner responsibility to get rid of
any 'ORDER BY' clauses from nested queries.

Sorting does not perform any projection, it outputs Ion rows without
modification. A query like:

    SELECT name, surname, age FROM users ORDER BY surname, name

is expected to be rewritten into the following form:

    SELECT * FROM (SELECT name, surname, age FROM users) ORDER BY surname, name

3. Floating point numbers should be ordered as follows:

- NaN,
- -infinity.
- normalized/denormalized numbers,
- +infinity.

The Go comparison operator returns true for NaN < non-NaN, there's no
workaround for this in our code.


Design

There are three major procedures:

1. multi-column sorting (SELECT * FROM users ORDER BY city, surname, name)
2. single column sorting (SELECT * FROM users ORDER BY last_login DESC)
3. k-top sorting (SELECT * FROM table ORDER BY column LIMIT value)

The generic multi-column sorting uses a parallel variant of quicksort.  It
sorts arbitrary tuples having values of arbitrary types.  A tuple comparator
decodes Ion values lazily. The generic sorting also takes into account the
query limits (set with `LIMIT` and `OFFSET` clauses) and avoids unnecessary
work when possible.

The single column sorting caches the key values. First it splits them into
different types: null, bool, int [zero, negative, positive], float, string
and timestamp. Then it runs specialized routines for each data type, that sort
pairs (key, row ID).

There are specializations for ints, floats and timestamps based on an
AVX512-assisted quicksort, which is also multithreaded.  They are generated
with `go generate` from code templates stored in `_generate` directory.
Note that these sorters are guaranteed to fully sort only the indices array;
they keep intact portions of the key array. Details can be found in
intfloat.in/avx512/counting-sort.go.in.

Ktop sorting returns the k firstrows from a sorted collection. It is much
faster and scales better than generic sorting routines for reasonably small
k values. The upper value of k was experimentally estimated to approx 10k
rows.
*/
package sorting
