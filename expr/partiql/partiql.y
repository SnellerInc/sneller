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
%{
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

package partiql

import (
    "strings"

    "github.com/SnellerInc/sneller/expr"
)
%}

%union {
    bytes    []byte
    str      string
    yesno    bool
    integer  int
    exprint  *expr.Integer
    expr     expr.Node
    order    expr.Order
    sel      *expr.Select
    selinto  selectWithInto
    wind     *expr.Window
    bind     expr.Binding
    jk       expr.JoinKind
    from     expr.From
    with     []expr.CTE
    bindings []expr.Binding
    limbs    []expr.CaseLimb
    values   []expr.Node
    orders   []expr.Order
    unions   []unionItem
}

%token ERROR EOF
%left UNION
%token SELECT FROM WHERE GROUP ORDER BY HAVING LIMIT OFFSET WITH INTO EXPLAIN
%token DISTINCT ALL AS EXISTS NULLS FIRST LAST ASC DESC UNPIVOT AT
%token PARTITION
%token VALUE
%token LEADING TRAILING BOTH
%right COALESCE NULLIF EXTRACT DATE_TRUNC
%right CAST UTCNOW
%right DATE_ADD DATE_DIFF EARLIEST LATEST
%left JOIN LEFT RIGHT CROSS INNER OUTER FULL
%left ON
%left APPROX_COUNT_DISTINCT
%token <integer> AGGREGATE
%token <str> ID
%token <empty> '(' ',' ')' '[' ']' '{' '}'
%token <empty> NULL TRUE FALSE MISSING

%left OR
%left AND
%right '!' '~' NOT
%left BETWEEN CASE WHEN THEN ELSE END TO TRIM
%left <empty> EQ NE LT LE GT GE
%left <empty> SIMILAR REGEXP_MATCH_CI ILIKE LIKE IN IS OVER FILTER ESCAPE
%left <empty> '|'
%left <empty> '^'
%left <empty> '&'
%left <empty> SHIFT_LEFT_LOGICAL SHIFT_RIGHT_ARITHMETIC SHIFT_RIGHT_LOGICAL
%left <empty> '+' '-'
%left <empty> '*' '/' '%'
%left <empty> CONCAT APPEND
%left NEGATION_PRECEDENCE
%nonassoc <empty> '.'

%token <expr> NUMBER ION
%token <str> STRING

%type <query> query
%type <expr> expr datum datum_or_parens maybe_into
%type <expr> where_expr having_expr case_optional_expr case_optional_else parenthesized_expr
%type <expr> optional_filter
%type <expr> unpivot unpivot_source
%type <with> maybe_cte_bindings cte_bindings
%type <yesno> ascdesc nullslast maybe_distinct
%type <str> identifier
%type <integer> literal_int
%type <sel> select_stmt
%type <selinto> select_with_into_stmt
%type <bindings> group_expr binding_list
%type <bind> value_binding
%type <from> from_expr lhs_from_expr
%type <values> partition_expr value_list any_value_list field_value_list field_value_pair node_list maybe_toplevel_distinct
%type <order> order_one_col
%type <orders> order_expr order_cols
%type <jk> join_kind
%type <exprint> limit_expr
%type <exprint> offset_expr
%type <limbs> case_limbs
%type <wind> maybe_window
%type <integer> trim_type
%type <str> maybe_explain
%type <unions> maybe_union
%start query

%%

query:
maybe_explain maybe_cte_bindings select_with_into_stmt maybe_union
{
  query, err := buildQuery($1, $2, $3, $4)
  if err != nil {
    yylex.Error(err.Error())
  }

  yylex.(*scanner).result = query
}

select_with_into_stmt:
SELECT maybe_toplevel_distinct binding_list maybe_into from_expr where_expr group_expr having_expr order_expr limit_expr offset_expr
{
    distinct, distinctExpr := decodeDistinct($2)
    $$.sel = &expr.Select{Distinct: distinct, DistinctExpr: distinctExpr, Columns: $3, From: $5, Where: $6, GroupBy: $7, Having: $8, OrderBy: $9, Limit: $10, Offset: $11}
    $$.into = $4
}

select_stmt:
SELECT maybe_toplevel_distinct binding_list from_expr where_expr group_expr having_expr order_expr limit_expr offset_expr
{
    distinct, distinctExpr := decodeDistinct($2)
    $$ = &expr.Select{Distinct: distinct, DistinctExpr: distinctExpr, Columns: $3, From: $4, Where: $5, GroupBy: $6, Having: $7, OrderBy: $8, Limit: $9, Offset: $10}
}

maybe_explain:
  EXPLAIN               { $$ = "default" }
| EXPLAIN AS identifier { $$ = $3 }
|                       { $$ = "" }

maybe_into:
INTO datum { $$ = $2 } | { $$ = nil }

maybe_cte_bindings:
cte_bindings { $$ = $1 } | { $$ = nil }

maybe_union:
  { $$ = []unionItem{} }
| UNION select_stmt maybe_union {
    $$ = append($$, unionItem{typ: expr.UnionDistinct, sel: $2})
    $$ = append($$, $3...)
  }
| UNION ALL select_stmt maybe_union {
    $$ = append($$, unionItem{typ: expr.UnionAll, sel: $3})
    $$ = append($$, $4...)
  }

cte_bindings:
WITH identifier AS '(' select_stmt ')' { $$ = []expr.CTE{{Table: $2, As: $5}} } |
cte_bindings ',' identifier AS '(' select_stmt ')' { $$ = append($1, expr.CTE{Table: $3, As: $6})}

// a regular value expression OR
// a value expression plus a binding
// (with an optional AS)
value_binding:
expr AS identifier { $$ = expr.Bind($1, $3) } |
expr identifier { $$ = expr.Bind($1, $2) } |
expr { $$ = expr.Bind($1, "") } |
'*' { $$ = expr.Bind(expr.Star{}, "") } |
unpivot { $$ = expr.Bind($1, "") }

// match exactly a single datum
datum:
identifier { $$ = expr.Ident($1) } |
NUMBER { $$ = $1 } |
TRUE { $$ = expr.Bool(true) } |
FALSE { $$ = expr.Bool(false) } |
NULL { $$ = expr.Null{} } |
MISSING { $$ = expr.Missing{} } |
STRING { $$ = expr.String($1) } |
ION { $$ = $1 } |
'{' field_value_list '}' { $$ = expr.Call(expr.MakeStruct, $2...) } |
'[' any_value_list ']' { $$ = expr.Call(expr.MakeList, $2...) } |
datum '.' identifier { $$ = &expr.Dot{Inner: $1, Field: $3} } |
datum '[' literal_int ']' { $$ = &expr.Index{Inner: $1, Offset: $3} } |
datum '[' STRING ']' { $$ = &expr.Dot{Inner: $1, Field: $3} }

// datum_or_parens is guaranteed to
// avoid shift-reduce conflicts with BETWEEN,
// since it cannot contain an AND inside it
// without parentheses
//
// datum_or_parens also matches parenthesized
// SELECT expressions so that (expr) can be
// disambiguated from (SELECT ...) in one place
// in order to avoid reduce/reduce conflicts
datum_or_parens:
datum { $$ = $1 } |
'(' parenthesized_expr ')' { $$ = $2 }

parenthesized_expr:
select_stmt { $$ = $1 } |
expr { $$ = $1 }

maybe_distinct:
DISTINCT { $$ = true } | { $$ = false }

maybe_toplevel_distinct:
DISTINCT ON '(' node_list ')' { $$ = $4 } |
DISTINCT { $$ = []expr.Node{} } |
{ $$ = nil}


// any expression:
expr:
datum_or_parens
{
  $$ = $1
}
| AGGREGATE '(' ')' optional_filter maybe_window
{
  agg, err := toAggregate(expr.AggregateOp($1), false, nil, $4, $5)
  if err != nil {
    yylex.Error(err.Error())
  }
  $$ = agg
}
| AGGREGATE '(' maybe_distinct value_list ')' optional_filter maybe_window
{
  agg, err := toAggregate(expr.AggregateOp($1), $3, $4, $6, $7)
  if err != nil {
    yylex.Error(err.Error())
  }
  $$ = agg
}
| CASE case_optional_expr case_limbs case_optional_else END
{
  $$ = createCase($2, $3, $4)
}
| COALESCE '(' value_list ')'
{
  $$ = expr.Coalesce($3)
}
| NULLIF '(' expr ',' expr ')'
{
  $$ = expr.NullIf($3, $5)
}
| CAST '(' expr AS ID ')'
{
  nod, ok := buildCast($3, $5)
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad CAST type %q", $5))
  }
  $$ = nod
}
| DATE_ADD '(' ID ',' expr ',' expr ')'
{
  part, ok := timePartFor($3, "DATE_ADD")
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_ADD part %q", $3))
  }
  $$ = expr.DateAdd(part, $5, $7)
}
| DATE_DIFF '(' ID ',' expr ',' expr ')'
{
  part, ok := timePartFor($3, "DATE_DIFF")
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_DIFF part %q", $3))
  }
  $$ = expr.DateDiff(part, $5, $7)
}
| DATE_TRUNC '(' ID '(' ID ')' ',' expr ')'
{
  dow, ok := weekday($5)
  if strings.ToUpper($3) != "WEEK" || !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_TRUNC part %q(%q)", $3, $5))
  }
  $$ = expr.DateTruncWeekday($8, dow)
}
| DATE_TRUNC '(' ID ',' expr ')'
{
  part, ok := timePartFor($3, "DATE_TRUNC")
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_TRUNC part %q", $3))
  }
  $$ = expr.DateTrunc(part, $5)
}
| EXTRACT '(' ID FROM expr ')'
{
  part, ok := timePartFor($3, "EXTRACT")
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad EXTRACT part %q", $3))
  }
  $$ = expr.DateExtract(part, $5)
}
| UTCNOW '(' ')'
{
  $$ = yylex.(*scanner).utcnow()
}
| TRIM '(' expr ')'
{
  node, err := createTrimInvocation(trimBoth, $3, nil)
  if err != nil {
    yylex.Error(err.Error())
  }
  $$ = node
}
| TRIM '(' expr ',' expr ')'
{
  node, err := createTrimInvocation(trimBoth, $3, $5)
  if err != nil {
    yylex.Error(err.Error())
  }
  $$ = node
}
| TRIM '(' expr FROM expr ')'
{
  node, err := createTrimInvocation(trimBoth, $5, $3)
  if err != nil {
    yylex.Error(err.Error())
  }
  $$ = node
}
| TRIM '(' trim_type expr FROM expr ')'
{
  node, err := createTrimInvocation($3, $6, $4)
  if err != nil {
    yylex.Error(err.Error())
  }
  $$ = node
}
| identifier '(' ')'
{
  op := expr.CallByName($1)
  if op.Private() {
    yylex.Error(__yyfmt__.Sprintf("cannot use reserved builtin %q", $1))
  }
  $$ = op
}
| identifier '(' value_list ')'
{
  op := expr.CallByName($1, $3...)
  if op.Private() {
    yylex.Error(__yyfmt__.Sprintf("cannot use reserved builtin %q", $1))
  }
  $$ = op
}
| expr IN '(' select_stmt ')'
{
  $$ = expr.Call(expr.InSubquery, $1, $4)
}
| expr IN '(' value_list ')'
{
  $$ = expr.In($1, $4...)
}
| EXISTS '(' select_stmt ')'
{
  $$ = exists($3)
}
| expr '|' expr
{
  $$ = expr.BitOr($1, $3)
}
| expr '^' expr
{
  $$ = expr.BitXor($1, $3)
}
| expr '&' expr
{
  $$ = expr.BitAnd($1, $3)
}
| expr SHIFT_LEFT_LOGICAL expr
{
  $$ = expr.ShiftLeftLogical($1, $3)
}
| expr SHIFT_RIGHT_LOGICAL expr
{
  $$ = expr.ShiftRightLogical($1, $3)
}
| expr SHIFT_RIGHT_ARITHMETIC expr
{
  $$ = expr.ShiftRightArithmetic($1, $3)
}
| expr '+' expr
{
  $$ = expr.Add($1, $3)
}
| expr '-' expr
{
  $$ = expr.Sub($1, $3)
}
| expr '*' expr
{
  $$ = expr.Mul($1, $3)
}
| expr '/' expr
{
  $$ = expr.Div($1, $3)
}
| expr '%' expr
{
  $$ = expr.Mod($1, $3)
}
| expr CONCAT expr
{
  $$ = expr.Call(expr.Concat, $1, $3)
}
| expr APPEND expr
{
  $$ = expr.Append($1, $3)
}
| '-' expr %prec NEGATION_PRECEDENCE
{
  $$ = expr.Neg($2)
}
| expr ILIKE STRING ESCAPE STRING
{
  $$ = &expr.StringMatch{Op: expr.Ilike, Expr: $1, Pattern: $3, Escape: $5}
}
| expr ILIKE STRING
{
  $$ = &expr.StringMatch{Op: expr.Ilike, Expr: $1, Pattern: $3}
}
| expr LIKE STRING ESCAPE STRING
{
  $$ = &expr.StringMatch{Op: expr.Like, Expr: $1, Pattern: $3, Escape: $5}
}
| expr LIKE STRING
{
  $$ = &expr.StringMatch{Op: expr.Like, Expr: $1, Pattern: $3}
}
| expr SIMILAR TO STRING
{
  $$ = &expr.StringMatch{Op: expr.SimilarTo, Expr: $1, Pattern: $4}
}
| expr '~' STRING
{
  $$ = &expr.StringMatch{Op: expr.RegexpMatch, Expr: $1, Pattern: $3}
}
| expr REGEXP_MATCH_CI STRING
{
  $$ = &expr.StringMatch{Op: expr.RegexpMatchCi, Expr: $1, Pattern: $3}
}
| expr EQ expr
{
  $$ = expr.Compare(expr.Equals, $1, $3)
}
| expr NE expr
{
  $$ = expr.Compare(expr.NotEquals, $1, $3)
}
| expr LT expr
{
  $$ = expr.Compare(expr.Less, $1, $3)
}
| expr LE expr
{
  $$ = expr.Compare(expr.LessEquals, $1, $3)
}
| expr GT expr
{
  $$ = expr.Compare(expr.Greater, $1, $3)
}
| expr GE expr
{
  $$ = expr.Compare(expr.GreaterEquals, $1, $3)
}
| expr BETWEEN datum_or_parens AND datum_or_parens
{
  $$ = expr.Between($1, $3, $5)
}
| expr NOT LIKE STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.Like, Expr: $1, Pattern: $4}}
}
| expr NOT LIKE STRING ESCAPE STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.Like, Expr: $1, Pattern: $4, Escape: $6}}
}
| expr NOT ILIKE STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.Like, Expr: $1, Pattern: $4}}
}
| expr NOT ILIKE STRING ESCAPE STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.Ilike, Expr: $1, Pattern: $4, Escape: $6}}
}
| expr NOT SIMILAR TO STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.SimilarTo, Expr: $1, Pattern: $5}}
}
| expr NOT '~' STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.RegexpMatch, Expr: $1, Pattern: $4}}
}
| expr NOT REGEXP_MATCH_CI STRING
{
  $$ = &expr.Not{Expr: &expr.StringMatch{Op: expr.RegexpMatchCi, Expr: $1, Pattern: $4}}
}
| NOT expr
{
  $$ = &expr.Not{Expr: $2}
}
| '~' expr
{
  $$ = expr.BitNot($2)
}
| expr AND expr
{
  $$ = expr.And($1, $3)
}
| expr OR expr
{
  $$ = expr.Or($1, $3)
}
| expr IS NULL
{
  $$ = &expr.IsKey{Key: expr.IsNull, Expr: $1}
}
| expr IS NOT NULL
{
  $$ = &expr.IsKey{Key: expr.IsNotNull, Expr: $1}
}
| expr IS MISSING
{
  $$ = &expr.IsKey{Key: expr.IsMissing, Expr: $1}
}
| expr IS NOT MISSING
{
  $$ = &expr.IsKey{Key: expr.IsNotMissing, Expr: $1}
}
| expr IS TRUE
{
  $$ = &expr.IsKey{Key: expr.IsTrue, Expr: $1}
}
| expr IS NOT TRUE
{
  $$ = &expr.IsKey{Key: expr.IsNotTrue, Expr: $1}
}
| expr IS FALSE
{
  $$ = &expr.IsKey{Key: expr.IsFalse, Expr: $1}
}
| expr IS NOT FALSE
{
  $$ = &expr.IsKey{Key: expr.IsNotFalse, Expr: $1}
}

// match (binding)+
binding_list:
value_binding { $$ = []expr.Binding{$1} } |
binding_list ',' value_binding { $$ = append($1, $3) }

// match (value)+
node_list:
expr { $$ = []expr.Node{$1} } |
node_list ',' expr { $$ = append($1, $3) }

// match (value)+ including '*' as a special value
value_list:
expr { $$ = []expr.Node{$1} } |
'*' { $$ = []expr.Node{expr.Star{}} } |
value_list ',' expr { $$ = append($1, $3) }

// match (value)*
any_value_list:
expr { $$ = []expr.Node{$1} } |
any_value_list ',' expr { $$ = append($1, $3) } |
{ $$ = nil }

// match [field_value_pair (, field_value_pair)*]
field_value_list:
field_value_pair { $$ = $1 } |
field_value_list ',' field_value_pair { $$ = append($1, $3...) } |
{ $$ = nil }

// match 'field': value
field_value_pair:
STRING ':' expr { $$ = []expr.Node{expr.String($1), $3} }

partition_expr:
PARTITION BY value_list
{
  $$ = $3
}
| { $$ = nil }

maybe_window:
OVER '(' partition_expr order_expr ')'
{
  $$ = &expr.Window{PartitionBy: $3, OrderBy: $4}
}
| { $$ = nil }

join_kind:
JOIN { $$ = expr.InnerJoin } |
INNER JOIN { $$ = expr.InnerJoin } |
LEFT JOIN { $$  = expr.LeftJoin } |
LEFT OUTER JOIN { $$ = expr.LeftJoin } |
RIGHT JOIN { $$ = expr.RightJoin } |
RIGHT OUTER JOIN { $$ = expr.RightJoin } |
FULL JOIN { $$ = expr.FullJoin }

cross_symbol: ',' | CROSS JOIN

from_expr:
lhs_from_expr { $$ = $1 } |
{ $$ = nil }

lhs_from_expr:
FROM value_binding { $$ = &expr.Table{Binding: $2} } |
lhs_from_expr cross_symbol value_binding { $$ = &expr.Join{Kind: expr.CrossJoin, Left: $1, Right: $3} } |
lhs_from_expr join_kind value_binding ON expr
{ $$ = &expr.Join{Kind: $2, Left: $1, Right: $3, On: $5 } }

literal_int:
NUMBER { var idxerr error; $$, idxerr = toint($1); if idxerr != nil { yylex.Error(idxerr.Error()) } }

// note: arithmetic_expression
// and primary_expr are declared
// so that there is no shift-reduce
// conflict between compound expressions;
// they are automatically left-associative

identifier:
ID { $$ = $1 }

case_optional_else:
{ $$ = nil } |
ELSE expr { $$ = $2 }

case_limbs:
WHEN expr THEN expr { $$ = []expr.CaseLimb{{When: $2, Then: $4}} }
| case_limbs WHEN expr THEN expr { $$ = append($1, expr.CaseLimb{When: $3, Then: $5}) }

case_optional_expr:
{ $$ = nil } |
expr { $$ = $1 }

optional_filter:
{ $$ = nil } |
FILTER '(' WHERE expr ')' { $$ = $4 }

where_expr:
{ $$ = nil } |
WHERE expr { $$ = $2 }

having_expr:
{ $$ = nil } |
HAVING expr { $$ = $2 }

group_expr:
{ $$ = nil } |
GROUP BY binding_list { $$ = $3 }

// match optional NULLS FIRST / NULLS LAST
nullslast:
{ $$ = false } |
NULLS FIRST { $$ = false } |
NULLS LAST  { $$ = true }

// match optional ASC/DESC
ascdesc:
{ $$ = false } |
ASC { $$ = false } |
DESC { $$ = true }

// match <expr> <ASC/DESC> <NULLS FIRST/NULLS LAST>
order_one_col:
expr ascdesc nullslast { $$ = expr.Order{Column: $1, Desc: $2, NullsLast: $3} }

order_cols:
order_cols ',' order_one_col { $$ = append($1, $3) } |
order_one_col { $$ = []expr.Order{$1} }

order_expr:
{ $$ = nil } |
ORDER BY order_cols { $$ = $3 }

limit_expr:
{ $$ = nil } |
LIMIT literal_int { n := expr.Integer($2); $$ = &n }

offset_expr:
{ $$ = nil } |
OFFSET literal_int { n := expr.Integer($2); $$ = &n }

unpivot:
UNPIVOT unpivot_source AS identifier AT identifier { /*Cloning, as the buffer gets overwritten*/ as := $4; at := $6; $$ = &expr.Unpivot{ TupleRef: $2, As: &as, At: &at } } |
UNPIVOT unpivot_source AT identifier AS identifier { /*Cloning, as the buffer gets overwritten*/ as := $6; at := $4; $$ = &expr.Unpivot{ TupleRef: $2, As: &as, At: &at } } |
UNPIVOT unpivot_source AS identifier { /*Cloning, as the buffer gets overwritten*/ as := $4; $$ = &expr.Unpivot{ TupleRef: $2, As: &as, At: nil } } |
UNPIVOT unpivot_source AT identifier { /*Cloning, as the buffer gets overwritten*/ at := $4; $$ = &expr.Unpivot{ TupleRef: $2, As: nil, At: &at } }

unpivot_source:
expr { $$ = &expr.Table{Binding: expr.Bind($1, "")} }


trim_type:
LEADING { $$ = trimLeading } |
TRAILING { $$ = trimTrailing } |
BOTH { $$ = trimBoth }
