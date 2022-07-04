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
    pc       expr.PathComponent
    order    expr.Order
    sel      *expr.Select
    wind     *expr.Window
    bind     expr.Binding
    jk       expr.JoinKind
    from     expr.From
    with     []expr.CTE
    bindings []expr.Binding
    limbs    []expr.CaseLimb
    values   []expr.Node
    orders   []expr.Order
}

%token ERROR EOF
%left UNION
%token SELECT FROM WHERE GROUP ORDER BY HAVING LIMIT OFFSET WITH INTO
%token DISTINCT ALL AS EXISTS NULLS FIRST LAST ASC DESC
%token PARTITION
%token VALUE
%right COALESCE NULLIF EXTRACT DATE_TRUNC
%right CAST UTCNOW
%right DATE_ADD DATE_DIFF EARLIEST LATEST
%left JOIN LEFT RIGHT CROSS INNER OUTER FULL
%left ON
%token <integer> AGGREGATE
%token <str> ID
%token <empty> '(' ',' ')' '[' ']' '{' '}'
%token <empty> NULL TRUE FALSE MISSING

%left OR
%left AND
%right '!' '~' NOT
%left BETWEEN CASE WHEN THEN ELSE END
%left <empty> EQ NE LT LE GT GE
%left <empty> ILIKE LIKE IN IS OVER FILTER
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

%type <expr> query expr datum datum_or_parens path_expression maybe_into
%type <expr> where_expr having_expr case_optional_else parenthesized_expr
%type <expr> optional_filter
%type <with> maybe_cte_bindings cte_bindings
%type <pc> path_component
%type <yesno> ascdesc nullslast maybe_distinct
%type <str> identifier
%type <integer> literal_int
%type <sel> select_stmt
%type <bindings> group_expr binding_list
%type <bind> value_binding
%type <from> from_expr lhs_from_expr
%type <values> value_list
%type <order> order_one_col
%type <orders> order_expr order_cols
%type <jk> join_kind
%type <exprint> limit_expr
%type <exprint> offset_expr
%type <limbs> case_limbs
%type <wind> maybe_window
%start query

%%

query:
maybe_cte_bindings SELECT maybe_distinct binding_list maybe_into from_expr where_expr group_expr having_expr order_expr limit_expr offset_expr
{
  yylex.(*scanner).with = $1
  yylex.(*scanner).into = $5
  yylex.(*scanner).result = &expr.Select{Distinct: $3, Columns: $4, From: $6, Where: $7, GroupBy: $8, Having: $9, OrderBy: $10, Limit: $11, Offset: $12};
}

select_stmt:
SELECT maybe_distinct binding_list from_expr where_expr group_expr having_expr order_expr limit_expr offset_expr
{
    $$ = &expr.Select{Distinct: $2, Columns: $3, From: $4, Where: $5, GroupBy: $6, Having: $7, OrderBy: $8, Limit: $9, Offset: $10};
}

maybe_into:
INTO path_expression { $$ = $2 } | { $$ = nil }

maybe_cte_bindings:
cte_bindings { $$ = $1 } | { $$ = nil }

cte_bindings:
WITH identifier AS '(' select_stmt ')' { $$ = []expr.CTE{{$2, $5}} } |
cte_bindings ',' identifier AS '(' select_stmt ')' { $$ = append($1, expr.CTE{$3, $6})}

// a regular value expression OR
// a value expression plus a binding
// (with an optional AS)
value_binding:
expr AS identifier { $$ = expr.Bind($1, $3) } |
expr identifier { $$ = expr.Bind($1, $2) } |
expr { $$ = expr.Bind($1, "") } |
'*' { $$ = expr.Bind(expr.Star{}, "") }

path_expression:
identifier path_component { $$ = &expr.Path{First: $1, Rest: $2} }

// match exactly a single datum
datum:
NUMBER { $$ = $1 } |
TRUE { $$ = expr.Bool(true) } |
FALSE { $$ = expr.Bool(false) } |
NULL { $$ = expr.Null{} } |
MISSING { $$ = expr.Missing{} } |
STRING { $$ = expr.String($1) } |
ION { $$ = $1 } |
path_expression { $$ = $1 }

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

// any expression:
expr:
datum_or_parens
{
  $$ = $1
}
| AGGREGATE '(' maybe_distinct expr ')' optional_filter maybe_window
{
  $$ = toAggregate(expr.AggregateOp($1), $4, $3, $6, $7)
}
| AGGREGATE '(' '*' ')' optional_filter maybe_window // realistically only COUNT(*)
{
  distinct := false
  $$ = toAggregate(expr.AggregateOp($1), expr.Star{}, distinct, $5, $6)
}
| CASE case_limbs case_optional_else END
{
  $$ = &expr.Case{Limbs: $2, Else: $3}
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
    return 1
  }
  $$ = nod
}
| DATE_ADD '(' ID ',' expr ',' expr ')'
{
  part, ok := timePart($3)
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_ADD part %q", $3))
  }
  $$ = expr.DateAdd(part, $5, $7)
}
| DATE_DIFF '(' ID ',' expr ',' expr ')'
{
  part, ok := timePart($3)
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_DIFF part %q", $3))
  }
  $$ = expr.DateDiff(part, $5, $7)
}
| DATE_TRUNC '(' ID ',' expr ')'
{
  part, ok := timePart($3)
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad DATE_TRUNC part %q", $3))
  }
  $$ = expr.DateTrunc(part, $5)
}
| EXTRACT '(' ID FROM expr ')'
{
  part, ok := timePart($3)
  if !ok {
    yylex.Error(__yyfmt__.Sprintf("bad EXTRACT part %q", $3))
  }
  $$ = expr.DateExtract(part, $5)
}
| UTCNOW '(' ')'
{
  $$ = yylex.(*scanner).utcnow()
}
| identifier '(' ')'
{
  op := expr.Call($1)
  if op.Private() {
    yylex.Error(__yyfmt__.Sprintf("cannot use reserved builtin %q", $1))
  }
  $$ = op
}
| identifier '(' value_list ')'
{
  op := expr.Call($1, $3...)
  if op.Private() {
    yylex.Error(__yyfmt__.Sprintf("cannot use reserved builtin %q", $1))
  }
  $$ = op
}
| expr IN '(' select_stmt ')'
{
  $$ = expr.CallOp(expr.InSubquery, $1, $4)
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
  $$ = expr.Call("CONCAT", $1, $3)
}
| expr APPEND expr
{
  $$ = expr.Append($1, $3)
}
| '-' expr %prec NEGATION_PRECEDENCE
{
  $$ = expr.Neg($2)
}
| expr ILIKE STRING
{
  $$ = expr.Compare(expr.Ilike, $1, expr.String($3))
}
| expr LIKE STRING
{
  $$ = expr.Compare(expr.Like, $1, expr.String($3))
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
  $$ = &expr.Not{Expr: expr.Compare(expr.Like, $1, expr.String($4))}
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
value_list:
expr { $$ = []expr.Node{$1} } |
'*' { $$ = []expr.Node{expr.Star{}} } |
value_list ',' expr { $$ = append($1, $3) }

maybe_window:
OVER '(' PARTITION BY value_list order_expr ')'
{
  $$ = &expr.Window{PartitionBy: $5, OrderBy: $6}
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

// FIXME:
//   - add support for nested queries
//   - add support for CROSS JOIN with ON and JOIN without ON
//   (right now the grammar prohibits both of those)
lhs_from_expr:
FROM value_binding { $$ = &expr.Table{Binding: $2} } |
lhs_from_expr cross_symbol value_binding { $$ = &expr.Join{Kind: expr.CrossJoin, Left: $1, Right: $3} } |
lhs_from_expr join_kind value_binding ON expr EQ expr
{ $$ = &expr.Join{Kind: $2, Left: $1, Right: $3, On: &expr.OnEquals{Left: $5, Right: $7} } }

literal_int:
NUMBER { var idxerr error; $$, idxerr = toint($1); if idxerr != nil { yylex.Error(idxerr.Error()) } }

path_component:
{ $$ = nil }
| '.' identifier path_component { $$ = &expr.Dot{Field: $2, Rest: $3}}
| '[' literal_int ']' path_component { $$ = &expr.LiteralIndex{Field: $2, Rest: $4} }
| '[' ID ']' path_component { $$ = &expr.Dot{Field: $2, Rest: $4} }

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
