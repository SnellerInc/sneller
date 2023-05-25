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

// uses golang.org/x/tools/cmd/goyacc
%{

package elastic_proxy

import "fmt"
%}

%union{
  boolean  bool
  text     string
  numFloat float64
  numInt   int64
  incl     bool
  field    qsFieldName
  expr     qsExpression
  exprs    []qsExpression
}

%token <boolean>  tokBool
%token <text>     tokAlpha tokAlphaQuoted tokAlphaRegex tokOperator
%token <numFloat> tokFloat tokBoost tokFuzzy
%token <numInt>   tokInt
%token <incl>     tokRangeStart tokRangeEnd

%token tokTo tokExists

%left tokAnd
%left tokOr
%left tokNot

%type <field> fieldName
%type <expr>  main query term range rangeBoost fieldValue fieldConstantNonAnnotated fieldConstant fieldConstantBoost fieldExists
%type <exprs> terms fieldValues

%%
main: query   { yylex.(*queryStringLexer).result = $$ }
    ;

query: query tokAnd query     { $$ = &qsExpression2{Operator: "AND", Expr1: $1, Expr2: $3}; }
     | query tokOr query      { $$ = &qsExpression2{Operator: "OR", Expr1: $1, Expr2: $3}; }
     | tokNot query           { $$ = &qsExpression1{Operator: "NOT", Expr: $2} }
     | '(' query ')'          { $$ = $2; }
     | '(' query ')' tokBoost { $$ = $2; $$.SetBoost($4); }
     | terms                  { $$ = combine(yylex.(*queryStringLexer).defaultOperator, $1); }
     ;

terms: term       { $$ = []qsExpression{$1}; }
     | term terms { $$ = append([]qsExpression{$1}, $2...);  }
     ;

term: fieldName ':' fieldValue                   { $$ = $3; $$.SetFieldName($1); }
    | fieldName ':' '(' fieldValues ')'          { $$ = combine(yylex.(*queryStringLexer).defaultOperator, $4); $$.SetFieldName($1); }
    | fieldName ':' '(' fieldValues ')' tokBoost { $$ = combine(yylex.(*queryStringLexer).defaultOperator, $4); $$.SetFieldName($1); $$.SetBoost($6); }
    | fieldValue                                 { $$ = $1; }
    | fieldName ':' rangeBoost                   { $$ = $3; $$.SetFieldName($1); }
    |               rangeBoost                   { $$ = $1; }
    | fieldExists                                { $$ = $1; }
    ;

fieldName: tokAlpha               { $$ = newQSFieldName($1) }
         | tokAlpha '.' fieldName { $$ = $3.Prepend($1) }
         ;

fieldValues: fieldValue             { $$ = []qsExpression{$1}; }
           | fieldValue fieldValues { $$ = append([]qsExpression{$1}, $2...);  }
           ;

fieldValue: fieldConstantBoost     { $$ = $1; }
          | '+' fieldConstantBoost { $$ = &qsMustExpression{Operator: "AND", Expr: $2}; }
          | '|' fieldConstantBoost { $$ = &qsMustExpression{Operator: "OR",  Expr: $2}; }
          | '-' fieldConstantBoost { $$ = &qsMustExpression{Operator: "AND", Expr: &qsExpression1{Operator: "NOT", Expr: $2}}; }
          ;

rangeBoost: range          { $$ = $1; }
          | range tokBoost { $$ = $1; $$.SetBoost($2); }
          ;

range: tokRangeStart fieldConstantNonAnnotated tokTo fieldConstantNonAnnotated tokRangeEnd
          {
            from, to := $2.(*qsFieldExpression), $4.(*qsFieldExpression)
            if $1 { from.Operator = ">=" } else { from.Operator = ">" }
            if $5 { to.Operator = "<=" } else { to.Operator = "<" }
            if isRangeStar(from) {
              if isRangeStar(to) {
                // Both stars, so no check at all
					      $$ = &qsValue{Value: true}
              } else {
                $$ = to
              }
            } else if isRangeStar(to) {
              $$ = from
            } else {
              $$ = &qsExpression2{
                Operator: "AND",
                Expr1: from,
                Expr2: to,
              }
            }
          }
     ;

fieldConstantNonAnnotated: tokFloat       { $$ = &qsFieldExpression{Value: fmt.Sprintf("%g", $1), Type: valueTypeFloat ,Boost: -1, Fuzzy: -1} }
                         | tokInt         { $$ = &qsFieldExpression{Value: fmt.Sprintf("%d", $1), Type: valueTypeInt ,Boost: -1, Fuzzy: -1} }
                         | tokAlpha       { $$ = &qsFieldExpression{Value: $1, Type: valueTypeText, Boost: -1, Fuzzy: -1} }
                         | tokAlphaQuoted { $$ = &qsFieldExpression{Value: $1, Type: valueTypeText, Boost: -1, Fuzzy: -1} }
                         ;

fieldConstantBoost: fieldConstant          { $$ = $1; }
                  | fieldConstant tokBoost { $$ = $1; $$.(*qsFieldExpression).Boost = $2; }
                  ;

fieldConstant: tokFloat                { $$ = &qsFieldExpression{Value: fmt.Sprintf("%g", $1), Type: valueTypeFloat, Operator: "=", Boost: -1, Fuzzy: -1} }
             | tokInt                  { $$ = &qsFieldExpression{Value: fmt.Sprintf("%d", $1), Type: valueTypeInt, Operator: "=", Boost: -1, Fuzzy: -1} }
             | tokBool                 { $$ = &qsFieldExpression{Value: fmt.Sprintf("%v", $1), Type: valueTypeBoolean, Operator: "=", Boost: -1, Fuzzy: -1} }
             | tokAlpha                { $$ = &qsFieldExpression{Value: $1, Type: valueTypeText, Operator: "=", Boost: -1, Fuzzy: -1} }
             | tokAlpha tokFuzzy       { $$ = &qsFieldExpression{Value: $1, Type: valueTypeText, Operator: "=", Boost: -1, Fuzzy: $2} }
             | tokAlphaQuoted          { $$ = &qsFieldExpression{Value: $1, Type: valueTypeText, Operator: "=", Boost: -1, Fuzzy: -1} }
             | tokAlphaQuoted tokFuzzy { $$ = &qsFieldExpression{Value: $1, Type: valueTypeText, Operator: "=", Boost: -1, Fuzzy: $2} }
             | tokAlphaRegex           { $$ = &qsFieldExpression{Value: $1, Type: valueTypeRegex, Operator: "=", Boost: -1, Fuzzy: -1} }
             | tokAlphaRegex tokFuzzy  { $$ = &qsFieldExpression{Value: $1, Type: valueTypeRegex, Operator: "=", Boost: -1, Fuzzy: $2} }
             | tokOperator tokFloat    { $$ = &qsFieldExpression{Value: fmt.Sprintf("%g", $2), Type: valueTypeFloat, Operator: $1, Boost: -1, Fuzzy: -1} }
             | tokOperator tokInt      { $$ = &qsFieldExpression{Value: fmt.Sprintf("%d", $2), Type: valueTypeInt, Operator: $1, Boost: -1, Fuzzy: -1} }
             ;

fieldExists: tokExists ':' fieldName { $$ = &qsFieldExpression{FieldName: $3, Operator: "EXISTS", Boost: -1, Fuzzy: -1}; }
           ;
