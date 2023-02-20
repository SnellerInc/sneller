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

package elastic_proxy

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

//go:generate ragel -L -Z -G2 qs_lexer.rl
//go:generate goyacc -l -o qs_parser.go qs_parser.y
//go:generate goimports -w qs_lexer.go qs_parser.go

var (
	ErrNoFieldName          = errors.New("no fieldname set")
	ErrInvalidRegexOperator = errors.New("invalid regex operator")
)

type qsFieldName struct {
	fields []string
}

func newQSFieldName(fieldName string) qsFieldName {
	return qsFieldName{
		fields: []string{fieldName},
	}
}

func parseQSFieldName(fieldName string) qsFieldName {
	return qsFieldName{
		fields: strings.Split(fieldName, "."),
	}
}
func (qsfn qsFieldName) Prepend(fieldName string) qsFieldName {
	return qsFieldName{
		fields: append([]string{fieldName}, qsfn.fields...),
	}
}

func (qsfn qsFieldName) isEmpty() bool {
	return len(qsfn.fields) == 0
}

type qsExpression interface {
	SetFieldName(fieldName qsFieldName)
	SetBoost(value float64)
	Expression(qc *QueryContext, defaultFieldName qsFieldName) (expression, error)
}

func combine(defaultOperator string, exprs []qsExpression) qsExpression {
	if len(exprs) == 0 {
		panic("cannot combine if there are no expressions")
	}

	var expr qsExpression

	// Always AND the expressions that use MUST
	for _, e := range exprs {
		if mustExpr, ok := e.(*qsMustExpression); ok {
			if mustExpr.Operator == "AND" {
				if expr == nil {
					expr = mustExpr.Expr
				} else {
					expr = &qsExpression2{
						Operator: "AND",
						Expr1:    expr,
						Expr2:    mustExpr.Expr,
					}
				}
			}
		}
	}

	// Use the default expression if there is already
	// a MUST expression and we OR the rest. Because
	// the A B +C is always true when C is true, there
	// is no need to check for A and B.
	if defaultOperator != "OR" || expr == nil {
		for _, e := range exprs {
			operator := defaultOperator
			if mustExpr, ok := e.(*qsMustExpression); ok {
				if mustExpr.Operator == "OR" {
					continue
				}
				operator = mustExpr.Operator
				e = mustExpr.Expr
			}

			if expr == nil {
				expr = e
			} else {
				expr = &qsExpression2{
					Operator: operator,
					Expr1:    expr,
					Expr2:    e,
				}
			}
		}
	}

	return expr
}

type qsValue struct {
	Value any
}

func (q *qsValue) SetFieldName(fieldName qsFieldName) {
}

func (q *qsValue) SetBoost(value float64) {
}

func (q *qsValue) Expression(qc *QueryContext, defaultFieldName qsFieldName) (expression, error) {
	v, err := NewJSONLiteral(q.Value)
	if err != nil {
		return nil, err
	}
	return &exprJSONLiteral{
		Context: qc,
		Value:   v,
	}, nil
}

type qsExpression1 struct {
	Operator string
	Expr     qsExpression
}

func (e *qsExpression1) SetFieldName(fieldName qsFieldName) {
	e.Expr.SetFieldName(fieldName)
}

func (e *qsExpression1) SetBoost(value float64) {
	e.Expr.SetBoost(value)
}

func (e *qsExpression1) Expression(qc *QueryContext, defaultFieldName qsFieldName) (expression, error) {
	e1, err := e.Expr.Expression(qc, defaultFieldName)
	if err != nil {
		return nil, err
	}
	return &exprOperator1{
		Context:  qc,
		Operator: e.Operator,
		Expr1:    e1,
	}, nil
}

type qsExpression2 struct {
	Operator string
	Expr1    qsExpression
	Expr2    qsExpression
}

func (e *qsExpression2) SetFieldName(fieldName qsFieldName) {
	e.Expr1.SetFieldName(fieldName)
	e.Expr2.SetFieldName(fieldName)
}

func (e *qsExpression2) SetBoost(value float64) {
	e.Expr1.SetBoost(value)
	e.Expr2.SetBoost(value)
}

func (e *qsExpression2) Expression(qc *QueryContext, defaultFieldName qsFieldName) (expression, error) {
	e1, err := e.Expr1.Expression(qc, defaultFieldName)
	if err != nil {
		return nil, err
	}
	e2, err := e.Expr2.Expression(qc, defaultFieldName)
	if err != nil {
		return nil, err
	}
	return &exprOperator2{
		Context:  qc,
		Operator: e.Operator,
		Expr1:    e1,
		Expr2:    e2,
	}, nil
}

type qsMustExpression struct {
	Operator string
	Expr     qsExpression
}

func (e *qsMustExpression) SetFieldName(fieldName qsFieldName) {
	e.Expr.SetFieldName(fieldName)
}

func (e *qsMustExpression) SetBoost(value float64) {
	e.Expr.SetBoost(value)
}

func (e *qsMustExpression) Expression(qc *QueryContext, defaultFieldName qsFieldName) (expression, error) {
	return e.Expr.Expression(qc, defaultFieldName)
}

type qsFieldExpression struct {
	FieldName qsFieldName
	Value     string    // Actual value that need comparison
	Type      valueType // Value type (text, numeric, regex)
	Operator  string    // Operator (<, <=, >=, >, =)
	Boost     float64   // Boost value (-1, no boosting)
	Fuzzy     float64   // Fuzzy value (-1, not set)
}

func (e *qsFieldExpression) SetFieldName(fieldName qsFieldName) {
	if e.FieldName.isEmpty() {
		e.FieldName = fieldName
	}
}

func (e *qsFieldExpression) SetBoost(value float64) {
	if e.Boost == -1 {
		e.Boost = value
	} else {
		e.Boost = e.Boost + value
	}
}

func (e *qsFieldExpression) Expression(qc *QueryContext, defaultFieldName qsFieldName) (expression, error) {
	fieldName := e.FieldName
	if fieldName.isEmpty() {
		fieldName = defaultFieldName
	}
	if fieldName.isEmpty() {
		return nil, ErrNoFieldName
	}

	fn := ParseExprFieldNameParts(qc, fieldName.fields)

	if e.Operator == "EXISTS" {
		return &exprOperator1{
			Context:  qc,
			Operator: "IS NOT MISSING",
			Expr1:    fn,
		}, nil
	}

	if e.Operator == "=" && e.Type == valueTypeText {
		switch strings.ToLower(e.Value) {
		case "false":
			return &exprOperator2{
				Context:  qc,
				Operator: "=",
				Expr1:    fn,
				Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{false}},
			}, nil
		case "true":
			return &exprOperator2{
				Context:  qc,
				Operator: "=",
				Expr1:    fn,
				Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{true}},
			}, nil
		}
	}

	var value any
	switch e.Type {
	case valueTypeFloat:
		value, _ = strconv.ParseFloat(e.Value, 64)
	case valueTypeInt:
		value, _ = strconv.ParseInt(e.Value, 10, 64)
	case valueTypeText:
		switch e.Operator {
		case "=":
			switch fn.Type() {
			case "keyword":
				if re, wildcard := translateWildcard(e.Value); wildcard {
					return &exprOperator2{
						Context:  qc,
						Operator: "SIMILAR TO",
						Expr1:    fn,
						Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{re}},
					}, nil
				}
				return &exprOperator2{
					Context:  qc,
					Operator: "=",
					Expr1:    fn,
					Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{e.Value}},
				}, nil
			case "keyword-ignore-case":
				if re, wildcard := translateWildcard(e.Value); wildcard {
					return &exprOperator2{
						Context:  qc,
						Operator: "SIMILAR TO", // TODO: case-insensitive SIMILAR TO might be more efficient
						Expr1:    &exprFunction{Context: qc, Name: "LOWER", Exprs: []expression{fn}},
						Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{strings.ToLower(re)}},
					}, nil
				}
				return &exprOperator2{
					Context:  qc,
					Operator: "=",
					Expr1:    &exprFunction{Context: qc, Name: "LOWER", Exprs: []expression{fn}},
					Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{strings.ToLower(e.Value)}},
				}, nil
			case "", "text":
				esc, _ := translateWildcardToRegex(e.Value)
				re := `(^|[ \t])(?i)` + esc + `([ \t]|$)`
				return &exprOperator2{
					Context:  qc,
					Operator: "~",
					Expr1:    fn,
					Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{re}},
				}, nil
			case "contains":
				esc, _ := translateWildcardToRegex(e.Value)
				re := `(?i)` + esc
				return &exprOperator2{
					Context:  qc,
					Operator: "~",
					Expr1:    fn,
					Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{re}},
				}, nil
			}

		default:
			value = e.Value
		}
	case valueTypeRegex:
		if e.Operator != "=" {
			return nil, ErrInvalidRegexOperator
		}

		switch fn.Type() {
		case "keyword":
			return &exprOperator2{
				Context:  qc,
				Operator: "~",
				Expr1:    fn,
				Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{`^` + e.Value + `$`}},
			}, nil
		case "keyword-ignore-case":
			return &exprOperator2{
				Context:  qc,
				Operator: "~",
				Expr1:    fn,
				Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{`^(?i)` + e.Value + `$`}},
			}, nil
		case "contains":
			return &exprOperator2{
				Context:  qc,
				Operator: "~",
				Expr1:    fn,
				Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{`(?i)` + e.Value}},
			}, nil
		case "", "text":
			return &exprOperator2{
				Context:  qc,
				Operator: "~",
				Expr1:    &exprFunction{Context: qc, Name: "LOWER", Exprs: []expression{fn}},
				Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{`(^|[ \t])` + e.Value + `([ \t]|$)`}},
			}, nil
		}
	case valueTypeBoolean:
		value = e.Value == "true"
	}

	return &exprOperator2{
		Context:  qc,
		Operator: e.Operator,
		Expr1:    fn,
		Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{value}},
	}, nil

	// FUZZY and BOOST are not used in our SQL translation
}

func translateWildcard(in string) (string, bool) {
	isWildCard := false
	var sb strings.Builder
	escaping := false
	for _, ch := range in {
		if escaping {
			sb.WriteRune(ch)
			escaping = false
		} else {
			switch ch {
			case '\\':
				escaping = true
				isWildCard = true
			case '*':
				sb.WriteRune('%')
				isWildCard = true
			case '?':
				sb.WriteRune('_')
				isWildCard = true
			default:
				sb.WriteRune(ch)
			}
		}
	}
	return sb.String(), isWildCard
}

func translateWildcardToRegex(in string) (string, bool) {
	isWildCard := false
	var sb strings.Builder
	escaping := false
	for _, ch := range in {
		if escaping {
			sb.WriteRune(ch)
			escaping = false
		} else {
			switch ch {
			case '\\':
				escaping = true
				isWildCard = true
			case '*':
				sb.WriteString("___SNELLER_STAR_WILDCARD___")
				isWildCard = true
			case '?':
				sb.WriteString("___SNELLER_QUESTIONMARK_WILDCARD___")
				isWildCard = true
			default:
				sb.WriteRune(ch)
			}
		}
	}
	if !isWildCard {
		return regexp.QuoteMeta(in), false
	}
	quoted := strings.ReplaceAll(strings.ReplaceAll(regexp.QuoteMeta(sb.String()), "___SNELLER_STAR_WILDCARD___", ".*"), "___SNELLER_QUESTIONMARK_WILDCARD___", ".")
	return quoted, true
}

type valueType int

const (
	valueTypeText    = valueType(1)
	valueTypeFloat   = valueType(2)
	valueTypeInt     = valueType(3)
	valueTypeRegex   = valueType(4)
	valueTypeBoolean = valueType(5)
)

func isRangeStar(fe *qsFieldExpression) bool {
	return fe.Type == valueTypeText && fe.Value == "*"
}
