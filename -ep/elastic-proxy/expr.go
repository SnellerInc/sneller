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
	"fmt"
	"strconv"
	"strings"
)

// current expressions don't use precedence
// which results in a lot of brackets, but
// coding is much easier and additional
// brackets are less readable, but have the
// same query performance. If this is
// undesirable, then we can add a precedence
// to the expressions and join them more
// intelligently.
type expression interface {
	QueryContext() *QueryContext
	Print(pc *printContext)
}

func PrintExprPretty(e expression) string {
	return printExpr(e, true)
}

func PrintExpr(e expression) string {
	return printExpr(e, false)
}

func printExpr(e expression, pretty bool) string {
	pc := printContext{
		Pretty: pretty,
		Indent: 2,
	}
	e.Print(&pc)
	return pc.sb.String()
}

type printContext struct {
	Pretty            bool
	Indent            int
	indentSpaces      int
	indentSpacesStack []int
	sb                strings.Builder
	inline            bool
}

func (pc *printContext) Push() {
	pc.pushN(pc.Indent)
}

func (pc *printContext) PushString(text string) {
	pc.WriteString(text)
	pc.pushN(len(text))
}

func (pc *printContext) Pop() {
	if len(pc.indentSpacesStack) > 1 {
		pc.indentSpacesStack = pc.indentSpacesStack[:len(pc.indentSpacesStack)-1]
		pc.indentSpaces = pc.indentSpacesStack[len(pc.indentSpacesStack)-1]
	} else {
		pc.indentSpacesStack = nil
		pc.indentSpaces = 0
	}
}

func (pc *printContext) WriteString(text string) {
	if text != "" {
		pc.indentIfNeeded()
		pc.sb.WriteString(text)
		pc.inline = true
	}
}

func (pc *printContext) WriteRune(r rune) {
	pc.indentIfNeeded()
	pc.sb.WriteRune(r)
	pc.inline = true
}

func (pc *printContext) WriteNewline() {
	if pc.inline {
		if pc.Pretty {
			pc.sb.WriteRune('\n')
		}
		pc.inline = false
	}
}

func (pc *printContext) WriteNewlineN(n int) {
	if pc.inline {
		if pc.Pretty {
			pc.sb.WriteRune('\n')
		}
		pc.inline = false
	}
	if pc.Pretty {
		for i := 0; i < n-1; i++ {
			pc.sb.WriteRune('\n')
		}
	}
}

func (pc *printContext) Level() int {
	return len(pc.indentSpacesStack)
}

func (pc *printContext) pushN(n int) {
	pc.indentSpaces = pc.indentSpaces + n
	pc.indentSpacesStack = append(pc.indentSpacesStack, pc.indentSpaces)
}

func (pc *printContext) indentIfNeeded() {
	if !pc.inline {
		if pc.Pretty {
			for i := 0; i < pc.indentSpaces; i++ {
				pc.sb.WriteRune(' ')
			}
		} else if pc.sb.Len() > 0 {
			pc.sb.WriteRune(' ')
		}
		pc.inline = false
	}
}

type exprSelect struct {
	Context    *QueryContext
	With       []projectAliasExpr
	Projection []projectAliasExpr
	From       []expression
	Where      expression
	Having     expression
	GroupBy    []expression
	OrderBy    []orderByExpr
	Offset     int
	Limit      int
}

func (e *exprSelect) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprSelect) Print(pc *printContext) {
	if len(e.With) > 0 {
		pc.WriteString("WITH")
		pc.WriteNewline()
		pc.Push()
		for i, with := range e.With {
			if i > 0 {
				pc.WriteString(",")
				pc.WriteNewlineN(2)
			}
			pc.WriteString(`"` + with.Alias + `"`)
			pc.WriteString(" AS")
			pc.Push()
			pc.WriteNewline()

			if selectExpr, ok := with.expression.(*exprSelect); ok && selectExpr.Limit == 1 {
				pc.WriteString("(SELECT [(")
				pc.Push()
				pc.WriteNewline()
				with.expression.Print(pc)
				pc.Pop()
				pc.WriteNewline()
				pc.WriteString(")])")
			} else {
				pc.PushString("(")
				with.expression.Print(pc)
				pc.Pop()
				pc.WriteNewline()
				pc.WriteString(")")
			}

			pc.Pop()
		}
		pc.Pop()
		pc.WriteNewlineN(2)
	}
	topLevelSelect := pc.Level() == 0
	if topLevelSelect {
		pc.WriteString("SELECT ")
		pc.WriteNewline()
		pc.Push()
	} else {
		pc.PushString("SELECT ")
	}
	for i, pe := range e.Projection {
		if i > 0 {
			pc.WriteString(",")
			if topLevelSelect {
				pc.WriteNewlineN(2)
			} else {
				pc.WriteNewline()
			}
		}
		pe.Print(pc)
	}
	pc.Pop()
	if len(e.From) > 0 {
		pc.WriteNewline()
		pc.PushString("FROM ")
		for i, from := range e.From {
			if i > 0 {
				pc.WriteString(",")
				pc.WriteNewline()
			}
			switch f := from.(type) {
			case *exprFieldName, *exprSourceName:
				f.Print(pc)
			case *exprFieldNameAlias:
				f.Print(pc)
			case *projectAliasExpr:
				pc.PushString("(")
				f.expression.Print(pc)
				pc.Pop()
				pc.WriteNewline()
				pc.WriteString(fmt.Sprintf(") AS %q", f.Alias))
			default:
				pc.PushString("(")
				f.Print(pc)
				pc.Pop()
				pc.WriteNewline()
				pc.WriteString(") AS \"$nested\"")
			}
		}
		pc.Pop()
	}
	if e.Where != nil {
		pc.WriteNewline()
		pc.WriteString("WHERE ")
		e.Where.Print(pc)
	}
	if len(e.GroupBy) > 0 {
		pc.WriteNewline()
		pc.PushString("GROUP BY ")
		for i, e := range e.GroupBy {
			if i > 0 {
				pc.WriteString(",")
				pc.WriteNewline()
			}
			e.Print(pc)
		}
		pc.Pop()
	}
	if e.Having != nil {
		pc.WriteNewline()
		pc.WriteString("HAVING ")
		e.Having.Print(pc)
	}
	if len(e.OrderBy) > 0 {
		pc.WriteNewline()
		pc.WriteString("ORDER BY ")
		for i, e := range e.OrderBy {
			if i > 0 {
				pc.WriteString(",")
				pc.WriteNewline()
			}
			e.Print(pc)
		}
	}
	if e.Limit > 0 {
		pc.WriteNewline()
		pc.WriteString(fmt.Sprintf("LIMIT %d", e.Limit))
	}
	if e.Offset > 0 {
		pc.WriteNewline()
		pc.WriteString(fmt.Sprintf("OFFSET %d", e.Offset))
	}
}

type projectAliasExpr struct {
	Context *QueryContext
	expression
	Alias string
}

func (e *projectAliasExpr) QueryContext() *QueryContext {
	return e.Context
}

func (e *projectAliasExpr) Print(pc *printContext) {
	if e.Alias != "" {
		switch v := e.expression.(type) {
		case *exprJSONLiteral, *exprFieldName, *exprFunction, *exprOperator2:
			e.expression.Print(pc)
			pc.WriteString(fmt.Sprintf(" AS %q", e.Alias))
		case *exprSelect:
			if v.Limit == 1 {
				pc.PushString("[(")
				e.expression.Print(pc)
				pc.Pop()
				pc.WriteString(")]")
			} else {
				pc.PushString("(")
				e.expression.Print(pc)
				pc.Pop()
				pc.WriteNewline()
				pc.WriteString(")")
			}
			pc.WriteString(fmt.Sprintf(" AS %q", e.Alias))
		default:
			pc.WriteString("(")
			e.expression.Print(pc)
			pc.WriteString(")")
			pc.WriteString(fmt.Sprintf(" AS %q", e.Alias))
		}
		return
	}

	if v, ok := e.expression.(*exprSelect); ok && v.Limit == 1 {
		pc.WriteString("[")
		v.Print(pc)
		pc.WriteString("]")
	}

	e.expression.Print(pc)
}

type orderByExpr struct {
	Context *QueryContext
	expression
	Order Ordering
}

func (e *orderByExpr) QueryContext() *QueryContext {
	return e.Context
}

func (e *orderByExpr) Print(pc *printContext) {
	e.expression.Print(pc)
	if e.Order != "" {
		pc.WriteString(fmt.Sprintf(" %s", e.Order))
	}
}

type exprObjectField struct {
	Name string
	Expr expression
}

type exprObject struct {
	Context *QueryContext
	Fields  []exprObjectField
}

func (e *exprObject) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprObject) Print(pc *printContext) {
	pc.WriteRune('{')
	for i, f := range e.Fields {
		if i > 0 {
			pc.WriteRune(',')
		}
		pc.WriteString(singleQuote(f.Name))
		pc.WriteRune(':')
		f.Expr.Print(pc)
	}
	pc.WriteRune('}')
}

func singleQuote(field string) string {
	var sb strings.Builder
	sb.WriteRune('\'')
	for _, ch := range field {
		if ch == '\'' {
			sb.WriteRune('\\')
		}
		sb.WriteRune(ch)
	}
	sb.WriteRune('\'')
	return sb.String()
}

type exprFunction struct {
	Context *QueryContext
	Name    string
	Exprs   []expression
	Filter  expression
}

func (e *exprFunction) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprFunction) Print(pc *printContext) {
	pc.WriteString(e.Name)
	pc.WriteRune('(')
	if len(e.Exprs) > 0 {
		for i, se := range e.Exprs {
			if i > 0 {
				pc.WriteRune(',')
			}
			se.Print(pc)
		}
	}
	pc.WriteRune(')')
	if e.Filter != nil {
		pc.WriteString(" FILTER (WHERE ")
		e.Filter.Print(pc)
		pc.WriteRune(')')
	}
}

// exprOperator1 represents an
// unary expression that has an
// operator and a single
// sub-expression.
type exprOperator1 struct {
	Context  *QueryContext
	Operator string
	Expr1    expression
}

func (e *exprOperator1) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprOperator1) Print(pc *printContext) {
	switch e.Operator {
	case "DISTINCT":
		pc.WriteString(fmt.Sprintf("%s ", e.Operator))
		e.Expr1.Print(pc)
	case "NOT":
		pc.WriteString(fmt.Sprintf("(%s ", e.Operator))
		e.Expr1.Print(pc)
		pc.WriteRune(')')
	case "IS MISSING", "IS NOT MISSING":
		pc.WriteRune('(')
		e.Expr1.Print(pc)
		pc.WriteString(fmt.Sprintf(" %s)", e.Operator))
	default:
		panic("unsupported unary operator")
	}
}

// exprOperator2 represents an
// binary expression that has an
// operator and two sub-expressions.
type exprOperator2 struct {
	Context  *QueryContext
	Operator string
	Expr1    expression
	Expr2    expression
}

func (e *exprOperator2) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprOperator2) Print(pc *printContext) {
	op, e1, e2 := e.Operator, e.Expr1, e.Expr2

	switch op {
	case ">", ">=", "<", "<=", "=", "<>":
		if fn, ok := e1.(*exprFieldName); ok {
			if eValue, ok := e2.(*exprJSONLiteral); ok {
				fieldName := strings.Join(fn.Fields, ".")
				value, err := formatIn(fieldName, eValue.Value.Value, e.Context.TypeMapping)
				if err == nil {
					ts, _ := NewJSONLiteral(value)
					e2 = &exprJSONLiteral{Context: e.Context, Value: ts}
				}
			}
		}

	case "CONTAINS":
		if eValue, ok := e2.(*exprJSONLiteral); ok {
			if textValue, ok := eValue.Value.Value.(string); ok {
				op = "LIKE"
				v, _ := NewJSONLiteral("%" + textValue + "%")
				e2 = &exprJSONLiteral{Context: e.Context, Value: v}
			}
		}
	}

	pc.WriteRune('(')
	e1.Print(pc)
	pc.WriteString(fmt.Sprintf(" %s ", op))

	if selExpr, ok := e2.(*exprSelect); ok {
		pc.WriteRune('(')
		selExpr.Print(pc)
		pc.WriteRune(')')
	} else {
		e2.Print(pc)
	}
	pc.WriteRune(')')
}

// exprFieldName represents a table (or
// alias) that references a data-source.
type exprSourceName struct {
	Context *QueryContext
	Source  string
	Alias   string
}

func (e *exprSourceName) QueryContext() *QueryContext {
	return e.Context
}

func ParseExprSourceName(qc *QueryContext, source string) *exprSourceName {
	return &exprSourceName{
		Context: qc,
		Source:  source,
	}
}

func ParseExprSourceNameWithAlias(qc *QueryContext, source, alias string) *exprSourceName {
	return &exprSourceName{
		Context: qc,
		Source:  source,
		Alias:   alias,
	}
}

func (e *exprSourceName) Print(pc *printContext) {
	pc.WriteString(`"` + e.Source + `"`)
	if e.Alias != "" {
		pc.WriteString(` AS "` + e.Alias + `"`)
	}
}

// exprFieldName represents an expression
// that references a field within the
// data. The field may be hierarchical, so
// it can represent sub-items.
type exprFieldName struct {
	Context  *QueryContext
	Source   string
	Fields   []string
	SubField string
}

func (e *exprFieldName) Type() string {
	key := strings.Join(e.Fields, ".")
	tm, ok := mapType(key, e.Context.TypeMapping)
	if !ok {
		return ""
	}
	if e.SubField != "" {
		return tm.Fields[e.SubField]
	}
	return tm.Type
}

func (e *exprFieldName) QueryContext() *QueryContext {
	return e.Context
}

func ParseExprFieldName(qc *QueryContext, fieldName string) *exprFieldName {
	return ParseExprFieldNameParts(qc, strings.Split(fieldName, "."))
}

func ParseExprFieldNameParts(qc *QueryContext, fields []string) *exprFieldName {
	e := exprFieldName{Context: qc}
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		// what is the difference between @timestamp and timestamp?
		// (https://discuss.elastic.co/t/what-is-the-difference-between-timestamp-and-timestamp/4646)
		field = strings.TrimPrefix(field, "@")

		// check for a subfield
		if i == len(fields)-1 {
			fieldName := strings.Join(e.Fields, ".")
			tm, ok := mapType(fieldName, qc.TypeMapping)
			if ok {
				if _, ok := tm.Fields[field]; ok {
					e.SubField = field
					break
				}
			} else {
				// Support default keyword field (if there was no explicit mapping)
				if field == "keyword" {
					e.SubField = "keyword"
					break
				}
			}

		}
		e.Fields = append(e.Fields, field)
	}

	fullFieldName := strings.Join(e.Fields, ".")
	for field, typeMapping := range qc.TypeMapping {
		if typeMapping.Type == "list" && strings.HasPrefix(fullFieldName, field+".") {
			fieldPartCount := strings.Count(field, ".") + 1
			e.Source = fmt.Sprintf("%s%s", SourceAliasPrefix, field)
			e.Fields = e.Fields[fieldPartCount:]

			foundAlias := false
			for _, s := range qc.Sources {
				if alias, ok := s.(*exprFieldNameAlias); ok {
					if alias.Alias == e.Source {
						foundAlias = true
						break
					}
				}
			}
			if !foundAlias {
				alias := ParseExprFieldNameAlias(qc, e.Source, fmt.Sprintf("%s.%s", DefaultSource, field))
				qc.Sources = append(qc.Sources, alias)
			}
			break
		}
	}

	return &e
}

func (e *exprFieldName) Print(pc *printContext) {
	// This "if" block should be removed once we have support
	// for `"source".*` syntax. It now just selects everything
	// and we hope we can filter it out later. See also
	// https://github.com/SnellerInc/sneller-core/issues/2358#issuecomment-1406379279
	if len(e.Fields) == 0 {
		pc.WriteRune('*')
		return
	}

	source := DefaultSource
	if e.Source != "" {
		source = e.Source
	}

	pc.WriteString(fmt.Sprintf("%q.", source))

	if len(e.Fields) == 0 {
		pc.WriteRune('*')
		return
	}

	for i, field := range e.Fields {
		if i > 0 {
			pc.WriteRune('.')
		}
		pc.WriteString(strconv.Quote(field))
	}
}

type exprFieldNameAlias struct {
	Context *QueryContext
	Alias   string
	Fields  []string
}

func (e *exprFieldNameAlias) QueryContext() *QueryContext {
	return e.Context
}

func ParseExprFieldNameAlias(qc *QueryContext, alias, fieldName string) *exprFieldNameAlias {
	return &exprFieldNameAlias{
		Context: qc,
		Alias:   alias,
		Fields:  strings.Split(fieldName, "."),
	}
}

func (e *exprFieldNameAlias) Print(pc *printContext) {
	for i, field := range e.Fields {
		if i > 0 {
			pc.WriteRune('.')
		}
		pc.WriteString(strconv.Quote(field))
	}
	pc.WriteString(" AS ")
	pc.WriteString(strconv.Quote(e.Alias))
}

type exprText struct {
	Context *QueryContext
	Value   string
}

func (e *exprText) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprText) Print(pc *printContext) {
	pc.WriteString(e.Value)
}

// exprJSONLiteral represents an expression
// that represents a literal value. For now
// only "boolean", "float64" (numeric) and
// "string" (text) are supported.
type exprJSONLiteral struct {
	Context *QueryContext
	Value   JSONLiteral
}

func (e *exprJSONLiteral) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprJSONLiteral) Print(pc *printContext) {
	pc.WriteString(e.Value.String())
}

// exprJSONLiteralArray represents an
// expression that represents a list of
// literal values.
type exprJSONLiteralArray struct {
	Context *QueryContext
	Values  []JSONLiteral
}

func (e *exprJSONLiteralArray) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprJSONLiteralArray) Print(pc *printContext) {
	pc.WriteRune('(')
	for i, value := range e.Values {
		if i > 0 {
			pc.WriteRune(',')
		}
		pc.WriteString(value.String())
	}
	pc.WriteRune(')')
}

// andExpressions merges an array of
// expressions in a single new
// expression that ANDs all expressions
// together.
func andExpressions(exprs []expression) expression {
	return joinExpressions(exprs, "AND")
}

func joinExpressions(exprs []expression, operator string) expression {
	var e expression
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		if e == nil {
			e = expr
		} else {
			e = &exprOperator2{
				Context:  e.QueryContext(),
				Operator: operator,
				Expr1:    e,
				Expr2:    expr,
			}
		}
	}
	return e
}

type exprOperatorOver struct {
	Context     *QueryContext
	Function    exprFunction
	PartitionBy []expression
	OrderBy     []orderByExpr
}

func (e *exprOperatorOver) QueryContext() *QueryContext {
	return e.Context
}

func (e *exprOperatorOver) Print(pc *printContext) {
	e.Function.Print(pc)
	pc.WriteString(" OVER (")
	if len(e.PartitionBy) > 0 {
		pc.WriteString("PARTITION BY ")
		for i, e := range e.PartitionBy {
			if i > 0 {
				pc.WriteString(", ")
			}
			e.Print(pc)
		}
	}
	if len(e.OrderBy) > 0 {
		pc.WriteString(" ORDER BY ")
		for i, e := range e.OrderBy {
			if i > 0 {
				pc.WriteString(", ")
			}
			e.Print(pc)
		}
	}
	pc.WriteRune(')')
}
