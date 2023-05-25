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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrNotSupported        = errors.New("unsupported element")
	ErrUnsupportedNumber   = errors.New("unsupported number")
	ErrTermOnlySingleField = errors.New("term supports only a single field")
)

// Query implements the parsing and
// expression logic for the Elastic
// Query.
//
// The parser canonicalizes all
// the input, so all deprecated or
// shortened representations should
// be handled by the `UnmarshalJSON`
// method.
type Query struct {
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/compound-queries.html
	Bool          *boolean       `json:"bool"`           // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
	Boosting      *notSupported  `json:"boosting"`       // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html
	ConstantScore *constantScore `json:"constant_score"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html
	DisMax        *notSupported  `json:"dis_max"`        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html
	FunctionScore *notSupported  `json:"function_score"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/full-text-queries.html
	Intervals         *notSupported     `json:"intervals"`           // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-intervals-query.html
	Match             *match            `json:"match"`               // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
	MatchBoolPrefix   *map[string]field `json:"match_bool_prefix"`   // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-bool-prefix-query.html
	MatchPhrase       *matchPhrase      `json:"match_phrase"`        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html
	MatchPhrasePrefix *map[string]field `json:"match_phrase_prefix"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html
	CombinedFields    *map[string]field `json:"combined_fields"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-combined-fields-query.html
	MultiMatch        *map[string]field `json:"multi_match"`         // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
	QueryString       *QueryString      `json:"query_string"`        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
	SimpleQueryString *notSupported     `json:"simple_query_string"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-queries.html
	GeoBoundingBox *geoBoundingBox `json:"geo_bounding_box"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-bounding-box-query.html
	GeoDistance    *notSupported   `json:"geo_distance"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html
	GeoPolygon     *notSupported   `json:"geo_polygon"`      // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-polygon-query.html
	GeoShape       *notSupported   `json:"geo_shape"`        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/shape-queries.html
	Shape *notSupported `json:"shape"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-shape-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/joining-queries.html
	Nested    *notSupported `json:"nested"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
	HasChild  *notSupported `json:"has_child"`  // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-has-child-query.html
	HasParent *notSupported `json:"has_parent"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-has-parent-query.html
	ParentID  *notSupported `json:"parent_id"`  // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-parent-id-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html
	MatchAll  *matchAll  `json:"match_all"`  // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html#query-dsl-match-all-query
	MatchNone *matchNone `json:"match_none"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html#query-dsl-match-none-query

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/span-queries.html
	SpanContaining   *notSupported `json:"span_containing"`    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-containing-query.html
	SpanFieldMasking *notSupported `json:"span_field_masking"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-field-masking-query.html
	SpanFirst        *notSupported `json:"span_first"`         // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-first-query.html
	SpanMulti        *notSupported `json:"span_multi"`         // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-multi-term-query.html
	SpanNear         *notSupported `json:"span_near"`          // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-near-query.html
	SpanNot          *notSupported `json:"span_not"`           // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-not-query.html
	SpanOr           *notSupported `json:"span_or"`            // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-or-query.html
	SpanTerm         *notSupported `json:"span_term"`          // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-term-query.html
	SpanWithin       *notSupported `json:"span_within"`        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-within-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/specialized-queries.html
	DistanceFeature *notSupported `json:"distance_feature"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-distance-feature-query.html
	MoreLikeThis    *notSupported `json:"more_like_this"`   // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html
	Percolate       *notSupported `json:"percolate"`        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-percolate-query.html
	RankFeature     *notSupported `json:"rank_feature"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-rank-feature-query.html
	Script          *notSupported `json:"script"`           // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-query.html
	ScriptScore     *notSupported `json:"script_score"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-score-query.html
	Wrapper         *notSupported `json:"wrapper"`          // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wrapper-query.html
	Pinned          *notSupported `json:"pinned"`           // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-pinned-query.html

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/term-level-queries.html
	Exists   *exists       `json:"exists"`    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
	Fuzzy    *notSupported `json:"fuzzy"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html
	Prefix   *notSupported `json:"prefix"`    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-ids-query.html
	Range    *ranges       `json:"range"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
	Regexp   *notSupported `json:"regexp"`    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
	Term     *term         `json:"term"`      // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
	Terms    *terms        `json:"terms"`     // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
	TermsSet *notSupported `json:"terms_set"` // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-set-query.html
	Wildcard *wildCard     `json:"wildcard"`  // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
}

// Expression obtains the full
// expression (that can be converted)
// to a SQL WHERE clause for the
// query
func (q *Query) Expression(qc *QueryContext) (expression, error) {
	// TODO: Hope this can be done more elegant
	type expr interface {
		Expression(qc *QueryContext) (expression, error)
	}

	items := []expr{q.Bool, q.ConstantScore, q.MatchAll, q.MatchPhrase,
		q.Match, q.MatchNone, q.Exists, q.Term, q.Terms, q.Range,
		q.QueryString, q.GeoBoundingBox, q.Wildcard}

	var exprs []expression
	for _, item := range items {
		if !reflect.ValueOf(item).IsNil() {
			e, err := item.Expression(qc)
			if err != nil {
				return nil, err
			}
			if e != nil {
				exprs = append(exprs, e)
			}
		}
	}

	return andExpressions(exprs), nil
}

type notSupported struct{}

func (*notSupported) UnmarshalJSON(data []byte) error {
	return ErrNotSupported
}

type boostValue float32

type queries []*Query

func (q *queries) Expression(qc *QueryContext, op string) (expression, error) {
	var exprs []expression
	for _, sq := range *q {
		e, err := sq.Expression(qc)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}
	return joinExpressions(exprs, op), nil

}

func (q *queries) UnmarshalJSON(data []byte) error {
	if data[0] == '[' {
		var multipleQueries []*Query
		if err := json.Unmarshal(data, &multipleQueries); err != nil {
			return err
		}
		*q = queries(multipleQueries)
	} else {
		var singleQuery Query
		if err := json.Unmarshal(data, &singleQuery); err != nil {
			return err
		}
		*q = queries{&singleQuery}
	}
	return nil
}

type andQueries queries

func (q *andQueries) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, (*queries)(q))
}

func (q *andQueries) Expression(qc *QueryContext) (expression, error) {
	return ((*queries)(q)).Expression(qc, "AND")
}

type orQueries queries

func (q *orQueries) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, (*queries)(q))
}

func (q *orQueries) Expression(qc *QueryContext) (expression, error) {
	return ((*queries)(q)).Expression(qc, "OR")
}

type boolean struct {
	Must               *andQueries `json:"must"`
	Filter             *andQueries `json:"filter"`
	Should             *orQueries  `json:"should"`
	MustNot            *andQueries `json:"must_not"`
	MinimumShouldMatch *boostValue `json:"minimum_should_match"`
	Boost              *boostValue `json:"boost"`
}

func (b *boolean) Expression(qc *QueryContext) (expression, error) {
	var exprs []expression

	if b.Must != nil {
		e, err := b.Must.Expression(qc)
		if err != nil {
			return nil, err
		}
		if e != nil {
			exprs = append(exprs, e)
		}
	}

	if b.Filter != nil {
		e, err := b.Filter.Expression(qc)
		if err != nil {
			return nil, err
		}
		if e != nil {
			exprs = append(exprs, e)
		}
	}

	if b.Should != nil {
		e, err := b.Should.Expression(qc)
		if err != nil {
			return nil, err
		}
		if e != nil {
			exprs = append(exprs, e)
		}
	}

	if b.MustNot != nil {
		e, err := b.MustNot.Expression(qc)
		if err != nil {
			return nil, err
		}
		if e != nil {
			exprs = append(exprs, &exprOperator1{
				Context:  qc,
				Operator: "NOT",
				Expr1:    e,
			})
		}
	}

	return andExpressions(exprs), nil
}

type constantScore struct {
	Filter *andQueries `json:"filter"`
	Boost  *boostValue `json:"boost"`
}

func (cs *constantScore) Expression(qc *QueryContext) (expression, error) {
	if cs.Filter != nil {
		return cs.Filter.Expression(qc)
	}
	return nil, nil
}

type matchPhrase map[string]field

func (m *matchPhrase) Expression(qc *QueryContext) (expression, error) {
	var exprs []expression
	for fieldName, mp := range *m {
		e := &exprOperator2{
			Context:  qc,
			Operator: "=",
			Expr1:    ParseExprFieldName(qc, fieldName),
			Expr2:    &exprJSONLiteral{Context: qc, Value: mp.Query},
		}
		exprs = append(exprs, e)
	}
	return andExpressions(exprs), nil
}

type geoBoundingBox map[string]geoBounds

func (gbb *geoBoundingBox) Expression(qc *QueryContext) (expression, error) {
	var exprs []expression
	for field, value := range *gbb {
		lat := ParseExprFieldName(qc, field+LatExt)
		long := ParseExprFieldName(qc, field+LonExt)

		top, _ := NewJSONLiteral(value.TopLeft.Lat)
		left, _ := NewJSONLiteral(value.TopLeft.Lon)
		bottom, _ := NewJSONLiteral(value.BottomRight.Lat)
		right, _ := NewJSONLiteral(value.BottomRight.Lon)
		exprs = append(exprs,
			&exprOperator2{Context: qc, Operator: "<=", Expr1: lat, Expr2: &exprJSONLiteral{Context: qc, Value: top}},
			&exprOperator2{Context: qc, Operator: ">=", Expr1: long, Expr2: &exprJSONLiteral{Context: qc, Value: left}},
			&exprOperator2{Context: qc, Operator: ">=", Expr1: lat, Expr2: &exprJSONLiteral{Context: qc, Value: bottom}},
			&exprOperator2{Context: qc, Operator: "<=", Expr1: long, Expr2: &exprJSONLiteral{Context: qc, Value: right}},
		)
	}
	return andExpressions(exprs), nil
}

type match map[string]JSONLiteral

func (m *match) Expression(qc *QueryContext) (expression, error) {
	var exprs []expression
	for field, value := range *m {
		exprs = append(exprs, fieldEquals(field, value, qc))
	}
	return andExpressions(exprs), nil
}

type term struct {
	Field           string
	Value           JSONLiteral `json:"value"`
	Boost           *boostValue `json:"boost"`
	CaseInsensitive *bool       `json:"case_insensitive"`
}

func (t *term) Expression(qc *QueryContext) (expression, error) {
	return fieldEquals(t.Field, t.Value, qc), nil
}

func (t *term) UnmarshalJSON(data []byte) error {
	type _term term
	var tm map[string]_term
	if err := json.Unmarshal(data, &tm); err != nil {
		// try the short-version if the
		// long version cannot be unmarshalled
		var tmShort map[string]JSONLiteral
		if err = json.Unmarshal(data, &tmShort); err != nil {
			return err
		}
		if len(tm) != 1 {
			return ErrTermOnlySingleField
		}
		for f, v := range tmShort {
			t.Field = f
			t.Value = v
		}
		return nil
	}
	if len(tm) != 1 {
		return ErrTermOnlySingleField
	}
	for f, tmv := range tm {
		t.Field = f
		t.Value = tmv.Value
		t.Boost = tmv.Boost
		t.CaseInsensitive = tmv.CaseInsensitive
	}
	return nil
}

type terms struct {
	Field  string
	Values []JSONLiteral
	Boost  *boostValue `json:"boost"`
}

func (t *terms) Expression(qc *QueryContext) (expression, error) {
	return &exprOperator2{
		Context:  qc,
		Operator: "IN",
		Expr1:    ParseExprFieldName(qc, t.Field),
		Expr2:    &exprJSONLiteralArray{Context: qc, Values: t.Values},
	}, nil
}

func (t *terms) UnmarshalJSON(data []byte) error {
	type _terms terms
	if err := json.Unmarshal(data, (*_terms)(t)); err != nil {
		return err
	}
	var tm map[string]any
	if err := json.Unmarshal(data, &tm); err != nil {
		return err
	}
	for f, values := range tm {
		if f != "boost" {
			if values, ok := values.([]any); ok {
				t.Field = f
				for _, v := range values {
					e, err := NewJSONLiteral(v)
					if err != nil {
						return err
					}
					t.Values = append(t.Values, e)
				}
			} else {
				return errors.New("field should contain an array of strings")
			}
		}
	}
	return nil
}

type wildCard map[string]struct {
	Value           *string `json:"value"`
	WildCard        *string `json:"wildcard"`
	Boost           float64 `json:"boost"`
	CaseInsensitive bool    `json:"case_insensitive"`
	Rewrite         string  `json:"rewrite"`
}

func (w wildCard) Expression(qc *QueryContext) (expression, error) {
	var exprs []expression
	for fieldName, wc := range w {
		if wc.Rewrite != "constant_score" {
			return nil, ErrNotSupported
		}
		operator := "LIKE"
		if wc.CaseInsensitive {
			operator = "ILIKE"
		}
		value := wc.Value
		if value == nil {
			value = wc.WildCard
			if value == nil {
				return nil, ErrNotSupported
			}
		} else if wc.WildCard != nil && *wc.WildCard != *value {
			// From the spec:
			// "An alias for the value parameter.
			//  If you specify both value and
			//  wildcard, the query uses the last
			//  one in the request body."
			//
			// JSON is unordered, so this can't be
			// implemented. When both are specified
			// (and not equal), then it returns an
			// unsupported error.
			return nil, ErrNotSupported
		}
		exprs = append(exprs, &exprOperator2{
			Context:  qc,
			Operator: operator,
			Expr1:    ParseExprFieldName(qc, fieldName),
			Expr2:    &exprJSONLiteral{Context: qc, Value: JSONLiteral{Value: *value}},
		})
	}
	return andExpressions(exprs), nil
}

type QueryString struct {
	Query                           string      `json:"query"`
	DefaultField                    *string     `json:"default_field"`
	AllowLeadingWildcard            *bool       `json:"allow_leading_wildcard"`
	AnalyzeWildcard                 *bool       `json:"analyze_wildcard"`
	Analyzer                        *string     `json:"analyzer"`
	AutoGenerateSynonymsPhraseQuery *bool       `json:"auto_generate_synonyms_phrase_query"`
	Boost                           *boostValue `json:"boost"`
	DefaultOperator                 *string     `json:"default_operator"`
	EnablePositionIncrements        *bool       `json:"enable_position_increments"`
	Fields                          *[]string   `json:"fields"`
	Fuzziness                       *string     `json:"fuzziness"`
	FuzzyMaxExpansions              *int        `json:"fuzzy_max_expansions"`
	FuzzyPrefixLength               *int        `json:"fuzzy_prefix_length"`
	FuzzyTranspositions             *bool       `json:"fuzzy_transpositions"`
	Lenient                         *bool       `json:"lenient"`
	MaxDeterminizedStates           *int        `json:"max_determinized_states"`
	MinimumShouldMatch              *string     `json:"minimum_should_match"`
	QuoteAnalyzer                   *string     `json:"quote_analyzer"`
	PhraseSlop                      *int        `json:"phrase_slop"`
	QuoteFieldSuffix                *string     `json:"quote_field_suffix"`
	Rewrite                         *string     `json:"rewrite"`
	TimeZone                        *string     `json:"time_zone"`
}

func (qs *QueryString) Expression(qc *QueryContext) (expression, error) {
	lex := newQueryStringLexer([]byte(qs.Query))
	lex.defaultOperator = "OR"
	if qs.DefaultOperator != nil {
		lex.defaultOperator = *qs.DefaultOperator
	}
	if yyParse(lex) != 0 {
		return nil, fmt.Errorf("error parsing %q", qs.Query)
	}
	qsExpression := lex.result

	var exprs []expression
	if qs.Fields != nil && len(*qs.Fields) > 0 {
		for _, f := range *qs.Fields {
			e, err := qsExpression.Expression(qc, parseQSFieldName(f))
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, e)
		}
	} else {
		defaultField := ""
		if qs.DefaultField != nil {
			defaultField = *qs.DefaultField
		}
		e, err := qsExpression.Expression(qc, parseQSFieldName(defaultField))
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}

	return andExpressions(exprs), nil
}

type matchAll struct {
	Boost *boostValue `json:"boost"`
}

func (ma *matchAll) Expression(qc *QueryContext) (expression, error) {
	return &exprJSONLiteral{Context: qc, Value: JSONLiteral{true}}, nil
}

type matchNone struct {
}

func (mn *matchNone) Expression(qc *QueryContext) (expression, error) {
	return &exprJSONLiteral{Context: qc, Value: JSONLiteral{false}}, nil
}

type exists struct {
	Field string `json:"field"`
}

func (e *exists) Expression(qc *QueryContext) (expression, error) {
	return &exprOperator1{
		Context:  qc,
		Operator: "IS NOT MISSING",
		Expr1:    ParseExprFieldName(qc, e.Field),
	}, nil
}

type ranges map[string]Range

func (rs *ranges) Expression(qc *QueryContext) (expression, error) {
	var rangeExprs []expression
	for f, r := range *rs {
		if r.GreaterThanOrEqualTo != nil {
			rangeExprs = append(rangeExprs, &exprOperator2{Context: qc, Operator: ">=", Expr1: ParseExprFieldName(qc, f), Expr2: &exprJSONLiteral{Context: qc, Value: *r.GreaterThanOrEqualTo}})
		}
		if r.GreaterThan != nil {
			rangeExprs = append(rangeExprs, &exprOperator2{Context: qc, Operator: ">", Expr1: ParseExprFieldName(qc, f), Expr2: &exprJSONLiteral{Context: qc, Value: *r.GreaterThan}})
		}
		if r.LessThanOrEqualTo != nil {
			rangeExprs = append(rangeExprs, &exprOperator2{Context: qc, Operator: "<=", Expr1: ParseExprFieldName(qc, f), Expr2: &exprJSONLiteral{Context: qc, Value: *r.LessThanOrEqualTo}})
		}
		if r.LessThan != nil {
			rangeExprs = append(rangeExprs, &exprOperator2{Context: qc, Operator: "<", Expr1: ParseExprFieldName(qc, f), Expr2: &exprJSONLiteral{Context: qc, Value: *r.LessThan}})
		}
	}
	return andExpressions(rangeExprs), nil
}

type Range struct {
	GreaterThan          *JSONLiteral `json:"gt"`
	GreaterThanOrEqualTo *JSONLiteral `json:"gte"`
	LessThan             *JSONLiteral `json:"lt"`
	LessThanOrEqualTo    *JSONLiteral `json:"lte"`
	Format               *string      `json:"format"`
	Relation             *string      `json:"relation"`
	TimeZone             *string      `json:"time_zone"`
	Boost                *boostValue  `json:"boost"`
}

func (r *Range) UnmarshalJSON(data []byte) error {
	type _range Range
	if err := json.Unmarshal(data, (*_range)(r)); err != nil {
		return err
	}
	if r.GreaterThan == nil && r.GreaterThanOrEqualTo == nil && r.LessThan == nil && r.LessThanOrEqualTo == nil {
		// try obsoleted syntax (generated by https://github.com/olivere/elastic)
		// deprecated since: https://github.com/elastic/elasticsearch/commit/d6ecdecc19d54b4a37b033a2237a4c76601e9a2d
		var oldRange struct {
			From         *JSONLiteral `json:"from"`
			To           *JSONLiteral `json:"to"`
			IncludeLower *bool        `json:"include_lower"`
			IncludeUpper *bool        `json:"include_upper"`
		}
		if err := json.Unmarshal(data, &oldRange); err != nil {
			return err
		}

		if oldRange.From != nil {
			if oldRange.IncludeLower != nil && !*oldRange.IncludeLower {
				r.GreaterThan = oldRange.From
			} else {
				r.GreaterThanOrEqualTo = oldRange.From
			}
		}

		if oldRange.To != nil {
			if oldRange.IncludeUpper != nil && !*oldRange.IncludeUpper {
				r.LessThan = oldRange.To
			} else {
				r.LessThanOrEqualTo = oldRange.To
			}
		}
	}

	return nil
}

const (
	ZeroTermsQueryNone ZeroTermsQuery = "none"
	ZeroTermsQueryAll  ZeroTermsQuery = "all"
)

type field struct {
	Query                           JSONLiteral     `json:"query"`
	Analyzer                        *string         `json:"analyzer"`
	AutoGenerateSynonymsPhraseQuery *bool           `json:"auto_generate_synonyms_phrase_query"`
	Fuzziness                       *string         `json:"fuzziness"`
	MaxExpansions                   *int            `json:"max_expansions"`
	PrefixLength                    *int            `json:"prefix_length"`
	FuzzyTranspositions             *bool           `json:"fuzzy_transpositions"`
	FuzzyRewrite                    *string         `json:"fuzzy_rewrite"`
	Lenient                         *bool           `json:"lenient"`
	Operator                        *Operator       `json:"operator"`
	MinimumShouldMatch              *string         `json:"minimum_should_match"`
	ZeroTermsQuery                  *ZeroTermsQuery `json:"zero_terms_query"`
	Slop                            *int            `json:"slop"`
	Fields                          *[]string       `json:"fields"`
	Type                            *string         `json:"type"`
	TieBreaker                      *float64        `json:"tie_breaker"`
}

func (f *field) UnmarshalJSON(data []byte) error {
	if data[0] != '{' {
		var query JSONLiteral
		if err := json.Unmarshal(data, &query); err != nil {
			return err
		}
		f.Query = query
	} else {
		type _field field
		if err := json.Unmarshal(data, (*_field)(f)); err != nil {
			return err
		}
	}
	return nil
}

type Operator string

const (
	OperatorOr  Operator = "or"
	OperatorAnd Operator = "and"
)

type ZeroTermsQuery string
